//! `lua`: a sandboxed Lua 5.4 stage, the escape hatch for logic the declarative nodes
//! cannot express.
//!
//! ```yaml
//! - id: status_class
//!   type: lua
//!   script: scripts/status_class.lua   # a file, or `source: |` with the script inline
//!   limits: { instructions: 1000000, memory_kib: 16384, output_kib: 1024 }
//!   on_error: pass                     # pass (default) | drop | nak
//!   on_state_error: nak                # nak (default) | pass, when the script uses `state`
//! ```
//!
//! The script defines `process(record)`. It gets the record as a plain table with OTLP
//! field names (`record.attributes["http.path"]`) and returns it to pass, `nil` to drop
//! (reason `lua_drop`), or a list of records to split. Every returned record keeps the
//! original `id` and `resource.tenant.id`, `kind` stays `log`, typed fields keep their
//! types, the maps stay flat, and the strings together stay under `output_kib`; anything
//! else is refused and counts as an error of kind `output`.
//!
//! A script that loops is stopped by the instruction budget (`instructions`), one that
//! allocates without bound by the memory cap (`memory_kib`), each per record; a runtime
//! error is `runtime`. Every kind counts on `lua_errors_total{kind}` and then `on_error`
//! decides: `pass` forwards the record as it came in, `drop` drops it with reason
//! `lua_error`, `nak` fails it so the source message redelivers.
//!
//! The sandbox has `string`, `table`, `math` and `utf8`, plus `state.get/set_nx/incr/del`
//! on the node's state handle, `log.info/warn` and `now_ns()`. `os`, `io`, `package`,
//! `require`, `load` and `debug` are not there, and a script that names one of them is
//! refused at load, as is one that does not parse (the message carries the line) or does
//! not define `process`. One VM per worker per node, the script loaded once, so a counter
//! in its upvalues persists across the records that worker sees.

mod convert;
mod scan;
mod vm;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::record::Record;
use fusion_core::stage::{Context, DropReason, Stage, StageError, StageOutput};
use fusion_core::state::StateErrorPolicy;
use serde::Deserialize;

use vm::{LuaError, Returned, Script, Stopped, Vm};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    script: Option<String>,
    source: Option<String>,
    #[serde(default)]
    limits: Limits,
    #[serde(default)]
    on_error: OnError,
    on_state_error: Option<StateErrorPolicy>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, default)]
struct Limits {
    instructions: u64,
    memory_kib: usize,
    output_kib: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            instructions: 1_000_000,
            memory_kib: 16 * 1024,
            output_kib: 1024,
        }
    }
}

/// The smallest memory cap accepted: the sandbox itself needs a few tens of KiB.
const MIN_MEMORY_KIB: usize = 64;

/// What the node does with a record whose run of `process` failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnError {
    /// The record goes on unchanged.
    #[default]
    Pass,
    /// The record drops with reason `lua_error`.
    Drop,
    /// The record fails; its source message is negatively acknowledged.
    Nak,
}

/// The `lua` stage.
pub struct Lua {
    /// Distinguishes this node's VMs from another node's in the worker's cache. Not a key
    /// in the glossary's sense: nothing about the state store is named here. A new compiled
    /// pipeline takes new ids, so its records build their own VMs; the ones the replaced
    /// pipeline left behind are not removed (#40), which is why nothing may read this as a
    /// swap releasing them.
    vm_id: u64,
    script: Arc<Script>,
    on_error: OnError,
    on_state_error: StateErrorPolicy,
    uses_state: bool,
}

impl std::fmt::Debug for Lua {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lua")
            .field("script", &self.script.chunk_name)
            .field("on_error", &self.on_error)
            .finish_non_exhaustive()
    }
}

static NEXT_VM_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// This worker's VMs, one per `lua` node it has seen a record for, under that node's
    /// [`Lua::vm_id`].
    static VMS: RefCell<HashMap<u64, Rc<Vm>>> = RefCell::new(HashMap::new());
}

impl Lua {
    /// Build from a node's `script` or `source`, `limits`, `on_error` and `on_state_error`.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidParams`] naming the node when neither or both of `script` and
    /// `source` are given, the file cannot be read, the script names a forbidden global
    /// (with the line), does not parse (with the line), does not define `process`, its top
    /// level fails, a limit is out of range, or a policy is outside its values.
    pub fn from_node(node: &NodeConfig) -> Result<Self, ConfigError> {
        let params: Params = node.parse_params()?;
        let (chunk_name, source) = match (params.script, params.source) {
            (Some(path), None) => {
                let source = std::fs::read_to_string(&path).map_err(|e| {
                    node.invalid_params(format!("`script`: cannot read `{path}`: {e}"))
                })?;
                (format!("@{path}"), source)
            }
            (None, Some(source)) => (format!("={}", node.id), source),
            (None, None) => {
                return Err(node.invalid_params(
                    "give `script` (a file path) or `source` (the script inline)",
                ));
            }
            (Some(_), Some(_)) => {
                return Err(node.invalid_params("`script` and `source` are alternatives; give one"));
            }
        };
        if params.limits.instructions == 0 {
            return Err(node.invalid_params("`limits.instructions` must be at least 1"));
        }
        if params.limits.memory_kib < MIN_MEMORY_KIB {
            return Err(node.invalid_params(format!(
                "`limits.memory_kib` must be at least {MIN_MEMORY_KIB}"
            )));
        }
        if params.limits.output_kib == 0 {
            return Err(node.invalid_params("`limits.output_kib` must be at least 1"));
        }
        let mut uses_state = false;
        for (name, line) in scan::free_names(&source) {
            if vm::FORBIDDEN.contains(&name) {
                let instead = vm::instead_of(name).map_or(String::new(), |i| format!("; use {i}"));
                return Err(node.invalid_params(format!(
                    "{}:{line}: `{name}` is not available in the sandbox{instead}",
                    chunk_name.trim_start_matches(['@', '='])
                )));
            }
            if name == "state" {
                uses_state = true;
            }
        }
        if params.on_state_error.is_some() && !uses_state {
            return Err(
                node.invalid_params("`on_state_error` is given but the script never uses `state`")
            );
        }
        let script = Arc::new(Script {
            chunk_name,
            source,
            node: node.id.clone(),
            instructions: params.limits.instructions,
            memory_bytes: params.limits.memory_kib * 1024,
            output_bytes: params.limits.output_kib * 1024,
        });
        // Compiled and run once in a throwaway sandbox, so a syntax error, a missing
        // `process` or a top level that misbehaves fails the config, not the first record.
        Vm::new(Arc::clone(&script)).map_err(|error| node.invalid_params(error.describe()))?;
        Ok(Self {
            vm_id: NEXT_VM_ID.fetch_add(1, Ordering::Relaxed),
            script,
            on_error: params.on_error,
            on_state_error: params.on_state_error.unwrap_or(StateErrorPolicy::Nak),
            uses_state,
        })
    }

    /// Factory for a [`fusion_core::registry::Registry`].
    ///
    /// # Errors
    ///
    /// See [`Lua::from_node`].
    pub fn build(node: &NodeConfig) -> Result<Box<dyn Stage>, ConfigError> {
        Ok(Box::new(Self::from_node(node)?))
    }

    /// This worker's VM for this node, built on the worker's first record through it.
    fn vm(&self) -> Result<Rc<Vm>, LuaError> {
        VMS.with(|vms| {
            if let Some(vm) = vms.borrow().get(&self.vm_id) {
                return Ok(Rc::clone(vm));
            }
            let vm = Rc::new(Vm::new(Arc::clone(&self.script))?);
            vms.borrow_mut().insert(self.vm_id, Rc::clone(&vm));
            Ok(vm)
        })
    }
}

impl Stage for Lua {
    fn process(&self, record: Record, ctx: &Context<'_>) -> StageOutput {
        let run = match self.vm() {
            Ok(vm) => vm.run(&record, &ctx.state),
            Err(error) => Err(Stopped::Lua(error)),
        };
        let error = match run {
            Ok(Returned::Record(record)) => return StageOutput::Pass(*record),
            Ok(Returned::Drop) => return StageOutput::Drop(DropReason::LuaDrop),
            Ok(Returned::Split(records)) => return StageOutput::Split(records),
            Err(Stopped::State(error)) => return StageOutput::StateError { record, error },
            Err(Stopped::Lua(error)) => error,
        };
        if matches!(error, LuaError::Memory) {
            // A VM at its cap stays there when the growth is in the script's upvalues, so
            // the next record starts a fresh one; the persistent state is what was leaking.
            VMS.with(|vms| vms.borrow_mut().remove(&self.vm_id));
        }
        ctx.metrics.lua_error(error.kind());
        let message = error.describe();
        eprintln!(
            "pipeline: lua `{}` record {}: {message}",
            ctx.node_id, ctx.record_id
        );
        match self.on_error {
            OnError::Pass => StageOutput::Pass(record),
            OnError::Drop => StageOutput::Drop(DropReason::LuaError),
            OnError::Nak => StageOutput::Error(StageError::new(message)),
        }
    }

    fn uses_state(&self) -> bool {
        self.uses_state
    }

    fn on_state_error(&self) -> StateErrorPolicy {
        self.on_state_error
    }
}

/// Register the `lua` stage under its config `type` name.
pub fn register(registry: &mut fusion_core::registry::Registry) {
    registry.register_stage("lua", Lua::build);
}
