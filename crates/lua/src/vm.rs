//! One worker's VM for one `lua` node: the sandbox, the API a script sees (`state`, `log`,
//! `now_ns`), the guardrails, and one run of `process` over one record.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use fusion_core::meta::unix_nanos_now;
use fusion_core::metrics::LuaErrorKind;
use fusion_core::record::{Record, RecordId};
use fusion_core::stage::{Context, State};
use fusion_core::state::StateError;
use mlua::{
    Function, HookTriggers, LuaOptions, LuaString, MultiValue, StdLib, Value as LuaValue, VmState,
};

use crate::convert::{self, OutputError, type_name};

/// The globals a script may not name, and the names the sandbox leaves undefined. `load`
/// and its siblings come with the base library and are removed; `os`, `io`, `package`
/// (and so `require`) and `debug` are never loaded; `print` is stdout, a side channel the
/// `log` table replaces.
pub(crate) const FORBIDDEN: [&str; 10] = [
    "os",
    "io",
    "package",
    "require",
    "load",
    "loadfile",
    "dofile",
    "loadstring",
    "debug",
    "print",
];

/// What to write instead of a forbidden name, where there is something.
pub(crate) fn instead_of(name: &str) -> Option<&'static str> {
    match name {
        "print" => Some("log.info"),
        _ => None,
    }
}

/// The base library's loaders and `print`, removed from the globals after the VM is built.
/// `pcall` and `xpcall` are replaced, not removed: see [`install_pcall`].
const REMOVED_GLOBALS: [&str; 5] = ["load", "loadfile", "dofile", "collectgarbage", "print"];

/// The instruction budget is checked at this granularity, or at the budget itself when
/// that is smaller, so a small budget still trips exactly.
const HOOK_EVERY: u64 = 1000;

/// The script and its limits, as the node holds them. Shared by every worker's VM.
#[derive(Debug)]
pub(crate) struct Script {
    /// The chunk name Lua reports errors under: `@path` for a file, `=id` for inline source.
    pub(crate) chunk_name: String,
    pub(crate) source: String,
    pub(crate) node: String,
    pub(crate) instructions: u64,
    pub(crate) memory_bytes: usize,
    pub(crate) output_bytes: usize,
}

impl Script {
    /// The script as a message names it: the file path, or the node id for an inline
    /// `source`, without the prefix Lua wants on a chunk name.
    pub(crate) fn display_name(&self) -> &str {
        self.chunk_name.trim_start_matches(['@', '='])
    }
}

/// A run of `process` that produced no records, the glossary's Lua error: every variant is
/// a `kind` of `lua_errors_total`, and the node's `on_error` decides what happens to the
/// record. A state error is not one of these; see [`Stopped`].
#[derive(Debug)]
pub(crate) enum LuaError {
    /// The instruction budget tripped.
    Instructions,
    /// The memory cap tripped.
    Memory,
    /// The script raised, or the VM failed to run it.
    Runtime(String),
    /// The script returned something the stage refuses.
    Output(OutputError),
}

impl LuaError {
    /// The `kind` label of `lua_errors_total`.
    pub(crate) const fn kind(&self) -> LuaErrorKind {
        match self {
            Self::Instructions => LuaErrorKind::Instructions,
            Self::Memory => LuaErrorKind::Memory,
            Self::Runtime(_) => LuaErrorKind::Runtime,
            Self::Output(_) => LuaErrorKind::Output,
        }
    }

    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Instructions => BUDGET_EXCEEDED.to_owned(),
            Self::Memory => "memory cap exceeded".to_owned(),
            Self::Runtime(message) => message.clone(),
            Self::Output(error) => format!("returned record refused: {error}"),
        }
    }
}

/// What stopped a run of `process`, and so who decides. A [`LuaError`] is the node's, through
/// its `on_error`; a state error is the engine's, through the node's `on_state_error`. The
/// two are kept apart here because the glossary keeps them apart: a store that could not
/// answer is not something the script did.
#[derive(Debug)]
pub(crate) enum Stopped {
    /// The script failed, or what it returned was refused.
    Lua(LuaError),
    /// A `state.*` call could not reach the store.
    State(StateError),
}

impl Stopped {
    /// This stop as a load-time failure. At load there is no record and no state handle, so
    /// the API refuses before it reaches the store and the state arm cannot be taken; it is
    /// reported like any other failure of the script's top level rather than by a panic.
    fn at_load(self) -> LuaError {
        match self {
            Self::Lua(error) => error,
            Self::State(error) => LuaError::Runtime(error.to_string()),
        }
    }
}

/// What the budget's marker error and [`LuaError::Instructions`] both say.
const BUDGET_EXCEEDED: &str = "instruction budget exceeded";

/// What `process` returned, once checked.
pub(crate) enum Returned {
    /// Boxed so the enum stays the size of its other variants.
    Record(Box<Record>),
    Drop,
    Split(Vec<Record>),
}

/// The marker error the instruction hook raises, so the stage can tell it from a script's
/// own `error()`.
#[derive(Debug)]
struct BudgetExceeded;

impl std::fmt::Display for BudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(BUDGET_EXCEEDED)
    }
}

impl std::error::Error for BudgetExceeded {}

/// What the API functions need about the record being processed: set before each run.
struct Current {
    state: State,
    record_id: RecordId,
}

/// One worker's VM for one node.
pub(crate) struct Vm {
    lua: mlua::Lua,
    process: Function,
    /// Instructions used by the current run, counted by the hook in steps of the trigger.
    used: Rc<Cell<u64>>,
    script: Arc<Script>,
}

impl Vm {
    /// Build the sandbox, install the API and the guardrails, run the script's top level
    /// and take its `process`.
    pub(crate) fn new(script: Arc<Script>) -> Result<Self, LuaError> {
        let lua = mlua::Lua::new_with(
            StdLib::STRING | StdLib::TABLE | StdLib::MATH | StdLib::UTF8,
            LuaOptions::default(),
        )
        .map_err(runtime)?;
        let globals = lua.globals();
        for name in REMOVED_GLOBALS {
            globals.raw_set(name, LuaValue::Nil).map_err(runtime)?;
        }
        install_api(&lua, &script.node).map_err(runtime)?;
        install_pcall(&lua).map_err(runtime)?;
        lua.set_memory_limit(script.memory_bytes).map_err(runtime)?;

        let used = Rc::new(Cell::new(0));
        let every = script.instructions.clamp(1, HOOK_EVERY);
        let budget = script.instructions;
        let counter = Rc::clone(&used);
        lua.set_hook(
            HookTriggers {
                every_nth_instruction: Some(u32::try_from(every).unwrap_or(u32::MAX)),
                ..HookTriggers::default()
            },
            move |_, _| {
                let used = counter.get() + every;
                counter.set(used);
                if used > budget {
                    return Err(mlua::Error::external(BudgetExceeded));
                }
                Ok(VmState::Continue)
            },
        )
        .map_err(runtime)?;

        // The top level runs under the same budget and cap as a record.
        lua.load(script.source.as_str())
            .set_name(script.chunk_name.clone())
            .exec()
            .map_err(|e| classify(e).at_load())?;
        let process: Function = match globals.raw_get::<LuaValue>("process") {
            Ok(LuaValue::Function(f)) => f,
            Ok(_) => {
                return Err(LuaError::Runtime(format!(
                    "{}: the script must define a function `process(record)`",
                    script.display_name()
                )));
            }
            Err(e) => return Err(runtime(e)),
        };
        Ok(Self {
            lua,
            process,
            used,
            script,
        })
    }

    /// Run `process` over `record` with `ctx`'s state handle and record id.
    pub(crate) fn run(&self, record: &Record, ctx: &Context<'_>) -> Result<Returned, Stopped> {
        let record_id = ctx.meta.record_id;
        self.lua.set_app_data(Current {
            state: ctx.state.clone(),
            record_id,
        });
        self.used.set(0);
        let table = convert::to_table(&self.lua, record).map_err(classify)?;
        let returned: LuaValue = self.process.call(table).map_err(classify)?;
        if self.used.get() > self.script.instructions {
            // Cannot happen while `pcall` re-raises the budget; kept so a run that somehow
            // swallowed the trip is still refused.
            return Err(Stopped::Lua(LuaError::Instructions));
        }
        let output_bytes = self.script.output_bytes;
        match returned {
            LuaValue::Nil => Ok(Returned::Drop),
            LuaValue::Table(t) => {
                if t.raw_len() == 0 {
                    if t.is_empty() {
                        return Err(output(OutputError(
                            "an empty table is neither a record nor a list".to_owned(),
                        )));
                    }
                    return convert::from_table(&t, output_bytes)
                        .map(|record| Returned::Record(Box::new(record)))
                        .map_err(output);
                }
                let mut records = Vec::with_capacity(t.raw_len());
                for item in t.sequence_values::<LuaValue>() {
                    let item = item.map_err(classify)?;
                    let LuaValue::Table(item) = item else {
                        return Err(output(OutputError(
                            "every entry of a returned list must be a record table".to_owned(),
                        )));
                    };
                    records.push(convert::from_table(&item, output_bytes).map_err(output)?);
                }
                Ok(Returned::Split(records))
            }
            other => Err(output(OutputError(format!(
                "`process` must return a record table, a list of them, or nil; got {}",
                type_name(&other)
            )))),
        }
    }
}

/// What an `mlua` error stopped the run as: the budget marker, a memory error, a state
/// error carried out of an API call, or anything else the script did.
fn classify(error: mlua::Error) -> Stopped {
    match guardrail(&error) {
        Some(stopped) => stopped,
        None => Stopped::Lua(LuaError::Runtime(error.to_string())),
    }
}

/// An `mlua` error as a runtime [`LuaError`], for the paths where no other kind can arise.
fn runtime(error: mlua::Error) -> LuaError {
    LuaError::Runtime(error.to_string())
}

/// A refused output as a stop.
fn output(error: OutputError) -> Stopped {
    Stopped::Lua(LuaError::Output(error))
}

/// The stop when `error` is one a script must not catch: the budget marker, a memory
/// error anywhere in the chain, or a state error carried out of an API call. `None` for the
/// script's own errors.
fn guardrail(error: &mlua::Error) -> Option<Stopped> {
    if error.downcast_ref::<BudgetExceeded>().is_some() {
        return Some(Stopped::Lua(LuaError::Instructions));
    }
    if let Some(state) = error.downcast_ref::<StateError>() {
        return Some(Stopped::State(state.clone()));
    }
    match root_cause(error) {
        mlua::Error::MemoryError(_) => Some(Stopped::Lua(LuaError::Memory)),
        _ => None,
    }
}

/// The error under `mlua`'s wrappers: a callback error and a context error each carry the
/// one that caused them, and neither says anything the caller needs.
fn root_cause(error: &mlua::Error) -> &mlua::Error {
    match error {
        mlua::Error::CallbackError { cause, .. } | mlua::Error::WithContext { cause, .. } => {
            root_cause(cause)
        }
        other => other,
    }
}

/// The message a caught error reaches the script as: the script's own text for a
/// runtime error, the display form otherwise. A non-string error value is flattened to its
/// text, one difference from Lua's `pcall`.
fn caught_message(error: &mlua::Error) -> String {
    match root_cause(error) {
        mlua::Error::RuntimeError(message) => message.clone(),
        other => other.to_string(),
    }
}

/// `pcall` and `xpcall` that re-raise what a script must not catch. The base library's
/// own catch everything, so `while true do pcall(function() while true do end end) end`
/// would outlive the budget by a thousand instructions per iteration, a loop over a
/// caught memory error would outlive the cap, and `pcall(state.get, k)` would take the
/// `on_state_error` decision away from the engine. The replacements let the script's own
/// errors through as `false, message` and propagate a guardrail as if there were no
/// `pcall` at all.
fn install_pcall(lua: &mlua::Lua) -> mlua::Result<()> {
    let globals = lua.globals();
    globals.raw_set(
        "pcall",
        lua.create_function(|lua, (f, args): (Function, MultiValue)| {
            protected(&f, args, |error| {
                Ok(MultiValue::from_vec(vec![LuaValue::String(
                    lua.create_string(caught_message(error))?,
                )]))
            })
        })?,
    )?;
    globals.raw_set(
        "xpcall",
        lua.create_function(
            |lua, (f, handler, args): (Function, Function, MultiValue)| {
                protected(&f, args, |error| {
                    handler.call(lua.create_string(caught_message(error))?)
                })
            },
        )?,
    )?;
    Ok(())
}

/// One `pcall`-shaped call: what `f` returned with `true` in front of it, a guardrail or a
/// state error re-raised as if this call were not here, and any other error handed to
/// `caught` with `false` in front of whatever that gives back.
fn protected(
    f: &Function,
    args: MultiValue,
    caught: impl FnOnce(&mlua::Error) -> mlua::Result<MultiValue>,
) -> mlua::Result<MultiValue> {
    match f.call::<MultiValue>(args) {
        Ok(mut values) => {
            values.push_front(LuaValue::Boolean(true));
            Ok(values)
        }
        Err(error) if guardrail(&error).is_some() => Err(error),
        Err(error) => {
            let mut values = caught(&error)?;
            values.push_front(LuaValue::Boolean(false));
            Ok(values)
        }
    }
}

/// `state`, `log` and `now_ns` as globals.
fn install_api(lua: &mlua::Lua, node: &str) -> mlua::Result<()> {
    let globals = lua.globals();

    let state = lua.create_table()?;
    state.raw_set(
        "get",
        lua.create_function(|lua, key: String| {
            let current = current(lua)?;
            match current.state.get(&key).map_err(mlua::Error::external)? {
                Some(bytes) => Ok(LuaValue::String(lua.create_string(&bytes)?)),
                None => Ok(LuaValue::Nil),
            }
        })?,
    )?;
    state.raw_set(
        "set_nx",
        lua.create_function(|lua, (key, value, ttl_ms): (String, LuaString, u64)| {
            let current = current(lua)?;
            match current
                .state
                .set_nx(&key, &value.as_bytes(), Duration::from_millis(ttl_ms))
                .map_err(mlua::Error::external)?
            {
                None => Ok((true, LuaValue::Nil)),
                Some(existing) => Ok((false, LuaValue::String(lua.create_string(&existing)?))),
            }
        })?,
    )?;
    state.raw_set(
        "incr",
        lua.create_function(|lua, (key, by, ttl_ms): (String, i64, u64)| {
            let current = current(lua)?;
            current
                .state
                .incr(&key, by, Duration::from_millis(ttl_ms))
                .map_err(mlua::Error::external)
        })?,
    )?;
    state.raw_set(
        "del",
        lua.create_function(|lua, key: String| {
            let current = current(lua)?;
            current.state.del(&key).map_err(mlua::Error::external)
        })?,
    )?;
    globals.raw_set("state", state)?;

    let log = lua.create_table()?;
    for level in ["info", "warn"] {
        let node = node.to_owned();
        log.raw_set(
            level,
            lua.create_function(move |lua, message: String| {
                let record = current(lua)
                    .map(|c| c.record_id.to_string())
                    .unwrap_or_default();
                // Structured logging over OTLP lands with the logs ticket; stderr until then,
                // as the engine does.
                eprintln!("pipeline: lua `{node}` record {record} {level}: {message}");
                Ok(())
            })?,
        )?;
    }
    globals.raw_set("log", log)?;

    globals.raw_set(
        "now_ns",
        lua.create_function(|_, ()| Ok(i64::try_from(unix_nanos_now()).unwrap_or(i64::MAX)))?,
    )?;
    Ok(())
}

fn current(lua: &mlua::Lua) -> mlua::Result<mlua::AppDataRef<'_, Current>> {
    lua.app_data_ref::<Current>()
        .ok_or_else(|| mlua::Error::runtime("the pipeline API is only available inside `process`"))
}
