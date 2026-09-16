//! One worker's VM for one `lua` node: the sandbox, the API a script sees (`state`, `log`,
//! `now_ns`), the guardrails, and one run of `process` over one record.

use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fusion_core::metrics::LuaErrorKind;
use fusion_core::record::{Record, RecordId};
use fusion_core::stage::State;
use fusion_core::state::StateError;
use mlua::{Function, HookTriggers, LuaOptions, LuaString, StdLib, Value as LuaValue, VmState};

use crate::convert::{self, Expected, OutputError};

/// The globals a script may not name, and the names the sandbox leaves undefined. `load`
/// and its siblings come with the base library and are removed; `os`, `io`, `package`
/// (and so `require`) and `debug` are never loaded.
pub(crate) const FORBIDDEN: [&str; 9] = [
    "os",
    "io",
    "package",
    "require",
    "load",
    "loadfile",
    "dofile",
    "loadstring",
    "debug",
];

/// The base library's loaders, removed from the globals after the VM is built.
const REMOVED_GLOBALS: [&str; 4] = ["load", "loadfile", "dofile", "collectgarbage"];

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

/// Why a run of `process` did not produce records: the `kind` label of `lua_errors_total`
/// is [`Fault::kind`].
#[derive(Debug)]
pub(crate) enum Fault {
    /// The instruction budget tripped.
    Instructions,
    /// The memory cap tripped.
    Memory,
    /// The script raised, or the VM failed to run it.
    Runtime(String),
    /// The script returned something the stage refuses.
    Output(OutputError),
    /// A `state.*` call could not reach the store. Not a Lua error: the engine applies the
    /// node's `on_state_error`.
    State(StateError),
}

impl Fault {
    /// The `kind` label, `None` for a state error, which is the store's to count.
    pub(crate) const fn kind(&self) -> Option<LuaErrorKind> {
        match self {
            Self::Instructions => Some(LuaErrorKind::Instructions),
            Self::Memory => Some(LuaErrorKind::Memory),
            Self::Runtime(_) => Some(LuaErrorKind::Runtime),
            Self::Output(_) => Some(LuaErrorKind::Output),
            Self::State(_) => None,
        }
    }

    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Instructions => "instruction budget exceeded".to_owned(),
            Self::Memory => "memory cap exceeded".to_owned(),
            Self::Runtime(message) => message.clone(),
            Self::Output(error) => format!("returned record refused: {error}"),
            Self::State(error) => error.to_string(),
        }
    }
}

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
        f.write_str("instruction budget exceeded")
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
    script: Rc<Script>,
}

impl Vm {
    /// Build the sandbox, install the API and the guardrails, run the script's top level
    /// and take its `process`.
    pub(crate) fn new(script: Rc<Script>) -> Result<Self, Fault> {
        let lua = mlua::Lua::new_with(
            StdLib::STRING | StdLib::TABLE | StdLib::MATH | StdLib::UTF8,
            LuaOptions::default(),
        )
        .map_err(|e| Fault::Runtime(e.to_string()))?;
        let globals = lua.globals();
        for name in REMOVED_GLOBALS {
            globals
                .raw_set(name, LuaValue::Nil)
                .map_err(|e| Fault::Runtime(e.to_string()))?;
        }
        install_api(&lua, &script.node).map_err(|e| Fault::Runtime(e.to_string()))?;
        lua.set_memory_limit(script.memory_bytes)
            .map_err(|e| Fault::Runtime(e.to_string()))?;

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
        .map_err(|e| Fault::Runtime(e.to_string()))?;

        // The top level runs under the same budget and cap as a record.
        lua.load(script.source.as_str())
            .set_name(script.chunk_name.clone())
            .exec()
            .map_err(classify)?;
        let process: Function = match globals.raw_get::<LuaValue>("process") {
            Ok(LuaValue::Function(f)) => f,
            Ok(_) => {
                return Err(Fault::Runtime(format!(
                    "{}: the script must define a function `process(record)`",
                    script.chunk_name.trim_start_matches(['@', '='])
                )));
            }
            Err(e) => return Err(Fault::Runtime(e.to_string())),
        };
        Ok(Self {
            lua,
            process,
            used,
            script,
        })
    }

    /// Run `process` over `record` with `state` as the record's handle.
    pub(crate) fn run(&self, record: &Record, state: &State) -> Result<Returned, Fault> {
        let Some(record_id) = record.id else {
            return Err(Fault::Runtime("record has no id".to_owned()));
        };
        self.lua.set_app_data(Current {
            state: state.clone(),
            record_id,
        });
        self.used.set(0);
        let table = convert::to_table(&self.lua, record).map_err(classify)?;
        let returned: LuaValue = self.process.call(table).map_err(classify)?;
        let expected = Expected {
            id: record_id,
            tenant: record.tenant(),
            output_bytes: self.script.output_bytes,
        };
        match returned {
            LuaValue::Nil => Ok(Returned::Drop),
            LuaValue::Table(t) => {
                if t.raw_len() == 0 {
                    // A record, or an empty table, which is neither a record nor a list.
                    return convert::from_table(&t, &expected)
                        .map(|record| Returned::Record(Box::new(record)))
                        .map_err(Fault::Output);
                }
                let mut records = Vec::with_capacity(t.raw_len());
                for item in t.sequence_values::<LuaValue>() {
                    let item = item.map_err(classify)?;
                    let LuaValue::Table(item) = item else {
                        return Err(Fault::Output(OutputError(
                            "every entry of a returned list must be a record table".to_owned(),
                        )));
                    };
                    records.push(convert::from_table(&item, &expected).map_err(Fault::Output)?);
                }
                Ok(Returned::Split(records))
            }
            other => Err(Fault::Output(OutputError(format!(
                "`process` must return a record table, a list of them, or nil; got {}",
                lua_type(&other)
            )))),
        }
    }
}

/// The `kind` of an `mlua` error: the budget marker, a memory error, a state error carried
/// out of an API call, or anything else the script did.
fn classify(error: mlua::Error) -> Fault {
    if error.downcast_ref::<BudgetExceeded>().is_some() {
        return Fault::Instructions;
    }
    if let Some(state) = error.downcast_ref::<StateError>() {
        return Fault::State(state.clone());
    }
    let mut current = &error;
    loop {
        match current {
            mlua::Error::MemoryError(_) => return Fault::Memory,
            mlua::Error::CallbackError { cause, .. } | mlua::Error::WithContext { cause, .. } => {
                current = cause;
            }
            _ => break,
        }
    }
    Fault::Runtime(error.to_string())
}

fn lua_type(value: &LuaValue) -> &'static str {
    match value {
        LuaValue::Boolean(_) => "a boolean",
        LuaValue::Integer(_) | LuaValue::Number(_) => "a number",
        LuaValue::String(_) => "a string",
        LuaValue::Function(_) => "a function",
        _ => "an unsupported value",
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
        lua.create_function(|_, ()| {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            Ok(i64::try_from(nanos).unwrap_or(i64::MAX))
        })?,
    )?;
    Ok(())
}

fn current(lua: &mlua::Lua) -> mlua::Result<mlua::AppDataRef<'_, Current>> {
    lua.app_data_ref::<Current>()
        .ok_or_else(|| mlua::Error::runtime("the pipeline API is only available inside `process`"))
}

/// `source` compiled and run once in a throwaway sandbox at load, so a syntax error, a
/// missing `process` or a top level that misbehaves fails the config, not the first record.
pub(crate) fn check(script: Rc<Script>) -> Result<(), Fault> {
    Vm::new(script).map(|_| ())
}
