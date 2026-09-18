//! One worker's VM for one `lua` node: the sandbox, the API a script sees (`state`, `log`,
//! `now_ns`, `json`, `record:copy()`, the read-only `meta` argument), the guardrails,
//! and one run of `process` over one record.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use fusion_core::meta::{Meta, MetaField, unix_nanos_now};
use fusion_core::metrics::LuaErrorKind;
use fusion_core::record::Record;
use fusion_core::stage::{Context, State};
use fusion_core::state::StateError;
use mlua::{
    Function, HookTriggers, LuaOptions, LuaString, MultiValue, StdLib, Table, Value as LuaValue,
    VmState,
};

use crate::convert::{self, ListMark, OutputError};

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
    meta: Meta,
}

/// One worker's VM for one node.
pub(crate) struct Vm {
    lua: mlua::Lua,
    process: Function,
    /// The metatable of every `meta` table `process` receives: reads come from the current
    /// record's `Meta`, writes raise.
    meta_metatable: Table,
    /// The metatable of every record table `process` receives: it gives the table `copy`.
    record_metatable: Table,
    /// The metatable of the `json` table each run gets: reads come from the API, writes
    /// raise.
    json_metatable: Table,
    /// The mark of a table that is a JSON list, so an empty one stays a list.
    list: ListMark,
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
        let meta_metatable = meta_metatable(&lua).map_err(runtime)?;
        let list = ListMark::new(&lua).map_err(runtime)?;
        let json_metatable = json_metatable(&lua, &list).map_err(runtime)?;
        let record_metatable = record_metatable(&lua, &list).map_err(runtime)?;
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

        // The top level runs under the same budget and cap as a record, and sees `json` too.
        install_json(&lua, &json_metatable).map_err(runtime)?;
        lua.load(script.source.as_str())
            .set_name(script.chunk_name.clone())
            .exec()
            .map_err(|e| classify(e).at_load())?;
        let process: Function = match globals.raw_get::<LuaValue>("process") {
            Ok(LuaValue::Function(f)) => f,
            Ok(_) => {
                return Err(LuaError::Runtime(format!(
                    "{}: the script must define a function `process(record, meta)`",
                    script.display_name()
                )));
            }
            Err(e) => return Err(runtime(e)),
        };
        Ok(Self {
            lua,
            process,
            meta_metatable,
            record_metatable,
            json_metatable,
            list,
            used,
            script,
        })
    }

    /// Run `process(record, meta)` over `record` with `ctx`'s state handle and `Meta`.
    pub(crate) fn run(&self, record: &Record, ctx: &Context<'_>) -> Result<Returned, Stopped> {
        self.lua.set_app_data(Current {
            state: ctx.state.clone(),
            meta: ctx.meta.clone(),
        });
        self.used.set(0);
        let value = convert::to_lua(&self.lua, record, &self.list).map_err(classify)?;
        // A record that is an object or a list crosses as a table, and that table carries
        // the record metatable: it is what `record:copy()` hangs off, and what tells a
        // returned record from a returned split (issue #79).
        if let LuaValue::Table(table) = &value {
            table
                .set_metatable(Some(self.record_metatable.clone()))
                .map_err(classify)?;
        }
        // A fresh table per run, so a `rawset` on one run's `meta` is gone by the next;
        // nothing a script does to it reaches the pipeline either way.
        let meta = self.lua.create_table().map_err(classify)?;
        meta.set_metatable(Some(self.meta_metatable.clone()))
            .map_err(classify)?;
        // The same for `json`, which lives in the globals: a `rawset` on it, or a script
        // assigning the global, is gone by the next run.
        install_json(&self.lua, &self.json_metatable).map_err(classify)?;
        let returned: LuaValue = self.process.call((value, meta)).map_err(classify)?;
        if self.used.get() > self.script.instructions {
            // Cannot happen while `pcall` re-raises the budget; kept so a run that somehow
            // swallowed the trip is still refused.
            return Err(Stopped::Lua(LuaError::Instructions));
        }
        let output_bytes = self.script.output_bytes;
        match returned {
            LuaValue::Nil => Ok(Returned::Drop),
            LuaValue::Table(t) => {
                // The record table, and a `record:copy()` of it, carry the record
                // metatable, so `return record` is one record whatever shape the record
                // has — a list record would otherwise read as a split of its items.
                let is_record = t.metatable().is_some_and(|mt| mt == self.record_metatable);
                if is_record || (t.raw_len() == 0 && !self.list.is_list(&t)) {
                    if !is_record && t.is_empty() {
                        return Err(output(OutputError(
                            "an empty table is neither a record nor a list".to_owned(),
                        )));
                    }
                    return convert::from_lua(&LuaValue::Table(t), output_bytes, &self.list)
                        .map(|record| Returned::Record(Box::new(record)))
                        .map_err(output);
                }
                let len = convert::list_len(&t, "the returned list").map_err(output)?;
                if len == 0 {
                    return Err(output(OutputError(
                        "an empty list splits the record into nothing; return nil to drop it"
                            .to_owned(),
                    )));
                }
                let mut records = Vec::with_capacity(len);
                for i in 1..=len {
                    let item: LuaValue = t.raw_get(i).map_err(classify)?;
                    let LuaValue::Table(item) = item else {
                        return Err(output(OutputError(
                            "every entry of a returned list must be a record table".to_owned(),
                        )));
                    };
                    records.push(
                        convert::from_lua(&LuaValue::Table(item), output_bytes, &self.list)
                            .map_err(output)?,
                    );
                }
                Ok(Returned::Split(records))
            }
            // `false` is almost always a script meaning to drop the record, and taking it
            // as a one-bool record would swallow that silently. `nil` is how a script drops
            // one, so a bool is refused and says so.
            LuaValue::Boolean(_) => Err(output(OutputError(
                "`process` returned a boolean; return nil to drop the record".to_owned(),
            ))),
            // Any other scalar is a record: a `codec: text` line comes in as a string and a
            // script may return one.
            other => convert::from_lua(&other, output_bytes, &self.list)
                .map(|record| Returned::Record(Box::new(record)))
                .map_err(output),
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
                    .map(|c| c.meta.record_id.to_string())
                    .unwrap_or_default();
                // A script's own lines are not pipeline events (spec amendment, issue #12):
                // they stay on stderr, and shipping them to Loki is a follow-up.
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

/// The metatable of `json`, read-only: `json.null`, the value a JSON `null` is while a
/// script holds it, and `json.list(t)`, which marks `t` (a new table when omitted) as a list
/// and returns it, so a script can make a list that stays one when empty. A table with a
/// metatable other than the list mark is refused, so a record table or `meta` cannot lose
/// theirs; a list is returned as it is.
fn json_metatable(lua: &mlua::Lua, list: &ListMark) -> mlua::Result<Table> {
    let fields = lua.create_table()?;
    fields.raw_set("null", LuaValue::NULL)?;
    let list = list.clone();
    fields.raw_set(
        "list",
        lua.create_function(move |lua, table: Option<Table>| {
            let table = match table {
                Some(table) => table,
                None => lua.create_table()?,
            };
            if !list.accepts(&table) {
                return Err(mlua::Error::runtime(
                    "`json.list` takes a plain table or a list, not a table with a metatable",
                ));
            }
            list.mark(&table)?;
            Ok(table)
        })?,
    )?;
    let metatable = lua.create_table()?;
    metatable.raw_set("__index", fields)?;
    metatable.raw_set(
        "__newindex",
        lua.create_function(|_, _: MultiValue| -> mlua::Result<()> {
            Err(mlua::Error::runtime("`json` is read-only"))
        })?,
    )?;
    metatable.raw_set("__metatable", "json")?;
    Ok(metatable)
}

/// A fresh `json` global behind `metatable`.
fn install_json(lua: &mlua::Lua, metatable: &Table) -> mlua::Result<()> {
    let json = lua.create_table()?;
    json.set_metatable(Some(metatable.clone()))?;
    lua.globals().raw_set("json", json)
}

/// `record:copy()`: a deep copy of the table, shared structure and cycles kept as they are,
/// every list still marked as one, with the record metatable so the copy can be copied too.
/// Written in Lua so it runs under the instruction budget and the memory cap like the
/// script's own code. The chunk takes the metatable and a function that marks the copy of a
/// list, and returns the method.
const COPY: &str = r#"
local metatable, mark = ...
local next, type, setmetatable = next, type, setmetatable
local function deep(value, seen)
  if type(value) ~= "table" then return value end
  local done = seen[value]
  if done then return done end
  local out = {}
  mark(value, out)
  seen[value] = out
  for k, v in next, value do
    out[deep(k, seen)] = deep(v, seen)
  end
  return out
end
return function(record)
  return setmetatable(deep(record, {}), metatable)
end
"#;

/// The metatable behind every record table: `__index` holds `copy`, and `__metatable` hides
/// the table from `getmetatable` and refuses `setmetatable`, so a script cannot change
/// `copy` for the records after it.
fn record_metatable(lua: &mlua::Lua, list: &ListMark) -> mlua::Result<Table> {
    let metatable = lua.create_table()?;
    // A script cannot read the list mark, so the copy asks Rust which tables carry it.
    let list = list.clone();
    let mark = lua.create_function(move |_, (from, to): (Table, Table)| {
        if list.is_list(&from) {
            list.mark(&to)?;
        }
        Ok(())
    })?;
    let copy: Function = lua
        .load(COPY)
        .set_name("=record:copy")
        .call((metatable.clone(), mark))?;
    let methods = lua.create_table()?;
    methods.raw_set("copy", copy)?;
    metatable.raw_set("__index", methods)?;
    metatable.raw_set("__metatable", "record")?;
    Ok(metatable)
}

/// The metatable behind `meta`: `__index` reads the current record's `Meta`, `__newindex`
/// raises, `__pairs` walks the four keys, and `__metatable` hides it from `getmetatable` and
/// refuses `setmetatable`: `getmetatable` answers `"meta"`, as the record table's answers
/// `"record"`.
fn meta_metatable(lua: &mlua::Lua) -> mlua::Result<Table> {
    let metatable = lua.create_table()?;
    metatable.raw_set(
        "__index",
        lua.create_function(|lua, (_, key): (Table, LuaValue)| {
            let LuaValue::String(key) = key else {
                return Ok(LuaValue::Nil);
            };
            let Some(field) = MetaField::parse(&key.to_str()?) else {
                return Ok(LuaValue::Nil);
            };
            let current = current(lua)?;
            convert::meta_value(lua, &current.meta, field)
        })?,
    )?;
    metatable.raw_set(
        "__newindex",
        lua.create_function(|_, (_, key): (Table, LuaValue)| -> mlua::Result<()> {
            Err(mlua::Error::runtime(format!(
                "`meta` is the pipeline's and read-only (writing `{}`); copy a value into the \
                 record instead",
                key.to_string().unwrap_or_default()
            )))
        })?,
    )?;
    metatable.raw_set(
        "__pairs",
        lua.create_function(|lua, _: Table| {
            let current = current(lua)?;
            let snapshot = lua.create_table()?;
            for field in MetaField::ALL {
                snapshot.raw_set(
                    field.as_str(),
                    convert::meta_value(lua, &current.meta, field)?,
                )?;
            }
            let next: Function = lua.globals().raw_get("next")?;
            Ok((next, snapshot, LuaValue::Nil))
        })?,
    )?;
    metatable.raw_set("__metatable", "meta")?;
    Ok(metatable)
}

fn current(lua: &mlua::Lua) -> mlua::Result<mlua::AppDataRef<'_, Current>> {
    lua.app_data_ref::<Current>()
        .ok_or_else(|| mlua::Error::runtime("the pipeline API is only available inside `process`"))
}
