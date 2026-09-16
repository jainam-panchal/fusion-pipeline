//! The record as a plain Lua table with OTLP field names, and the way back with the
//! checks the spec asks for: required fields present, types right, `id` and the tenant
//! unchanged, strings under the size cap.

use fusion_core::record::{Kind, Record, RecordId};
use mlua::{Integer, Table, Value as LuaValue};
use serde_json::{Map, Value};

/// A returned record the stage refuses. The message names the field.
#[derive(Debug)]
pub(crate) struct OutputError(pub(crate) String);

impl std::fmt::Display for OutputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn refuse<T>(message: impl Into<String>) -> Result<T, OutputError> {
    Err(OutputError(message.into()))
}

/// What the script gets: every present field under its OTLP name, maps as tables of
/// scalars, `body` as the JSON value it is. `id` is an integer, or its decimal text when
/// it does not fit Lua's signed 64 bits.
pub(crate) fn to_table(lua: &mlua::Lua, record: &Record) -> mlua::Result<Table> {
    let t = lua.create_table_with_capacity(0, 12)?;
    if let Some(id) = record.id {
        t.raw_set("id", id_value(lua, id)?)?;
    }
    t.raw_set("kind", record.kind.as_str())?;
    if let Some(n) = record.time_unix_nano {
        t.raw_set("time_unix_nano", unsigned(n))?;
    }
    if let Some(n) = record.observed_time_unix_nano {
        t.raw_set("observed_time_unix_nano", unsigned(n))?;
    }
    if let Some(s) = &record.severity_text {
        t.raw_set("severity_text", s.as_str())?;
    }
    if let Some(n) = record.severity_number {
        t.raw_set("severity_number", Integer::from(n))?;
    }
    if let Some(body) = &record.body {
        t.raw_set("body", json_to_lua(lua, body)?)?;
    }
    t.raw_set("attributes", map_to_table(lua, &record.attributes)?)?;
    t.raw_set("resource", map_to_table(lua, &record.resource)?)?;
    t.raw_set("scope", map_to_table(lua, &record.scope)?)?;
    if let Some(s) = &record.trace_id {
        t.raw_set("trace_id", s.as_str())?;
    }
    if let Some(s) = &record.span_id {
        t.raw_set("span_id", s.as_str())?;
    }
    Ok(t)
}

fn id_value(lua: &mlua::Lua, id: RecordId) -> mlua::Result<LuaValue> {
    Ok(match Integer::try_from(id.0) {
        Ok(i) => LuaValue::Integer(i),
        Err(_) => LuaValue::String(lua.create_string(id.0.to_string())?),
    })
}

/// A `u64` as Lua sees it: an integer when it fits, else a float (lossy, and only above
/// 2^63, which no timestamp reaches).
fn unsigned(n: u64) -> LuaValue {
    Integer::try_from(n).map_or(LuaValue::Number(n as f64), LuaValue::Integer)
}

fn map_to_table(lua: &mlua::Lua, map: &Map<String, Value>) -> mlua::Result<Table> {
    let t = lua.create_table_with_capacity(0, map.len())?;
    for (k, v) in map {
        t.raw_set(k.as_str(), json_to_lua(lua, v)?)?;
    }
    Ok(t)
}

fn json_to_lua(lua: &mlua::Lua, v: &Value) -> mlua::Result<LuaValue> {
    Ok(match v {
        Value::Null => LuaValue::Nil,
        Value::Bool(b) => LuaValue::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                LuaValue::Integer(i)
            } else if let Some(u) = n.as_u64() {
                unsigned(u)
            } else {
                LuaValue::Number(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        Value::String(s) => LuaValue::String(lua.create_string(s)?),
        Value::Array(items) => {
            let t = lua.create_table_with_capacity(items.len(), 0)?;
            for (i, item) in items.iter().enumerate() {
                t.raw_seti(i + 1, json_to_lua(lua, item)?)?;
            }
            LuaValue::Table(t)
        }
        Value::Object(map) => LuaValue::Table(map_to_table(lua, map)?),
    })
}

/// What the script must leave alone: the id (every returned record keeps it; a script
/// cannot mint ids) and the tenant (the engine fixed it once for every label and state
/// key), plus the size cap on the strings it returns.
pub(crate) struct Expected<'a> {
    pub(crate) id: RecordId,
    pub(crate) tenant: Option<&'a str>,
    pub(crate) output_bytes: usize,
}

/// Counts the bytes of every string in a returned record against the cap.
struct Budget {
    used: usize,
    cap: usize,
}

impl Budget {
    fn take(&mut self, bytes: usize, field: &str) -> Result<(), OutputError> {
        self.used += bytes;
        if self.used > self.cap {
            return refuse(format!(
                "`{field}`: the returned record exceeds `limits.output_kib` ({} bytes)",
                self.cap
            ));
        }
        Ok(())
    }
}

/// The table the script returned as a record, checked against `expected`. The table must
/// carry `id` (unchanged) and `kind` (`log`), every other field is optional, and a key that
/// is not a record field is refused, so a typo cannot silently drop data.
pub(crate) fn from_table(table: &Table, expected: &Expected<'_>) -> Result<Record, OutputError> {
    let mut record = Record::default();
    let mut budget = Budget {
        used: 0,
        cap: expected.output_bytes,
    };
    let mut saw_id = false;
    let mut saw_kind = false;
    for pair in table.pairs::<LuaValue, LuaValue>() {
        let (key, value) =
            pair.map_err(|e| OutputError(format!("cannot read the returned table: {e}")))?;
        let LuaValue::String(key) = key else {
            return refuse(format!("key {} is not a field name", type_name(&key)));
        };
        let key = key
            .to_str()
            .map_err(|_| OutputError("a key is not valid UTF-8".into()))?;
        match &*key {
            "id" => {
                if !id_matches(&value, expected.id) {
                    return refuse("`id` must be returned unchanged");
                }
                saw_id = true;
                record.id = Some(expected.id);
            }
            "kind" => {
                if string(&value, "kind", &mut budget)?.as_deref() != Some(Kind::Log.as_str()) {
                    return refuse("`kind` must be returned as `log`");
                }
                saw_kind = true;
                record.kind = Kind::Log;
            }
            "time_unix_nano" => record.time_unix_nano = time(&value, "time_unix_nano")?,
            "observed_time_unix_nano" => {
                record.observed_time_unix_nano = time(&value, "observed_time_unix_nano")?;
            }
            "severity_text" => record.severity_text = string(&value, "severity_text", &mut budget)?,
            "severity_number" => {
                record.severity_number = match value {
                    LuaValue::Nil => None,
                    LuaValue::Integer(i) => Some(i32::try_from(i).map_err(|_| {
                        OutputError("`severity_number` must be an integer in i32".into())
                    })?),
                    other => {
                        return refuse(format!(
                            "`severity_number` must be an integer, not {}",
                            type_name(&other)
                        ));
                    }
                };
            }
            "body" => record.body = lua_to_json(&value, "body", &mut budget)?,
            "attributes" => record.attributes = map_from(&value, "attributes", &mut budget)?,
            "resource" => record.resource = map_from(&value, "resource", &mut budget)?,
            "scope" => record.scope = map_from(&value, "scope", &mut budget)?,
            "trace_id" => record.trace_id = string(&value, "trace_id", &mut budget)?,
            "span_id" => record.span_id = string(&value, "span_id", &mut budget)?,
            other => return refuse(format!("`{other}` is not a record field")),
        }
    }
    if !saw_id {
        return refuse("`id` is missing from the returned record");
    }
    if !saw_kind {
        return refuse("`kind` is missing from the returned record");
    }
    if record.tenant() != expected.tenant {
        return refuse("`resource.tenant.id` must be returned unchanged");
    }
    Ok(record)
}

fn id_matches(value: &LuaValue, id: RecordId) -> bool {
    match value {
        LuaValue::Integer(i) => u64::try_from(*i) == Ok(id.0),
        LuaValue::String(s) => s.to_str().is_ok_and(|s| s.parse() == Ok(id.0)),
        _ => false,
    }
}

fn time(value: &LuaValue, field: &str) -> Result<Option<u64>, OutputError> {
    match value {
        LuaValue::Nil => Ok(None),
        LuaValue::Integer(i) => u64::try_from(*i)
            .map(Some)
            .map_err(|_| OutputError(format!("`{field}` must not be negative"))),
        other => refuse(format!(
            "`{field}` must be an integer, not {}",
            type_name(other)
        )),
    }
}

fn string(
    value: &LuaValue,
    field: &str,
    budget: &mut Budget,
) -> Result<Option<String>, OutputError> {
    match value {
        LuaValue::Nil => Ok(None),
        LuaValue::String(s) => {
            budget.take(s.as_bytes().len(), field)?;
            s.to_str()
                .map(|s| Some(s.to_owned()))
                .map_err(|_| OutputError(format!("`{field}` is not valid UTF-8")))
        }
        other => refuse(format!(
            "`{field}` must be a string, not {}",
            type_name(other)
        )),
    }
}

fn map_from(
    value: &LuaValue,
    field: &str,
    budget: &mut Budget,
) -> Result<Map<String, Value>, OutputError> {
    let table = match value {
        LuaValue::Nil => return Ok(Map::new()),
        LuaValue::Table(t) => t,
        other => {
            return refuse(format!(
                "`{field}` must be a table, not {}",
                type_name(other)
            ));
        }
    };
    let mut map = Map::new();
    for pair in table.pairs::<LuaValue, LuaValue>() {
        let (key, value) = pair.map_err(|e| OutputError(format!("cannot read `{field}`: {e}")))?;
        let LuaValue::String(key) = key else {
            return refuse(format!("`{field}` key {} is not a string", type_name(&key)));
        };
        let key = key
            .to_str()
            .map_err(|_| OutputError(format!("a `{field}` key is not valid UTF-8")))?
            .to_owned();
        let at = format!("{field}.{key}");
        let scalar = match value {
            LuaValue::Table(_) => {
                return refuse(format!("`{at}` must be a scalar; the maps are flat"));
            }
            other => lua_to_json(&other, &at, budget)?,
        };
        map.insert(key, scalar.unwrap_or(Value::Null));
    }
    Ok(map)
}

/// A Lua value as JSON: `nil` is absent, a table is an array when its keys are `1..n` and
/// an object otherwise. Every string counts against the cap.
fn lua_to_json(
    value: &LuaValue,
    field: &str,
    budget: &mut Budget,
) -> Result<Option<Value>, OutputError> {
    Ok(Some(match value {
        LuaValue::Nil => return Ok(None),
        LuaValue::Boolean(b) => Value::Bool(*b),
        LuaValue::Integer(i) => Value::from(*i),
        LuaValue::Number(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .ok_or_else(|| OutputError(format!("`{field}` is not a finite number")))?,
        LuaValue::String(s) => {
            budget.take(s.as_bytes().len(), field)?;
            Value::String(
                s.to_str()
                    .map_err(|_| OutputError(format!("`{field}` is not valid UTF-8")))?
                    .to_owned(),
            )
        }
        LuaValue::Table(t) => {
            let len = t.raw_len();
            if len > 0 {
                let mut items = Vec::with_capacity(len);
                for (i, item) in t.sequence_values::<LuaValue>().enumerate() {
                    let item =
                        item.map_err(|e| OutputError(format!("cannot read `{field}`: {e}")))?;
                    let at = format!("{field}[{}]", i + 1);
                    items.push(lua_to_json(&item, &at, budget)?.unwrap_or(Value::Null));
                }
                Value::Array(items)
            } else {
                let mut map = Map::new();
                for pair in t.pairs::<LuaValue, LuaValue>() {
                    let (key, item) =
                        pair.map_err(|e| OutputError(format!("cannot read `{field}`: {e}")))?;
                    let LuaValue::String(key) = key else {
                        return refuse(format!(
                            "`{field}` key {} is not a string",
                            type_name(&key)
                        ));
                    };
                    let key = key
                        .to_str()
                        .map_err(|_| OutputError(format!("a `{field}` key is not valid UTF-8")))?
                        .to_owned();
                    let at = format!("{field}.{key}");
                    let item = lua_to_json(&item, &at, budget)?.unwrap_or(Value::Null);
                    map.insert(key, item);
                }
                Value::Object(map)
            }
        }
        other => {
            return refuse(format!(
                "`{field}` is {}, which has no JSON form",
                type_name(other)
            ));
        }
    }))
}

fn type_name(value: &LuaValue) -> &'static str {
    match value {
        LuaValue::Nil => "nil",
        LuaValue::Boolean(_) => "a boolean",
        LuaValue::Integer(_) => "an integer",
        LuaValue::Number(_) => "a number",
        LuaValue::String(_) => "a string",
        LuaValue::Table(_) => "a table",
        LuaValue::Function(_) => "a function",
        LuaValue::Thread(_) => "a coroutine",
        LuaValue::UserData(_) | LuaValue::LightUserData(_) => "userdata",
        _ => "an unknown value",
    }
}
