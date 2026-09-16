//! The record as a plain Lua table with OTLP field names, and the way back with the
//! checks the spec asks for: types right, strings under the size cap. Every field is
//! payload (ADR 0005), so a script may change or drop any of them.

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

/// The way back: Lua values read as JSON, with the bytes of every string counted against
/// the output cap. One reader per returned record, so the cap is per record.
struct Reader {
    used: usize,
    cap: usize,
}

impl Reader {
    /// Charge `bytes` to the cap, naming `field` if that is what exceeds it.
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

    /// A string field, `None` for `nil`.
    fn string(&mut self, value: &LuaValue, field: &str) -> Result<Option<String>, OutputError> {
        match value {
            LuaValue::Nil => Ok(None),
            LuaValue::String(s) => {
                self.take(s.as_bytes().len(), field)?;
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

    /// One of the record's maps: `nil` is the empty map, a table is its entries, anything
    /// else is refused.
    fn map(&mut self, value: &LuaValue, field: &str) -> Result<Map<String, Value>, OutputError> {
        match value {
            LuaValue::Nil => Ok(Map::new()),
            LuaValue::Table(t) => self.entries(t, field),
            other => refuse(format!(
                "`{field}` must be a table, not {}",
                type_name(other)
            )),
        }
    }

    /// A table's string-keyed entries as a JSON object. A value that is itself a table is
    /// taken as the JSON it is: the flat-map rule is the source's contract, so a value that
    /// arrived composite must leave an untouched script the way it came in.
    fn entries(&mut self, table: &Table, field: &str) -> Result<Map<String, Value>, OutputError> {
        let mut map = Map::new();
        for pair in table.pairs::<LuaValue, LuaValue>() {
            let (key, value) =
                pair.map_err(|e| OutputError(format!("cannot read `{field}`: {e}")))?;
            let LuaValue::String(key) = key else {
                return refuse(format!("`{field}` key {} is not a string", type_name(&key)));
            };
            let key = key
                .to_str()
                .map_err(|_| OutputError(format!("a `{field}` key is not valid UTF-8")))?
                .to_owned();
            let at = format!("{field}.{key}");
            let value = self.json(&value, &at)?.unwrap_or(Value::Null);
            map.insert(key, value);
        }
        Ok(map)
    }

    /// A Lua value as JSON: `nil` is absent, a table is an array when its keys are `1..n`
    /// and an object otherwise. Every string counts against the cap.
    fn json(&mut self, value: &LuaValue, field: &str) -> Result<Option<Value>, OutputError> {
        Ok(Some(match value {
            LuaValue::Nil => return Ok(None),
            LuaValue::Boolean(b) => Value::Bool(*b),
            LuaValue::Integer(i) => Value::from(*i),
            LuaValue::Number(f) => serde_json::Number::from_f64(*f)
                .map(Value::Number)
                .ok_or_else(|| OutputError(format!("`{field}` is not a finite number")))?,
            LuaValue::String(s) => {
                self.take(s.as_bytes().len(), field)?;
                Value::String(
                    s.to_str()
                        .map_err(|_| OutputError(format!("`{field}` is not valid UTF-8")))?
                        .to_owned(),
                )
            }
            LuaValue::Table(t) => {
                let len = t.raw_len();
                if len == 0 {
                    Value::Object(self.entries(t, field)?)
                } else {
                    let mut items = Vec::with_capacity(len);
                    for (i, item) in t.sequence_values::<LuaValue>().enumerate() {
                        let item =
                            item.map_err(|e| OutputError(format!("cannot read `{field}`: {e}")))?;
                        let at = format!("{field}[{}]", i + 1);
                        items.push(self.json(&item, &at)?.unwrap_or(Value::Null));
                    }
                    Value::Array(items)
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
}

/// The table the script returned as a record, its strings together under `output_bytes`.
/// Every field is optional and typed as core types it (`kind` is `log` when left out), and
/// a key that is not a record field is refused, so a typo cannot silently drop data.
pub(crate) fn from_table(table: &Table, output_bytes: usize) -> Result<Record, OutputError> {
    let mut record = Record::default();
    let mut reader = Reader {
        used: 0,
        cap: output_bytes,
    };
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
            "id" => record.id = id(&value)?,
            "kind" => record.kind = kind(&value)?,
            "time_unix_nano" => record.time_unix_nano = time(&value, "time_unix_nano")?,
            "observed_time_unix_nano" => {
                record.observed_time_unix_nano = time(&value, "observed_time_unix_nano")?;
            }
            "severity_text" => record.severity_text = reader.string(&value, "severity_text")?,
            "severity_number" => {
                record.severity_number = integer(&value, "severity_number")?
                    .map(|i| {
                        i32::try_from(i).map_err(|_| {
                            OutputError("`severity_number` must be an integer in i32".into())
                        })
                    })
                    .transpose()?;
            }
            "body" => record.body = reader.json(&value, "body")?,
            "attributes" => record.attributes = reader.map(&value, "attributes")?,
            "resource" => record.resource = reader.map(&value, "resource")?,
            "scope" => record.scope = reader.map(&value, "scope")?,
            "trace_id" => record.trace_id = reader.string(&value, "trace_id")?,
            "span_id" => record.span_id = reader.string(&value, "span_id")?,
            other => return refuse(format!("`{other}` is not a record field")),
        }
    }
    Ok(record)
}

/// An `id` as [`to_table`] hands it over: a non-negative integer, or its decimal text when
/// it does not fit Lua's signed 64 bits.
fn id(value: &LuaValue) -> Result<Option<RecordId>, OutputError> {
    const EXPECTED: &str = "`id` must be a non-negative integer or its decimal text";
    match value {
        LuaValue::String(s) => s
            .to_str()
            .ok()
            .and_then(|s| s.parse().ok())
            .map(|n| Some(RecordId(n)))
            .ok_or_else(|| OutputError(EXPECTED.into())),
        LuaValue::Integer(_) | LuaValue::Number(_) | LuaValue::Nil => integer(value, "id")?
            .map(|i| {
                u64::try_from(i)
                    .map(RecordId)
                    .map_err(|_| OutputError(EXPECTED.into()))
            })
            .transpose(),
        other => refuse(format!("{EXPECTED}, not {}", type_name(other))),
    }
}

/// A `kind`: one of [`Kind::ONE_OF`]; `log` when absent, the wire default.
fn kind(value: &LuaValue) -> Result<Kind, OutputError> {
    let expected = || format!("`kind` must be {}", Kind::ONE_OF);
    match value {
        LuaValue::Nil => Ok(Kind::Log),
        LuaValue::String(s) => s
            .to_str()
            .ok()
            .and_then(|s| Kind::parse(&s))
            .ok_or_else(|| OutputError(expected())),
        other => refuse(format!("{}, not {}", expected(), type_name(other))),
    }
}

/// An integer field: a Lua integer, or a float with no fraction, since `/` always yields a
/// float in Lua 5.4 and `18 / 2` is the integer 9 to any author.
fn integer(value: &LuaValue, field: &str) -> Result<Option<i64>, OutputError> {
    match value {
        LuaValue::Nil => Ok(None),
        LuaValue::Integer(i) => Ok(Some(*i)),
        LuaValue::Number(f) if f.fract() == 0.0 && f.abs() < 9_007_199_254_740_992.0 => {
            Ok(Some(*f as i64))
        }
        other => refuse(format!(
            "`{field}` must be an integer, not {}",
            type_name(other)
        )),
    }
}

fn time(value: &LuaValue, field: &str) -> Result<Option<u64>, OutputError> {
    integer(value, field)?
        .map(|i| {
            u64::try_from(i).map_err(|_| OutputError(format!("`{field}` must not be negative")))
        })
        .transpose()
}

/// A Lua value's type as an error message names it.
pub(crate) fn type_name(value: &LuaValue) -> &'static str {
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
