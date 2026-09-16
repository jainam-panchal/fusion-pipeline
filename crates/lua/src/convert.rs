//! The record as a plain Lua table with OTLP field names, and the way back: Lua values
//! converted to JSON, strings under the size cap, and every field written through core's
//! write rules, so no field's type is spelled here (issue #43). Every field is payload
//! (ADR 0005), so a script may change or drop any of them.
//!
//! A value crosses both ways unchanged when the script leaves it alone. Lua has no empty
//! list and no `nil` inside a table, so a JSON list becomes a table marked with the VM's
//! list metatable, and a JSON `null` inside a list or a map becomes `json.null`; the way
//! back reads a marked table as a list, whatever its length, and `json.null` as `null`. A
//! record field is never `null` (the decoder leaves it out), and a field set to
//! `json.null` is left out, as `nil` is.

use fusion_core::meta::{Meta, MetaField, MetaValue};
use fusion_core::path::{FieldPath, TopLevel};
use fusion_core::record::{Record, RecordId};
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

/// What the script gets: every present field under its OTLP name, maps as tables of JSON
/// values, `body` as the JSON value it is, every list marked with `list`. `id` is an
/// integer, or its decimal text when it does not fit Lua's signed 64 bits.
pub(crate) fn to_table(lua: &mlua::Lua, record: &Record, list: &Table) -> mlua::Result<Table> {
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
        t.raw_set("body", json_to_lua(lua, body, list)?)?;
    }
    t.raw_set("attributes", map_to_table(lua, &record.attributes, list)?)?;
    t.raw_set("resource", map_to_table(lua, &record.resource, list)?)?;
    t.raw_set("scope", map_to_table(lua, &record.scope, list)?)?;
    if let Some(s) = &record.trace_id {
        t.raw_set("trace_id", s.as_str())?;
    }
    if let Some(s) = &record.span_id {
        t.raw_set("span_id", s.as_str())?;
    }
    Ok(t)
}

/// `meta[field]` as a script reads it: the record id as `id` is in the record table, the
/// tenant as a string, the ingestion time and delivery count as integers.
pub(crate) fn meta_value(lua: &mlua::Lua, meta: &Meta, field: MetaField) -> mlua::Result<LuaValue> {
    // The id crosses as `to_table` hands it over, text above 2^63; every other value as
    // `Meta::get` gives it.
    if field == MetaField::Id {
        return id_value(lua, meta.record_id);
    }
    Ok(match meta.get(field) {
        MetaValue::Str(text) => LuaValue::String(lua.create_string(text)?),
        MetaValue::U64(n) => unsigned(n),
    })
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

fn map_to_table(lua: &mlua::Lua, map: &Map<String, Value>, list: &Table) -> mlua::Result<Table> {
    let t = lua.create_table_with_capacity(0, map.len())?;
    for (k, v) in map {
        t.raw_set(k.as_str(), json_to_lua(lua, v, list)?)?;
    }
    Ok(t)
}

fn json_to_lua(lua: &mlua::Lua, v: &Value, list: &Table) -> mlua::Result<LuaValue> {
    Ok(match v {
        Value::Null => LuaValue::NULL,
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
                t.raw_seti(i + 1, json_to_lua(lua, item, list)?)?;
            }
            t.set_metatable(Some(list.clone()))?;
            LuaValue::Table(t)
        }
        Value::Object(map) => LuaValue::Table(map_to_table(lua, map, list)?),
    })
}

/// Whether `table` is marked as a list by `list`, the VM's list metatable.
pub(crate) fn is_list(table: &Table, list: &Table) -> bool {
    table.metatable().is_some_and(|mt| mt == *list)
}

/// The length of a table read as a list, whose only keys must be its positions `1..n`: any
/// other key would be lost, and a `nil` below the last position is a hole Lua cannot keep.
pub(crate) fn positions(table: &Table, field: &str) -> Result<usize, OutputError> {
    let mut count = 0;
    let mut last = 0;
    for pair in table.pairs::<LuaValue, LuaValue>() {
        let (key, _) = pair.map_err(|e| OutputError(format!("cannot read `{field}`: {e}")))?;
        let position = match key {
            LuaValue::Integer(i) => usize::try_from(i).ok().filter(|&i| i > 0),
            _ => None,
        };
        let Some(position) = position else {
            return refuse(format!(
                "`{field}` is a list, and its key {} is not a position in it",
                key.to_string()
                    .unwrap_or_else(|_| type_name(&key).to_owned())
            ));
        };
        count += 1;
        last = last.max(position);
    }
    if count != last {
        return refuse(format!(
            "`{field}` is a list with a `nil` below position {last}; write `json.null` for a null"
        ));
    }
    Ok(last)
}

/// How many tables below the record a returned value may nest: serde_json's recursion limit,
/// which stops a decoded record short of it, so every record the source decoded can come
/// back unchanged, while a table that contains itself is refused instead of recursing until
/// the worker's stack overflows.
const MAX_DEPTH: usize = 128;

/// The way back: Lua values read as JSON, with the bytes of every string counted against
/// the output cap. One reader per returned record, so the cap is per record.
struct Reader<'l> {
    used: usize,
    cap: usize,
    /// How many tables the value being read is inside.
    depth: usize,
    /// The metatable that marks a table as a list.
    list: &'l Table,
}

impl Reader<'_> {
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

    /// A table's string-keyed entries as a JSON object. A value that is itself a table is
    /// taken as the JSON it is: the flat-map rule is the source's contract, so a value that
    /// arrived composite must leave an untouched script the way it came in.
    fn entries(&mut self, table: &Table, field: &str) -> Result<Map<String, Value>, OutputError> {
        self.enter(field)?;
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
        self.depth -= 1;
        Ok(map)
    }

    /// Step into a table, refusing past [`MAX_DEPTH`]. A refusal ends the whole returned
    /// record, so only a successful read steps back out.
    fn enter(&mut self, field: &str) -> Result<(), OutputError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return refuse(format!(
                "`{field}` nests deeper than {MAX_DEPTH} tables, or contains itself"
            ));
        }
        Ok(())
    }

    /// A table's entries as a JSON array, its keys checked by [`positions`].
    fn items(&mut self, table: &Table, field: &str) -> Result<Vec<Value>, OutputError> {
        self.enter(field)?;
        let len = positions(table, field)?;
        let mut items = Vec::with_capacity(len);
        for i in 1..=len {
            let item: LuaValue = table
                .raw_get(i)
                .map_err(|e| OutputError(format!("cannot read `{field}`: {e}")))?;
            let at = format!("{field}[{i}]");
            items.push(self.json(&item, &at)?.unwrap_or(Value::Null));
        }
        self.depth -= 1;
        Ok(items)
    }

    /// A Lua value as JSON: `nil` is absent and `json.null` is `null`; a table is an array
    /// when it is marked as a list or its keys are `1..n`, and an object otherwise. Every
    /// string counts against the cap.
    fn json(&mut self, value: &LuaValue, field: &str) -> Result<Option<Value>, OutputError> {
        Ok(Some(match value {
            LuaValue::Nil => return Ok(None),
            null if null.is_null() => Value::Null,
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
                if is_list(t, self.list) || t.raw_len() > 0 {
                    Value::Array(self.items(t, field)?)
                } else {
                    Value::Object(self.entries(t, field)?)
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
/// A key must be a record field, so a typo cannot silently drop data; a map field must be a
/// table (or `nil`, the empty map); every value goes through core's write rules. Every field
/// is optional (`kind` is `log` when left out).
pub(crate) fn from_table(
    table: &Table,
    output_bytes: usize,
    list: &Table,
) -> Result<Record, OutputError> {
    let mut record = Record::default();
    let mut reader = Reader {
        used: 0,
        cap: output_bytes,
        depth: 0,
        list,
    };
    for pair in table.pairs::<LuaValue, LuaValue>() {
        let (key, value) =
            pair.map_err(|e| OutputError(format!("cannot read the returned table: {e}")))?;
        let LuaValue::String(key) = key else {
            return refuse(format!("key {} is not a field name", type_name(&key)));
        };
        let key = key
            .to_str()
            .map_err(|_| OutputError("a key is not valid UTF-8".into()))?
            .to_owned();
        match FieldPath::top_level(&key) {
            None => return refuse(format!("`{key}` is not a record field")),
            Some(TopLevel::Map(map)) => {
                let entries = match &value {
                    LuaValue::Nil => continue,
                    LuaValue::Table(t) => reader.entries(t, &key)?,
                    other => {
                        return refuse(format!(
                            "`{key}` must be a table, not {}",
                            type_name(other)
                        ));
                    }
                };
                for (name, value) in entries {
                    write(&map.key(&name), &mut record, value)?;
                }
            }
            Some(TopLevel::Field(path)) => {
                // `json.null` on a field is left out, as `nil` is: a field is never `null`.
                let value = reader.json(&value, &key)?.filter(|value| !value.is_null());
                if let Some(value) = value {
                    write(&path, &mut record, value)?;
                }
            }
        }
    }
    Ok(record)
}

/// Write `value` through core's write rules. When the rules refuse it as given, the value
/// is tried once more in the form a Lua author means: an integral float as the integer
/// (`18 / 2` is a float in Lua 5.4), and for the record id, decimal text as the integer (the
/// form [`to_table`] hands over an id above 2^63 in). The refusal reported is core's.
fn write(path: &FieldPath, record: &mut Record, value: Value) -> Result<(), OutputError> {
    let meant = integral(&value).or_else(|| if path.is_id() { decimal(&value) } else { None });
    let Err(refused) = path.write(record, value) else {
        return Ok(());
    };
    match meant.map(|meant| path.write(record, meant)) {
        Some(Ok(())) => Ok(()),
        _ => refuse(refused.to_string()),
    }
}

/// The integer an integral float within `i64` stands for (a float that large is an integer
/// already, and the cast is exact).
fn integral(value: &Value) -> Option<Value> {
    // 2^63, the first float outside `i64`.
    const I64_END: f64 = 9_223_372_036_854_775_808.0;
    if !value.is_f64() {
        return None;
    }
    let f = value.as_f64()?;
    (f.fract() == 0.0 && (-I64_END..I64_END).contains(&f)).then(|| Value::from(f as i64))
}

/// The `u64` that decimal digits spell, the form an id above 2^63 crosses in.
fn decimal(value: &Value) -> Option<Value> {
    let text = value
        .as_str()
        .filter(|text| !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()))?;
    text.parse::<u64>().ok().map(Value::from)
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
