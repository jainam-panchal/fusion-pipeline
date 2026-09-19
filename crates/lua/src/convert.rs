//! The record as a plain Lua value, and the way back: Lua values converted to JSON with
//! every string under the size cap. A record is any JSON (issue #79), so an object crosses
//! as a table, a list as a marked table and a string, number or bool as itself. No field
//! name and no field type is spelled here: a script may return whatever shape it likes.
//!
//! A value crosses both ways unchanged when the script leaves it alone. Lua has no empty
//! list and no `nil` inside a table, so a JSON list becomes a table marked with the VM's
//! list metatable, and a JSON `null` inside a list or a map becomes `json.null`; the way
//! back reads a marked table as a list, whatever its length, and `json.null` as `null`. A
//! record field is never `null` (the decoder leaves it out), and a field set to
//! `json.null` is left out, as `nil` is.

use fusion_core::meta::{Meta, MetaField, MetaValue};
use fusion_core::record::{Record, RecordId};
use mlua::{Function, Integer, Table, Value as LuaValue};
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

/// The VM's mark for a table that is a JSON list: one shared metatable with no fields, whose
/// `__metatable` hides it from `getmetatable` and refuses `setmetatable`, so a script can
/// neither unmark a list nor change the table every list shares.
#[derive(Clone)]
pub(crate) struct ListMark(Table);

impl ListMark {
    pub(crate) fn new(lua: &mlua::Lua) -> mlua::Result<Self> {
        let metatable = lua.create_table()?;
        metatable.raw_set("__metatable", "list")?;
        Ok(Self(metatable))
    }

    /// Mark `table` as a list.
    pub(crate) fn mark(&self, table: &Table) -> mlua::Result<()> {
        table.set_metatable(Some(self.0.clone()))
    }

    /// Whether `table` is marked as a list.
    pub(crate) fn is_list(&self, table: &Table) -> bool {
        table.metatable().is_some_and(|mt| mt == self.0)
    }

    /// Whether `table` may be marked: it has no metatable, or it is a list already. Marking
    /// any other table would strip the metatable it has, a record table's or `meta`'s.
    pub(crate) fn accepts(&self, table: &Table) -> bool {
        table.metatable().is_none() || self.is_list(table)
    }
}

/// What the script gets: the record as a Lua value. An object is a table, a list is a
/// marked table, and a scalar record is the Lua scalar. The caller marks a returned table as
/// a record table.
pub(crate) fn to_lua(lua: &mlua::Lua, record: &Record, list: &ListMark) -> mlua::Result<LuaValue> {
    json_to_lua(lua, record.value(), list)
}

/// `meta[field]` as a script reads it: the record id in the form the record table gives an
/// `id`, the tenant as a string, the ingestion time and delivery count as integers.
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

fn map_to_table(lua: &mlua::Lua, map: &Map<String, Value>, list: &ListMark) -> mlua::Result<Table> {
    let t = lua.create_table_with_capacity(0, map.len())?;
    for (k, v) in map {
        t.raw_set(k.as_str(), json_to_lua(lua, v, list)?)?;
    }
    Ok(t)
}

fn json_to_lua(lua: &mlua::Lua, v: &Value, list: &ListMark) -> mlua::Result<LuaValue> {
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
            list.mark(&t)?;
            LuaValue::Table(t)
        }
        Value::Object(map) => LuaValue::Table(map_to_table(lua, map, list)?),
    })
}

/// The length of a table read as a list, whose only keys must be its positions `1..n`: any
/// other key would be lost, and a `nil` below the last position is a hole Lua cannot keep.
pub(crate) fn list_len(table: &Table, field: &str) -> Result<usize, OutputError> {
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
    /// The mark of a table that is a list.
    list: &'l ListMark,
    /// The record marks, so a record table's shape is read rather than guessed.
    records: &'l RecordMark,
}

impl<'l> Reader<'l> {
    fn new(cap: usize, list: &'l ListMark, records: &'l RecordMark) -> Self {
        Self {
            used: 0,
            cap,
            depth: 0,
            list,
            records,
        }
    }
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
    /// taken as the JSON it is, however deep, so a record that arrived nested leaves an
    /// untouched script the way it came in.
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
            let value = self.json(&value, &at)?;
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

    /// A table's entries as a JSON array, its keys checked by [`list_len`].
    fn items(&mut self, table: &Table, field: &str) -> Result<Vec<Value>, OutputError> {
        self.enter(field)?;
        let len = list_len(table, field)?;
        let mut items = Vec::with_capacity(len);
        for i in 1..=len {
            let item: LuaValue = table
                .raw_get(i)
                .map_err(|e| OutputError(format!("cannot read `{field}`: {e}")))?;
            let at = format!("{field}[{i}]");
            items.push(self.json(&item, &at)?);
        }
        self.depth -= 1;
        Ok(items)
    }

    /// A Lua value as JSON: `nil` and `json.null` are `null` (a table's reader never meets
    /// `nil`, and a field treats both as absent); a table is an array when it is marked as a
    /// list or its keys are `1..n`, and an object otherwise. Every string counts against the
    /// cap.
    fn json(&mut self, value: &LuaValue, field: &str) -> Result<Value, OutputError> {
        Ok(match value {
            absent if absent.is_nil() || absent.is_null() => Value::Null,
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
                // A record table carries its shape, so a record nested in a returned value
                // (`return {a = record}`) keeps it too; any other table is read by what it
                // holds.
                let shape = self
                    .records
                    .shape(t)
                    .unwrap_or_else(|| Shape::of_table(t, self.list));
                match shape {
                    Shape::List => Value::Array(self.items(t, field)?),
                    Shape::Object => Value::Object(self.entries(t, field)?),
                }
            }
            other => {
                return refuse(format!(
                    "`{field}` is {}, which has no JSON form",
                    type_name(other)
                ));
            }
        })
    }
}

/// The value the script returned as a record, its strings together under `output_bytes`.
/// Any JSON is a record, so nothing here judges a key or a type; what is refused is what has
/// no JSON form at all (a function, a coroutine, userdata, a non-finite number, a string
/// that is not UTF-8), the output cap, and a table nested past [`MAX_DEPTH`].
pub(crate) fn from_lua(
    value: &LuaValue,
    output_bytes: usize,
    list: &ListMark,
    records: &RecordMark,
) -> Result<Record, OutputError> {
    let mut reader = Reader::new(output_bytes, list, records);
    reader.json(value, "the returned record").map(Record::new)
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

/// Which JSON shape a record table stands for. A table alone cannot say: an empty one is both
/// an empty object and an empty list, so the shape rides on the metatable instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Shape {
    /// A JSON object.
    Object,
    /// A JSON list.
    List,
}

impl Shape {
    /// The shape a JSON `value` has, for one already known to be an object or a list.
    pub(crate) fn of(value: &Value) -> Self {
        if value.is_array() {
            Self::List
        } else {
            Self::Object
        }
    }

    /// The shape a table that is not a record table stands for: marked as a list, or holding
    /// positions, makes a list; anything else an object.
    pub(crate) fn of_table(table: &Table, list: &ListMark) -> Self {
        if list.is_list(table) || table.raw_len() > 0 {
            Self::List
        } else {
            Self::Object
        }
    }
}

/// `record:copy()`: a deep copy of the table, shared structure and cycles kept as they are,
/// every list still marked as one, with the record metatable so the copy can be copied too.
/// Written in Lua so it runs under the instruction budget and the memory cap like the
/// script's own code. The chunk takes the metatable and a function that marks the copy of a
/// list, and returns the method.
const COPY: &str = r#"
local mark, finish = ...
local next, type = next, type
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
  return finish(record, deep(record, {}))
end
"#;

/// The two metatables a record table can carry: one for a record that is an object, one for
/// a record that is a list. Both answer `"record"` to `getmetatable`, both refuse
/// `setmetatable`, and both share the `copy` method, so a script cannot tell them apart or
/// change `copy` for the records after it. Which one a table carries is how the stage reads a
/// returned record back in the shape it handed over, so an empty list does not come back an
/// empty object.
#[derive(Clone)]
pub(crate) struct RecordMark {
    object: Table,
    list: Table,
}

impl RecordMark {
    pub(crate) fn new(lua: &mlua::Lua, lists: &ListMark) -> mlua::Result<Self> {
        let object = lua.create_table()?;
        let list = lua.create_table()?;
        // A script cannot read the list mark, so the copy asks Rust which tables carry it.
        let marker = lists.clone();
        let mark = lua.create_function(move |_, (from, to): (Table, Table)| {
            if marker.is_list(&from) {
                marker.mark(&to)?;
            }
            Ok(())
        })?;
        // The copy of a record table is a record table of the same flavour.
        let this = Self {
            object: object.clone(),
            list: list.clone(),
        };
        let for_copy = this.clone();
        let finish = lua.create_function(move |_, (from, to): (Table, Table)| {
            if let Some(shape) = for_copy.shape(&from) {
                for_copy.mark(&to, shape)?;
            }
            Ok(to)
        })?;
        let copy: Function = lua
            .load(COPY)
            .set_name("=record:copy")
            .call((mark, finish))?;
        let methods = lua.create_table()?;
        methods.raw_set("copy", copy)?;
        for metatable in [&object, &list] {
            metatable.raw_set("__index", methods.clone())?;
            metatable.raw_set("__metatable", "record")?;
        }
        Ok(this)
    }

    /// Mark `table` as the record, in the flavour its JSON shape calls for.
    pub(crate) fn mark(&self, table: &Table, shape: Shape) -> mlua::Result<()> {
        let metatable = match shape {
            Shape::List => &self.list,
            Shape::Object => &self.object,
        };
        table.set_metatable(Some(metatable.clone()))
    }

    /// The shape `table` carries, or `None` when it is not a record table.
    pub(crate) fn shape(&self, table: &Table) -> Option<Shape> {
        let metatable = table.metatable()?;
        if metatable == self.list {
            Some(Shape::List)
        } else if metatable == self.object {
            Some(Shape::Object)
        } else {
            None
        }
    }
}
