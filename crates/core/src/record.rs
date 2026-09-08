//! The one record type: OTLP-semantic but flat. `body` is opaque; sources
//! never interpret it, stages parse content out of it into `attributes`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Producer-supplied snowflake. Always present on a valid record; a record
/// without one is negatively acknowledged by the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RecordId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    #[default]
    Log,
    Metric,
    Span,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Record {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<RecordId>,
    #[serde(default)]
    pub kind: Kind,
    #[serde(default)]
    pub time_unix_nano: u64,
    #[serde(default)]
    pub observed_time_unix_nano: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity_number: Option<i32>,
    #[serde(default)]
    pub body: Value,
    #[serde(default)]
    pub attributes: Map<String, Value>,
    #[serde(default)]
    pub resource: Map<String, Value>,
    #[serde(default)]
    pub scope: Map<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
}

/// Where the tenant lives on every record.
pub const TENANT_KEY: &str = "tenant.id";

impl Record {
    /// A log record with an id and nothing else set. Test and producer helper.
    pub fn log(id: u64) -> Self {
        Record {
            id: Some(RecordId(id)),
            ..Default::default()
        }
    }

    pub fn tenant(&self) -> Option<&str> {
        self.resource.get(TENANT_KEY).and_then(Value::as_str)
    }

    /// Resolve a path into the record. The first segment names a top-level
    /// field; later segments index into maps (`attributes`, `resource`,
    /// `scope`) or into a structured `body`. Returns an owned `Value` for the
    /// scalar top-level fields and a borrowed one otherwise.
    pub fn get_path(&self, path: &[String]) -> Option<Value> {
        let (head, rest) = path.split_first()?;
        let root: Value = match head.as_str() {
            "id" => return rest.is_empty().then(|| self.id.map(|i| Value::from(i.0)))?,
            "kind" => serde_json::to_value(self.kind).ok()?,
            "time_unix_nano" => Value::from(self.time_unix_nano),
            "observed_time_unix_nano" => Value::from(self.observed_time_unix_nano),
            "severity_text" => self.severity_text.clone().map(Value::from)?,
            "severity_number" => self.severity_number.map(Value::from)?,
            "trace_id" => self.trace_id.clone().map(Value::from)?,
            "span_id" => self.span_id.clone().map(Value::from)?,
            "body" => return walk(&self.body, rest).cloned(),
            "attributes" => return walk_map(&self.attributes, rest).cloned(),
            "resource" => return walk_map(&self.resource, rest).cloned(),
            "scope" => return walk_map(&self.scope, rest).cloned(),
            _ => return None,
        };
        walk(&root, rest).cloned()
    }
}

fn walk_map<'a>(map: &'a Map<String, Value>, rest: &[String]) -> Option<&'a Value> {
    let (key, rest) = rest.split_first()?;
    walk(map.get(key)?, rest)
}

fn walk<'a>(mut value: &'a Value, rest: &[String]) -> Option<&'a Value> {
    for key in rest {
        value = match value {
            Value::Object(m) => m.get(key)?,
            Value::Array(a) => a.get(key.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(value)
}
