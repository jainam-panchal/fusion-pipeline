//! The one record type: OTLP-semantic but flat. `body` is opaque; sources
//! never interpret it, stages parse content out of it into `attributes`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::borrow::Cow;

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
    /// `scope`) or into a structured `body`. Borrows wherever the value
    /// already exists as a `Value`; the scalar top-level fields are boxed
    /// into an owned `Value` on the way out.
    pub fn get_path(&self, path: &[String]) -> Option<Cow<'_, Value>> {
        let (head, rest) = path.split_first()?;
        let borrowed = match head.as_str() {
            "body" => walk(&self.body, rest),
            "attributes" => walk_map(&self.attributes, rest),
            "resource" => walk_map(&self.resource, rest),
            "scope" => walk_map(&self.scope, rest),
            _ => {
                if !rest.is_empty() {
                    return None;
                }
                let owned = match head.as_str() {
                    "id" => Value::from(self.id?.0),
                    "kind" => serde_json::to_value(self.kind).ok()?,
                    "time_unix_nano" => Value::from(self.time_unix_nano),
                    "observed_time_unix_nano" => Value::from(self.observed_time_unix_nano),
                    "severity_text" => Value::from(self.severity_text.as_deref()?),
                    "severity_number" => Value::from(self.severity_number?),
                    "trace_id" => Value::from(self.trace_id.as_deref()?),
                    "span_id" => Value::from(self.span_id.as_deref()?),
                    _ => return None,
                };
                return Some(Cow::Owned(owned));
            }
        };
        borrowed.map(Cow::Borrowed)
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
