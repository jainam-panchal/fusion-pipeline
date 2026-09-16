//! The flat, OTLP-semantic record every stage operates on.
//!
//! One JSON object per message on the wire, using OTLP field names. `body` is opaque to
//! sources; stages parse content out of it into `attributes`.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

use crate::closed_set::closed_set;

/// Producer-supplied snowflake id. Present on every record the engine walks, as it arrived
/// (a record that arrives without one is negatively acknowledged); the pipeline decides with
/// the copy on the record's `Meta`, and a stage may change or drop the field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct RecordId(pub u64);

impl fmt::Display for RecordId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl<'de> Deserialize<'de> for RecordId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Number(u64),
            Text(String),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Number(n) => Ok(Self(n)),
            Raw::Text(s) => s.parse().map(Self).map_err(serde::de::Error::custom),
        }
    }
}

closed_set! {
    serde;
    /// Signal kind. Only `log` is processed, decided by the engine at intake. Its JSON form is
    /// its name, [`Kind::as_str`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[non_exhaustive]
    pub enum Kind {
        /// A log record.
        Log = "log",
        /// A metric data point. Rejected by the engine at intake.
        Metric = "metric",
        /// A span. Rejected by the engine at intake.
        Span = "span",
    }
}

/// A record with no `kind` is a `log`.
impl Default for Kind {
    fn default() -> Self {
        Self::Log
    }
}

/// A flat OTLP-semantic record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[non_exhaustive]
pub struct Record {
    /// Producer-supplied id; `None` for a record that arrived without one or whose id a
    /// stage removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<RecordId>,
    /// Signal kind, `log` by default.
    #[serde(default)]
    pub kind: Kind,
    /// Event time in nanoseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_unix_nano: Option<u64>,
    /// Time the record was observed by the collector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_time_unix_nano: Option<u64>,
    /// Severity as text, e.g. `ERROR`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity_text: Option<String>,
    /// OTLP severity number, 1..=24.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity_number: Option<i32>,
    /// Opaque body. Usually a string; never interpreted by sources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
    /// Record attributes.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub attributes: Map<String, Value>,
    /// Resource attributes. The producer's tenant lives at `resource.tenant.id`.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub resource: Map<String, Value>,
    /// Instrumentation scope attributes.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub scope: Map<String, Value>,
    /// Trace id, hex.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    /// Span id, hex.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
}

impl Record {
    /// Parse a record from its JSON wire form.
    ///
    /// # Errors
    ///
    /// Returns the serde error when `json` is not a record-shaped object.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Serialize to the JSON wire form.
    ///
    /// # Errors
    ///
    /// Returns the serde error if a value cannot be serialized (it cannot for this shape).
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// The `resource` key that carries the producer's tenant.
    pub const TENANT_KEY: &'static str = "tenant.id";

    /// The producer's tenant, `resource.tenant.id` when it is a string. The engine reads it
    /// at intake only for a record whose arrival names no tenant.
    #[must_use]
    pub fn tenant(&self) -> Option<&str> {
        self.resource.get(Self::TENANT_KEY).and_then(Value::as_str)
    }
}
