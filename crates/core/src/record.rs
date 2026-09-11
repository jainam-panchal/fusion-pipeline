//! The flat, OTLP-semantic record every stage operates on.
//!
//! One JSON object per message on the wire, using OTLP field names. `body` is opaque to
//! sources; stages parse content out of it into `attributes`.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

/// Producer-supplied snowflake id. Always present on a record the engine processes; a
/// record that arrives without one is negatively acknowledged.
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

/// Signal kind. Only `log` is processed by the POC engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum Kind {
    /// A log record.
    #[default]
    Log,
    /// A metric data point. Rejected by the engine.
    Metric,
    /// A span. Rejected by the engine.
    Span,
}

impl Kind {
    /// The wire name of this kind.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Log => "log",
            Self::Metric => "metric",
            Self::Span => "span",
        }
    }
}

/// A flat OTLP-semantic record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[non_exhaustive]
pub struct Record {
    /// Producer-supplied id; `None` only for records that arrived without one.
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
    /// Resource attributes. The tenant lives at `resource.tenant.id`.
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

    /// The tenant, read from `resource.tenant.id`.
    #[must_use]
    pub fn tenant(&self) -> Option<&str> {
        self.resource.get("tenant.id").and_then(Value::as_str)
    }
}
