//! The record every stage operates on: whatever JSON the producer sent.
//!
//! A record is one [`serde_json::Value`]. There is no field list, no declared type and no
//! default: an object, an array, a string and a number all decode, nothing is dropped on the
//! way in and nothing is added on the way out. The sink writes the record as the last stage
//! left it (issue #79, ADR 0008).
//!
//! The pipeline's own view of the record — its id, tenant, ingestion time and delivery count
//! — is [`crate::meta::Meta`], which lives beside the record and never inside it (ADR 0005).
//! [`RecordId`] and [`Kind`] below are that view's types, resolved at intake from the
//! message's headers (ADR 0007); a payload field spelled `id` or `kind` is the producer's
//! data, which the pipeline never reads.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::closed_set::closed_set;

/// A record id: the one a message's transport gives, on the record's `Meta`. A message
/// without one is negatively acknowledged. The payload may hold a field spelled `id`; it is
/// data like any other and is never read for this (ADR 0007).
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
    /// Signal kind, as the message's `Fusion-Record-Kind` header gives it. Only `log` is
    /// processed, decided by the engine at intake from the arrival; a payload field spelled
    /// `kind` is data the pipeline never reads. Its JSON form is its name, [`Kind::as_str`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[non_exhaustive]
    pub enum Kind {
        /// A log record.
        Log = "log",
        /// A metric data point. A message the transport says is one is rejected at intake.
        Metric = "metric",
        /// A span. A message the transport says is one is rejected at intake.
        Span = "span",
    }
}

/// A message with no `Fusion-Record-Kind` header is a `log`.
impl Default for Kind {
    fn default() -> Self {
        Self::Log
    }
}

/// One record: any JSON value, kept as it was sent.
///
/// The pipeline never reads a field of its own out of it and never writes one into it. A
/// stage reaches inside through a [`crate::path::FieldPath`], which is the only thing that
/// knows the shape of a particular producer's data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Record(Value);

/// A record nothing has been put in yet. Only tests and a source that skips decoding (the
/// NATS source, for a message the arrival already rejected) build one.
impl Default for Record {
    fn default() -> Self {
        Self(Value::Null)
    }
}

impl Record {
    /// A record holding `value`.
    #[must_use]
    pub const fn new(value: Value) -> Self {
        Self(value)
    }

    /// The record as a JSON value.
    #[must_use]
    pub const fn value(&self) -> &Value {
        &self.0
    }

    /// The record as a JSON value, to change in place. A stage reaches inside through a
    /// [`crate::path::WritePath`]; this is for a caller holding the whole record, such as a
    /// source decoding one or a test building one.
    pub const fn value_mut(&mut self) -> &mut Value {
        &mut self.0
    }

    /// The record's JSON value, consuming the record.
    #[must_use]
    pub fn into_value(self) -> Value {
        self.0
    }

    /// Parse a record from its JSON wire form. Any JSON value is a record.
    ///
    /// # Errors
    ///
    /// Returns the serde error when `json` is not JSON at all.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Serialize to the JSON wire form.
    ///
    /// # Errors
    ///
    /// Returns the serde error if a value cannot be serialized. A record decoded from JSON,
    /// or built by writing through a path, always can.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.0)
    }
}
