//! How a payload becomes a record, and a record a payload.
//!
//! A record is any JSON (issue #79, ADR 0008), and not every producer sends JSON, so the
//! source and the sink each name how to read and write the bytes. Both are pure functions
//! over bytes, beside `headers` and `subject`, and are tested directly.

use fusion_core::record::Record;
use serde::Deserialize;
use serde_json::Value;

/// Why a payload is not a record: the two ways a message can be `undecodable` (ADR 0008).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// Under [`Codec::Json`], the payload is not JSON.
    #[error("payload is not JSON: {0}")]
    NotJson(#[source] serde_json::Error),
    /// Under [`Codec::Text`], the bytes are not UTF-8.
    #[error("payload is not UTF-8: {0}")]
    NotUtf8(#[source] std::str::Utf8Error),
}

/// How the source reads a message's payload into a record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Codec {
    /// The payload is JSON, and the record is the value it decodes to. A payload that is
    /// not JSON is `undecodable`.
    #[default]
    Json,
    /// The payload is the record: one string holding the bytes as they arrived. Bytes that
    /// are not UTF-8 are `undecodable`.
    Text,
}

impl Codec {
    /// The record `payload` holds under this codec.
    ///
    /// # Errors
    ///
    /// A [`DecodeError`], which the source reports as `undecodable`: a payload that is not
    /// JSON under [`Codec::Json`], or bytes that are not UTF-8 under [`Codec::Text`].
    pub fn decode(self, payload: &[u8]) -> Result<Record, DecodeError> {
        match self {
            Self::Json => serde_json::from_slice::<Value>(payload)
                .map(Record::new)
                .map_err(DecodeError::NotJson),
            Self::Text => std::str::from_utf8(payload)
                .map(|text| Record::new(Value::String(text.to_owned())))
                .map_err(DecodeError::NotUtf8),
        }
    }
}

/// How the sink writes a record onto the wire.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Encoding {
    /// The record's JSON, as the last stage left it.
    #[default]
    Json,
    /// A record that is a string, as its bytes. Any other record is written as its JSON, so
    /// a stage that turned a line into an object still delivers rather than naks.
    Text,
}

impl Encoding {
    /// `record` as the bytes this encoding writes.
    ///
    /// # Errors
    ///
    /// The serde error when the record has no JSON form. A record decoded from a payload,
    /// or built by writing through a path, always has one.
    pub fn encode(self, record: &Record) -> Result<Vec<u8>, serde_json::Error> {
        match (self, record.value()) {
            (Self::Text, Value::String(text)) => Ok(text.as_bytes().to_vec()),
            _ => record.to_json().map(String::into_bytes),
        }
    }
}
