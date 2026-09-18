//! Source and sink parameters, and where the server URL comes from.
//!
//! Endpoints come from the environment first: `NATS_URL`, when set and non-empty, overrides
//! the `url` in the YAML for both the source and every sink. Without either, the default
//! local server is used.

use fusion_core::record::Record;
use serde::Deserialize;
use serde_json::Value;

/// The server URL when neither the environment nor the YAML gives one.
pub const DEFAULT_URL: &str = "nats://127.0.0.1:4222";

/// Environment variable that overrides every configured `url`.
pub const URL_ENV: &str = "NATS_URL";

/// Pick the server URL: a non-empty `env` value wins, then `yaml`, then [`DEFAULT_URL`].
#[must_use]
pub fn resolve_url(yaml: Option<&str>, env: Option<&str>) -> String {
    env.filter(|url| !url.is_empty())
        .or(yaml)
        .unwrap_or(DEFAULT_URL)
        .to_owned()
}

/// [`resolve_url`] against the process environment.
#[must_use]
pub fn url_from_env(yaml: Option<&str>) -> String {
    let env = std::env::var(URL_ENV).ok();
    resolve_url(yaml, env.as_deref())
}

/// The first subject token of `{prefix}.{tenant}.>` when the config names none.
pub const DEFAULT_TENANT_PREFIX: &str = "logs";

/// `source` block parameters for `type: nats`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceParams {
    /// Server URL; overridden by `NATS_URL`.
    #[serde(default)]
    pub url: Option<String>,
    /// The stream the consumer belongs to. Must already exist.
    pub stream: String,
    /// The durable pull consumer to read from. Must already exist with explicit ack.
    pub consumer: String,
    /// The first token of the subjects that name a tenant, `{tenant_prefix}.{tenant}.>`;
    /// `logs` by default. A message on any other subject names no tenant, and its tenant
    /// comes from the `Fusion-Tenant` header, as for a pipeline reading another's output.
    #[serde(default = "default_tenant_prefix")]
    pub tenant_prefix: String,
    /// The first token of the dead-letter subjects, `{dlq_prefix}.{tenant}`; `dlq` by
    /// default. A stream capturing every such subject must already exist.
    #[serde(default = "default_dlq_prefix")]
    pub dlq_prefix: String,
    /// How a payload becomes a record; `json` by default.
    #[serde(default)]
    pub codec: Codec,
}

fn default_tenant_prefix() -> String {
    DEFAULT_TENANT_PREFIX.to_owned()
}

/// The first token of the dead-letter subjects when the config names none.
pub const DEFAULT_DLQ_PREFIX: &str = "dlq";

fn default_dlq_prefix() -> String {
    DEFAULT_DLQ_PREFIX.to_owned()
}

/// `sink.nats` node parameters.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinkParams {
    /// Server URL; overridden by `NATS_URL`.
    #[serde(default)]
    pub url: Option<String>,
    /// The stream expected to capture `subject`. Checked at load so a missing stream fails
    /// fast rather than on the first record.
    pub stream: String,
    /// Subject every record is published to.
    pub subject: String,
    /// How a record becomes a payload; `json` by default.
    #[serde(default)]
    pub encoding: Encoding,
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
    /// The message the source reports as `undecodable`: a payload that is not JSON under
    /// [`Codec::Json`], or bytes that are not UTF-8 under [`Codec::Text`].
    pub fn decode(self, payload: &[u8]) -> Result<Record, String> {
        match self {
            Self::Json => serde_json::from_slice::<Value>(payload)
                .map(Record::new)
                .map_err(|e| e.to_string()),
            Self::Text => std::str::from_utf8(payload)
                .map(|text| Record::new(Value::String(text.to_owned())))
                .map_err(|e| e.to_string()),
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
    /// or built through core's write rules, always has one.
    pub fn encode(self, record: &Record) -> Result<Vec<u8>, serde_json::Error> {
        match (self, record.value()) {
            (Self::Text, Value::String(text)) => Ok(text.as_bytes().to_vec()),
            _ => record.to_json().map(String::into_bytes),
        }
    }
}
