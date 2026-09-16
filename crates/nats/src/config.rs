//! Source and sink parameters, and where the server URL comes from.
//!
//! Endpoints come from the environment first: `NATS_URL`, when set and non-empty, overrides
//! the `url` in the YAML for both the source and every sink. Without either, the default
//! local server is used.

use serde::Deserialize;

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
}

fn default_tenant_prefix() -> String {
    DEFAULT_TENANT_PREFIX.to_owned()
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
}
