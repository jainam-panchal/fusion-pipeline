//! Subject helpers: tenant derivation and wildcard matching.
//!
//! Records arrive on `logs.{tenant}.>`; the tenant lives at `resource["tenant.id"]` and is
//! stamped from the subject when the producer left it out. [`captures`] answers whether a
//! stream's subject filter covers a concrete subject, so a sink can fail at load instead of
//! on its first publish.

use fusion_core::record::Record;
use serde_json::Value;

/// Resource attribute that carries the tenant.
pub const TENANT_KEY: &str = "tenant.id";

/// The tenant token of a `logs.{tenant}.>` subject, if the subject has a non-empty second
/// token.
#[must_use]
pub fn tenant_from_subject(subject: &str) -> Option<&str> {
    subject
        .split('.')
        .nth(1)
        .filter(|tenant| !tenant.is_empty())
}

/// Set `resource["tenant.id"]` to `tenant` unless the record already carries one.
pub fn stamp_tenant(record: &mut Record, tenant: &str) {
    record
        .resource
        .entry(TENANT_KEY)
        .or_insert_with(|| Value::String(tenant.to_owned()));
}

/// Whether the NATS subject `pattern` (`*` matches one token, a trailing `>` matches one or
/// more) matches the concrete `subject`.
#[must_use]
pub fn captures(pattern: &str, subject: &str) -> bool {
    let mut tokens = subject.split('.');
    let mut pattern = pattern.split('.').peekable();
    while let Some(want) = pattern.next() {
        if want == ">" && pattern.peek().is_none() {
            return tokens.next().is_some();
        }
        match tokens.next() {
            Some(have) if want == "*" || want == have => {}
            _ => return false,
        }
    }
    tokens.next().is_none()
}
