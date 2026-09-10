//! Tenant derivation from the source subject.
//!
//! Records arrive on `logs.{tenant}.>`; the tenant lives at `resource["tenant.id"]` and is
//! stamped from the subject when the producer left it out.

use fusion_core::record::Record;
use serde_json::Value;

/// Resource attribute that carries the tenant.
pub const TENANT_KEY: &str = "tenant.id";

/// The tenant token of a `logs.{tenant}.>` subject, if the subject has a non-empty second
/// token.
#[must_use]
pub fn tenant_from_subject(subject: &str) -> Option<&str> {
    subject.split('.').nth(1).filter(|tenant| !tenant.is_empty())
}

/// Set `resource["tenant.id"]` to `tenant` unless the record already carries one.
pub fn stamp_tenant(record: &mut Record, tenant: &str) {
    record
        .resource
        .entry(TENANT_KEY)
        .or_insert_with(|| Value::String(tenant.to_owned()));
}
