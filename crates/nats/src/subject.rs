//! Subject helpers: tenant derivation and wildcard matching.
//!
//! Records arrive on `logs.{tenant}.>`; the subject's tenant goes on the message's arrival,
//! never into the record. [`captures`] answers whether a stream's subject filter covers a
//! concrete subject, so a sink can fail at load instead of on its first publish.

use async_nats::jetstream::stream::Stream;

/// The tenant token of a `{prefix}.{tenant}.>` subject: the second token, when the first is
/// `prefix`, the second is not empty and at least one token follows it. Any other subject,
/// such as `processed.logs` or `processed.logs.v2`, names no tenant, so a pipeline consuming
/// another's output takes the tenant from the `Fusion-Tenant` header instead.
#[must_use]
pub fn tenant_from_subject<'s>(subject: &'s str, prefix: &str) -> Option<&'s str> {
    let mut tokens = subject.split('.');
    if tokens.next() != Some(prefix) {
        return None;
    }
    let tenant = tokens.next().filter(|tenant| !tenant.is_empty())?;
    tokens.next().is_some().then_some(tenant)
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

/// Whether any of `stream`'s configured subjects [`captures`] `subject`.
#[must_use]
pub fn stream_captures(stream: &Stream, subject: &str) -> bool {
    stream
        .cached_info()
        .config
        .subjects
        .iter()
        .any(|pattern| captures(pattern, subject))
}
