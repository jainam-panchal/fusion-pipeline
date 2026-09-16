//! Subject helpers: tenant derivation, dead-letter subjects and wildcard matching.
//!
//! Records arrive on `logs.{tenant}.>`; the subject's tenant goes on the message's arrival,
//! never into the record. A message the source gives up on goes to `{prefix}.{tenant}`
//! ([`dead_letter`]). [`captures`] answers whether a stream's subject filter covers a
//! concrete subject, so a sink can fail at load instead of on its first publish, and
//! [`covers_every_tenant`] whether it covers every dead-letter subject.

use std::fmt::Write as _;

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

/// The dead-letter subject of `tenant` under `prefix`: `{prefix}.{tenant}`, with the tenant
/// as one subject token. A tenant from a `Fusion-Tenant` header can hold what a token
/// cannot (`.`, `*`, `>`, whitespace), so every byte outside printable ASCII, those three
/// and `%` itself are written `%XX`; escaping `%` keeps two tenants from sharing a subject.
#[must_use]
pub fn dead_letter(prefix: &str, tenant: &str) -> String {
    let mut subject = String::with_capacity(prefix.len() + 1 + tenant.len());
    subject.push_str(prefix);
    subject.push('.');
    for byte in tenant.bytes() {
        if byte.is_ascii_graphic() && !matches!(byte, b'.' | b'*' | b'>' | b'%') {
            subject.push(char::from(byte));
        } else {
            // Writing to a `String` cannot fail.
            let _ = write!(subject, "%{byte:02X}");
        }
    }
    subject
}

/// Whether the subject `pattern` captures `{prefix}.{tenant}` for every tenant token. A
/// literal `*` token is matched by exactly the patterns whose token there is a wildcard or
/// `*` itself, so asking [`captures`] about `{prefix}.*` answers for every tenant.
#[must_use]
pub fn covers_every_tenant(pattern: &str, prefix: &str) -> bool {
    captures(pattern, &format!("{prefix}.*"))
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

/// Whether one of `stream`'s configured subjects [`covers_every_tenant`] under `prefix`.
#[must_use]
pub fn stream_covers_every_tenant(stream: &Stream, prefix: &str) -> bool {
    stream
        .cached_info()
        .config
        .subjects
        .iter()
        .any(|pattern| covers_every_tenant(pattern, prefix))
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
