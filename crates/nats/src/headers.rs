//! Pipeline headers: a record's `Meta` on the wire, beside the record and never in it
//! (ADR 0005).
//!
//! The sink writes three headers on every message it publishes:
//!
//! | Header | Value |
//! |---|---|
//! | `Fusion-Tenant` | the `Meta` tenant |
//! | `Fusion-Ingestion-Time` | the ingestion time, nanoseconds since the Unix epoch, decimal |
//! | `Fusion-Ingestion-Time-Kind` | `reported` or `clock` |
//!
//! The source reads them back into the message's [`Arrival`] with [`arrival()`], so a pipeline
//! consuming another's output keeps the first pipeline's tenant and ingestion time. The
//! subject's tenant wins over the header, because NATS permissions back the subject and any
//! producer can set a header; the header's time wins over the JetStream publish time, so the
//! first pipeline's time survives every hop. A header that does not parse is ignored,
//! reported and counted once, never a reason to nak: the record is still valid.

use async_nats::{HeaderMap, HeaderValue};
use fusion_core::meta::{Arrival, IngestionTime, Meta, TimeKind, is_valid_tenant};

use crate::subject::tenant_from_subject;

/// The `Meta` tenant.
pub const TENANT: &str = "Fusion-Tenant";
/// The `Meta` ingestion time in nanoseconds since the Unix epoch, decimal.
pub const INGESTION_TIME: &str = "Fusion-Ingestion-Time";
/// Whether a transport reported the ingestion time (`reported`) or a worker clock read it
/// (`clock`).
pub const INGESTION_TIME_KIND: &str = "Fusion-Ingestion-Time-Kind";

/// The pipeline headers for a record with `meta`. A `Meta` tenant always passes
/// [`is_valid_tenant`], so it is always a valid header value; the check is kept so a value
/// that could not be one is left out rather than panicking inside the client.
#[must_use]
pub fn for_meta(meta: &Meta) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        (TENANT, meta.tenant.to_string()),
        (INGESTION_TIME, meta.ingestion_time.unix_nanos().to_string()),
        (INGESTION_TIME_KIND, meta.ingestion_time.kind().to_string()),
    ] {
        if let Ok(value) = value.parse::<HeaderValue>() {
            headers.insert(name, value);
        }
    }
    headers
}

/// A pipeline header the source ignored because it did not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum InvalidHeader {
    /// `Fusion-Tenant` is empty or holds a control character.
    #[error("`{TENANT}` is empty or holds a control character")]
    Tenant,
    /// A pipeline header is present more than once.
    #[error("`{0}` is present more than once")]
    Repeated(&'static str),
    /// `Fusion-Ingestion-Time` is not a decimal `u64`.
    #[error("`{INGESTION_TIME}` is `{0}`, not nanoseconds as a decimal integer")]
    Time(String),
    /// `Fusion-Ingestion-Time-Kind` is neither `reported` nor `clock`.
    #[error("`{INGESTION_TIME_KIND}` is `{0}`, not `reported` or `clock`")]
    Kind(String),
    /// One of the two time headers is present without the other.
    #[error("`{INGESTION_TIME}` and `{INGESTION_TIME_KIND}` come together; only `{0}` is present")]
    Unpaired(&'static str),
}

/// What JetStream handed the source for one message, apart from its payload.
#[derive(Debug, Clone, Copy)]
pub struct Received<'m> {
    /// The subject it was published on.
    pub subject: &'m str,
    /// Its headers, if any.
    pub headers: Option<&'m HeaderMap>,
    /// The JetStream publish time, nanoseconds since the Unix epoch.
    pub published: Option<u64>,
    /// The JetStream delivery count.
    pub delivered: u64,
}

/// The arrival of `received` by a source whose tenant subjects are
/// `{tenant_prefix}.{tenant}.>`, and the pipeline headers ignored because they did not
/// parse:
///
/// - tenant: the subject's tenant token, else `Fusion-Tenant`, each only when it passes
///   [`is_valid_tenant`]; the header is not read when the subject names a valid tenant;
/// - ingestion time: `Fusion-Ingestion-Time` with its kind, else the publish time as
///   reported;
/// - delivery count: the delivery count.
///
/// Each header is ignored and reported at most once: a time header given twice is not
/// reported again as unpaired, and its partner is reported only if its own value does not
/// parse.
#[must_use]
pub fn arrival(tenant_prefix: &str, received: Received<'_>) -> (Arrival, Vec<InvalidHeader>) {
    let Received {
        subject,
        headers,
        published,
        delivered,
    } = received;
    let mut invalid = Vec::new();
    let tenant = tenant_from_subject(subject, tenant_prefix)
        .filter(|tenant| is_valid_tenant(tenant))
        .or_else(|| header_tenant(headers, &mut invalid))
        .map(str::to_owned);
    let header_time = ingestion_time(
        header(headers, INGESTION_TIME),
        header(headers, INGESTION_TIME_KIND),
        &mut invalid,
    );
    let arrival = Arrival {
        tenant,
        ingestion_time: header_time.or(published.map(IngestionTime::Reported)),
        delivery_count: delivered,
    };
    (arrival, invalid)
}

/// The `Fusion-Tenant` header when it is given once and passes [`is_valid_tenant`];
/// otherwise `None`, with the reason pushed onto `invalid` when the header was present.
fn header_tenant<'h>(
    headers: Option<&'h HeaderMap>,
    invalid: &mut Vec<InvalidHeader>,
) -> Option<&'h str> {
    match header(headers, TENANT) {
        Ok(Some(tenant)) if is_valid_tenant(tenant) => Some(tenant),
        Ok(None) => None,
        Ok(Some(_)) => {
            invalid.push(InvalidHeader::Tenant);
            None
        }
        Err(problem) => {
            invalid.push(problem);
            None
        }
    }
}

/// The ingestion time the two time headers give, `None` when either is missing or ignored.
/// Each present value is parsed on its own, so a header is pushed onto `invalid` once, for
/// its own problem; a well-formed header is reported as unpaired only when its partner is
/// absent.
fn ingestion_time(
    time: Result<Option<&str>, InvalidHeader>,
    kind: Result<Option<&str>, InvalidHeader>,
    invalid: &mut Vec<InvalidHeader>,
) -> Option<IngestionTime> {
    let time = time.and_then(|time| {
        time.map(|text| parse_nanos(text).ok_or_else(|| InvalidHeader::Time(text.to_owned())))
            .transpose()
    });
    let kind = kind.and_then(|kind| {
        kind.map(|text| TimeKind::parse(text).ok_or_else(|| InvalidHeader::Kind(text.to_owned())))
            .transpose()
    });
    match (time, kind) {
        (Ok(Some(nanos)), Ok(Some(kind))) => Some(IngestionTime::new(kind, nanos)),
        (Ok(None), Ok(None)) => None,
        (Ok(Some(_)), Ok(None)) => {
            invalid.push(InvalidHeader::Unpaired(INGESTION_TIME));
            None
        }
        (Ok(None), Ok(Some(_))) => {
            invalid.push(InvalidHeader::Unpaired(INGESTION_TIME_KIND));
            None
        }
        (time, kind) => {
            invalid.extend(time.err());
            invalid.extend(kind.err());
            None
        }
    }
}

/// Nanoseconds written as a decimal `u64`.
fn parse_nanos(text: &str) -> Option<u64> {
    text.parse().ok().filter(|_| is_decimal(text))
}

/// Only ASCII digits: `u64::from_str` also takes a leading `+`.
fn is_decimal(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit())
}

/// The one value of header `name`, `None` when it is absent; a header given more than once
/// is refused, since which value is meant cannot be told.
fn header<'h>(
    headers: Option<&'h HeaderMap>,
    name: &'static str,
) -> Result<Option<&'h str>, InvalidHeader> {
    let Some(headers) = headers else {
        return Ok(None);
    };
    let mut values = headers.get_all(name);
    let first = values.next();
    if values.next().is_some() {
        return Err(InvalidHeader::Repeated(name));
    }
    Ok(first.map(HeaderValue::as_str))
}
