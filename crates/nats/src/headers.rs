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
//! The source reads them back into the message's [`Arrival`] with [`arrival`], so a pipeline
//! consuming another's output keeps the first pipeline's tenant and ingestion time. The
//! subject's tenant wins over the header, because NATS permissions back the subject and any
//! producer can set a header; the header's time wins over the JetStream publish time, so the
//! first pipeline's time survives every hop. A header that does not parse is left out of the
//! arrival and reported, never a reason to nak: the record is still valid.

use async_nats::{HeaderMap, HeaderValue};
use fusion_core::meta::{Arrival, IngestionTime, Meta, is_valid_tenant};

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
        (
            INGESTION_TIME_KIND,
            meta.ingestion_time.kind_name().to_owned(),
        ),
    ] {
        if let Ok(value) = value.parse::<HeaderValue>() {
            headers.insert(name, value);
        }
    }
    headers
}

/// A pipeline header the source left out of the arrival.
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

/// What a message says about itself: the arrival the engine resolves `Meta` from, and the
/// pipeline headers that were left out of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arrived {
    /// The arrival for the envelope.
    pub arrival: Arrival,
    /// The pipeline headers that did not parse, each left out of the arrival.
    pub invalid: Vec<InvalidHeader>,
}

/// Where a message came from: its subject, its headers, the JetStream publish time
/// (nanoseconds since the Unix epoch) and how many times it has been delivered.
#[derive(Debug, Clone, Copy)]
pub struct Message<'m> {
    /// The subject it was published on.
    pub subject: &'m str,
    /// The first token of the subjects that name a tenant.
    pub tenant_prefix: &'m str,
    /// Its headers, if any.
    pub headers: Option<&'m HeaderMap>,
    /// The JetStream publish time.
    pub published: Option<u64>,
    /// The JetStream delivery count.
    pub delivered: u64,
}

/// The arrival of `message`:
///
/// - tenant: the subject's `{tenant_prefix}.{tenant}.>` token, else `Fusion-Tenant`;
/// - ingestion time: `Fusion-Ingestion-Time` with its kind, else the publish time as
///   reported;
/// - delivery count: the delivery count.
#[must_use]
pub fn arrival(message: Message<'_>) -> Arrived {
    let Message {
        subject,
        tenant_prefix,
        headers,
        published,
        delivered,
    } = message;
    let mut invalid = Vec::new();
    let mut read = |name| {
        header(headers, name).unwrap_or_else(|problem| {
            invalid.push(problem);
            None
        })
    };
    let header_tenant = read(TENANT);
    let time = read(INGESTION_TIME);
    let kind = read(INGESTION_TIME_KIND);
    let header_tenant = header_tenant.filter(|tenant| {
        let valid = is_valid_tenant(tenant);
        if !valid {
            invalid.push(InvalidHeader::Tenant);
        }
        valid
    });
    let tenant = tenant_from_subject(subject, tenant_prefix)
        .filter(|tenant| is_valid_tenant(tenant))
        .or(header_tenant)
        .map(str::to_owned);
    let header_time = ingestion_time(time, kind).unwrap_or_else(|problems| {
        invalid.extend(problems);
        None
    });
    Arrived {
        arrival: Arrival {
            tenant,
            ingestion_time: header_time.or(published.map(IngestionTime::Reported)),
            delivery_count: delivered,
        },
        invalid,
    }
}

/// The ingestion time the two time headers give, `None` when neither is present.
fn ingestion_time(
    time: Option<&str>,
    kind: Option<&str>,
) -> Result<Option<IngestionTime>, Vec<InvalidHeader>> {
    match (time, kind) {
        (None, None) => Ok(None),
        (Some(_), None) => Err(vec![InvalidHeader::Unpaired(INGESTION_TIME)]),
        (None, Some(_)) => Err(vec![InvalidHeader::Unpaired(INGESTION_TIME_KIND)]),
        (Some(time), Some(kind)) => {
            let nanos = time.parse::<u64>().ok().filter(|_| is_decimal(time));
            let mut problems = Vec::new();
            if nanos.is_none() {
                problems.push(InvalidHeader::Time(time.to_owned()));
            }
            let parsed = nanos.and_then(|nanos| IngestionTime::from_kind_name(kind, nanos));
            if IngestionTime::from_kind_name(kind, 0).is_none() {
                problems.push(InvalidHeader::Kind(kind.to_owned()));
            }
            if problems.is_empty() {
                Ok(parsed)
            } else {
                Err(problems)
            }
        }
    }
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
