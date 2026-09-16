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

use async_nats::HeaderMap;
use fusion_core::meta::{Arrival, IngestionTime, Meta};

use crate::subject::tenant_from_subject;

/// The `Meta` tenant.
pub const TENANT: &str = "Fusion-Tenant";
/// The `Meta` ingestion time in nanoseconds since the Unix epoch, decimal.
pub const INGESTION_TIME: &str = "Fusion-Ingestion-Time";
/// Whether a transport reported the ingestion time (`reported`) or a worker clock read it
/// (`clock`).
pub const INGESTION_TIME_KIND: &str = "Fusion-Ingestion-Time-Kind";

/// The pipeline headers for a record with `meta`.
#[must_use]
pub fn for_meta(meta: &Meta) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(TENANT, &*meta.tenant);
    headers.insert(INGESTION_TIME, meta.ingestion_time.unix_nanos().to_string());
    headers.insert(INGESTION_TIME_KIND, meta.ingestion_time.kind_name());
    headers
}

/// A pipeline header the source left out of the arrival.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum InvalidHeader {
    /// `Fusion-Tenant` is present and empty.
    #[error("`{TENANT}` is empty")]
    EmptyTenant,
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

/// The arrival of a message on `subject` with `headers`, published at `published` (the
/// JetStream publish time, nanoseconds since the Unix epoch) and delivered `delivered`
/// times.
///
/// - tenant: the subject's `logs.{tenant}.>` token, else `Fusion-Tenant`;
/// - ingestion time: `Fusion-Ingestion-Time` with its kind, else `published` as reported;
/// - delivery count: `delivered`.
#[must_use]
pub fn arrival(
    subject: &str,
    headers: Option<&HeaderMap>,
    published: Option<u64>,
    delivered: u64,
) -> Arrived {
    let mut invalid = Vec::new();
    let header_tenant = header(headers, TENANT).and_then(|tenant| {
        if tenant.is_empty() {
            invalid.push(InvalidHeader::EmptyTenant);
            None
        } else {
            Some(tenant)
        }
    });
    let tenant = tenant_from_subject(subject)
        .or(header_tenant)
        .map(str::to_owned);
    let header_time = ingestion_time(headers).unwrap_or_else(|problems| {
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
    headers: Option<&HeaderMap>,
) -> Result<Option<IngestionTime>, Vec<InvalidHeader>> {
    match (
        header(headers, INGESTION_TIME),
        header(headers, INGESTION_TIME_KIND),
    ) {
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

fn header<'h>(headers: Option<&'h HeaderMap>, name: &str) -> Option<&'h str> {
    headers?.get(name).map(|value| value.as_str())
}
