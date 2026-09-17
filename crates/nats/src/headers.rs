//! Pipeline headers: a record's `Meta` on the wire, beside the record and never in it
//! (ADR 0005), and the arrival-only `Fusion-Record-Kind`, which no `Meta` carries (ADR 0007).
//!
//! The sink writes four headers on every message it publishes:
//!
//! | Header | Value |
//! |---|---|
//! | `Fusion-Record-Id` | the `Meta` record id, decimal |
//! | `Fusion-Tenant` | the `Meta` tenant |
//! | `Fusion-Ingestion-Time` | the ingestion time, nanoseconds since the Unix epoch, decimal |
//! | `Fusion-Ingestion-Time-Kind` | `reported` or `clock` |
//!
//! A producer sets `Fusion-Record-Id` on every message, and `Fusion-Record-Kind` (`log`,
//! `metric` or `span`; absent is `log`) when it sends anything but logs (ADR 0007). The sink
//! writes no kind: every record it writes was walked, so a log.
//!
//! The source reads them back into the message's [`Arrival`] with [`arrival()`], so a
//! pipeline consuming another's output keeps the first pipeline's record id, tenant and
//! ingestion time. The subject's tenant wins over the header, because NATS permissions back
//! the subject and any producer can set a header; the header's time wins over the JetStream
//! publish time, so the first pipeline's time survives every hop. A header that does not
//! parse is ignored, reported and counted once, never a reason to nak: the record is still
//! valid. `Fusion-Record-Kind` is the exception to "ignored": one that does not parse, or is
//! given twice, is reported and counted, and the message is not walked, since only an absent
//! kind or `log` says it is a log.
//!
//! A dead letter carries the message as it arrived, with [`for_dead_letter`]: the
//! producer's headers, the record id, kind and ingestion time the arrival gave and the tenant
//! its `Meta` gets (so a replay keeps them), `Fusion-Dlq-Reason` (the failing node and its error),
//! `Fusion-Dlq-Subject` (where it arrived) and `Nats-Msg-Id` (its stream and sequence, so a
//! second dead letter of the same message is dropped as a duplicate). No `Nats-*` header of
//! the producer's is kept: `Nats-Expected-Stream` and its kind would make the publish fail.

use async_nats::{HeaderMap, HeaderValue};
use fusion_core::io::Failure;
use fusion_core::meta::{Arrival, ArrivalKind, IngestionTime, Meta, TimeKind, is_valid_tenant};
use fusion_core::record::{Kind, RecordId};

use crate::subject::tenant_from_subject;

/// The record id, decimal.
pub const RECORD_ID: &str = "Fusion-Record-Id";
/// The signal kind: `log`, `metric` or `span`.
pub const RECORD_KIND: &str = "Fusion-Record-Kind";
/// The `Meta` tenant.
pub const TENANT: &str = "Fusion-Tenant";
/// The `Meta` ingestion time in nanoseconds since the Unix epoch, decimal.
pub const INGESTION_TIME: &str = "Fusion-Ingestion-Time";
/// Whether a transport reported the ingestion time (`reported`) or a worker clock read it
/// (`clock`).
pub const INGESTION_TIME_KIND: &str = "Fusion-Ingestion-Time-Kind";

/// On a dead letter: `{node}: {error}` of the failure that made the source give up.
pub const DLQ_REASON: &str = "Fusion-Dlq-Reason";
/// On a dead letter: the subject the message arrived on.
pub const DLQ_SUBJECT: &str = "Fusion-Dlq-Subject";
/// JetStream's deduplication id; on a dead letter, `{stream}:{stream sequence}`.
pub const MSG_ID: &str = "Nats-Msg-Id";
/// The longest `Fusion-Dlq-Reason` value, in bytes.
pub const REASON_CAP: usize = 1024;

/// The prefix of the headers the pipeline writes.
const PIPELINE_PREFIX: &str = "fusion-";
/// The prefix of the headers JetStream reads on a publish.
const NATS_PREFIX: &str = "nats-";

/// The pipeline headers for a record with `meta`. A `Meta` tenant always passes
/// [`is_valid_tenant`], so it is always a valid header value; the check is kept so a value
/// that could not be one is left out rather than panicking inside the client.
#[must_use]
pub fn for_meta(meta: &Meta) -> HeaderMap {
    let mut headers = HeaderMap::new();
    write_meta(
        &mut headers,
        Some(meta.record_id),
        &meta.tenant,
        Some(meta.ingestion_time),
    );
    headers
}

/// Insert the tenant and, when given, the record id and the two ingestion time headers. A
/// value that cannot be a header is left out.
fn write_meta(
    headers: &mut HeaderMap,
    record_id: Option<RecordId>,
    tenant: &str,
    ingestion_time: Option<IngestionTime>,
) {
    if let Some(record_id) = record_id {
        insert(headers, RECORD_ID, &record_id.to_string());
    }
    insert(headers, TENANT, tenant);
    if let Some(time) = ingestion_time {
        insert(headers, INGESTION_TIME, &time.unix_nanos().to_string());
        insert(headers, INGESTION_TIME_KIND, &time.kind().to_string());
    }
}

/// Insert `value` under `name` when it can be a header value; parsed rather than converted,
/// since the conversion panics on a line break.
fn insert(headers: &mut HeaderMap, name: &'static str, value: &str) {
    if let Ok(value) = value.parse::<HeaderValue>() {
        headers.insert(name, value);
    }
}

/// A message the source gives up on, as its dead letter is written.
#[derive(Debug, Clone, Copy)]
pub struct DeadLetter<'m> {
    /// The stream the message is stored in.
    pub stream: &'m str,
    /// Its sequence in that stream.
    pub stream_sequence: u64,
    /// The subject it arrived on.
    pub subject: &'m str,
    /// The headers it arrived with, if any.
    pub headers: Option<&'m HeaderMap>,
    /// Its arrival: the dead letter carries the record id, kind and ingestion time it gave,
    /// and the tenant [`Meta::tenant_of`] makes of it.
    pub arrival: &'m Arrival,
    /// Why the pipeline gave up on it.
    pub failure: &'m Failure,
}

/// The headers of `letter`'s dead letter: the producer's headers except `Nats-*` and
/// `Fusion-*`, then the record id, kind, `Meta` tenant and ingestion time, [`DLQ_REASON`],
/// [`DLQ_SUBJECT`] and [`MSG_ID`].
#[must_use]
pub fn for_dead_letter(letter: &DeadLetter<'_>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, values) in letter.headers.into_iter().flat_map(HeaderMap::iter) {
        let lower = AsRef::<str>::as_ref(name).to_ascii_lowercase();
        if lower.starts_with(PIPELINE_PREFIX) || lower.starts_with(NATS_PREFIX) {
            continue;
        }
        for value in values {
            headers.append(name.clone(), value.clone());
        }
    }
    let arrival = letter.arrival;
    write_meta(
        &mut headers,
        arrival.record_id,
        &Meta::tenant_of(arrival),
        arrival.ingestion_time,
    );
    if let ArrivalKind::Named(kind) = arrival.kind {
        insert(&mut headers, RECORD_KIND, kind.as_str());
    }
    let failure = letter.failure;
    insert(
        &mut headers,
        DLQ_REASON,
        &header_line(&format!("{}: {}", failure.node, failure.error)),
    );
    insert(&mut headers, DLQ_SUBJECT, letter.subject);
    insert(
        &mut headers,
        MSG_ID,
        &format!("{}:{}", letter.stream, letter.stream_sequence),
    );
    headers
}

/// `text` as one header line: every control character a space, cut to [`REASON_CAP`]
/// bytes on a character boundary.
fn header_line(text: &str) -> String {
    let mut line = String::with_capacity(text.len().min(REASON_CAP));
    for c in text.chars() {
        let c = if c.is_control() { ' ' } else { c };
        if line.len() + c.len_utf8() > REASON_CAP {
            break;
        }
        line.push(c);
    }
    line
}

/// A pipeline header the source ignored because it did not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum InvalidHeader {
    /// `Fusion-Record-Id` is not a decimal `u64`.
    #[error("`{RECORD_ID}` is `{0}`, not a decimal integer")]
    RecordId(String),
    /// `Fusion-Record-Kind` is not `log`, `metric` or `span`.
    #[error("`{RECORD_KIND}` is `{0}`, not `log`, `metric` or `span`")]
    RecordKind(String),
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

impl InvalidHeader {
    /// Whether the message is not walked because of this header: a `Fusion-Record-Kind` that
    /// does not parse or is given twice leaves the kind unreadable, which is not a log. Every
    /// other header is only ignored.
    #[must_use]
    pub fn refuses_message(&self) -> bool {
        match self {
            Self::RecordKind(_) => true,
            Self::Repeated(name) => *name == RECORD_KIND,
            _ => false,
        }
    }
}

/// What JetStream handed the source for one message, apart from its payload's content.
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
    /// The payload's length in bytes.
    pub bytes: u64,
}

/// The arrival of `received` by a source whose tenant subjects are
/// `{tenant_prefix}.{tenant}.>`, and the pipeline headers ignored because they did not
/// parse:
///
/// - record id: `Fusion-Record-Id`;
/// - kind: `Fusion-Record-Kind`, [`ArrivalKind::Unreadable`] when it is present but ignored;
/// - tenant: the subject's tenant token, else `Fusion-Tenant`, each only when it passes
///   [`is_valid_tenant`]; the header is not read when the subject names a valid tenant;
/// - ingestion time: `Fusion-Ingestion-Time` with its kind, else the publish time as
///   reported;
/// - delivery count: the delivery count;
/// - bytes: the payload's length.
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
        bytes,
    } = received;
    let mut invalid = Vec::new();
    let record_id = parse_or_report(header(headers, RECORD_ID), &mut invalid, |text| {
        parse_decimal(text)
            .map(RecordId)
            .ok_or_else(|| InvalidHeader::RecordId(text.to_owned()))
    });
    // Unlike the other headers, a kind that is present but unusable is not taken as absent:
    // absent means log, and a producer that named a kind did not say log.
    let kind = match parse_header(header(headers, RECORD_KIND), |text| {
        Kind::parse(text).ok_or_else(|| InvalidHeader::RecordKind(text.to_owned()))
    }) {
        Ok(None) => ArrivalKind::Unnamed,
        Ok(Some(kind)) => ArrivalKind::Named(kind),
        Err(problem) => {
            invalid.push(problem);
            ArrivalKind::Unreadable
        }
    };
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
        record_id,
        kind,
        tenant,
        ingestion_time: header_time.or(published.map(IngestionTime::Reported)),
        delivery_count: delivered,
        bytes: Some(bytes),
    };
    (arrival, invalid)
}

/// The value `parse` makes of a header `read` gave once, `None` when it is absent, or why it
/// is ignored.
fn parse_header<'h, T>(
    read: Result<Option<&'h str>, InvalidHeader>,
    parse: impl FnOnce(&'h str) -> Result<T, InvalidHeader>,
) -> Result<Option<T>, InvalidHeader> {
    read.and_then(|text| text.map(parse).transpose())
}

/// As [`parse_header`], with the reason a present header is ignored pushed onto `invalid`.
fn parse_or_report<'h, T>(
    read: Result<Option<&'h str>, InvalidHeader>,
    invalid: &mut Vec<InvalidHeader>,
    parse: impl FnOnce(&'h str) -> Result<T, InvalidHeader>,
) -> Option<T> {
    parse_header(read, parse)
        .map_err(|problem| invalid.push(problem))
        .ok()
        .flatten()
}

/// The `Fusion-Tenant` header when it is given once and passes [`is_valid_tenant`];
/// otherwise `None`, with the reason pushed onto `invalid` when the header was present.
fn header_tenant<'h>(
    headers: Option<&'h HeaderMap>,
    invalid: &mut Vec<InvalidHeader>,
) -> Option<&'h str> {
    parse_or_report(header(headers, TENANT), invalid, |tenant| {
        if is_valid_tenant(tenant) {
            Ok(tenant)
        } else {
            Err(InvalidHeader::Tenant)
        }
    })
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
    let time = parse_header(time, |text| {
        parse_decimal(text).ok_or_else(|| InvalidHeader::Time(text.to_owned()))
    });
    let kind = parse_header(kind, |text| {
        TimeKind::parse(text).ok_or_else(|| InvalidHeader::Kind(text.to_owned()))
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

/// A decimal `u64`: nanoseconds or a record id.
fn parse_decimal(text: &str) -> Option<u64> {
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
