//! The pipeline's view of a record, beside it rather than inside it (ADR 0005).
//!
//! A source says what its transport knows about a message in an [`Arrival`]; the engine
//! resolves that and the record, once, at intake, into a [`Meta`] with [`Meta::resolve`]
//! and hands it to every stage read-only. The arrival comes first and the record second:
//! the transport's tenant is authenticated and its time is the pipeline's, while the
//! record's fields are the producer's word. That is the one place the pipeline reads the
//! payload for itself. Every decision after it (metric labels, state keys, windows) reads
//! `Meta`, so a stage rewriting any record field changes the data the sink writes and
//! nothing else. `Meta` is never written into the record: a sink carries it beside the
//! record, as the NATS sink's pipeline headers do.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::record::{Kind, Record, RecordId};

/// The tenant of a record that names none and arrived on a transport that names none.
pub const UNKNOWN_TENANT: &str = "unknown";

/// Whether `tenant` can be a tenant: not empty, and no control characters. A tenant is a
/// metric label, a state-key segment and a message header, and a header value cannot hold a
/// line break. A source leaves a tenant that fails this out of the arrival, and a record's
/// `resource.tenant.id` that fails it is no tenant.
#[must_use]
pub fn is_valid_tenant(tenant: &str) -> bool {
    !tenant.is_empty() && !tenant.chars().any(char::is_control)
}

/// What a source's transport says about a message, apart from the record it carries.
/// Everything but the delivery count is optional: a source that fills nothing leaves the
/// engine to the record and then to its defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arrival {
    /// The tenant the transport names (the NATS subject's, else an upstream pipeline's
    /// `Fusion-Tenant` header).
    pub tenant: Option<String>,
    /// When the message entered the pipeline's transport: an upstream pipeline's
    /// `Fusion-Ingestion-Time` with its kind, else the JetStream publish time as
    /// [`IngestionTime::Reported`].
    pub ingestion_time: Option<IngestionTime>,
    /// How many times the transport has delivered this message, this one included.
    pub delivery_count: u64,
}

impl Default for Arrival {
    fn default() -> Self {
        Self {
            tenant: None,
            ingestion_time: None,
            delivery_count: 1,
        }
    }
}

/// The pipeline's view of one record, fixed at intake for the whole walk and shared by
/// every record a stage emits from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Meta {
    /// The record's id, as it arrived.
    pub record_id: RecordId,
    /// The tenant every metric label and state key uses: see [`Meta::tenant_of`].
    pub tenant: Arc<str>,
    /// When the record entered: the arrival's, else the record's
    /// `observed_time_unix_nano`, else its `time_unix_nano`, else the worker clock.
    pub ingestion_time: IngestionTime,
    /// How many times the message has been delivered, this one included.
    pub delivery_count: u64,
}

/// A record's ingestion time, and whether anyone reported it. The two are never combined:
/// a clock reading stays a clock reading, across stages and across pipelines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestionTime {
    /// A transport or the record said when it entered, in nanoseconds since the Unix epoch.
    Reported(u64),
    /// Neither did, and this is a worker clock at intake, in nanoseconds since the Unix
    /// epoch. Only a source that fills nothing (the in-memory one tests use) gets here, or a
    /// pipeline downstream of one; end to end is not measured against it.
    Clock(u64),
}

impl IngestionTime {
    /// The time in nanoseconds since the Unix epoch, whoever said it.
    #[must_use]
    pub const fn unix_nanos(self) -> u64 {
        match self {
            Self::Reported(nanos) | Self::Clock(nanos) => nanos,
        }
    }

    /// `reported` or `clock`, the spelling of the `Fusion-Ingestion-Time-Kind` header.
    #[must_use]
    pub const fn kind_name(self) -> &'static str {
        match self {
            Self::Reported(_) => REPORTED,
            Self::Clock(_) => CLOCK,
        }
    }

    /// The time `unix_nanos` with the kind spelled `kind`, the inverse of
    /// [`IngestionTime::kind_name`]; `None` for any other spelling.
    #[must_use]
    pub fn from_kind_name(kind: &str, unix_nanos: u64) -> Option<Self> {
        match kind {
            REPORTED => Some(Self::Reported(unix_nanos)),
            CLOCK => Some(Self::Clock(unix_nanos)),
            _ => None,
        }
    }
}

const REPORTED: &str = "reported";
const CLOCK: &str = "clock";

/// A record the engine does not walk. `tenant` is the one [`Meta::tenant_of`] gives it, so
/// the rejection is counted where the record's other metrics would have been.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejected {
    /// Why the record is not walked.
    pub reason: Rejection,
    /// The tenant the rejection is counted under.
    pub tenant: Arc<str>,
}

/// Why the engine does not walk a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    /// The record arrived without an `id`. Its message is nakked.
    MissingId,
    /// The record is not a log. It is dropped and its message acked.
    NotLog,
}

impl Meta {
    /// The pipeline's view of `record` as it arrived with `arrival`, or why it is not
    /// walked: a record without an id, or of a kind other than `log`. The arrival's tenant
    /// and time come first; the record's are read only when the arrival names none.
    ///
    /// # Errors
    ///
    /// [`Rejected`] with the reason and the tenant to count it under.
    pub fn resolve(record: &Record, arrival: &Arrival) -> Result<Self, Rejected> {
        let tenant = Self::tenant_of(record, arrival);
        let reject = |reason| Rejected {
            reason,
            tenant: Arc::clone(&tenant),
        };
        let Some(record_id) = record.id else {
            return Err(reject(Rejection::MissingId));
        };
        if record.kind != Kind::Log {
            return Err(reject(Rejection::NotLog));
        }
        let ingestion_time = arrival.ingestion_time.unwrap_or_else(|| {
            record
                .observed_time_unix_nano
                .or(record.time_unix_nano)
                .map_or_else(
                    || IngestionTime::Clock(unix_nanos_now()),
                    IngestionTime::Reported,
                )
        });
        Ok(Self {
            record_id,
            tenant,
            ingestion_time,
            delivery_count: arrival.delivery_count,
        })
    }

    /// The tenant the pipeline gives `record`: the one the transport names, else the
    /// record's `resource.tenant.id` when that is a string, else [`UNKNOWN_TENANT`]; a
    /// candidate that fails [`is_valid_tenant`] is skipped.
    #[must_use]
    pub fn tenant_of(record: &Record, arrival: &Arrival) -> Arc<str> {
        arrival
            .tenant
            .as_deref()
            .filter(|tenant| is_valid_tenant(tenant))
            .or_else(|| record.tenant().filter(|tenant| is_valid_tenant(tenant)))
            .unwrap_or(UNKNOWN_TENANT)
            .into()
    }
}

/// The worker clock in nanoseconds since the Unix epoch; zero before the epoch, saturated
/// past `u64`.
#[must_use]
pub fn unix_nanos_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}
