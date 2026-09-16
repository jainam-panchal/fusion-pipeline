//! The pipeline's view of a record, beside it rather than inside it (ADR 0005).
//!
//! A source says what its transport knows about a message in an [`Arrival`]; the engine
//! resolves that and the record, once, at intake, into a [`Meta`] with [`Meta::resolve`]
//! and hands it to every stage read-only. That is the one place the pipeline reads the
//! payload for itself. Every decision after it (metric labels, state keys, windows) reads
//! `Meta`, so a stage rewriting any record field changes the data the sink writes and
//! nothing else.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::record::{Kind, Record, RecordId};

/// The tenant of a record that names none and arrived on a transport that names none.
pub const UNKNOWN_TENANT: &str = "unknown";

/// What a source's transport says about a message, apart from the record it carries.
/// Everything but the delivery count is optional: a source that fills nothing leaves the
/// engine to the record and then to its defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arrival {
    /// The tenant the transport names (the NATS subject's).
    pub tenant: Option<String>,
    /// When the message entered the transport (the JetStream publish time), in
    /// nanoseconds since the Unix epoch.
    pub ingestion_time: Option<u64>,
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
    /// Ingestion time in nanoseconds since the Unix epoch: the record's
    /// `observed_time_unix_nano`, else its `time_unix_nano`, else the transport's, else the
    /// worker clock.
    pub ingestion_time: u64,
    /// Whether `ingestion_time` is the worker clock's, because neither the record nor the
    /// transport said when it entered. Only a source that fills nothing (the in-memory one
    /// tests use) gets here; end to end is not measured against such a time.
    pub ingestion_time_from_clock: bool,
    /// How many times the message has been delivered, this one included.
    pub delivery_count: u64,
}

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
    /// walked: a record without an id, or of a kind other than `log`.
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
        let stamped = record
            .observed_time_unix_nano
            .or(record.time_unix_nano)
            .or(arrival.ingestion_time);
        Ok(Self {
            record_id,
            tenant,
            ingestion_time: stamped.unwrap_or_else(unix_nanos_now),
            ingestion_time_from_clock: stamped.is_none(),
            delivery_count: arrival.delivery_count,
        })
    }

    /// The tenant the pipeline gives `record`: its `resource.tenant.id` when that is a
    /// string, else the one the transport names, else [`UNKNOWN_TENANT`].
    #[must_use]
    pub fn tenant_of(record: &Record, arrival: &Arrival) -> Arc<str> {
        record
            .tenant()
            .or(arrival.tenant.as_deref())
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
