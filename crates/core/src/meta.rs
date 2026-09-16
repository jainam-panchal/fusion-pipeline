//! The pipeline's view of a record, beside it rather than inside it (ADR 0005).
//!
//! A source says what its transport knows about a message in an [`Arrival`]; the engine
//! resolves that and the record, once, at intake, into a [`Meta`] with [`Meta::resolve`]
//! and hands it to every stage read-only. The tenant and the ingestion time come from the
//! arrival and nowhere else: the transport's tenant is authenticated and its time is the
//! pipeline's, while the record's fields are the producer's data. The only payload fields
//! the pipeline reads for itself are `id` and `kind`, once, to decide whether the record is
//! walked at all. Every decision after it (metric labels, state keys, windows) reads
//! `Meta`, so a stage rewriting any record field changes the data the sink writes and
//! nothing else. `Meta` is never written into the record: a sink carries it beside the
//! record, as the NATS sink's pipeline headers do.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::closed_set::closed_set;
use crate::record::{Kind, Record, RecordId};

/// The tenant of a record that names none and arrived on a transport that names none.
pub const UNKNOWN_TENANT: &str = "unknown";

/// Whether `tenant` can be a tenant: not empty, and no control characters. A tenant is a
/// metric label, a state-key segment and a message header, and a header value cannot hold a
/// line break. A source leaves a tenant that fails this out of the arrival, and
/// [`Meta::resolve`] treats an arrival tenant that fails it as none.
#[must_use]
pub fn is_valid_tenant(tenant: &str) -> bool {
    !tenant.is_empty() && !tenant.chars().any(char::is_control)
}

/// What a source's transport says about a message, apart from the record it carries.
/// Everything but the delivery count is optional: a tenant the transport does not name is
/// `unknown`, and a time it does not give is the worker clock's.
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
    /// When the record entered: the arrival's, else the worker clock at intake.
    pub ingestion_time: IngestionTime,
    /// How many times the message has been delivered, this one included.
    pub delivery_count: u64,
}

/// A record's ingestion time, and whether anyone reported it. The two are never combined:
/// a clock reading stays a clock reading, across stages and across pipelines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestionTime {
    /// A transport said when the message entered, in nanoseconds since the Unix epoch.
    Reported(u64),
    /// No transport did, and this is a worker clock at intake, in nanoseconds since the Unix
    /// epoch. Only a source that fills no time (the in-memory one tests use) gets here, or a
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

    /// Who said it.
    #[must_use]
    pub const fn kind(self) -> TimeKind {
        match self {
            Self::Reported(_) => TimeKind::Reported,
            Self::Clock(_) => TimeKind::Clock,
        }
    }

    /// The time `unix_nanos`, said by `kind`.
    #[must_use]
    pub const fn new(kind: TimeKind, unix_nanos: u64) -> Self {
        match kind {
            TimeKind::Reported => Self::Reported(unix_nanos),
            TimeKind::Clock => Self::Clock(unix_nanos),
        }
    }
}

closed_set! {
    /// Who said an [`IngestionTime`], by the spelling of the `Fusion-Ingestion-Time-Kind`
    /// pipeline header.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum TimeKind {
        /// A transport.
        Reported = "reported",
        /// A worker clock.
        Clock = "clock",
    }
}

closed_set! {
    /// A value of [`Meta`], by its spelling after `meta.` in a field path and as a key of the
    /// `meta` table a `lua` script receives.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum MetaField {
        /// [`Meta::record_id`].
        Id = "id",
        /// [`Meta::tenant`].
        Tenant = "tenant",
        /// [`Meta::ingestion_time`], in nanoseconds since the Unix epoch.
        IngestionTime = "ingestion_time",
        /// [`Meta::delivery_count`].
        DeliveryCount = "delivery_count",
    }
}

/// One value of [`Meta`] as a reader sees it: the tenant as text, every other value as a
/// number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaValue<'a> {
    /// The tenant.
    Str(&'a str),
    /// The record id, the ingestion time in nanoseconds, or the delivery count.
    U64(u64),
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
    /// walked: a record without an id, or of a kind other than `log`. The tenant and the
    /// ingestion time are the arrival's; the record's own tenant and time fields are never
    /// read.
    ///
    /// # Errors
    ///
    /// [`Rejected`] with the reason and the tenant to count it under.
    pub fn resolve(record: &Record, arrival: &Arrival) -> Result<Self, Rejected> {
        let tenant = Self::tenant_of(arrival);
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
        let ingestion_time = arrival
            .ingestion_time
            .unwrap_or_else(|| IngestionTime::Clock(unix_nanos_now()));
        Ok(Self {
            record_id,
            tenant,
            ingestion_time,
            delivery_count: arrival.delivery_count,
        })
    }

    /// The value `field` names.
    #[must_use]
    pub fn get(&self, field: MetaField) -> MetaValue<'_> {
        match field {
            MetaField::Id => MetaValue::U64(self.record_id.0),
            MetaField::Tenant => MetaValue::Str(&self.tenant),
            MetaField::IngestionTime => MetaValue::U64(self.ingestion_time.unix_nanos()),
            MetaField::DeliveryCount => MetaValue::U64(self.delivery_count),
        }
    }

    /// The tenant the pipeline gives a message that arrived with `arrival`: the one the
    /// transport names when it passes [`is_valid_tenant`], else [`UNKNOWN_TENANT`].
    #[must_use]
    pub fn tenant_of(arrival: &Arrival) -> Arc<str> {
        arrival
            .tenant
            .as_deref()
            .filter(|tenant| is_valid_tenant(tenant))
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
