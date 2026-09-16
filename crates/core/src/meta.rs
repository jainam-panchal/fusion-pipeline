//! The pipeline's view of a record, beside it rather than inside it (ADR 0005).
//!
//! A source says what it knows about a message in an [`Arrival`]; the engine resolves that
//! and the record, once, at intake, into a [`Meta`] and hands it to every stage read-only.
//! Every decision the pipeline takes about a record (its metric labels, its state keys, its
//! window) reads `Meta`, never the payload, so a stage rewriting the record's tenant or time
//! fields changes the data the sink writes and nothing else.

use std::sync::Arc;

use crate::record::RecordId;

/// What a source says about a message and its record. Everything but the delivery count is
/// optional: a source that fills nothing leaves the engine to read the record at intake.
/// The NATS source gives the record's own word first and its transport's as the fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arrival {
    /// The record's tenant: its `resource.tenant.id`, else the one the transport names (the
    /// NATS subject's).
    pub tenant: Option<String>,
    /// Ingestion time in nanoseconds since the Unix epoch: the record's own time fields,
    /// else when the message entered the transport (the JetStream publish time).
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
    /// The tenant every metric label and state key uses: the arrival's, else the record's
    /// `resource.tenant.id` at intake, else `unknown`.
    pub tenant: Arc<str>,
    /// Ingestion time in nanoseconds since the Unix epoch: the arrival's, else the record's
    /// `observed_time_unix_nano` then `time_unix_nano` at intake, else the worker clock.
    pub ingestion_time: u64,
    /// How many times the message has been delivered, this one included.
    pub delivery_count: u64,
}
