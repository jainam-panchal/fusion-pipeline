//! Source, sink and acknowledgement contracts.
//!
//! A source yields envelopes (record, arrival and ack handle) into the engine's intake. The
//! engine acknowledges the handle once every branch has ended in a sink success or an
//! intentional drop, and negatively acknowledges it on any failure so the source can
//! redeliver. A sink receives outgoing records: each record beside its `Meta`, which the
//! sink carries next to the record and never inside it (ADR 0005).

use std::time::Duration;

use crate::closed_set::closed_set;
use crate::config::SOURCE_ID;
use crate::meta::{Arrival, Meta};
use crate::record::{Record, RecordId};

/// Settles the source message behind a record. Exactly one of `ack` or `nak` is called.
pub trait AckHandle: Send {
    /// The record was durably handled downstream (or intentionally dropped).
    fn ack(self: Box<Self>);
    /// Handling failed because of `failure`; the source should redeliver, after `delay` if
    /// given. A source that gives up on the message (the NATS source on its final delivery)
    /// says why with `failure`.
    fn nak(self: Box<Self>, delay: Option<Duration>, failure: Failure);
}

closed_set! {
    /// What kind of failure made a record's message nak: the `reason` label of `dlq_total`.
    /// Closed set; adding a value is a spec amendment.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum FailureKind {
        /// A stage returned an error.
        StageError = "stage_error",
        /// A stage could not reach the state store and its node's `on_state_error` is `nak`.
        StateError = "state_error",
        /// A sink could not confirm durable acceptance.
        SinkError = "sink_error",
        /// A stage or sink panicked. Only a dev build gets here: a release build aborts.
        Panic = "panic",
        /// The message arrived without a record id.
        MissingId = "missing_id",
        /// The payload is not a record. Set by a source, never by the engine.
        Undecodable = "undecodable",
    }
}

/// Why a record's message was nakked: the first failure of its walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    /// The node that failed, or `source` for a failure before any node ran.
    pub node: String,
    /// The `Meta` record id of the failed record; `None` for a message without a record id
    /// or a payload that is not a record. A source names the record by it when it logs.
    pub record_id: Option<RecordId>,
    /// What kind of failure it was.
    pub kind: FailureKind,
    /// What the node said, for people; never a metric label.
    pub error: String,
}

impl Failure {
    /// A failure before any node ran, charged to the reserved `source` node.
    #[must_use]
    pub fn at_source(kind: FailureKind, error: impl Into<String>) -> Self {
        Self {
            node: SOURCE_ID.to_owned(),
            record_id: None,
            kind,
            error: error.into(),
        }
    }
}

/// A record together with what the source knows about its message and the handle that
/// settles it.
pub struct Envelope {
    /// The record as decoded by the source.
    pub record: Record,
    /// What the source knows about how the message arrived.
    pub arrival: Arrival,
    /// Settles the source message.
    pub ack: Box<dyn AckHandle>,
}

impl std::fmt::Debug for Envelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Envelope")
            .field("record", &self.record)
            .field("arrival", &self.arrival)
            .finish_non_exhaustive()
    }
}

/// The engine side of the source bridge. Bounded: `send` blocks when workers are behind.
/// Not `Clone`: the engine drains and exits once the source returns and this is dropped.
#[derive(Debug)]
pub struct Intake {
    tx: crossbeam_channel::Sender<Envelope>,
}

impl Intake {
    pub(crate) fn new(tx: crossbeam_channel::Sender<Envelope>) -> Self {
        Self { tx }
    }

    /// Hand an envelope to the engine, blocking while every worker is busy.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::EngineClosed`] once the engine has shut down.
    pub fn send(&self, envelope: Envelope) -> Result<(), SourceError> {
        self.tx
            .send(envelope)
            .map_err(|_| SourceError::EngineClosed)
    }
}

/// Errors a source can end with.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SourceError {
    /// The engine stopped accepting envelopes.
    #[error("engine intake is closed")]
    EngineClosed,
    /// Any other source failure.
    #[error("source failed: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Produces envelopes until it is exhausted or the engine closes.
///
/// A source must give every envelope an [`Arrival`] with the tenant its transport names,
/// an ingestion time from its transport, and the message's delivery count, and must not
/// write any of them into the record. `Meta` takes its tenant and ingestion time from the
/// arrival alone and never from the record: a tenant the arrival does not name is
/// `unknown`, and a time it does not give is the worker clock's. Stateful stages measure
/// windows in that ingestion time, and a redelivered record must get the value it had the
/// first time, so the clock fallback is for records pushed in tests, not for sources.
pub trait Source: Send {
    /// Run to completion, delivering every envelope into `intake`.
    ///
    /// # Errors
    ///
    /// Returns a [`SourceError`] when the source fails or the intake closes early.
    fn run(self: Box<Self>, intake: Intake) -> Result<(), SourceError>;
}

/// A sink write failure. The record's source message is negatively acknowledged.
#[derive(Debug, thiserror::Error)]
#[error("sink write failed: {0}")]
pub struct SinkError(#[from] Box<dyn std::error::Error + Send + Sync>);

impl SinkError {
    /// Wrap any error as a sink error.
    #[must_use]
    pub fn new(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self(Box::new(err))
    }
}

/// One record a sink writes, beside the pipeline's view of it.
#[derive(Debug, Clone, Copy)]
pub struct Outgoing<'a> {
    /// The pipeline's view of the record, to carry beside it (a header, a column) or not at
    /// all; never to write into it.
    pub meta: &'a Meta,
    /// The record as the last stage left it.
    pub record: &'a Record,
}

/// Accepts outgoing records. Returns `Ok` only once they are durably accepted downstream.
pub trait Sink: Send + Sync {
    /// Write a batch of outgoing records, returning how many bytes were written with durable
    /// acceptance (the `bytes_out_total` count).
    ///
    /// # Errors
    ///
    /// Returns a [`SinkError`] when durable acceptance could not be confirmed.
    fn write(&self, batch: &[Outgoing<'_>]) -> Result<u64, SinkError>;
}
