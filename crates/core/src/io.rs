//! Source, sink and acknowledgement contracts.
//!
//! A source yields envelopes (record plus ack handle) into the engine's intake. The engine
//! acknowledges the handle once every branch has ended in a sink success or an intentional
//! drop, and negatively acknowledges it on any failure so the source can redeliver.

use std::time::Duration;

use crate::record::Record;

/// Settles the source message behind a record. Exactly one of `ack` or `nak` is called.
pub trait AckHandle: Send {
    /// The record was durably handled downstream (or intentionally dropped).
    fn ack(self: Box<Self>);
    /// Handling failed; the source should redeliver, after `delay` if given.
    fn nak(self: Box<Self>, delay: Option<Duration>);
}

/// A record together with the handle that settles its source message.
pub struct Envelope {
    /// The record as decoded by the source.
    pub record: Record,
    /// Settles the source message.
    pub ack: Box<dyn AckHandle>,
}

impl std::fmt::Debug for Envelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Envelope")
            .field("record", &self.record)
            .finish_non_exhaustive()
    }
}

/// The engine side of the source bridge. Bounded: `send` blocks when workers are behind.
#[derive(Debug, Clone)]
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

/// Accepts records. Returns `Ok` only once they are durably accepted downstream.
pub trait Sink: Send + Sync {
    /// Write a batch of records.
    ///
    /// # Errors
    ///
    /// Returns a [`SinkError`] when durable acceptance could not be confirmed.
    fn write(&self, records: &[Record]) -> Result<(), SinkError>;
}
