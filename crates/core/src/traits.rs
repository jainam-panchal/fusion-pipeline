//! The boundaries the engine talks through: a `Source` yields envelopes, a
//! `Sink` accepts records and reports durable receipt, an `AckHandle` closes
//! the loop back to the source.

use crate::record::Record;
use std::time::Duration;

/// What an ack handle was told. Observable on the in-memory fake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    Ack,
    Nak(Option<Duration>),
}

/// Consumed exactly once, by either `ack` or `nak`.
pub trait AckHandle: Send {
    fn ack(self: Box<Self>);
    fn nak(self: Box<Self>, delay: Option<Duration>);
}

/// A record plus the handle that acknowledges its source message.
pub struct Envelope {
    pub record: Record,
    pub ack: Box<dyn AckHandle>,
}

/// Yields envelopes until closed. `None` means no more will come; the engine
/// drains what it has and returns.
pub trait Source: Send {
    fn next(&mut self) -> Option<Envelope>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("sink error: {0}")]
pub struct SinkError(pub String);

/// Accepts records; returns `Ok` only on durable acceptance (`PubAck` for
/// NATS). Shared across worker threads.
pub trait Sink: Send + Sync {
    fn publish(&self, records: &[Record]) -> Result<(), SinkError>;
}
