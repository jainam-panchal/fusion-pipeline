//! In-memory `Source`, `Sink` and `AckHandle` for tests at the trait boundary.

use crate::record::Record;
use crate::traits::{AckHandle, AckOutcome, Envelope, Sink, SinkError, Source};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Drains a fixed list of envelopes, then closes.
pub struct MemorySource {
    queue: VecDeque<Envelope>,
}

impl MemorySource {
    pub fn new(envelopes: Vec<Envelope>) -> Self {
        Self {
            queue: envelopes.into(),
        }
    }
}

impl Source for MemorySource {
    fn next(&mut self) -> Option<Envelope> {
        self.queue.pop_front()
    }
}

/// Collects every record published to it, or rejects every publish.
#[derive(Default)]
pub struct MemorySink {
    received: Mutex<Vec<Record>>,
    reject: bool,
}

impl MemorySink {
    /// A sink whose every publish fails, for nak tests.
    pub fn rejecting() -> Self {
        Self {
            received: Mutex::default(),
            reject: true,
        }
    }

    /// Records received so far, in arrival order.
    pub fn records(&self) -> Vec<Record> {
        self.received.lock().unwrap().clone()
    }
}

impl Sink for MemorySink {
    fn publish(&self, records: &[Record]) -> Result<(), SinkError> {
        if self.reject {
            return Err(SinkError("memory sink is rejecting".into()));
        }
        self.received.lock().unwrap().extend_from_slice(records);
        Ok(())
    }
}

/// Test-side view of what happened to an ack handle.
#[derive(Debug, Clone)]
pub struct AckProbe {
    outcome: Arc<Mutex<Option<AckOutcome>>>,
}

impl AckProbe {
    /// `None` until the engine has acked or nakked.
    pub fn outcome(&self) -> Option<AckOutcome> {
        *self.outcome.lock().unwrap()
    }
}

pub struct MemoryAck {
    outcome: Arc<Mutex<Option<AckOutcome>>>,
}

impl MemoryAck {
    pub fn new() -> (Self, AckProbe) {
        let outcome = Arc::new(Mutex::new(None));
        (
            Self {
                outcome: outcome.clone(),
            },
            AckProbe { outcome },
        )
    }

    fn set(&self, what: AckOutcome) {
        let mut slot = self.outcome.lock().unwrap();
        assert!(
            slot.is_none(),
            "ack handle used twice: {slot:?} then {what:?}"
        );
        *slot = Some(what);
    }
}

impl AckHandle for MemoryAck {
    fn ack(self: Box<Self>) {
        self.set(AckOutcome::Ack);
    }

    fn nak(self: Box<Self>, delay: Option<Duration>) {
        self.set(AckOutcome::Nak(delay));
    }
}

/// Wrap a record in an envelope with an observable ack handle.
pub fn envelope(record: Record) -> (Envelope, AckProbe) {
    let (ack, probe) = MemoryAck::new();
    (
        Envelope {
            record,
            ack: Box::new(ack),
        },
        probe,
    )
}
