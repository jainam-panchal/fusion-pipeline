//! Structured pipeline logs: the event vocabulary and the event-log boundary (ADR 0006).
//!
//! The pipeline logs a closed set of events, each with fixed fields, so a line without its
//! record id or tenant cannot be written and a test can assert every field. The engine
//! emits `stage_error`, `nak` and `redelivery`; the NATS source emits the two dead-letter
//! events. Drops are not events: `records_dropped_total` counts them. An [`EventLog`] is the
//! seam an exporter implements: [`InMemoryEventLog`] is the fake for tests,
//! [`StderrEventLog`] what the binary writes to when no collector is configured, and the
//! OTLP log exporter lives in the telemetry crate.

use std::fmt;
use std::sync::{Arc, Mutex};

use crate::closed_set::closed_set;
use crate::io::{Failure, FailureKind};
use crate::memory::lock_unpoisoned;
use crate::record::RecordId;
use crate::trace::TraceContext;

closed_set! {
    /// What happened to a record, as a log line says it. Closed set; adding one is a spec
    /// amendment.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum EventKind {
        /// A node failed: a stage error, a state error under `nak`, a sink error or a panic.
        StageError = "stage_error",
        /// A record's message was nakked, for the walk's first failure.
        Nak = "nak",
        /// A message came back: its delivery count is above one.
        Redelivery = "redelivery",
        /// A message on its final delivery was published to the dead-letter queue.
        DeadLetter = "dead_letter",
        /// A message on its final delivery could not be published to the dead-letter queue
        /// on any try, and was left in its stream.
        DeadLetterFailed = "dead_letter_failed",
    }
}

closed_set! {
    /// How loud an event is.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
    pub enum Severity {
        /// Expected in a healthy pipeline.
        Info = "info",
        /// A record did not get through this time.
        Warn = "warn",
        /// Something failed.
        Error = "error",
    }
}

impl EventKind {
    /// The severity every event of this kind is logged at.
    #[must_use]
    pub const fn severity(self) -> Severity {
        match self {
            Self::Redelivery => Severity::Info,
            Self::Nak | Self::DeadLetter => Severity::Warn,
            Self::StageError | Self::DeadLetterFailed => Severity::Error,
        }
    }
}

/// One log line. Built only on the paths that log, so it owns its fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    /// What happened.
    pub kind: EventKind,
    /// The record's id from its `Meta`; `None` for a record that arrived without one or a
    /// payload that is not a record.
    pub record_id: Option<RecordId>,
    /// The `Meta` tenant.
    pub tenant: Arc<str>,
    /// The node the event is about: the failing node, or `source`.
    pub node: String,
    /// The failure kind, exported as the `reason` attribute; `None` for a redelivery.
    pub failure: Option<FailureKind>,
    /// How many times the message has been delivered, this one included.
    pub delivery_count: u64,
    /// The message's stream sequence, for the dead-letter events.
    pub stream_sequence: Option<u64>,
    /// The error text, for people; never a label.
    pub message: String,
    /// The record trace this event belongs to, when the record has one.
    pub trace: Option<TraceContext>,
}

impl Event {
    /// An event of `kind` about `node` for a record of `tenant` on its `delivery_count`-th
    /// delivery, with no record id, failure, stream sequence, message or trace.
    #[must_use]
    pub fn new(
        kind: EventKind,
        tenant: Arc<str>,
        node: impl Into<String>,
        delivery_count: u64,
    ) -> Self {
        Self {
            kind,
            record_id: None,
            tenant,
            node: node.into(),
            failure: None,
            delivery_count,
            stream_sequence: None,
            message: String::new(),
            trace: None,
        }
    }

    /// An event of `kind` about `failure`: its record id, node, kind and error text.
    #[must_use]
    pub fn of_failure(
        kind: EventKind,
        tenant: Arc<str>,
        failure: &Failure,
        delivery_count: u64,
    ) -> Self {
        Self {
            record_id: failure.record_id,
            failure: Some(failure.kind),
            message: failure.error.clone(),
            ..Self::new(kind, tenant, failure.node.clone(), delivery_count)
        }
    }
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;
        if let Some(id) = self.record_id {
            write!(f, " record={id}")?;
        }
        write!(f, " tenant={} node={}", self.tenant, self.node)?;
        if let Some(failure) = self.failure {
            write!(f, " reason={failure}")?;
        }
        write!(f, " delivery={}", self.delivery_count)?;
        if let Some(sequence) = self.stream_sequence {
            write!(f, " stream_sequence={sequence}")?;
        }
        if let Some(trace) = self.trace {
            write!(f, " trace_id={}", trace.trace_id)?;
        }
        if !self.message.is_empty() {
            write!(f, ": {}", self.message)?;
        }
        Ok(())
    }
}

/// Where events go. Implemented by exporters; shared across threads. Must not block for
/// long: the engine emits from its workers.
pub trait EventLog: Send + Sync {
    /// Log one event.
    fn emit(&self, event: Event);
}

/// Discards every event.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoEvents;

impl EventLog for NoEvents {
    fn emit(&self, _: Event) {}
}

/// Writes every event as one line on stderr, prefixed `pipeline:`.
#[derive(Debug, Clone, Copy, Default)]
pub struct StderrEventLog;

impl EventLog for StderrEventLog {
    fn emit(&self, event: Event) {
        eprintln!("pipeline: {event}");
    }
}

/// An [`EventLog`] that keeps every event for a test to read back.
#[derive(Debug, Clone, Default)]
pub struct InMemoryEventLog {
    events: Arc<Mutex<Vec<Event>>>,
}

impl InMemoryEventLog {
    /// An empty log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every event so far, in emission order.
    #[must_use]
    pub fn events(&self) -> Vec<Event> {
        lock_unpoisoned(&self.events).clone()
    }

    /// The events of `kind` so far, in emission order.
    #[must_use]
    pub fn of_kind(&self, kind: EventKind) -> Vec<Event> {
        lock_unpoisoned(&self.events)
            .iter()
            .filter(|event| event.kind == kind)
            .cloned()
            .collect()
    }
}

impl EventLog for InMemoryEventLog {
    fn emit(&self, event: Event) {
        lock_unpoisoned(&self.events).push(event);
    }
}
