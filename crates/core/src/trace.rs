//! Record traces: one trace per record, one span per delivery and one per node it visited,
//! kept or dropped when the record settles (ADR 0006).
//!
//! A trace's ids are functions of the record: [`TraceKey`] mixes the record id with the
//! tenant, and the trace id and every span id are derived from it. Every delivery of a
//! record therefore lands in one trace, a log line can name the trace without the trace
//! existing yet, and the same key decides sampling, so a redelivered record gets the same
//! answer. The engine keeps a delivery's trace when a branch failed, when the delivery count
//! is above one, or when [`TraceSampling`] keeps the key; the rest are never built.
//!
//! During a walk the engine writes cheap span drafts into a buffer each worker reuses. Only
//! a kept trace becomes a [`RecordTrace`], with wall-clock times anchored on one clock
//! reading per walk, and goes to the [`TraceSink`]. [`InMemoryTraceSink`] is the fake for
//! tests; the OTLP sink lives in the telemetry crate.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime};

use crate::closed_set::closed_set;
use crate::hash::{fnv1a64, mix};
use crate::io::FailureKind;
use crate::memory::lock_unpoisoned;
use crate::meta::Meta;
use crate::record::RecordId;
use crate::stage::DropReason;

/// The share of passing records traced when nothing configures one: 1%.
pub const DEFAULT_SAMPLE_RATIO: f64 = 0.01;

/// Mixed into every trace key, so that record id 0 does not give the all-zero trace id,
/// which OpenTelemetry treats as invalid, and so that trace sampling is not correlated with
/// `sample`'s `random` coin.
const TRACE_SALT: u64 = 0x7472_6163_655f_6964;

/// Mixed into every span id, apart from the trace key.
const SPAN_SALT: u64 = 0x7370_616e_5f69_6421;

/// A 128-bit trace id, never zero. Displays as 32 lowercase hex digits, as Tempo and Loki
/// write it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TraceId(pub u128);

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

/// A 64-bit span id, never zero. Displays as 16 lowercase hex digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpanId(pub u64);

impl fmt::Display for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Where a log line's record trace is: the trace and the span the line is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceContext {
    /// The record's trace.
    pub trace_id: TraceId,
    /// The delivery or node span.
    pub span_id: SpanId,
}

/// The one value a record's trace ids and its sampling decision derive from:
/// `mix(record id ^ fnv1a(tenant) ^ salt)`. Two tenants that reuse an id get different keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceKey {
    key: u64,
    record_id: u64,
}

impl TraceKey {
    /// The key of the record `record_id` of `tenant`.
    #[must_use]
    pub const fn new(record_id: RecordId, tenant: &str) -> Self {
        Self {
            key: mix(record_id.0 ^ fnv1a64(tenant.as_bytes()) ^ TRACE_SALT),
            record_id: record_id.0,
        }
    }

    /// The key of the record `meta` describes.
    #[must_use]
    pub fn of(meta: &Meta) -> Self {
        Self::new(meta.record_id, &meta.tenant)
    }

    /// The record's trace id: the key in the upper half, the record id in the lower, so the
    /// id can be read off a trace id.
    #[must_use]
    pub const fn trace_id(self) -> TraceId {
        let id = ((self.key as u128) << 64) | self.record_id as u128;
        TraceId(if id == 0 { 1 } else { id })
    }

    /// The id of the span visited `visit`-th in the `delivery_count`-th delivery; visit 0 is
    /// the delivery span itself. A redelivery gets new span ids in the same trace.
    #[must_use]
    pub const fn span_id(self, delivery_count: u64, visit: u32) -> SpanId {
        let position = mix((delivery_count << 32) ^ visit as u64 ^ SPAN_SALT);
        let id = mix(self.key ^ position);
        SpanId(if id == 0 { 1 } else { id })
    }

    /// The id of the `delivery_count`-th delivery's own span.
    #[must_use]
    pub const fn delivery_span_id(self, delivery_count: u64) -> SpanId {
        self.span_id(delivery_count, 0)
    }

    /// The context of the `delivery_count`-th delivery's own span.
    #[must_use]
    pub const fn delivery_context(self, delivery_count: u64) -> TraceContext {
        TraceContext {
            trace_id: self.trace_id(),
            span_id: self.delivery_span_id(delivery_count),
        }
    }
}

/// The share of passing, first-delivery records whose trace is kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceSampling {
    /// Keys at or below this are kept; `None` keeps none.
    threshold: Option<u64>,
}

impl TraceSampling {
    /// Keep `ratio` of the keys: 0 or less (or NaN) keeps none, 1 or more keeps all.
    #[must_use]
    pub fn ratio(ratio: f64) -> Self {
        let threshold = if ratio.is_nan() || ratio <= 0.0 {
            None
        } else if ratio >= 1.0 {
            Some(u64::MAX)
        } else {
            // Below 2^64 by construction; the cast saturates anyway.
            Some((2f64.powi(64) * ratio) as u64)
        };
        Self { threshold }
    }

    /// Whether the trace of a record with `key` is kept when nothing else keeps it.
    #[must_use]
    pub fn keeps(self, key: TraceKey) -> bool {
        self.threshold.is_some_and(|threshold| key.key <= threshold)
    }
}

impl Default for TraceSampling {
    fn default() -> Self {
        Self::ratio(DEFAULT_SAMPLE_RATIO)
    }
}

closed_set! {
    /// How a delivery settled: the `settlement` attribute of its span.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum Settlement {
        /// Every branch ended in a sink success or a drop.
        Ack = "ack",
        /// A branch failed.
        Nak = "nak",
    }
}

closed_set! {
    /// What a node did with a record: the `outcome` attribute of its span.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum SpanOutcome {
        /// The stage passed the record on.
        Pass = "pass",
        /// The route sent it to a label (`label`).
        Routed = "routed",
        /// The stage split it (`records` out).
        Split = "split",
        /// The stage dropped it (`reason`).
        Drop = "drop",
        /// The stage could not reach the state store and its node passed the record on.
        StateErrorPass = "state_error_pass",
        /// The sink wrote it with durable acceptance.
        Written = "written",
        /// The node failed (`failure`), and the span has error status.
        Error = "error",
    }
}

/// One node's span in a kept trace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSpan {
    /// This span.
    pub span_id: SpanId,
    /// The span of the node the record came from, or the delivery span.
    pub parent_span_id: SpanId,
    /// The node id, which is also the span name.
    pub node: String,
    /// When the node started on the record.
    pub start: SystemTime,
    /// When the node finished with it, before its consumers ran.
    pub end: SystemTime,
    /// What the node did.
    pub outcome: SpanOutcome,
    /// The drop reason, for [`SpanOutcome::Drop`].
    pub reason: Option<DropReason>,
    /// The failure kind, for [`SpanOutcome::Error`].
    pub failure: Option<FailureKind>,
    /// The route label, for [`SpanOutcome::Routed`].
    pub label: Option<String>,
    /// How many records left, for [`SpanOutcome::Split`].
    pub records: Option<u64>,
    /// The error text, for [`SpanOutcome::Error`].
    pub error: Option<String>,
}

/// One kept delivery of one record: its delivery span and the spans of the nodes it visited,
/// in visit order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordTrace {
    /// The record's trace.
    pub trace_id: TraceId,
    /// The delivery span.
    pub span_id: SpanId,
    /// The `Meta` record id.
    pub record_id: RecordId,
    /// The `Meta` tenant.
    pub tenant: Arc<str>,
    /// This delivery's count.
    pub delivery_count: u64,
    /// How the delivery settled.
    pub settlement: Settlement,
    /// When the engine took the record.
    pub start: SystemTime,
    /// When it settled the record.
    pub end: SystemTime,
    /// The nodes visited.
    pub spans: Vec<NodeSpan>,
}

/// Where kept traces go. Implemented by exporters; shared across threads. Must not block for
/// long: the engine exports from its workers.
pub trait TraceSink: Send + Sync {
    /// Export one kept delivery.
    fn export(&self, trace: RecordTrace);
}

/// Discards every trace.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoTraces;

impl TraceSink for NoTraces {
    fn export(&self, _: RecordTrace) {}
}

/// A [`TraceSink`] that keeps every trace for a test to read back.
#[derive(Debug, Clone, Default)]
pub struct InMemoryTraceSink {
    traces: Arc<Mutex<Vec<RecordTrace>>>,
}

impl InMemoryTraceSink {
    /// An empty sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every trace so far, in export order.
    #[must_use]
    pub fn traces(&self) -> Vec<RecordTrace> {
        lock_unpoisoned(&self.traces).clone()
    }
}

impl TraceSink for InMemoryTraceSink {
    fn export(&self, trace: RecordTrace) {
        lock_unpoisoned(&self.traces).push(trace);
    }
}

/// What a node span says beyond its outcome. Owned values here were already allocated by
/// the stage (a route label) or are built only on failure (the error text).
#[derive(Debug)]
pub(crate) enum Detail {
    None,
    Drop(DropReason),
    Routed(String),
    Split(u64),
    Failure(FailureKind, String),
}

/// One node's span while the walk runs.
#[derive(Debug)]
struct Draft<'p> {
    node: &'p str,
    parent: Option<usize>,
    start: Instant,
    end: Option<Instant>,
    outcome: SpanOutcome,
    detail: Detail,
}

/// A worker's span drafts for the record it is walking, cleared and reused for the next.
#[derive(Debug)]
pub(crate) struct TraceBuffer<'p> {
    started: Instant,
    started_at: SystemTime,
    drafts: Vec<Draft<'p>>,
}

impl<'p> TraceBuffer<'p> {
    pub(crate) fn new() -> Self {
        Self {
            started: Instant::now(),
            started_at: SystemTime::now(),
            drafts: Vec::new(),
        }
    }

    /// Start a new walk: forget the last one and anchor the clock.
    pub(crate) fn begin(&mut self) {
        self.drafts.clear();
        self.started = Instant::now();
        self.started_at = SystemTime::now();
    }

    /// Open the span of `node`, reached from the span `parent` (`None` for the source).
    /// Returns its handle for [`TraceBuffer::close`] and for its consumers' parent.
    pub(crate) fn open(&mut self, node: &'p str, parent: Option<usize>, start: Instant) -> usize {
        self.drafts.push(Draft {
            node,
            parent,
            start,
            end: None,
            outcome: SpanOutcome::Pass,
            detail: Detail::None,
        });
        self.drafts.len() - 1
    }

    /// Close the span `handle` at `end` with what the node did.
    pub(crate) fn close(
        &mut self,
        handle: usize,
        end: Instant,
        outcome: SpanOutcome,
        detail: Detail,
    ) {
        if let Some(draft) = self.drafts.get_mut(handle) {
            draft.end = Some(end);
            draft.outcome = outcome;
            draft.detail = detail;
        }
    }

    /// Replace the detail of the span `handle`.
    pub(crate) fn detail(&mut self, handle: usize, detail: Detail) {
        if let Some(draft) = self.drafts.get_mut(handle) {
            draft.detail = detail;
        }
    }

    /// Close every span still open, as failed with `failure`: a panic unwound past them.
    pub(crate) fn fail_open(&mut self, end: Instant, failure: FailureKind, error: &str) {
        for draft in self.drafts.iter_mut().filter(|d| d.end.is_none()) {
            draft.end = Some(end);
            draft.outcome = SpanOutcome::Error;
            draft.detail = Detail::Failure(failure, error.to_owned());
        }
    }

    /// The span handle's context, for a log line about the node.
    pub(crate) fn context(key: TraceKey, delivery_count: u64, handle: usize) -> TraceContext {
        TraceContext {
            trace_id: key.trace_id(),
            span_id: key.span_id(delivery_count, visit(handle)),
        }
    }

    /// The kept trace of this walk.
    pub(crate) fn finish(&mut self, meta: &Meta, settlement: Settlement) -> RecordTrace {
        let key = TraceKey::of(meta);
        let delivery = meta.delivery_count;
        let root = key.delivery_span_id(delivery);
        let now = Instant::now();
        let (started, started_at) = (self.started, self.started_at);
        let at = |instant: Instant| started_at + instant.saturating_duration_since(started);
        let spans = self
            .drafts
            .drain(..)
            .enumerate()
            .map(|(handle, draft)| {
                let (reason, failure, label, records, error) = match draft.detail {
                    Detail::None => (None, None, None, None, None),
                    Detail::Drop(reason) => (Some(reason), None, None, None, None),
                    Detail::Routed(label) => (None, None, Some(label), None, None),
                    Detail::Split(records) => (None, None, None, Some(records), None),
                    Detail::Failure(kind, error) => (None, Some(kind), None, None, Some(error)),
                };
                NodeSpan {
                    span_id: key.span_id(delivery, visit(handle)),
                    parent_span_id: draft
                        .parent
                        .map_or(root, |parent| key.span_id(delivery, visit(parent))),
                    node: draft.node.to_owned(),
                    start: at(draft.start),
                    end: at(draft.end.unwrap_or(now)),
                    outcome: draft.outcome,
                    reason,
                    failure,
                    label,
                    records,
                    error,
                }
            })
            .collect();
        RecordTrace {
            trace_id: key.trace_id(),
            span_id: root,
            record_id: meta.record_id,
            tenant: Arc::clone(&meta.tenant),
            delivery_count: delivery,
            settlement,
            start: started_at,
            end: at(now),
            spans,
        }
    }
}

/// The visit index of the span `handle`: the delivery span is visit 0.
fn visit(handle: usize) -> u32 {
    u32::try_from(handle).map_or(u32::MAX, |h| h.saturating_add(1))
}
