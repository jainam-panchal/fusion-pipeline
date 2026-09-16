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
pub struct TraceId(u128);

impl TraceId {
    /// The id as a number.
    #[must_use]
    pub const fn get(self) -> u128 {
        self.0
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

/// A 64-bit span id, never zero. Displays as 16 lowercase hex digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpanId(u64);

impl SpanId {
    /// The id as a number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

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

/// What a node did with a record, with what goes with it. `L` is the route label: owned on a
/// kept trace, borrowed from the pipeline while the walk runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpanResult<L = String> {
    /// The stage passed the record on.
    Pass,
    /// The route sent it to this label.
    Routed(L),
    /// The stage split it into this many records.
    Split(u64),
    /// The stage dropped it for this reason.
    Drop(DropReason),
    /// The stage could not reach the state store and its node passed the record on.
    StateErrorPass,
    /// The sink wrote it with durable acceptance.
    Written,
    /// The node failed.
    Error {
        /// The kind of failure.
        failure: FailureKind,
        /// What the node said.
        error: String,
    },
}

impl<L> SpanResult<L> {
    /// The `outcome` attribute this result exports as.
    #[must_use]
    pub const fn outcome(&self) -> SpanOutcome {
        match self {
            Self::Pass => SpanOutcome::Pass,
            Self::Routed(_) => SpanOutcome::Routed,
            Self::Split(_) => SpanOutcome::Split,
            Self::Drop(_) => SpanOutcome::Drop,
            Self::StateErrorPass => SpanOutcome::StateErrorPass,
            Self::Written => SpanOutcome::Written,
            Self::Error { .. } => SpanOutcome::Error,
        }
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
    pub result: SpanResult,
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

/// One node's span while the walk runs.
#[derive(Debug)]
struct Draft<'p> {
    node: &'p str,
    parent: Option<usize>,
    start: Instant,
    /// `None` while the node runs.
    result: Option<SpanResult<&'p str>>,
    end: Instant,
}

/// The most drafts a buffer keeps room for between walks: a walk that split into more gives
/// the memory back when it ends.
const KEPT_CAPACITY: usize = 256;

/// A worker's span drafts for the record it is walking, cleared and reused for the next.
/// When nothing traces it records nothing and reads no clock; its handles are then only
/// counters.
#[derive(Debug)]
pub(crate) struct TraceBuffer<'p> {
    tracing: bool,
    started: Instant,
    started_at: SystemTime,
    drafts: Vec<Draft<'p>>,
    /// Handles given out this walk, which is the draft count when tracing.
    opened: usize,
}

impl<'p> TraceBuffer<'p> {
    pub(crate) fn new(tracing: bool) -> Self {
        Self {
            tracing,
            started: Instant::now(),
            started_at: SystemTime::now(),
            drafts: Vec::new(),
            opened: 0,
        }
    }

    /// Start a new walk: forget the last one and anchor the clock.
    pub(crate) fn begin(&mut self) {
        self.opened = 0;
        if !self.tracing {
            return;
        }
        self.drafts.clear();
        self.drafts.shrink_to(KEPT_CAPACITY);
        self.started = Instant::now();
        self.started_at = SystemTime::now();
    }

    /// Open the span of `node`, reached from the span `parent` (`None` for the source).
    /// Returns its handle for [`TraceBuffer::close`] and for its consumers' parent.
    pub(crate) fn open(&mut self, node: &'p str, parent: Option<usize>, start: Instant) -> usize {
        let handle = self.opened;
        self.opened += 1;
        if !self.tracing {
            return handle;
        }
        self.drafts.push(Draft {
            node,
            parent,
            start,
            result: None,
            end: start,
        });
        handle
    }

    /// Whether this buffer takes drafts at all.
    pub(crate) const fn tracing(&self) -> bool {
        self.tracing
    }

    /// Close the span `handle` at `end` with what the node did.
    pub(crate) fn close(&mut self, handle: usize, end: Instant, result: SpanResult<&'p str>) {
        if let Some(draft) = self.drafts.get_mut(handle) {
            draft.end = end;
            draft.result = Some(result);
        }
    }

    /// Close every span still open, as failed with `failure`: a panic unwound past them.
    pub(crate) fn fail_open(&mut self, end: Instant, failure: FailureKind, error: &str) {
        for draft in self.drafts.iter_mut().filter(|d| d.result.is_none()) {
            draft.end = end;
            draft.result = Some(SpanResult::Error {
                failure,
                error: error.to_owned(),
            });
        }
    }

    /// The span handle's context, for a log line about the node.
    pub(crate) fn context(key: TraceKey, delivery_count: u64, handle: usize) -> TraceContext {
        TraceContext {
            trace_id: key.trace_id(),
            span_id: key.span_id(delivery_count, visit(handle)),
        }
    }

    /// The kept trace of this walk, of the record `meta` describes and whose key is `key`.
    pub(crate) fn finish(
        &mut self,
        key: TraceKey,
        meta: &Meta,
        settlement: Settlement,
    ) -> RecordTrace {
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
                // A span still open when the walk ended was left by a panic that
                // `fail_open` did not see; it ends now, as it was.
                let (result, end) = match draft.result {
                    Some(result) => (result, draft.end),
                    None => (SpanResult::Pass, now),
                };
                NodeSpan {
                    span_id: key.span_id(delivery, visit(handle)),
                    parent_span_id: draft
                        .parent
                        .map_or(root, |parent| key.span_id(delivery, visit(parent))),
                    node: draft.node.to_owned(),
                    start: at(draft.start),
                    end: at(end),
                    result: match result {
                        SpanResult::Pass => SpanResult::Pass,
                        SpanResult::Routed(label) => SpanResult::Routed(label.to_owned()),
                        SpanResult::Split(records) => SpanResult::Split(records),
                        SpanResult::Drop(reason) => SpanResult::Drop(reason),
                        SpanResult::StateErrorPass => SpanResult::StateErrorPass,
                        SpanResult::Written => SpanResult::Written,
                        SpanResult::Error { failure, error } => {
                            SpanResult::Error { failure, error }
                        }
                    },
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
