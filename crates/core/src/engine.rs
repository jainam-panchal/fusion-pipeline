//! The engine: N worker threads pulling envelopes from a bounded intake and walking each
//! record through the DAG.
//!
//! Every branch of a record ends in a sink success, an intentional drop, or a failure. The
//! source message is acknowledged once all branches have ended without failure and
//! negatively acknowledged otherwise. Because a worker walks the graph synchronously, the
//! outstanding-branch count is the recursion itself: every branch runs to its end even after
//! one has failed, so the nak fires once all of them have finished, as the spec asks. The
//! one exception is a panic, which unwinds past the remaining branches; the record is still
//! nakked, so the outcome is safe.
//!
//! Records are copy-on-write across branches: a fan-out hands every branch the same
//! `Arc<Record>`. A sink reads through the shared pointer; a stage takes ownership, which
//! copies only while another branch still holds the record. A mutation on one branch is
//! therefore never visible on another. The copy is per stage, not per mutation: a stage that
//! would not have mutated the record still copies while a sibling holds it. Branches run in
//! file order of the consumers, so with a sink and a stage on one label, listing the sink
//! first means no copy at all.

use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::config::SOURCE_ID;
use crate::dag::NodeIndex;
use crate::events::{Event, EventKind};
use crate::io::{Envelope, Failure, FailureKind, Intake, Outgoing, Source, SourceError};
use crate::meta::{IngestionTime, Meta, Rejection, unix_nanos_now};
use crate::metrics::{Labels, Metrics};
use crate::pipeline::{CompiledNode, Pipeline};
use crate::record::Record;
use crate::signals::Signals;
use crate::stage::{DropReason, StageEnvironment, StageOutput};
use crate::state::{StateError, StateErrorPolicy, StateStore, StateStoreFactory};
use crate::trace::{Detail, Settlement, SpanOutcome, TraceBuffer, TraceContext, TraceKey};

/// Envelopes buffered between the source and the workers, per worker.
const INTAKE_DEPTH_PER_WORKER: usize = 64;

/// Errors from starting or stopping the engine.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EngineError {
    /// A thread could not be spawned.
    #[error("could not spawn {thread} thread")]
    Spawn {
        /// Which thread: `source` or `worker`.
        thread: &'static str,
        /// The OS error.
        #[source]
        source: std::io::Error,
    },
    /// The source returned an error.
    #[error(transparent)]
    Source(#[from] SourceError),
    /// A worker's state store connection could not be opened.
    #[error("could not open the state store for worker {index}: {source}")]
    State {
        /// Index of the worker whose connection failed.
        index: usize,
        /// What the store said.
        #[source]
        source: StateError,
    },
    /// The source thread panicked.
    #[error("source thread panicked")]
    SourcePanicked,
    /// A worker thread panicked.
    #[error("worker thread {index} panicked")]
    WorkerPanicked {
        /// Index of the worker that panicked.
        index: usize,
    },
}

/// A running engine. Call [`Engine::join`] to wait for the source to finish and the workers
/// to drain.
#[derive(Debug)]
pub struct Engine {
    source: JoinHandle<Result<(), SourceError>>,
    workers: Vec<JoinHandle<()>>,
}

impl Engine {
    /// Start `worker_count` worker threads and the source thread.
    ///
    /// `worker_count` of zero is treated as one. Use [`Pipeline::worker_count`] to honour the
    /// config's `workers` setting and its one-per-core default. Every measurement, event and
    /// kept record trace goes to `signals`; a [`Metrics`] converts into signals that only
    /// measure, and [`Signals::noop`] discards everything. When any node uses state,
    /// one connection per worker is opened from `state` before the first thread starts, so
    /// an unreachable store fails here; a pipeline with no stateful node never touches it.
    ///
    /// # Errors
    ///
    /// [`EngineError::State`] when a state store connection cannot be opened,
    /// [`EngineError::Spawn`] when the OS refuses a thread.
    pub fn start(
        pipeline: Pipeline,
        source: Box<dyn Source>,
        worker_count: usize,
        signals: impl Into<Signals>,
        state: Arc<dyn StateStoreFactory>,
    ) -> Result<Self, EngineError> {
        let signals = signals.into();
        let worker_count = worker_count.max(1);
        let stores = if pipeline.uses_state() {
            (0..worker_count)
                .map(|index| {
                    state
                        .open()
                        .map(Arc::from)
                        .map_err(|source| EngineError::State { index, source })
                })
                .collect::<Result<Vec<Arc<dyn StateStore>>, _>>()?
        } else {
            (0..worker_count)
                .map(|_| Arc::new(NoStateStore) as Arc<dyn StateStore>)
                .collect()
        };
        let pipeline = Arc::new(pipeline);
        let (tx, rx) =
            crossbeam_channel::bounded::<Envelope>(worker_count * INTAKE_DEPTH_PER_WORKER);

        let mut workers = Vec::with_capacity(worker_count);
        for (i, store) in stores.into_iter().enumerate() {
            let rx = rx.clone();
            let pipeline = Arc::clone(&pipeline);
            let signals = signals.clone();
            let handle = thread::Builder::new()
                .name(format!("pipeline-worker-{i}"))
                .spawn(move || {
                    let environment = StageEnvironment::new(
                        pipeline.name().to_owned(),
                        store,
                        signals.metrics().clone(),
                    );
                    let walker = Walker {
                        pipeline: &pipeline,
                        metrics: signals.metrics(),
                        signals: &signals,
                        environment: &environment,
                    };
                    let mut spans = TraceBuffer::new();
                    for envelope in rx {
                        walker.handle(envelope, &mut spans);
                    }
                })
                .map_err(|source| EngineError::Spawn {
                    thread: "worker",
                    source,
                })?;
            workers.push(handle);
        }

        let intake = Intake::new(tx);
        let source = thread::Builder::new()
            .name("pipeline-source".to_owned())
            .spawn(move || source.run(intake))
            .map_err(|source| EngineError::Spawn {
                thread: "source",
                source,
            })?;

        Ok(Self { source, workers })
    }

    /// Wait for the source to finish, then for every worker to drain and exit.
    ///
    /// # Errors
    ///
    /// The source's error if it failed, or a panic report for the source or a worker.
    pub fn join(self) -> Result<(), EngineError> {
        let source_result = self
            .source
            .join()
            .map_err(|_| EngineError::SourcePanicked)?;
        let mut first_worker_panic = None;
        for (index, worker) in self.workers.into_iter().enumerate() {
            if worker.join().is_err() && first_worker_panic.is_none() {
                first_worker_panic = Some(EngineError::WorkerPanicked { index });
            }
        }
        source_result?;
        first_worker_panic.map_or(Ok(()), Err)
    }
}

/// One record's walk through the graph: its [`Meta`], the node it is in, whether any branch
/// has failed, and the span drafts of the nodes it visited. `SOURCE_ID` is the `stage` label
/// for decisions the engine takes before any node runs.
struct Walk<'p: 't, 't> {
    /// Borrowed from the handling call, not owned, so labels built on its tenant never
    /// borrow the walk itself and `fail` can take them while the walk is mutated.
    meta: &'t Meta,
    /// What the record's trace ids derive from.
    key: TraceKey,
    /// The labels and span of the node whose stage or sink is running, so a panic is
    /// charged to it.
    at: Option<(Labels<'t>, usize)>,
    /// The first failure of the walk, the one the nak reports.
    failure: Option<Failure>,
    metrics: &'p Metrics,
    signals: &'p Signals,
    /// The worker's span drafts, begun for this record.
    spans: &'t mut TraceBuffer<'p>,
}

impl<'p> Walk<'p, '_> {
    /// Count and log a failure of the node `labels` names, whose span is `span`, and keep
    /// it if it is the walk's first. Returns the error text for the span.
    fn fail(
        &mut self,
        labels: &Labels<'_>,
        span: Option<usize>,
        kind: FailureKind,
        error: &dyn std::fmt::Display,
    ) -> String {
        let node = labels.stage().unwrap_or(SOURCE_ID);
        let error = error.to_string();
        let delivery = self.meta.delivery_count;
        self.metrics.errored(labels);
        self.signals.emit(Event {
            kind: EventKind::StageError,
            record_id: Some(self.meta.record_id),
            tenant: Arc::clone(&self.meta.tenant),
            node: node.to_owned(),
            reason: Some(kind),
            delivery_count: delivery,
            stream_sequence: None,
            message: error.clone(),
            trace: Some(span.map_or_else(
                || self.key.delivery_context(delivery),
                |span| TraceBuffer::context(self.key, delivery, span),
            )),
        });
        self.failure.get_or_insert_with(|| Failure {
            node: node.to_owned(),
            record_id: Some(self.meta.record_id),
            kind,
            error: error.clone(),
        });
        error
    }
}

/// The store a worker holds when no node uses state. Unreachable in practice: the handle
/// refuses an undeclared stage before the store, and a declared one means connections were
/// opened. Kept so the worker always holds a store.
struct NoStateStore;

impl StateStore for NoStateStore {
    fn set_nx(&self, _: &str, _: &[u8], _: Duration) -> Result<Option<Vec<u8>>, StateError> {
        Err(no_state_store())
    }

    fn set(&self, _: &str, _: &[u8], _: Duration) -> Result<(), StateError> {
        Err(no_state_store())
    }

    fn compare_and_set(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: Duration,
    ) -> Result<Option<Vec<u8>>, StateError> {
        Err(no_state_store())
    }

    fn get(&self, _: &str) -> Result<Option<Vec<u8>>, StateError> {
        Err(no_state_store())
    }

    fn incr(&self, _: &str, _: i64, _: Duration) -> Result<i64, StateError> {
        Err(no_state_store())
    }

    fn del(&self, _: &str) -> Result<(), StateError> {
        Err(no_state_store())
    }
}

fn no_state_store() -> StateError {
    StateError::new("no state store connection was opened for this worker")
}

struct Walker<'p> {
    pipeline: &'p Pipeline,
    metrics: &'p Metrics,
    /// Where events and kept traces go.
    signals: &'p Signals,
    /// What every stage this worker runs is given: the pipeline name, this worker's
    /// connection to the shared state store, the metrics.
    environment: &'p StageEnvironment,
}

impl<'p> Walker<'p> {
    fn handle(&self, envelope: Envelope, spans: &mut TraceBuffer<'p>) {
        let Envelope {
            record,
            arrival,
            ack,
        } = envelope;
        spans.begin();
        let resolved = Meta::resolve(&record, &arrival);
        let tenant = Arc::clone(match &resolved {
            Ok(meta) => &meta.tenant,
            Err(rejected) => &rejected.tenant,
        });
        let delivery = arrival.delivery_count;
        // `source` is a node like any other on the metrics: every record the source hands
        // over counts in, every record that enters the graph counts out, and the engine's
        // own rejections are its drops. Intake is then one series whatever the first node
        // is called.
        let source = Labels::new(&tenant, SOURCE_ID);
        self.metrics.records_in(&source);
        if let Some(bytes) = arrival.bytes {
            self.metrics.bytes_in(&tenant, bytes);
        }
        if delivery > 1 {
            self.metrics.source_redelivery(&tenant);
            let meta = resolved.as_ref().ok();
            self.signals.emit(Event {
                kind: EventKind::Redelivery,
                record_id: meta.map(|meta| meta.record_id),
                tenant: Arc::clone(&tenant),
                node: SOURCE_ID.to_owned(),
                reason: None,
                delivery_count: delivery,
                stream_sequence: None,
                message: String::new(),
                // A walked redelivery's trace is always kept, so the link resolves.
                trace: meta.map(|meta| TraceKey::of(meta).delivery_context(delivery)),
            });
        }
        let meta = match resolved {
            Ok(meta) => meta,
            Err(rejected) => {
                match rejected.reason {
                    // Spec: the idempotency guarantee has no unguarded path, so a record
                    // without an id is nak'd. The record was not forwarded, which is the
                    // drop the spec counts under `missing_id`; the message is nak'd, which
                    // is the nak it counts.
                    Rejection::MissingId => {
                        self.metrics.dropped(&source, DropReason::MissingId);
                        self.metrics.source_nak(&tenant);
                        let failure =
                            Failure::at_source(FailureKind::MissingId, "the record has no `id`");
                        self.log_nak(&tenant, delivery, &failure, None);
                        ack.nak(None, failure);
                    }
                    // Spec: metric and span are rejected by the engine (reason
                    // `invalid_record`). Rejection is a drop, and drops are acked.
                    Rejection::NotLog => {
                        self.metrics.dropped(&source, DropReason::InvalidRecord);
                        ack.ack();
                    }
                }
                return;
            }
        };
        self.metrics.records_out(&source, 1);

        let key = TraceKey::of(&meta);
        let mut walk = Walk {
            meta: &meta,
            key,
            at: None,
            failure: None,
            metrics: self.metrics,
            signals: self.signals,
            spans,
        };
        // A stage or sink that panics must not take the ack handle down with it: contain the
        // panic (in dev; release aborts) and settle the record as failed.
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
            let targets = self.pipeline.dag().source_successors().iter().copied();
            self.fan_out(targets, Arc::new(record), &mut walk, None);
        }));
        if outcome.is_err() {
            const PANICKED: &str = "stage or sink panicked";
            let (labels, span) = walk
                .at
                .map_or((source, None), |(labels, span)| (labels, Some(span)));
            walk.fail(&labels, span, FailureKind::Panic, &PANICKED);
            walk.spans
                .fail_open(Instant::now(), FailureKind::Panic, PANICKED);
        }
        let Walk { failure, spans, .. } = walk;
        // The trace of every failed or redelivered walk is kept, so a log line about it
        // always finds it; of the rest, the sampled share.
        let settlement = if failure.is_some() {
            Settlement::Nak
        } else {
            Settlement::Ack
        };
        if failure.is_some() || delivery > 1 || self.signals.sampling().keeps(key) {
            self.signals.export(spans.finish(&meta, settlement));
        }
        if let Some(failure) = failure {
            self.metrics.source_nak(&tenant);
            self.log_nak(
                &tenant,
                delivery,
                &failure,
                Some(key.delivery_context(delivery)),
            );
            ack.nak(None, failure);
        } else {
            // End to end is measured on the ack only: a nakked record comes back and is
            // measured when it finally settles. A worker-clock ingestion time says nothing
            // about how long the record has been on its way.
            if let IngestionTime::Reported(nanos) = meta.ingestion_time {
                if let Some(elapsed) = since_unix_nanos(nanos) {
                    self.metrics.end_to_end(&tenant, elapsed);
                }
            }
            ack.ack();
        }
    }

    /// Log the nak of a record for `failure`, before the source hears of it.
    fn log_nak(
        &self,
        tenant: &Arc<str>,
        delivery: u64,
        failure: &Failure,
        trace: Option<TraceContext>,
    ) {
        self.signals.emit(Event {
            kind: EventKind::Nak,
            record_id: failure.record_id,
            tenant: Arc::clone(tenant),
            node: failure.node.clone(),
            reason: Some(failure.kind),
            delivery_count: delivery,
            stream_sequence: None,
            message: failure.error.clone(),
            trace,
        });
    }

    /// Deliver `record` to every node in `targets`, sharing it until a branch needs to own it.
    /// The last target receives the walker's own reference, so a lone branch never copies.
    /// `parent` is the span of the node the record comes from, `None` for the source.
    fn fan_out(
        &self,
        targets: impl Iterator<Item = NodeIndex>,
        record: Arc<Record>,
        walk: &mut Walk<'p, '_>,
        parent: Option<usize>,
    ) {
        let mut targets = targets.peekable();
        while let Some(target) = targets.next() {
            if targets.peek().is_none() {
                self.run_node(target, record, walk, parent);
                return;
            }
            self.run_node(target, Arc::clone(&record), walk, parent);
        }
    }

    fn run_node(
        &self,
        index: NodeIndex,
        record: Arc<Record>,
        walk: &mut Walk<'p, '_>,
        parent: Option<usize>,
    ) {
        let dag = self.pipeline.dag();
        let node_id = dag.node(index).id.as_str();
        let metrics = self.metrics;
        let node = self.pipeline.node(index);
        let engine = match node {
            CompiledNode::Sink(_) => None,
            CompiledNode::Stage(stage) => stage.engine_label(),
        };
        let meta = walk.meta;
        let labels = Labels::new(&meta.tenant, node_id).with_engine(engine);
        metrics.records_in(&labels);
        match node {
            CompiledNode::Sink(sink) => {
                let started = Instant::now();
                let span = walk.spans.open(node_id, parent, started);
                walk.at = Some((labels, span));
                let written = sink.write(&[Outgoing {
                    meta,
                    record: &record,
                }]);
                let ended = Instant::now();
                metrics.sink_publish_duration(&labels, ended - started);
                match written {
                    Ok(bytes) => {
                        metrics.records_out(&labels, 1);
                        metrics.bytes_out(&labels, bytes);
                        walk.spans
                            .close(span, ended, SpanOutcome::Written, Detail::None);
                    }
                    Err(err) => {
                        metrics.sink_publish_error(&labels);
                        let error = walk.fail(&labels, Some(span), FailureKind::SinkError, &err);
                        walk.spans.close(
                            span,
                            ended,
                            SpanOutcome::Error,
                            Detail::Failure(FailureKind::SinkError, error),
                        );
                    }
                }
            }
            CompiledNode::Stage(stage) => {
                let ctx = self.environment.context(meta, node_id, stage.as_ref());
                // Copies only if another branch still shares the record.
                let owned = Arc::unwrap_or_clone(record);
                let started = Instant::now();
                let span = walk.spans.open(node_id, parent, started);
                walk.at = Some((labels, span));
                let output = stage.process(owned, &ctx);
                let ended = Instant::now();
                metrics.stage_duration(&labels, ended - started);
                let consumers = |label| dag.consumers(index, label);
                match output {
                    StageOutput::Pass(record) => {
                        metrics.records_out(&labels, 1);
                        walk.spans
                            .close(span, ended, SpanOutcome::Pass, Detail::None);
                        self.fan_out(consumers(None), Arc::new(record), walk, Some(span));
                    }
                    // The stage could not reach the store and hands the record back. The
                    // policy is the node's, applied here: `pass` forwards the record as if
                    // the node were not there (the failed operation is already on
                    // `state_errors_total`); `nak` fails it like any stage error.
                    StageOutput::StateError { record, error } => match stage.on_state_error() {
                        StateErrorPolicy::Pass => {
                            metrics.records_out(&labels, 1);
                            walk.spans.close(
                                span,
                                ended,
                                SpanOutcome::StateErrorPass,
                                Detail::None,
                            );
                            self.fan_out(consumers(None), Arc::new(record), walk, Some(span));
                        }
                        StateErrorPolicy::Nak => {
                            let error =
                                walk.fail(&labels, Some(span), FailureKind::StateError, &error);
                            walk.spans.close(
                                span,
                                ended,
                                SpanOutcome::Error,
                                Detail::Failure(FailureKind::StateError, error),
                            );
                        }
                    },
                    StageOutput::Split(records) => {
                        let count = records.len() as u64;
                        metrics.records_out(&labels, count);
                        walk.spans
                            .close(span, ended, SpanOutcome::Split, Detail::Split(count));
                        for record in records {
                            self.fan_out(consumers(None), Arc::new(record), walk, Some(span));
                        }
                    }
                    StageOutput::Drop(reason) => {
                        metrics.dropped(&labels, reason);
                        walk.spans
                            .close(span, ended, SpanOutcome::Drop, Detail::Drop(reason));
                    }
                    StageOutput::Routed(label, record) => {
                        metrics.records_out(&labels, 1);
                        let mut targets = consumers(Some(&label)).peekable();
                        if targets.peek().is_none() {
                            // Load validation guarantees every declared label a consumer, so
                            // this is a stage emitting a label it never declared.
                            let error = walk.fail(
                                &labels,
                                Some(span),
                                FailureKind::StageError,
                                &format!("no consumer for route label `{label}`"),
                            );
                            walk.spans.close(
                                span,
                                ended,
                                SpanOutcome::Error,
                                Detail::Failure(FailureKind::StageError, error),
                            );
                            return;
                        }
                        walk.spans
                            .close(span, ended, SpanOutcome::Routed, Detail::None);
                        self.fan_out(targets, Arc::new(record), walk, Some(span));
                        // The label is borrowed by the targets until the branch is done; it
                        // was allocated by the stage, so the span takes it rather than a
                        // copy.
                        walk.spans.detail(span, Detail::Routed(label));
                    }
                    StageOutput::Error(err) => {
                        let error = walk.fail(&labels, Some(span), FailureKind::StageError, &err);
                        walk.spans.close(
                            span,
                            ended,
                            SpanOutcome::Error,
                            Detail::Failure(FailureKind::StageError, error),
                        );
                    }
                }
            }
        }
    }
}

/// How long ago `nanos` (nanoseconds since the Unix epoch) was; `None` if it is in the future
/// or the clock is before the epoch.
fn since_unix_nanos(nanos: u64) -> Option<Duration> {
    unix_nanos_now()
        .checked_sub(nanos)
        .map(Duration::from_nanos)
}
