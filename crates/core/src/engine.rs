//! The engine: N worker threads pulling envelopes from a bounded intake and walking each
//! record through the DAG.
//!
//! Every branch of a record ends in a sink success, an intentional drop, or a failure. The
//! source message is acknowledged once all branches have ended without failure and
//! negatively acknowledged otherwise. Because a worker walks the graph synchronously, the
//! outstanding-branch count is the recursion itself: every branch runs to its end even after
//! one has failed, so the nak fires once all of them have finished, as the spec asks.
//!
//! Records are copy-on-write across branches: a fan-out hands every branch the same
//! `Arc<Record>`. A sink reads through the shared pointer; a stage takes ownership, which
//! copies only while another branch still holds the record. A mutation on one branch is
//! therefore never visible on another.

use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::dag::{Edge, NodeIndex};
use crate::io::{Envelope, Intake, Source, SourceError};
use crate::pipeline::{CompiledNode, Pipeline};
use crate::record::{Kind, Record, RecordId};
use crate::stage::{Context, StageOutput};

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
    /// config's `workers` setting and its one-per-core default.
    ///
    /// # Errors
    ///
    /// [`EngineError::Spawn`] when the OS refuses a thread.
    pub fn start(
        pipeline: Pipeline,
        source: Box<dyn Source>,
        worker_count: usize,
    ) -> Result<Self, EngineError> {
        let worker_count = worker_count.max(1);
        let pipeline = Arc::new(pipeline);
        let (tx, rx) =
            crossbeam_channel::bounded::<Envelope>(worker_count * INTAKE_DEPTH_PER_WORKER);

        let mut workers = Vec::with_capacity(worker_count);
        for i in 0..worker_count {
            let rx = rx.clone();
            let pipeline = Arc::clone(&pipeline);
            let handle = thread::Builder::new()
                .name(format!("pipeline-worker-{i}"))
                .spawn(move || {
                    for envelope in rx {
                        Walker {
                            pipeline: &pipeline,
                        }
                        .handle(envelope);
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

/// One record's walk through the graph: its id and whether any branch has failed.
struct Walk {
    record_id: RecordId,
    failed: bool,
}

impl Walk {
    fn fail(&mut self, node_id: &str, error: &dyn std::fmt::Display) {
        // Structured logging over OTLP lands with the telemetry ticket; until then the
        // failure is at least visible on stderr rather than swallowed.
        eprintln!(
            "pipeline: record {} failed at node `{node_id}`: {error}",
            self.record_id
        );
        self.failed = true;
    }
}

struct Walker<'p> {
    pipeline: &'p Pipeline,
}

impl Walker<'_> {
    fn handle(&self, envelope: Envelope) {
        let Envelope { record, ack } = envelope;

        let Some(record_id) = record.id else {
            // Spec: the idempotency guarantee has no unguarded path, so a record without an
            // id is nak'd (reason `missing_id`) rather than dropped.
            ack.nak(None);
            return;
        };
        if record.kind != Kind::Log {
            // Spec: metric and span are rejected by the engine (reason `invalid_record`).
            // Rejection is a drop, and drops are acked.
            ack.ack();
            return;
        }

        let mut walk = Walk {
            record_id,
            failed: false,
        };
        // A stage or sink that panics must not take the ack handle down with it: contain the
        // panic (in dev; release aborts) and settle the record as failed.
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
            let targets = self.pipeline.dag().source_successors().iter().copied();
            self.fan_out(targets, Arc::new(record), &mut walk);
        }));
        if outcome.is_err() {
            walk.fail("<panic>", &"stage or sink panicked");
        }
        if walk.failed {
            ack.nak(None);
        } else {
            ack.ack();
        }
    }

    /// Deliver `record` to every node in `targets`, sharing it until a branch needs to own it.
    /// The last target receives the walker's own reference, so a lone branch never copies.
    fn fan_out(
        &self,
        targets: impl Iterator<Item = NodeIndex>,
        record: Arc<Record>,
        walk: &mut Walk,
    ) {
        let mut targets = targets.peekable();
        while let Some(target) = targets.next() {
            if targets.peek().is_none() {
                self.run_node(target, record, walk);
                return;
            }
            self.run_node(target, Arc::clone(&record), walk);
        }
    }

    /// Every consumer of `index`, whatever label it subscribed to.
    fn all_successors(&self, index: NodeIndex) -> impl Iterator<Item = NodeIndex> + '_ {
        self.pipeline.dag().edges(index).iter().map(|e| e.target)
    }

    /// The consumers of `index` that subscribed to `label`.
    fn labelled_successors<'a>(
        &'a self,
        index: NodeIndex,
        label: &'a str,
    ) -> impl Iterator<Item = NodeIndex> + 'a {
        self.pipeline
            .dag()
            .edges(index)
            .iter()
            .filter(move |e: &&Edge| e.label.as_deref() == Some(label))
            .map(|e| e.target)
    }

    fn run_node(&self, index: NodeIndex, record: Arc<Record>, walk: &mut Walk) {
        let dag = self.pipeline.dag();
        let node_id = dag.node(index).id.as_str();
        match self.pipeline.node(index) {
            CompiledNode::Sink(sink) => {
                if let Err(err) = sink.write(std::slice::from_ref(&*record)) {
                    walk.fail(node_id, &err);
                }
            }
            CompiledNode::Stage(stage) => {
                let ctx = Context {
                    node_id,
                    record_id: walk.record_id,
                };
                // Copies only if another branch still shares the record.
                let owned = Arc::unwrap_or_clone(record);
                match stage.process(owned, &ctx) {
                    StageOutput::Pass(record) => {
                        self.fan_out(self.all_successors(index), Arc::new(record), walk);
                    }
                    StageOutput::Split(records) => {
                        for record in records {
                            self.fan_out(self.all_successors(index), Arc::new(record), walk);
                        }
                    }
                    StageOutput::Drop(_reason) => {}
                    StageOutput::Routed(label, record) => {
                        let mut targets = self.labelled_successors(index, &label).peekable();
                        if targets.peek().is_none() {
                            // Load validation guarantees every declared label a consumer, so
                            // this is a stage emitting a label it never declared.
                            walk.fail(node_id, &format!("no consumer for route label `{label}`"));
                            return;
                        }
                        self.fan_out(targets, Arc::new(record), walk);
                    }
                    StageOutput::Error(err) => walk.fail(node_id, &err),
                }
            }
        }
    }
}
