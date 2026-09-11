//! The engine: N worker threads pulling envelopes from a bounded intake and walking each
//! record through the DAG.
//!
//! Every branch of a record ends in a sink success, an intentional drop, or a failure. The
//! source message is acknowledged once all branches have ended without failure and
//! negatively acknowledged otherwise. Because a worker walks the graph synchronously, the
//! outstanding-branch count is the recursion itself: every branch runs to its end even after
//! one has failed, so the nak fires once all of them have finished, as the spec asks.
//!
//! Records are cloned per fan-out branch for now; the spec's copy-on-write sharing lands with
//! the route ticket, which is the first to fan out.

use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::dag::NodeIndex;
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

fn all_targets(edges: &[crate::dag::Edge]) -> Vec<NodeIndex> {
    edges.iter().map(|e| e.target).collect()
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
            self.fan_out(self.pipeline.dag().source_successors(), record, &mut walk);
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

    /// Deliver `record` to every node in `targets`. The last target takes it by move.
    fn fan_out(&self, targets: &[NodeIndex], record: Record, walk: &mut Walk) {
        let Some((last, rest)) = targets.split_last() else {
            return;
        };
        for &target in rest {
            self.run_node(target, record.clone(), walk);
        }
        self.run_node(*last, record, walk);
    }

    fn run_node(&self, index: NodeIndex, record: Record, walk: &mut Walk) {
        let dag = self.pipeline.dag();
        let node_id = dag.node(index).id.as_str();
        match self.pipeline.node(index) {
            CompiledNode::Sink(sink) => {
                if let Err(err) = sink.write(std::slice::from_ref(&record)) {
                    walk.fail(node_id, &err);
                }
            }
            CompiledNode::Stage(stage) => {
                let ctx = Context {
                    node_id,
                    record_id: walk.record_id,
                };
                match stage.process(record, &ctx) {
                    StageOutput::Pass(record) => {
                        self.fan_out(&all_targets(dag.edges(index)), record, walk);
                    }
                    StageOutput::Split(records) => {
                        for record in records {
                            self.fan_out(&all_targets(dag.edges(index)), record, walk);
                        }
                    }
                    StageOutput::Drop(_reason) => {}
                    // Route labels are wired in the route ticket. Until then a routed record
                    // has nowhere correct to go, so it fails rather than flowing silently.
                    StageOutput::Routed(label, _) => {
                        walk.fail(node_id, &format!("route label `{label}` is not wired yet"));
                    }
                    StageOutput::Error(err) => walk.fail(node_id, &err),
                }
            }
        }
    }
}
