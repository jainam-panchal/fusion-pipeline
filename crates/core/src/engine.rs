//! The engine: N worker threads pulling envelopes from a bounded intake and walking each
//! record through the DAG.
//!
//! Every branch of a record ends in a sink success, an intentional drop, or a failure. The
//! source message is acknowledged once all branches have ended without failure and
//! negatively acknowledged otherwise. Because a worker walks the graph synchronously, the
//! outstanding-branch count is the recursion itself.

use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::dag::NodeIndex;
use crate::io::{Envelope, Intake, Source, SourceError};
use crate::pipeline::{CompiledNode, Pipeline};
use crate::record::{Kind, Record};
use crate::stage::{Context, DropReason, StageOutput};

/// Envelopes buffered between the source and the workers, per worker.
const INTAKE_DEPTH_PER_WORKER: usize = 64;

/// Errors reported when the engine shuts down.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EngineError {
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
    /// Start `workers` worker threads and the source thread.
    ///
    /// `workers` of zero is treated as one.
    #[must_use]
    pub fn start(pipeline: Pipeline, source: Box<dyn Source>, workers: usize) -> Self {
        let workers = workers.max(1);
        let pipeline = Arc::new(pipeline);
        let (tx, rx) = crossbeam_channel::bounded::<Envelope>(workers * INTAKE_DEPTH_PER_WORKER);

        let workers = (0..workers)
            .map(|i| {
                let rx = rx.clone();
                let pipeline = Arc::clone(&pipeline);
                thread::Builder::new()
                    .name(format!("pipeline-worker-{i}"))
                    .spawn(move || {
                        for envelope in rx {
                            Walker {
                                pipeline: &pipeline,
                            }
                            .handle(envelope);
                        }
                    })
                    .expect("spawning a worker thread")
            })
            .collect();

        let intake = Intake::new(tx);
        let source = thread::Builder::new()
            .name("pipeline-source".to_owned())
            .spawn(move || source.run(intake))
            .expect("spawning the source thread");

        Self { source, workers }
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

/// Whether every branch of one record ended cleanly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Ok,
    Failed,
}

struct Walker<'p> {
    pipeline: &'p Pipeline,
}

impl Walker<'_> {
    fn handle(&self, envelope: Envelope) {
        let Envelope { record, ack } = envelope;

        let Some(record_id) = record.id else {
            // Spec: the idempotency guarantee has no unguarded path; nak so it is not lost.
            let _reason = DropReason::MissingId;
            ack.nak(None);
            return;
        };
        if record.kind != Kind::Log {
            let _reason = DropReason::InvalidRecord;
            ack.ack();
            return;
        }

        let mut verdict = Verdict::Ok;
        self.fan_out(
            self.pipeline.dag().source_successors(),
            record,
            record_id,
            &mut verdict,
        );
        match verdict {
            Verdict::Ok => ack.ack(),
            Verdict::Failed => ack.nak(None),
        }
    }

    /// Deliver `record` to every node in `targets`. The last target takes it by move.
    fn fan_out(
        &self,
        targets: &[NodeIndex],
        record: Record,
        record_id: crate::record::RecordId,
        verdict: &mut Verdict,
    ) {
        let Some((last, rest)) = targets.split_last() else {
            return;
        };
        for &target in rest {
            self.run_node(target, record.clone(), record_id, verdict);
        }
        self.run_node(*last, record, record_id, verdict);
    }

    fn run_node(
        &self,
        index: NodeIndex,
        record: Record,
        record_id: crate::record::RecordId,
        verdict: &mut Verdict,
    ) {
        let dag = self.pipeline.dag();
        match self.pipeline.node(index) {
            CompiledNode::Sink(sink) => {
                if sink.write(std::slice::from_ref(&record)).is_err() {
                    *verdict = Verdict::Failed;
                }
            }
            CompiledNode::Stage(stage) => {
                let ctx = Context {
                    node_id: &dag.node(index).id,
                    record_id,
                };
                match stage.process(record, &ctx) {
                    StageOutput::Pass(record) => {
                        self.fan_out(dag.successor_indices(index), record, record_id, verdict)
                    }
                    // Route labels are wired in the route ticket; until then a routed record
                    // continues to every successor like a pass.
                    StageOutput::Routed(_, record) => {
                        self.fan_out(dag.successor_indices(index), record, record_id, verdict);
                    }
                    StageOutput::Split(records) => {
                        for record in records {
                            self.fan_out(dag.successor_indices(index), record, record_id, verdict);
                        }
                    }
                    StageOutput::Drop(_reason) => {}
                    StageOutput::Error(_err) => *verdict = Verdict::Failed,
                }
            }
        }
    }
}
