//! The engine: compiles a validated config into a DAG of built stages and
//! bound sinks, then walks every record through it on N worker threads.
//!
//! Ack model: each record fans out along every edge leaving a node. A branch
//! ends in a sink success or a stage drop (both fine) or in a stage error or
//! sink failure (a failure). Every branch runs to its end; the source message
//! is acked when no branch failed and nakked otherwise. A record with no `id`
//! is nakked before any stage sees it.

use crate::config::{ConfigError, NodeConfig, PipelineConfig, SOURCE_ID};
use crate::record::Record;
use crate::stage::{Stage, StageContext, StageError, StageOutput, StageRegistry};
use crate::traits::{Envelope, Sink, Source};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("node `{node}` has unknown type `{kind}`")]
    UnknownStageType { node: String, kind: String },
    #[error("node `{node}` failed to build: {source}")]
    Stage { node: String, source: StageError },
    #[error("sink node `{node}` has no sink bound")]
    UnboundSink { node: String },
}

/// Sink implementations keyed by sink node id.
#[derive(Default)]
pub struct SinkBindings {
    sinks: HashMap<String, Arc<dyn Sink>>,
}

impl SinkBindings {
    pub fn bind(&mut self, node_id: &str, sink: Arc<dyn Sink>) -> &mut Self {
        self.sinks.insert(node_id.to_string(), sink);
        self
    }
}

enum NodeKind {
    Stage(Box<dyn Stage>),
    Sink(Arc<dyn Sink>),
}

struct Edge {
    to: usize,
    /// Set when the consumer subscribed to `node.label`.
    label: Option<String>,
}

struct Node {
    id: String,
    kind: NodeKind,
    outputs: Vec<Edge>,
}

/// A compiled, immutable pipeline. Cheap to share across worker threads.
pub struct Pipeline {
    nodes: Vec<Node>,
    from_source: Vec<Edge>,
}

/// Why a branch did not end in a sink success or a drop. Carried so the
/// telemetry ticket can log it with the record id; nothing reads it yet.
#[derive(Debug)]
#[allow(dead_code)]
enum BranchFailure {
    Stage { node: String, error: StageError },
    Sink { node: String, error: String },
    UnconsumedLabel { node: String, label: String },
}

impl Pipeline {
    pub fn build(
        config: PipelineConfig,
        registry: &StageRegistry,
        sinks: SinkBindings,
    ) -> Result<Pipeline, BuildError> {
        let index: HashMap<&str, usize> = config
            .nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.id.as_str(), i))
            .collect();

        let mut nodes: Vec<Node> = Vec::with_capacity(config.nodes.len());
        for node in &config.nodes {
            nodes.push(Node {
                id: node.id.clone(),
                kind: build_node(node, registry, &sinks)?,
                outputs: Vec::new(),
            });
        }

        let mut from_source = Vec::new();
        for (i, node) in config.nodes.iter().enumerate() {
            for target in &node.from {
                let (base, label) = match target.split_once('.') {
                    Some((base, label)) => (base, Some(label.to_string())),
                    None => (target.as_str(), None),
                };
                let edge = Edge { to: i, label };
                if base == SOURCE_ID {
                    from_source.push(edge);
                } else {
                    // Validated by the config loader; a sink never emits.
                    let j = index[base];
                    if !config.nodes[j].is_sink() {
                        nodes[j].outputs.push(edge);
                    }
                }
            }
        }

        Ok(Pipeline { nodes, from_source })
    }

    /// Pull from `source` until it closes, processing on `workers` threads.
    /// Returns when every pulled envelope has been acked or nakked.
    pub fn run(self, source: Box<dyn Source>, workers: usize) {
        let pipeline = Arc::new(self);
        let workers = workers.max(1);
        let (tx, rx) = crossbeam_channel::bounded::<Envelope>(workers * 2);

        let handles: Vec<_> = (0..workers)
            .map(|n| {
                let rx = rx.clone();
                let pipeline = pipeline.clone();
                thread::Builder::new()
                    .name(format!("pipeline-worker-{n}"))
                    .spawn(move || {
                        for envelope in rx {
                            pipeline.handle(envelope);
                        }
                    })
                    .expect("spawn worker thread")
            })
            .collect();
        drop(rx);

        let mut source = source;
        while let Some(envelope) = source.next() {
            if tx.send(envelope).is_err() {
                break;
            }
        }
        drop(tx);

        for handle in handles {
            handle.join().expect("worker thread panicked");
        }
    }

    /// One record, start to ack.
    fn handle(&self, envelope: Envelope) {
        let Envelope { record, ack } = envelope;
        let Some(record_id) = record.id else {
            ack.nak(None);
            return;
        };
        let tenant = record.tenant().map(str::to_owned);
        let ctx = StageContext {
            record_id,
            tenant: tenant.as_deref(),
            node_id: SOURCE_ID,
        };

        let failures = self.fan_out(&self.from_source, None, record, &ctx);
        if failures.is_empty() {
            ack.ack();
        } else {
            ack.nak(None);
        }
    }

    /// Send `record` down every edge in `edges` whose label matches. The
    /// last consumer takes the record by move; earlier ones get a clone.
    fn fan_out(
        &self,
        edges: &[Edge],
        label: Option<&str>,
        record: Record,
        ctx: &StageContext<'_>,
    ) -> Vec<BranchFailure> {
        let targets: Vec<usize> = edges
            .iter()
            .filter(|e| e.label.as_deref() == label)
            .map(|e| e.to)
            .collect();
        let mut failures = Vec::new();
        let mut record = Some(record);
        for (n, &to) in targets.iter().enumerate() {
            let rec = if n + 1 == targets.len() {
                record.take().expect("record moved once")
            } else {
                record.clone().expect("record present")
            };
            failures.extend(self.walk(to, rec, ctx));
        }
        failures
    }

    /// Run `record` through node `idx` and everything downstream of it.
    fn walk(&self, idx: usize, record: Record, ctx: &StageContext<'_>) -> Vec<BranchFailure> {
        let node = &self.nodes[idx];
        let ctx = StageContext {
            node_id: &node.id,
            ..ctx.clone()
        };
        match &node.kind {
            NodeKind::Sink(sink) => match sink.publish(std::slice::from_ref(&record)) {
                Ok(()) => Vec::new(),
                Err(e) => vec![BranchFailure::Sink {
                    node: node.id.clone(),
                    error: e.0,
                }],
            },
            NodeKind::Stage(stage) => match stage.process(record, &ctx) {
                StageOutput::Pass(rec) => self.fan_out(&node.outputs, None, rec, &ctx),
                StageOutput::Drop(_reason) => Vec::new(),
                StageOutput::Split(records) => records
                    .into_iter()
                    .flat_map(|rec| self.fan_out(&node.outputs, None, rec, &ctx))
                    .collect(),
                StageOutput::Routed(label, rec) => {
                    if node
                        .outputs
                        .iter()
                        .any(|e| e.label.as_deref() == Some(&label))
                    {
                        self.fan_out(&node.outputs, Some(&label), rec, &ctx)
                    } else {
                        vec![BranchFailure::UnconsumedLabel {
                            node: node.id.clone(),
                            label,
                        }]
                    }
                }
                StageOutput::Error(error) => vec![BranchFailure::Stage {
                    node: node.id.clone(),
                    error,
                }],
            },
        }
    }
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let edges = |edges: &[Edge]| -> Vec<String> {
            edges
                .iter()
                .map(|e| match &e.label {
                    Some(l) => format!("{}.{l}", self.nodes[e.to].id),
                    None => self.nodes[e.to].id.clone(),
                })
                .collect()
        };
        let mut d = f.debug_struct("Pipeline");
        d.field("source", &edges(&self.from_source));
        for node in &self.nodes {
            d.field(&node.id, &edges(&node.outputs));
        }
        d.finish()
    }
}

fn build_node(
    node: &NodeConfig,
    registry: &StageRegistry,
    sinks: &SinkBindings,
) -> Result<NodeKind, BuildError> {
    if node.is_sink() {
        return sinks
            .sinks
            .get(&node.id)
            .cloned()
            .map(NodeKind::Sink)
            .ok_or_else(|| BuildError::UnboundSink {
                node: node.id.clone(),
            });
    }
    let factory = registry
        .get(&node.kind)
        .ok_or_else(|| BuildError::UnknownStageType {
            node: node.id.clone(),
            kind: node.kind.clone(),
        })?;
    let stage = factory.build(node).map_err(|source| BuildError::Stage {
        node: node.id.clone(),
        source,
    })?;
    Ok(NodeKind::Stage(stage))
}
