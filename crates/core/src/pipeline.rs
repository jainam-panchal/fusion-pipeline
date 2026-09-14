//! A compiled pipeline: the validated DAG with every node built into a stage or a sink.
//!
//! Immutable once built. The spec keeps it behind an atomic swap pointer so a new version can
//! replace a running one; the POC loads version 1 at startup and never swaps.

use crate::config::{Config, ConfigError};
use crate::dag::{Dag, NodeIndex};
use crate::io::Sink;
use crate::registry::Registry;
use crate::stage::Stage;

/// A built node.
pub enum CompiledNode {
    /// A transform, filter, route or script node.
    Stage(Box<dyn Stage>),
    /// A terminal sink.
    Sink(Box<dyn Sink>),
}

/// A validated, compiled pipeline.
pub struct Pipeline {
    name: String,
    dag: Dag,
    nodes: Vec<CompiledNode>,
    workers: Option<usize>,
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("name", &self.name)
            .field("dag", &self.dag)
            .field("workers", &self.workers)
            .finish_non_exhaustive()
    }
}

impl Pipeline {
    /// Load, validate and compile a YAML config.
    ///
    /// # Errors
    ///
    /// Any [`ConfigError`] from parsing, graph validation, or node construction.
    pub fn from_yaml(yaml: &str, registry: &Registry) -> Result<Self, ConfigError> {
        let config = Config::from_yaml(yaml)?;
        Self::compile(&config, registry)
    }

    /// Validate and compile a parsed config.
    ///
    /// # Errors
    ///
    /// Any [`ConfigError`] from graph validation or node construction.
    pub fn compile(config: &Config, registry: &Registry) -> Result<Self, ConfigError> {
        let dag = Dag::from_config(config)?;
        let nodes = dag
            .order()
            .iter()
            .map(|node| {
                if node.is_sink() {
                    registry.build_sink(node).map(CompiledNode::Sink)
                } else {
                    registry.build_stage(node).map(CompiledNode::Stage)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            name: config.name.clone(),
            dag,
            nodes,
            workers: config.workers,
        })
    }

    /// The pipeline name: the first segment of every state key.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whether any node reaches the state store, so the engine knows to open connections.
    #[must_use]
    pub fn uses_state(&self) -> bool {
        self.nodes.iter().any(|node| match node {
            CompiledNode::Stage(stage) => stage.uses_state(),
            CompiledNode::Sink(_) => false,
        })
    }

    /// The `workers` setting from the config, if given.
    #[must_use]
    pub fn workers(&self) -> Option<usize> {
        self.workers
    }

    /// Worker threads to run: the config's `workers`, else one per core (at least one).
    #[must_use]
    pub fn worker_count(&self) -> usize {
        self.workers
            .or_else(|| std::thread::available_parallelism().ok().map(usize::from))
            .unwrap_or(1)
            .max(1)
    }

    /// The validated graph.
    #[must_use]
    pub fn dag(&self) -> &Dag {
        &self.dag
    }

    /// The built node at `index`.
    #[must_use]
    pub fn node(&self, index: NodeIndex) -> &CompiledNode {
        &self.nodes[index.index()]
    }
}
