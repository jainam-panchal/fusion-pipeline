//! Maps `type` strings to the factories that build stages, sinks and the source.

use std::collections::BTreeMap;

use crate::config::{ConfigError, NodeConfig, SOURCE_ID, SourceConfig};
use crate::io::{Sink, Source};
use crate::stage::Stage;

/// Builds a [`Stage`] from a node's config.
pub trait StageFactory: Send + Sync {
    /// Build the stage for `node`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidParams`] naming the node when its parameters are wrong.
    fn build(&self, node: &NodeConfig) -> Result<Box<dyn Stage>, ConfigError>;
}

impl<F> StageFactory for F
where
    F: Fn(&NodeConfig) -> Result<Box<dyn Stage>, ConfigError> + Send + Sync,
{
    fn build(&self, node: &NodeConfig) -> Result<Box<dyn Stage>, ConfigError> {
        self(node)
    }
}

/// Builds a [`Sink`] from a node's config.
pub trait SinkFactory: Send + Sync {
    /// Build the sink for `node`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidParams`] naming the node when its parameters are wrong.
    fn build(&self, node: &NodeConfig) -> Result<Box<dyn Sink>, ConfigError>;
}

impl<F> SinkFactory for F
where
    F: Fn(&NodeConfig) -> Result<Box<dyn Sink>, ConfigError> + Send + Sync,
{
    fn build(&self, node: &NodeConfig) -> Result<Box<dyn Sink>, ConfigError> {
        self(node)
    }
}

/// Builds a [`Source`] from the config's `source` block.
pub trait SourceFactory: Send + Sync {
    /// Build the source for `source`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidParams`] naming `source` when its parameters are wrong,
    /// or when the source cannot be set up (a missing stream, an unreachable server).
    fn build(&self, source: &SourceConfig) -> Result<Box<dyn Source>, ConfigError>;
}

impl<F> SourceFactory for F
where
    F: Fn(&SourceConfig) -> Result<Box<dyn Source>, ConfigError> + Send + Sync,
{
    fn build(&self, source: &SourceConfig) -> Result<Box<dyn Source>, ConfigError> {
        self(source)
    }
}

/// The set of node and source types a pipeline may use.
#[derive(Default)]
pub struct Registry {
    stages: BTreeMap<String, Box<dyn StageFactory>>,
    sinks: BTreeMap<String, Box<dyn SinkFactory>>,
    sources: BTreeMap<String, Box<dyn SourceFactory>>,
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("stages", &self.stages.keys().collect::<Vec<_>>())
            .field("sinks", &self.sinks.keys().collect::<Vec<_>>())
            .field("sources", &self.sources.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Registry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a stage type. Replaces any factory already under `kind`.
    pub fn register_stage(
        &mut self,
        kind: impl Into<String>,
        factory: impl StageFactory + 'static,
    ) -> &mut Self {
        self.stages.insert(kind.into(), Box::new(factory));
        self
    }

    /// Register a sink type. `kind` should start with `sink.`. Replaces any factory already
    /// under `kind`.
    pub fn register_sink(
        &mut self,
        kind: impl Into<String>,
        factory: impl SinkFactory + 'static,
    ) -> &mut Self {
        self.sinks.insert(kind.into(), Box::new(factory));
        self
    }

    /// Register a source type. Replaces any factory already under `kind`.
    pub fn register_source(
        &mut self,
        kind: impl Into<String>,
        factory: impl SourceFactory + 'static,
    ) -> &mut Self {
        self.sources.insert(kind.into(), Box::new(factory));
        self
    }

    /// Build the source for the config's `source` block.
    ///
    /// # Errors
    ///
    /// [`ConfigError::UnknownType`] when nothing is registered under the block's type, or the
    /// factory's own error.
    pub fn build_source(&self, source: &SourceConfig) -> Result<Box<dyn Source>, ConfigError> {
        self.sources
            .get(&source.kind)
            .ok_or_else(|| ConfigError::UnknownType {
                node: SOURCE_ID.to_owned(),
                kind: source.kind.clone(),
            })?
            .build(source)
    }

    /// Build the stage for a non-sink node.
    ///
    /// # Errors
    ///
    /// [`ConfigError::UnknownType`] when nothing is registered under the node's type, or the
    /// factory's own error.
    pub fn build_stage(&self, node: &NodeConfig) -> Result<Box<dyn Stage>, ConfigError> {
        self.stages
            .get(&node.kind)
            .ok_or_else(|| unknown_type(node))?
            .build(node)
    }

    /// Build the sink for a `sink.*` node.
    ///
    /// # Errors
    ///
    /// [`ConfigError::UnknownType`] when nothing is registered under the node's type, or the
    /// factory's own error.
    pub fn build_sink(&self, node: &NodeConfig) -> Result<Box<dyn Sink>, ConfigError> {
        self.sinks
            .get(&node.kind)
            .ok_or_else(|| unknown_type(node))?
            .build(node)
    }

    /// The registered stage types, in name order.
    pub fn stage_types(&self) -> impl Iterator<Item = &str> {
        self.stages.keys().map(String::as_str)
    }
}

fn unknown_type(node: &NodeConfig) -> ConfigError {
    ConfigError::UnknownType {
        node: node.id.clone(),
        kind: node.kind.clone(),
    }
}
