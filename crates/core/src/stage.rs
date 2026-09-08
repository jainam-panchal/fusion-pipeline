//! The stage contract: record in, one of `Pass`, `Drop`, `Routed`, `Split`
//! or `Error` out. Stages are built from `NodeConfig` by a factory looked up
//! in a `StageRegistry` by config `type`.

use crate::config::NodeConfig;
use crate::record::{Record, RecordId};
use serde::de::DeserializeOwned;
use std::collections::HashMap;

/// Closed set of drop reasons, the label set on `records_dropped_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DropReason {
    Filter,
    RouteDefaultDrop,
    Sample,
    Dedupe,
    LuaDrop,
    LuaError,
    RegexLimit,
    StateError,
    InvalidRecord,
    MissingId,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct StageError(pub String);

#[derive(Debug)]
pub enum StageOutput {
    Pass(Record),
    Drop(DropReason),
    Routed(String, Record),
    Split(Vec<Record>),
    Error(StageError),
}

/// Per-record context handed to every stage. State store and metrics
/// handles join this in their own tickets.
#[derive(Debug, Clone)]
pub struct StageContext<'a> {
    pub record_id: RecordId,
    pub tenant: Option<&'a str>,
    pub node_id: &'a str,
}

pub trait Stage: Send + Sync {
    fn process(&self, record: Record, ctx: &StageContext<'_>) -> StageOutput;
}

pub trait StageFactory: Send + Sync {
    fn build(&self, node: &NodeConfig) -> Result<Box<dyn Stage>, StageError>;
}

impl<F> StageFactory for F
where
    F: Fn(&NodeConfig) -> Result<Box<dyn Stage>, StageError> + Send + Sync,
{
    fn build(&self, node: &NodeConfig) -> Result<Box<dyn Stage>, StageError> {
        self(node)
    }
}

#[derive(Default)]
pub struct StageRegistry {
    factories: HashMap<String, Box<dyn StageFactory>>,
}

impl StageRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, kind: &str, factory: impl StageFactory + 'static) {
        self.factories.insert(kind.to_string(), Box::new(factory));
    }

    pub fn get(&self, kind: &str) -> Option<&dyn StageFactory> {
        self.factories.get(kind).map(Box::as_ref)
    }
}

impl NodeConfig {
    /// Deserialize this node's type-specific parameters.
    pub fn params<T: DeserializeOwned>(&self) -> Result<T, StageError> {
        serde_yaml_ng::from_value(serde_yaml_ng::Value::Mapping(self.params.clone()))
            .map_err(|e| StageError(e.to_string()))
    }
}
