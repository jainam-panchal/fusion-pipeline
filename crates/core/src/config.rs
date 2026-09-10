//! Pipeline config: an optional `source` block (`type` plus type-specific parameters) and a
//! YAML list of `nodes`, each with `id`, `type`, optional `from`, and type-specific parameters.
//!
//! Loading resolves the `from` default (the previous node in the file, or `source` for the
//! first node) and rejects reserved or duplicate ids. Graph validation lives in [`crate::dag`].

use std::collections::BTreeSet;

use serde::Deserialize;
use serde::de::DeserializeOwned;

/// The reserved id of the implicit source node every pipeline starts from.
pub const SOURCE_ID: &str = "source";

/// Errors raised while loading or validating a pipeline config.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The YAML document could not be parsed into the config shape.
    #[error("config is not valid YAML: {0}")]
    Yaml(#[from] serde_yaml_ng::Error),
    /// A node used the reserved `source` id.
    #[error("node id `{node}` is reserved")]
    ReservedId {
        /// The offending node id.
        node: String,
    },
    /// Two nodes share an id.
    #[error("node id `{node}` is declared more than once")]
    DuplicateId {
        /// The duplicated node id.
        node: String,
    },
    /// A node's `from` names a node that does not exist.
    #[error("node `{node}` reads from `{target}`, which does not exist")]
    UnknownFrom {
        /// The node whose `from` is wrong.
        node: String,
        /// The `from` entry that matched nothing.
        target: String,
    },
    /// The graph contains a cycle through the named node.
    #[error("node `{node}` is part of a cycle")]
    Cycle {
        /// A node on the cycle.
        node: String,
    },
    /// The named node cannot be reached from `source`.
    #[error("node `{node}` is unreachable from `source`")]
    Unreachable {
        /// The unreachable node id.
        node: String,
    },
    /// The pipeline declares no sink node.
    #[error("pipeline has no sink node")]
    NoSink,
    /// A node's `type` is not registered.
    #[error("node `{node}` has unknown type `{kind}`")]
    UnknownType {
        /// The node with the unknown type.
        node: String,
        /// The type string that matched no registered stage or sink.
        kind: String,
    },
    /// A node's type-specific parameters failed to parse.
    #[error("node `{node}`: {message}")]
    InvalidParams {
        /// The node whose parameters are invalid.
        node: String,
        /// What was wrong with them.
        message: String,
    },
}

/// A whole pipeline config as loaded from YAML.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    /// Number of worker threads, or `None` to use one per core.
    pub workers: Option<usize>,
    /// The source block, or `None` when the caller supplies the source (tests, embedding).
    pub source: Option<SourceConfig>,
    /// Nodes in file order, with `from` already resolved.
    pub nodes: Vec<NodeConfig>,
}

/// The `source` block: which source implementation feeds the pipeline and how it is set up.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceConfig {
    /// The source type string (`nats`, ...).
    pub kind: String,
    /// Type-specific parameters, everything except `type`.
    pub params: serde_yaml_ng::Value,
}

impl SourceConfig {
    /// Deserialize the type-specific parameters into `T`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidParams`] naming `source` when the parameters do not
    /// match `T`.
    pub fn parse_params<T: DeserializeOwned>(&self) -> Result<T, ConfigError> {
        serde_yaml_ng::from_value(self.params.clone())
            .map_err(|e| self.invalid_params(e.to_string()))
    }

    /// An [`ConfigError::InvalidParams`] naming `source`.
    #[must_use]
    pub fn invalid_params(&self, message: impl Into<String>) -> ConfigError {
        ConfigError::InvalidParams {
            node: SOURCE_ID.to_owned(),
            message: message.into(),
        }
    }
}

/// One node of the pipeline config.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeConfig {
    /// Node id, unique within the pipeline.
    pub id: String,
    /// The node type string (`filter`, `sink.memory`, ...).
    pub kind: String,
    /// Upstream node ids this node reads from. Never empty after loading.
    pub from: Vec<String>,
    /// Type-specific parameters, everything except `id`, `type` and `from`.
    pub params: serde_yaml_ng::Value,
}

impl NodeConfig {
    /// Whether this node is a sink (`type` starts with `sink.`).
    #[must_use]
    pub fn is_sink(&self) -> bool {
        self.kind.starts_with("sink.")
    }

    /// Deserialize the type-specific parameters into `T`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidParams`] naming this node when the parameters do not
    /// match `T`.
    pub fn parse_params<T: DeserializeOwned>(&self) -> Result<T, ConfigError> {
        serde_yaml_ng::from_value(self.params.clone())
            .map_err(|e| self.invalid_params(e.to_string()))
    }

    /// An [`ConfigError::InvalidParams`] naming this node.
    #[must_use]
    pub fn invalid_params(&self, message: impl Into<String>) -> ConfigError {
        ConfigError::InvalidParams {
            node: self.id.clone(),
            message: message.into(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    workers: Option<usize>,
    #[serde(default)]
    source: Option<RawSource>,
    #[serde(default)]
    nodes: Vec<RawNode>,
}

#[derive(Deserialize)]
struct RawSource {
    #[serde(rename = "type")]
    kind: String,
    #[serde(flatten)]
    params: serde_yaml_ng::Value,
}

#[derive(Deserialize)]
struct RawNode {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    from: Option<OneOrMany>,
    #[serde(flatten)]
    params: serde_yaml_ng::Value,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

impl From<OneOrMany> for Vec<String> {
    fn from(value: OneOrMany) -> Self {
        match value {
            OneOrMany::One(one) => vec![one],
            OneOrMany::Many(many) => many,
        }
    }
}

impl Config {
    /// Parse a YAML document, resolving each node's `from` default.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Yaml`] for malformed YAML, [`ConfigError::ReservedId`] when a
    /// node is called `source`, and [`ConfigError::DuplicateId`] when two nodes share an id.
    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        let raw: RawConfig = serde_yaml_ng::from_str(yaml)?;
        let mut seen = BTreeSet::new();
        let mut nodes = Vec::with_capacity(raw.nodes.len());
        let mut previous = SOURCE_ID.to_owned();

        for node in raw.nodes {
            if node.id == SOURCE_ID {
                return Err(ConfigError::ReservedId { node: node.id });
            }
            if !seen.insert(node.id.clone()) {
                return Err(ConfigError::DuplicateId { node: node.id });
            }
            let from = node.from.map_or_else(|| vec![previous.clone()], Vec::from);
            previous.clone_from(&node.id);
            nodes.push(NodeConfig {
                id: node.id,
                kind: node.kind,
                from,
                params: normalize_params(node.params),
            });
        }

        Ok(Self {
            workers: raw.workers,
            source: raw.source.map(|source| SourceConfig {
                kind: source.kind,
                params: normalize_params(source.params),
            }),
            nodes,
        })
    }
}

/// A node with no extra keys flattens to `Null`; stages expect a mapping.
fn normalize_params(params: serde_yaml_ng::Value) -> serde_yaml_ng::Value {
    match params {
        serde_yaml_ng::Value::Null => serde_yaml_ng::Value::Mapping(Default::default()),
        other => other,
    }
}
