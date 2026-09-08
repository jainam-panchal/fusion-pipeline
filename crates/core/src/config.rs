//! YAML pipeline config: a list of `nodes`, each with `id`, `type`, optional
//! `from` (string or list, defaulting to the previous node), and type-specific
//! parameters kept as raw YAML for the stage factory to interpret.

use serde::Deserialize;
use std::collections::{HashMap, HashSet};

/// The reserved id every pipeline reads from.
pub const SOURCE_ID: &str = "source";

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("config is not valid YAML: {0}")]
    Yaml(String),
    #[error("node id `{node}` is reserved")]
    ReservedId { node: String },
    #[error("node id `{node}` is declared more than once")]
    DuplicateId { node: String },
    #[error("node `{node}` reads from `{target}`, which does not exist")]
    UnknownFrom { node: String, target: String },
    #[error("node `{node}` is on a cycle")]
    Cycle { node: String },
    #[error("node `{node}` is not reachable from `source`")]
    Unreachable { node: String },
    #[error("pipeline has no sink")]
    NoSink,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FromField {
    One(String),
    Many(Vec<String>),
}

#[derive(Debug, Deserialize)]
struct RawNode {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    from: Option<FromField>,
    #[serde(flatten)]
    params: serde_yaml_ng::Mapping,
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    nodes: Vec<RawNode>,
}

/// One node after `from` resolution. `params` holds every key other than
/// `id`, `type` and `from`, untouched, for the stage factory.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: String,
    pub kind: String,
    pub from: Vec<String>,
    pub params: serde_yaml_ng::Mapping,
}

impl NodeConfig {
    /// True when this node is a sink (`type: sink.<kind>`).
    pub fn is_sink(&self) -> bool {
        self.kind.starts_with("sink.")
    }
}

/// One resolved `from` entry: `to` reads from `from`, optionally only the
/// branch called `label` (`from: router.label`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edge {
    /// Index into `nodes`, or `None` for `source`.
    pub from: Option<usize>,
    pub to: usize,
    pub label: Option<String>,
}

/// A loaded, validated topology: nodes in file order plus the edge list.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub nodes: Vec<NodeConfig>,
    pub edges: Vec<Edge>,
}

impl PipelineConfig {
    pub fn node(&self, id: &str) -> Option<&NodeConfig> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// The node ids this node reads from, in declaration order.
    pub fn inputs_of(&self, id: &str) -> Vec<&str> {
        self.node(id)
            .map(|n| n.from.iter().map(String::as_str).collect())
            .unwrap_or_default()
    }
}

/// Parse and validate a YAML pipeline config.
pub fn load_str(yaml: &str) -> Result<PipelineConfig, ConfigError> {
    let raw: RawConfig =
        serde_yaml_ng::from_str(yaml).map_err(|e| ConfigError::Yaml(e.to_string()))?;

    let mut seen: HashSet<String> = HashSet::new();
    let mut nodes = Vec::with_capacity(raw.nodes.len());
    let mut previous = SOURCE_ID.to_string();
    for node in raw.nodes {
        if node.id == SOURCE_ID {
            return Err(ConfigError::ReservedId { node: node.id });
        }
        if !seen.insert(node.id.clone()) {
            return Err(ConfigError::DuplicateId { node: node.id });
        }
        let from = match node.from {
            None => vec![previous.clone()],
            Some(FromField::One(s)) => vec![s],
            Some(FromField::Many(v)) => v,
        };
        previous = node.id.clone();
        nodes.push(NodeConfig {
            id: node.id,
            kind: node.kind,
            from,
            params: node.params,
        });
    }
    let edges = resolve_edges(&nodes)?;
    let config = PipelineConfig { nodes, edges };
    validate(&config)?;
    Ok(config)
}

/// Turn every `from` entry into an [`Edge`]. A target is `node` or
/// `node.label`; `source` takes no label.
fn resolve_edges(nodes: &[NodeConfig]) -> Result<Vec<Edge>, ConfigError> {
    let index: HashMap<&str, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.id.as_str(), i))
        .collect();
    let mut edges = Vec::new();
    for (to, node) in nodes.iter().enumerate() {
        for target in &node.from {
            let unknown = || ConfigError::UnknownFrom {
                node: node.id.clone(),
                target: target.clone(),
            };
            let (base, label) = match target.split_once('.') {
                Some((base, label)) => (base, Some(label.to_string())),
                None => (target.as_str(), None),
            };
            let from = if target == SOURCE_ID {
                None
            } else {
                Some(*index.get(base).ok_or_else(unknown)?)
            };
            edges.push(Edge { from, to, label });
        }
    }
    Ok(edges)
}

/// Load-time DAG checks, in order: every node is reachable from `source`,
/// no cycles, at least one sink. Unknown `from` targets are caught earlier
/// by [`resolve_edges`].
fn validate(config: &PipelineConfig) -> Result<(), ConfigError> {
    // Adjacency: out-edges per node index. Sinks emit nothing.
    let mut out: Vec<Vec<usize>> = vec![Vec::new(); config.nodes.len()];
    let mut from_source: Vec<usize> = Vec::new();
    for edge in &config.edges {
        match edge.from {
            None => from_source.push(edge.to),
            Some(j) if !config.nodes[j].is_sink() => out[j].push(edge.to),
            Some(_) => {}
        }
    }

    // Reachability from `source`.
    let mut reached = vec![false; config.nodes.len()];
    let mut stack = from_source;
    while let Some(i) = stack.pop() {
        if std::mem::replace(&mut reached[i], true) {
            continue;
        }
        stack.extend(out[i].iter().copied());
    }
    if let Some((i, _)) = reached.iter().enumerate().find(|(_, r)| !**r) {
        return Err(ConfigError::Unreachable {
            node: config.nodes[i].id.clone(),
        });
    }

    // Cycle detection: DFS with colours (0 unvisited, 1 on stack, 2 done).
    let mut colour = vec![0u8; config.nodes.len()];
    fn visit(i: usize, out: &[Vec<usize>], colour: &mut [u8]) -> Option<usize> {
        colour[i] = 1;
        for &j in &out[i] {
            match colour[j] {
                1 => return Some(j),
                0 => {
                    if let Some(c) = visit(j, out, colour) {
                        return Some(c);
                    }
                }
                _ => {}
            }
        }
        colour[i] = 2;
        None
    }
    for i in 0..config.nodes.len() {
        if colour[i] == 0 {
            if let Some(c) = visit(i, &out, &mut colour) {
                return Err(ConfigError::Cycle {
                    node: config.nodes[c].id.clone(),
                });
            }
        }
    }

    if !config.nodes.iter().any(NodeConfig::is_sink) {
        return Err(ConfigError::NoSink);
    }
    Ok(())
}
