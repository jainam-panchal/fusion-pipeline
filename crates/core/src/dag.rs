//! The validated pipeline graph: node ids plus data-flow edges.
//!
//! Validation at load time: every `from` target exists, every node is reachable from
//! `source`, the graph is acyclic, and at least one sink exists. Sinks are terminal: data
//! never flows out of a sink, so a node reading from one is unreachable.
//!
//! A `route` node's outputs are named. Its consumers read `<route>.<label>`, which becomes a
//! labelled edge; every label the route declares (including a default label) must have at
//! least one consumer, so no record can fall off the end of a router unnoticed.

use std::collections::{BTreeMap, BTreeSet};

use crate::config::{Config, ConfigError, NodeConfig, SOURCE_ID};
use crate::route::{ROUTE_KIND, RouteSpec};

/// Position of a node in [`Dag::order`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeIndex(usize);

impl NodeIndex {
    /// Position in [`Dag::order`].
    #[must_use]
    pub const fn index(self) -> usize {
        self.0
    }
}

/// A data-flow edge out of a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edge {
    /// The consuming node.
    pub target: NodeIndex,
    /// The route label the consumer subscribed to; `None` for edges out of non-route nodes.
    pub label: Option<String>,
}

/// A validated pipeline graph.
#[derive(Debug, Clone)]
pub struct Dag {
    /// Nodes in topological order (upstream before downstream).
    nodes: Vec<NodeConfig>,
    /// Out-edges per node, positionally aligned with `nodes`.
    edges: Vec<Vec<Edge>>,
    /// Successors of the implicit `source` node.
    source_successors: Vec<NodeIndex>,
    /// Successor ids keyed by node id, including `source`, for callers that speak in ids.
    successor_ids: BTreeMap<String, Vec<String>>,
    /// Node index keyed by id.
    by_id: BTreeMap<String, NodeIndex>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mark {
    Unvisited,
    InProgress,
    Done,
}

/// A `from` entry: a plain node id, or `<node>.<label>` on a route.
struct FromRef<'a> {
    target: &'a str,
    label: Option<&'a str>,
}

/// Resolve a `from` entry. A plain id wins even if it contains a dot; otherwise the text up
/// to the first dot is the node and the rest is the label.
fn parse_from<'a>(entry: &'a str, position: &BTreeMap<&str, usize>) -> FromRef<'a> {
    if entry == SOURCE_ID || position.contains_key(entry) {
        return FromRef {
            target: entry,
            label: None,
        };
    }
    match entry.split_once('.') {
        Some((target, label)) if !label.is_empty() => FromRef {
            target,
            label: Some(label),
        },
        _ => FromRef {
            target: entry,
            label: None,
        },
    }
}

impl Dag {
    /// Validate `config` and build the graph.
    ///
    /// # Errors
    ///
    /// [`ConfigError::NoSink`] when no node has a `sink.*` type; [`ConfigError::UnknownFrom`]
    /// when a `from` names no node; [`ConfigError::Unreachable`] when data cannot reach a node
    /// from `source`; [`ConfigError::Cycle`] when the graph loops. Route wiring:
    /// [`ConfigError::RouteNeedsLabel`], [`ConfigError::NotARoute`],
    /// [`ConfigError::UnknownRouteLabel`] and [`ConfigError::UnconsumedRouteLabel`], plus
    /// [`ConfigError::InvalidParams`] when a route's `routes`/`default` are malformed. Each
    /// names the node at fault.
    pub fn from_config(config: &Config) -> Result<Self, ConfigError> {
        let file_order = &config.nodes;
        if !file_order.iter().any(NodeConfig::is_sink) {
            return Err(ConfigError::NoSink);
        }

        let position: BTreeMap<&str, usize> = file_order
            .iter()
            .enumerate()
            .map(|(i, n)| (n.id.as_str(), i))
            .collect();

        // Route declarations, parsed once here so label checks and the stage agree.
        let mut routes: BTreeMap<usize, RouteSpec> = BTreeMap::new();
        for (i, node) in file_order.iter().enumerate() {
            if node.kind == ROUTE_KIND {
                routes.insert(i, RouteSpec::from_node(node)?);
            }
        }

        // Data-flow edges in file order of the consumer. Sinks emit nothing.
        let mut succ: Vec<Vec<(usize, Option<String>)>> = vec![Vec::new(); file_order.len()];
        let mut source_succ = Vec::new();
        let mut consumed: BTreeSet<(usize, &str)> = BTreeSet::new();
        for (i, node) in file_order.iter().enumerate() {
            for entry in &node.from {
                let from = parse_from(entry, &position);
                if from.target == SOURCE_ID {
                    source_succ.push(i);
                    continue;
                }
                let Some(&t) = position.get(from.target) else {
                    return Err(ConfigError::UnknownFrom {
                        node: node.id.clone(),
                        target: entry.clone(),
                    });
                };
                match (routes.get(&t), from.label) {
                    (Some(spec), Some(label)) => {
                        if !spec.labels().any(|l| l == label) {
                            return Err(ConfigError::UnknownRouteLabel {
                                node: node.id.clone(),
                                route: from.target.to_owned(),
                                label: label.to_owned(),
                            });
                        }
                        consumed.insert((t, label));
                    }
                    (Some(_), None) => {
                        return Err(ConfigError::RouteNeedsLabel {
                            node: node.id.clone(),
                            route: from.target.to_owned(),
                        });
                    }
                    (None, Some(label)) => {
                        return Err(ConfigError::NotARoute {
                            node: node.id.clone(),
                            target: from.target.to_owned(),
                            label: label.to_owned(),
                        });
                    }
                    (None, None) => {}
                }
                if !file_order[t].is_sink() {
                    succ[t].push((i, from.label.map(str::to_owned)));
                }
            }
        }

        for (&r, spec) in &routes {
            if let Some(label) = spec.labels().find(|l| !consumed.contains(&(r, l))) {
                return Err(ConfigError::UnconsumedRouteLabel {
                    route: file_order[r].id.clone(),
                    label: label.to_owned(),
                });
            }
        }

        let mut reachable = vec![false; file_order.len()];
        let mut stack: Vec<usize> = source_succ.clone();
        while let Some(i) = stack.pop() {
            if std::mem::replace(&mut reachable[i], true) {
                continue;
            }
            stack.extend(succ[i].iter().map(|(s, _)| *s));
        }
        if let Some(i) = reachable.iter().position(|r| !r) {
            return Err(ConfigError::Unreachable {
                node: file_order[i].id.clone(),
            });
        }

        // Every node is reachable, so a DFS from the source successors visits all of them
        // and its reverse post-order is a topological order.
        let plain_succ: Vec<Vec<usize>> = succ
            .iter()
            .map(|edges| edges.iter().map(|(s, _)| *s).collect())
            .collect();
        let mut marks = vec![Mark::Unvisited; file_order.len()];
        let mut post_order = Vec::with_capacity(file_order.len());
        for &start in &source_succ {
            visit(start, &plain_succ, &mut marks, &mut post_order).map_err(|i| {
                ConfigError::Cycle {
                    node: file_order[i].id.clone(),
                }
            })?;
        }
        post_order.reverse();

        let mut new_index = vec![NodeIndex(0); file_order.len()];
        for (new, &old) in post_order.iter().enumerate() {
            new_index[old] = NodeIndex(new);
        }

        let nodes: Vec<NodeConfig> = post_order
            .iter()
            .map(|&old| file_order[old].clone())
            .collect();
        let edges: Vec<Vec<Edge>> = post_order
            .iter()
            .map(|&old| {
                succ[old]
                    .iter()
                    .map(|(s, label)| Edge {
                        target: new_index[*s],
                        label: label.clone(),
                    })
                    .collect()
            })
            .collect();
        let source_successors: Vec<NodeIndex> = source_succ.iter().map(|&s| new_index[s]).collect();

        let ids = |list: &[NodeIndex]| {
            list.iter()
                .map(|&NodeIndex(i)| nodes[i].id.clone())
                .collect()
        };
        let mut successor_ids: BTreeMap<String, Vec<String>> = nodes
            .iter()
            .zip(&edges)
            .map(|(n, e)| {
                let targets: Vec<NodeIndex> = e.iter().map(|edge| edge.target).collect();
                (n.id.clone(), ids(&targets))
            })
            .collect();
        successor_ids.insert(SOURCE_ID.to_owned(), ids(&source_successors));
        let by_id = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.id.clone(), NodeIndex(i)))
            .collect();

        Ok(Self {
            nodes,
            edges,
            source_successors,
            successor_ids,
            by_id,
        })
    }

    /// Nodes in topological order.
    #[must_use]
    pub fn order(&self) -> &[NodeConfig] {
        &self.nodes
    }

    /// The node at `index`.
    #[must_use]
    pub fn node(&self, index: NodeIndex) -> &NodeConfig {
        &self.nodes[index.0]
    }

    /// The index of the node with id `id`, if any.
    #[must_use]
    pub fn index_of(&self, id: &str) -> Option<NodeIndex> {
        self.by_id.get(id).copied()
    }

    /// Ids of the nodes that read from `id` (`source` included). Empty for sinks and unknown ids.
    #[must_use]
    pub fn successors(&self, id: &str) -> &[String] {
        self.successor_ids.get(id).map_or(&[], Vec::as_slice)
    }

    /// Out-edges of the node at `index`, in file order of the consumers.
    #[must_use]
    pub fn edges(&self, index: NodeIndex) -> &[Edge] {
        &self.edges[index.0]
    }

    /// Indices of the nodes that read from `source`.
    #[must_use]
    pub fn source_successors(&self) -> &[NodeIndex] {
        &self.source_successors
    }
}

/// Iterative DFS. On a back edge returns the index of the node it points at.
fn visit(
    start: usize,
    succ: &[Vec<usize>],
    marks: &mut [Mark],
    post_order: &mut Vec<usize>,
) -> Result<(), usize> {
    if marks[start] == Mark::Done {
        return Ok(());
    }
    marks[start] = Mark::InProgress;
    let mut stack: Vec<(usize, usize)> = vec![(start, 0)];
    while let Some(&mut (node, ref mut next)) = stack.last_mut() {
        if let Some(&child) = succ[node].get(*next) {
            *next += 1;
            match marks[child] {
                Mark::InProgress => return Err(child),
                Mark::Done => {}
                Mark::Unvisited => {
                    marks[child] = Mark::InProgress;
                    stack.push((child, 0));
                }
            }
        } else {
            marks[node] = Mark::Done;
            post_order.push(node);
            stack.pop();
        }
    }
    Ok(())
}
