//! The validated pipeline graph: node ids plus data-flow edges.
//!
//! Validation at load time: every `from` target exists, every node is reachable from
//! `source`, the graph is acyclic, and at least one sink exists. Sinks are terminal: data
//! never flows out of a sink, so a node reading from one is unreachable.

use std::collections::BTreeMap;

use crate::config::{Config, ConfigError, NodeConfig, SOURCE_ID};

/// Position of a node in [`Dag::order`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeIndex(pub usize);

/// A validated pipeline graph.
#[derive(Debug, Clone)]
pub struct Dag {
    /// Nodes in topological order (upstream before downstream).
    nodes: Vec<NodeConfig>,
    /// Successor indices per node, positionally aligned with `nodes`.
    successors: Vec<Vec<NodeIndex>>,
    /// Successors of the implicit `source` node.
    source_successors: Vec<NodeIndex>,
    /// Successor ids keyed by node id, including `source`, for callers that speak in ids.
    successor_ids: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mark {
    Unvisited,
    InProgress,
    Done,
}

impl Dag {
    /// Validate `config` and build the graph.
    ///
    /// # Errors
    ///
    /// [`ConfigError::NoSink`] when no node has a `sink.*` type; [`ConfigError::UnknownFrom`]
    /// when a `from` names no node; [`ConfigError::Unreachable`] when data cannot reach a node
    /// from `source`; [`ConfigError::Cycle`] when the graph loops. Each names the node at fault.
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

        // Data-flow edges in file order of the consumer. Sinks emit nothing.
        let mut succ: Vec<Vec<usize>> = vec![Vec::new(); file_order.len()];
        let mut source_succ = Vec::new();
        for (i, node) in file_order.iter().enumerate() {
            for target in &node.from {
                if target == SOURCE_ID {
                    source_succ.push(i);
                    continue;
                }
                let Some(&t) = position.get(target.as_str()) else {
                    return Err(ConfigError::UnknownFrom {
                        node: node.id.clone(),
                        target: target.clone(),
                    });
                };
                if !file_order[t].is_sink() {
                    succ[t].push(i);
                }
            }
        }

        let mut reachable = vec![false; file_order.len()];
        let mut stack: Vec<usize> = source_succ.clone();
        while let Some(i) = stack.pop() {
            if std::mem::replace(&mut reachable[i], true) {
                continue;
            }
            stack.extend(succ[i].iter().copied());
        }
        if let Some(i) = reachable.iter().position(|r| !r) {
            return Err(ConfigError::Unreachable {
                node: file_order[i].id.clone(),
            });
        }

        // Every node is reachable, so a DFS from the source successors visits all of them
        // and its reverse post-order is a topological order.
        let mut marks = vec![Mark::Unvisited; file_order.len()];
        let mut post_order = Vec::with_capacity(file_order.len());
        for &start in &source_succ {
            visit(start, &succ, &mut marks, &mut post_order)
                .map_err(|i| ConfigError::Cycle { node: file_order[i].id.clone() })?;
        }
        post_order.reverse();

        let mut new_index = vec![NodeIndex(0); file_order.len()];
        for (new, &old) in post_order.iter().enumerate() {
            new_index[old] = NodeIndex(new);
        }

        let nodes: Vec<NodeConfig> = post_order.iter().map(|&old| file_order[old].clone()).collect();
        let successors: Vec<Vec<NodeIndex>> = post_order
            .iter()
            .map(|&old| succ[old].iter().map(|&s| new_index[s]).collect())
            .collect();
        let source_successors: Vec<NodeIndex> = source_succ.iter().map(|&s| new_index[s]).collect();

        let ids = |list: &[NodeIndex]| list.iter().map(|&NodeIndex(i)| nodes[i].id.clone()).collect();
        let mut successor_ids: BTreeMap<String, Vec<String>> = nodes
            .iter()
            .zip(&successors)
            .map(|(n, s)| (n.id.clone(), ids(s)))
            .collect();
        successor_ids.insert(SOURCE_ID.to_owned(), ids(&source_successors));

        Ok(Self {
            nodes,
            successors,
            source_successors,
            successor_ids,
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

    /// Ids of the nodes that read from `id` (`source` included). Empty for sinks and unknown ids.
    #[must_use]
    pub fn successors(&self, id: &str) -> &[String] {
        self.successor_ids.get(id).map_or(&[], Vec::as_slice)
    }

    /// Indices of the nodes that read from `index`.
    #[must_use]
    pub fn successor_indices(&self, index: NodeIndex) -> &[NodeIndex] {
        &self.successors[index.0]
    }

    /// Indices of the nodes that read from `source`.
    #[must_use]
    pub fn source_successors(&self) -> &[NodeIndex] {
        &self.source_successors
    }
}

/// Iterative DFS. On a back edge returns the index of the node it points at.
fn visit(start: usize, succ: &[Vec<usize>], marks: &mut [Mark], post_order: &mut Vec<usize>) -> Result<(), usize> {
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
