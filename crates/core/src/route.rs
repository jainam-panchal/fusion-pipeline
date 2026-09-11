//! The `route` node's declared outputs: ordered labelled conditions plus a required default.
//! The README's "Routing" section has the config example.
//!
//! This module owns the config shape so that graph validation (every label consumed) and the
//! stage that evaluates the conditions read the same declaration.

use serde::Deserialize;

use crate::config::{ConfigError, NodeConfig};

/// The `type` string of a route node.
pub const ROUTE_KIND: &str = "route";

/// The `default` value that drops unmatched records instead of naming a label.
pub const DROP: &str = "drop";

/// Where unmatched records go. Deliberately exhaustive: a consumer that could not name a
/// fallback would have to send records nowhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fallback {
    /// Send them down this label.
    Label(String),
    /// Drop them with reason `route_default_drop`.
    Drop,
}

/// One entry of `routes`: the label and the condition source that selects it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteRule {
    /// The output label.
    pub label: String,
    /// The condition source text, compiled by the stage.
    pub condition: String,
}

/// A route node's declaration: rules in file order and the default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteSpec {
    routes: Vec<RouteRule>,
    fallback: Fallback,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    routes: serde_yaml_ng::Mapping,
    default: String,
}

impl RouteSpec {
    /// Parse a route node's `routes` and `default`.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidParams`] naming the node when `routes` is missing or empty, a
    /// label or condition is not a string, a label is `drop`, or `default` is missing.
    pub fn from_node(node: &NodeConfig) -> Result<Self, ConfigError> {
        let params: Params = node.parse_params()?;
        if params.routes.is_empty() {
            return Err(node.invalid_params("`routes` must name at least one label"));
        }
        let mut routes = Vec::with_capacity(params.routes.len());
        for (label, condition) in params.routes {
            let Some(label) = label.as_str() else {
                return Err(node.invalid_params("route labels must be strings"));
            };
            if label == DROP {
                return Err(node.invalid_params(format!(
                    "route label `{DROP}` is reserved for `default: {DROP}`"
                )));
            }
            let Some(condition) = condition.as_str() else {
                return Err(
                    node.invalid_params(format!("route `{label}` must map to a condition string"))
                );
            };
            routes.push(RouteRule {
                label: label.to_owned(),
                condition: condition.to_owned(),
            });
        }
        let fallback = if params.default == DROP {
            Fallback::Drop
        } else {
            Fallback::Label(params.default)
        };
        Ok(Self { routes, fallback })
    }

    /// The rules in declaration order.
    #[must_use]
    pub fn routes(&self) -> &[RouteRule] {
        &self.routes
    }

    /// What happens to a record no route matches: the `default` key.
    #[must_use]
    pub fn fallback(&self) -> &Fallback {
        &self.fallback
    }

    /// Every label a downstream node may read from: the route labels in order, then the
    /// default label if it is not already one of them.
    pub fn labels(&self) -> impl Iterator<Item = &str> {
        let default = match &self.fallback {
            Fallback::Label(label) if !self.routes.iter().any(|r| &r.label == label) => {
                Some(label.as_str())
            }
            _ => None,
        };
        self.routes.iter().map(|r| r.label.as_str()).chain(default)
    }
}
