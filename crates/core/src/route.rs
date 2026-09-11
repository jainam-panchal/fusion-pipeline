//! The `route` node's declared outputs: ordered labelled conditions plus a required default.
//!
//! ```yaml
//! - id: by_format
//!   type: route
//!   routes:                     # ordered; first match wins
//!     linux: resource["log.format"] == "Linux"
//!     apache: resource["log.format"] == "Apache"
//!   default: other              # a label, or `drop`
//! - id: linux_parse
//!   type: pcre2_extract
//!   from: by_format.linux       # downstream nodes name `<route>.<label>`
//! ```
//!
//! This module owns the config shape so that graph validation (every label consumed) and the
//! stage that evaluates the conditions read the same declaration.

use serde::Deserialize;

use crate::config::{ConfigError, NodeConfig};

/// The `type` string of a route node.
pub const ROUTE_KIND: &str = "route";

/// The `default` value that drops unmatched records instead of naming a label.
pub const DROP: &str = "drop";

/// Where unmatched records go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Default {
    /// Send them down this label.
    Label(String),
    /// Drop them with reason `route_default_drop`.
    Drop,
}

/// A route node's declaration: `(label, condition source)` pairs in file order and the default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteSpec {
    routes: Vec<(String, String)>,
    default: Default,
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
            routes.push((label.to_owned(), condition.to_owned()));
        }
        let default = if params.default == DROP {
            Default::Drop
        } else {
            Default::Label(params.default)
        };
        Ok(Self { routes, default })
    }

    /// `(label, condition source)` pairs in declaration order.
    #[must_use]
    pub fn routes(&self) -> &[(String, String)] {
        &self.routes
    }

    /// The default.
    #[must_use]
    pub fn default(&self) -> &Default {
        &self.default
    }

    /// Every label a downstream node may read from: the route labels in order, then the
    /// default label if it is not already one of them.
    pub fn labels(&self) -> impl Iterator<Item = &str> {
        let default = match &self.default {
            Default::Label(label) if !self.routes.iter().any(|(l, _)| l == label) => {
                Some(label.as_str())
            }
            _ => None,
        };
        self.routes
            .iter()
            .map(|(label, _)| label.as_str())
            .chain(default)
    }
}
