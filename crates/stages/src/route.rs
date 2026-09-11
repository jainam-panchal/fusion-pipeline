//! `route`: send a record down the first label whose condition matches, else the default.
//!
//! ```yaml
//! - id: by_format
//!   type: route
//!   routes:                     # ordered; first match wins
//!     linux: resource["log.format"] == "Linux"
//!     apache: resource["log.format"] == "Apache"
//!   default: other              # a label, or `drop`
//! ```
//!
//! The declaration is parsed by [`fusion_core::route::RouteSpec`], which the graph validator
//! also uses to check that every label has a consumer.

use fusion_core::condition::Condition;
use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::record::Record;
use fusion_core::route::{Fallback, RouteSpec};
use fusion_core::stage::{Context, DropReason, Stage, StageOutput};

use crate::condition::parse_condition;

/// The `route` stage.
#[derive(Debug)]
pub struct Route {
    routes: Vec<(String, Condition)>,
    fallback: Fallback,
}

impl Route {
    /// Build from a node's `routes` and `default`.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidParams`] when the declaration is malformed (see
    /// [`RouteSpec::from_node`]), a condition does not parse, or it uses `=~`/`!~` (not
    /// wired until the regex ticket).
    pub fn from_node(node: &NodeConfig) -> Result<Self, ConfigError> {
        let spec = RouteSpec::from_node(node)?;
        let routes = spec
            .routes()
            .iter()
            .map(|(label, source)| {
                let condition = parse_condition(node, source, &format!("route `{label}`"))?;
                Ok((label.clone(), condition))
            })
            .collect::<Result<Vec<_>, ConfigError>>()?;
        Ok(Self {
            routes,
            fallback: spec.fallback().clone(),
        })
    }

    /// Factory for a [`fusion_core::registry::Registry`].
    ///
    /// # Errors
    ///
    /// See [`Route::from_node`].
    pub fn build(node: &NodeConfig) -> Result<Box<dyn Stage>, ConfigError> {
        Ok(Box::new(Self::from_node(node)?))
    }
}

impl Stage for Route {
    fn process(&self, record: Record, _ctx: &Context<'_>) -> StageOutput {
        for (label, condition) in &self.routes {
            if condition.matches(&record) {
                return StageOutput::Routed(label.clone(), record);
            }
        }
        match &self.fallback {
            Fallback::Label(label) => StageOutput::Routed(label.clone(), record),
            Fallback::Drop => StageOutput::Drop(DropReason::RouteDefaultDrop),
        }
    }
}
