//! `route`: send a record down the first label whose condition matches, else the default.
//! The README's "Routing" section has the config example.
//!
//! The declaration is parsed by [`fusion_core::route::RouteSpec`], which the graph validator
//! also uses to check that every label has a consumer. `limits` and `on_redos_risk` apply
//! to every `=~` and `!~` in the conditions; a tripped limit drops the record with reason
//! `regex_limit` before any label is chosen.

use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::metrics::EngineLabel;
use fusion_core::record::Record;
use fusion_core::route::{Fallback, RouteSpec};
use fusion_core::stage::{Context, DropReason, Stage, StageOutput};

use crate::condition::{CompiledCondition, worst_engine};
use crate::regex_stage::{RegexParams, engine_label, log_node_engine, match_failure};

/// A compiled rule: the label and the condition that selects it.
#[derive(Debug)]
struct Rule {
    label: String,
    condition: CompiledCondition,
}

/// The `route` stage.
#[derive(Debug)]
pub struct Route {
    routes: Vec<Rule>,
    fallback: Fallback,
    engine: Option<EngineLabel>,
}

impl Route {
    /// Build from a node's `routes` and `default`.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidParams`] when the declaration is malformed (see
    /// [`RouteSpec::from_node`]), a condition does not parse, or a pattern in one does not
    /// compile under the node's limits and ReDoS policy.
    pub fn from_node(node: &NodeConfig) -> Result<Self, ConfigError> {
        let spec = RouteSpec::from_node(node)?;
        let regex: RegexParams = node.parse_params()?;
        let routes = spec
            .routes()
            .iter()
            .map(|rule| {
                let label = rule.label.clone();
                let condition = CompiledCondition::compile(
                    node,
                    &rule.condition,
                    &format!("route `{label}`"),
                    &regex,
                )?;
                Ok(Rule { label, condition })
            })
            .collect::<Result<Vec<_>, ConfigError>>()?;
        // The node's label is its worst engine across every rule.
        let engine = worst_engine(routes.iter().filter_map(|rule| rule.condition.engine()))
            .map(engine_label);
        log_node_engine(node, engine);
        Ok(Self {
            routes,
            fallback: spec.fallback().clone(),
            engine,
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
    fn process(&self, record: Record, ctx: &Context<'_>) -> StageOutput {
        for rule in &self.routes {
            match rule.condition.matches(&record, ctx.meta) {
                Ok(true) => return StageOutput::Routed(rule.label.clone(), record),
                Ok(false) => {}
                Err(error) => return match_failure(ctx.node_id, error),
            }
        }
        match &self.fallback {
            Fallback::Label(label) => StageOutput::Routed(label.clone(), record),
            Fallback::Drop => StageOutput::Drop(DropReason::RouteDefaultDrop),
        }
    }

    fn engine_label(&self) -> Option<EngineLabel> {
        self.engine
    }
}
