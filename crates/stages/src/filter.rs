//! `filter`: keep or drop records that match a condition.
//!
//! ```yaml
//! - id: keep_errors
//!   type: filter
//!   condition: severity_text == "ERROR"
//!   action: keep                     # or drop
//!   limits: { input_bytes: 65536 }   # for `=~` and `!~`; see the regex_stage module
//!   on_redos_risk: reject            # reject (default) | warn
//! ```
//!
//! A tripped regex limit drops the record with reason `regex_limit`, whatever `action`
//! says; any other engine failure is a stage error.

use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::metrics::EngineLabel;
use fusion_core::record::Record;
use fusion_core::stage::{Context, DropReason, Stage, StageOutput};
use serde::Deserialize;

use crate::condition::CompiledCondition;
use crate::regex_stage::{RegexParams, log_node_engine, match_failure};

/// What to do with records the condition matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    /// Matching records continue; the rest are dropped.
    Keep,
    /// Matching records are dropped; the rest continue.
    Drop,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    condition: String,
    action: Action,
    #[serde(flatten)]
    regex: RegexParams,
}

/// The `filter` stage.
#[derive(Debug)]
pub struct Filter {
    condition: CompiledCondition,
    action: Action,
}

impl Filter {
    /// Build from a node's `condition` and `action`.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidParams`] when a parameter is missing, the condition does not
    /// parse, or a pattern in it does not compile under the node's limits and ReDoS policy.
    pub fn from_node(node: &NodeConfig) -> Result<Self, ConfigError> {
        let params: Params = node.parse_params()?;
        let condition =
            CompiledCondition::compile(node, &params.condition, "condition", &params.regex)?;
        log_node_engine(node, condition.engine_label());
        Ok(Self {
            condition,
            action: params.action,
        })
    }

    /// Factory for a [`fusion_core::registry::Registry`].
    ///
    /// # Errors
    ///
    /// See [`Filter::from_node`].
    pub fn build(node: &NodeConfig) -> Result<Box<dyn Stage>, ConfigError> {
        Ok(Box::new(Self::from_node(node)?))
    }
}

impl Stage for Filter {
    fn process(&self, record: Record, ctx: &Context<'_>) -> StageOutput {
        let matched = match self.condition.matches(&record, ctx.meta) {
            Ok(matched) => matched,
            Err(error) => return match_failure(ctx.node_id, error),
        };
        let keep = match self.action {
            Action::Keep => matched,
            Action::Drop => !matched,
        };
        if keep {
            StageOutput::Pass(record)
        } else {
            StageOutput::Drop(DropReason::Filter)
        }
    }

    fn engine_label(&self) -> Option<EngineLabel> {
        self.condition.engine_label()
    }
}
