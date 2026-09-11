//! `filter`: keep or drop records that match a condition.
//!
//! ```yaml
//! - id: keep_errors
//!   type: filter
//!   condition: severity_text == "ERROR"
//!   action: keep   # or drop
//! ```

use fusion_core::condition::Condition;
use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::record::Record;
use fusion_core::stage::{Context, DropReason, Stage, StageOutput};
use serde::Deserialize;

use crate::condition::parse_condition;

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
}

/// The `filter` stage.
#[derive(Debug)]
pub struct Filter {
    condition: Condition,
    action: Action,
}

impl Filter {
    /// Build from a node's `condition` and `action`.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidParams`] when a parameter is missing, the condition does not
    /// parse, or it uses `=~`/`!~` (not wired until the regex ticket).
    pub fn from_node(node: &NodeConfig) -> Result<Self, ConfigError> {
        let params: Params = node.parse_params()?;
        let condition = parse_condition(node, &params.condition, "condition")?;
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
    fn process(&self, record: Record, _ctx: &Context<'_>) -> StageOutput {
        let matched = self.condition.matches(&record);
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
}
