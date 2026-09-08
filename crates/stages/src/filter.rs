//! `filter`: evaluate a condition and either keep or drop matching records.

use pipeline_core::condition::Condition;
use pipeline_core::config::NodeConfig;
use pipeline_core::stage::{DropReason, Stage, StageContext, StageError, StageOutput};
use pipeline_core::Record;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Drop,
    Keep,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    condition: String,
    action: Action,
}

pub struct Filter {
    condition: Condition,
    action: Action,
}

impl Filter {
    pub fn build(node: &NodeConfig) -> Result<Box<dyn Stage>, StageError> {
        let params: Params = node.params()?;
        let condition = Condition::parse(&params.condition)
            .map_err(|e| StageError(format!("condition: {e}")))?;
        Ok(Box::new(Filter {
            condition,
            action: params.action,
        }))
    }
}

impl Stage for Filter {
    fn process(&self, record: Record, _ctx: &StageContext<'_>) -> StageOutput {
        let matched = match self.condition.eval(&record) {
            Ok(m) => m,
            Err(e) => return StageOutput::Error(StageError(e.to_string())),
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
}
