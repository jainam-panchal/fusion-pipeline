//! Shared condition parsing for stages that take a condition parameter.

use fusion_core::condition::Condition;
use fusion_core::config::{ConfigError, NodeConfig};

/// Parse `source` as a condition for `node`. `what` names the parameter in the error
/// (`condition`, ``route `linux` ``).
///
/// # Errors
///
/// [`ConfigError::InvalidParams`] naming the node when the condition does not parse or uses
/// `=~`/`!~` (not wired until the regex ticket).
pub(crate) fn parse_condition(
    node: &NodeConfig,
    source: &str,
    what: &str,
) -> Result<Condition, ConfigError> {
    let condition = Condition::parse(source)
        .map_err(|e| node.invalid_params(format!("{what} `{source}`: {e}")))?;
    if condition.has_regex_ops() {
        return Err(node.invalid_params(format!(
            "{what} `{source}`: regex operators `=~` and `!~` are not wired yet"
        )));
    }
    Ok(condition)
}
