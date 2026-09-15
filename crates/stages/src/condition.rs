//! A condition compiled for a stage: the parse tree from core plus, for every `=~` and
//! `!~` leaf, the pattern compiled through the regex facade under the node's `limits` and
//! `on_redos_risk`. Both `filter` and `route` build one per condition.

use std::collections::HashMap;

use fusion_core::condition::Condition;
use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::record::Record;
use fusion_regex::{Engine, MatchError, Regex};

use crate::regex::RegexParams;

/// A condition ready to evaluate, with its patterns compiled.
#[derive(Debug)]
pub(crate) struct CompiledCondition {
    condition: Condition,
    /// Compiled pattern by pattern text; one entry per distinct pattern.
    patterns: HashMap<String, Regex>,
}

impl CompiledCondition {
    /// Parse `source` as a condition for `node` and compile its patterns under `params`.
    /// `what` names the parameter in errors (`condition`, ``route `linux` ``).
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidParams`] naming the node when the condition does not parse,
    /// or a pattern does not compile under the limits and ReDoS policy.
    pub(crate) fn compile(
        node: &NodeConfig,
        source: &str,
        what: &str,
        params: &RegexParams,
    ) -> Result<Self, ConfigError> {
        let condition = Condition::parse(source)
            .map_err(|e| node.invalid_params(format!("{what} `{source}`: {e}")))?;
        let mut patterns = HashMap::new();
        for pattern in condition.regex_patterns() {
            if patterns.contains_key(pattern) {
                continue;
            }
            let regex = params.compile(node, what, pattern)?;
            patterns.insert(pattern.to_owned(), regex);
        }
        Ok(Self {
            condition,
            patterns,
        })
    }

    /// Evaluate against `record`.
    ///
    /// # Errors
    ///
    /// The first [`MatchError`] a pattern returns: a tripped limit, or an engine failure.
    pub(crate) fn matches(&self, record: &Record) -> Result<bool, MatchError> {
        self.condition.matches_with(record, &mut |pattern, text| {
            self.patterns
                .get(pattern)
                .map_or(Ok(false), |regex| regex.is_match(text))
        })
    }

    /// The engine a node running this condition reports: `None` without regex operators;
    /// otherwise the worst across its patterns.
    pub(crate) fn engine(&self) -> Option<Engine> {
        worst_engine(self.patterns.values().map(Regex::engine))
    }

    /// [`CompiledCondition::engine`] as the metric label.
    pub(crate) fn engine_label(&self) -> Option<&'static str> {
        self.engine().map(Engine::as_str)
    }
}

/// The worst of `engines`: `Backtracking` if any is, else `Linear`; `None` when empty.
pub(crate) fn worst_engine(engines: impl Iterator<Item = Engine>) -> Option<Engine> {
    engines.max_by_key(|engine| matches!(engine, Engine::Backtracking))
}
