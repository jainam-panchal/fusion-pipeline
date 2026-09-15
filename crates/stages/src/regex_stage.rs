//! What the regex stages share: the `limits` and `on_redos_risk` parameters, compiling a
//! pattern through the facade at load, the load-time log lines, writing extracted or
//! masked text back through the record path helpers, and turning a match error into a
//! stage output.
//!
//! ```yaml
//! limits:               # every key optional
//!   match: 1000000      # PCRE2 backtracking steps per start position
//!   depth: 1000000      # PCRE2 backtracking depth
//!   heap_kib: 20000     # PCRE2 heap for backtracking frames
//!   work: 10000000      # PCRE2 pattern items per call; 0 turns the count off
//!   input_bytes: 65536  # largest field the pattern is run on, both engines
//! on_redos_risk: reject # reject (default) | warn
//! ```

use std::num::NonZeroU32;

use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::metrics::EngineLabel;
use fusion_core::path::FieldPath;
use fusion_core::record::Record;
use fusion_core::stage::{DropReason, StageError, StageOutput};
use fusion_regex::{Engine, Limits, MatchError, Options, RedosPolicy, Regex};
use serde::Deserialize;
use serde_json::Value;

/// The `limits` block as written. Absent keys take the facade's defaults.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LimitsParams {
    #[serde(rename = "match")]
    match_limit: Option<u32>,
    depth: Option<u32>,
    heap_kib: Option<u32>,
    work: Option<u32>,
    input_bytes: Option<usize>,
}

impl LimitsParams {
    fn to_limits(&self) -> Limits {
        let defaults = Limits::default();
        Limits {
            match_limit: self.match_limit.unwrap_or(defaults.match_limit),
            depth_limit: self.depth.unwrap_or(defaults.depth_limit),
            heap_limit_kib: self.heap_kib.unwrap_or(defaults.heap_limit_kib),
            work_limit: self.work.map_or(defaults.work_limit, NonZeroU32::new),
            input_bytes: self.input_bytes.unwrap_or(defaults.input_bytes),
            ..defaults
        }
    }
}

/// `on_redos_risk` as written.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum RedosRiskParam {
    /// Refuse to load a node whose pattern fails the lint or the canary.
    #[default]
    Reject,
    /// Load it and log the findings.
    Warn,
}

impl RedosRiskParam {
    fn policy(self) -> RedosPolicy {
        match self {
            Self::Reject => RedosPolicy::Reject,
            Self::Warn => RedosPolicy::Warn,
        }
    }
}

/// The two parameters every regex stage takes. Flattened into each stage's params.
#[derive(Debug, Default, Clone, Deserialize)]
pub(crate) struct RegexParams {
    #[serde(default)]
    pub(crate) limits: LimitsParams,
    #[serde(default)]
    pub(crate) on_redos_risk: RedosRiskParam,
}

impl RegexParams {
    /// The facade options these parameters select: lint and canary on, policy as written.
    pub(crate) fn options(&self) -> Options {
        Options {
            limits: self.limits.to_limits(),
            on_redos_risk: self.on_redos_risk.policy(),
            ..Options::checked()
        }
    }

    /// Compile `pattern` for `node` under these parameters and print the load-time line
    /// for it: the node, the parameter, the pattern, and any lint or canary finding kept
    /// under `warn`. The node's engine is one line, [`log_node_engine`], whatever the
    /// number of patterns.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidParams`] naming the node with the facade's compile error: a
    /// syntax error with its offset, a guard, or a lint or canary finding under `reject`.
    pub(crate) fn compile(
        &self,
        node: &NodeConfig,
        what: &str,
        pattern: &str,
    ) -> Result<Regex, ConfigError> {
        let regex = Regex::with_options(pattern, &self.options())
            .map_err(|e| node.invalid_params(format!("{what} `{pattern}`: {e}")))?;
        // Structured logging over OTLP lands with the logs ticket; until then the load-time
        // classification is at least visible on stderr.
        eprintln!("pipeline: node `{}` {what}={pattern:?}", node.id);
        for risk in regex.redos_warnings() {
            eprintln!(
                "pipeline: node `{}` on_redos_risk=warn lint: {risk}",
                node.id
            );
        }
        if let Some(trip) = regex.canary_warning() {
            eprintln!(
                "pipeline: node `{}` on_redos_risk=warn canary: {trip}",
                node.id
            );
        }
        Ok(regex)
    }
}

/// The facade's engine as the metric label. The two closed sets are the same two engines.
pub(crate) fn engine_label(engine: Engine) -> EngineLabel {
    match engine {
        Engine::Linear => EngineLabel::Linear,
        Engine::Backtracking => EngineLabel::Backtracking,
    }
}

/// The load-time line for a node whose metrics carry `engine`: the worst engine across its
/// patterns, the value the `engine` label takes. Nothing is printed for `None`, a node
/// without a regex.
pub(crate) fn log_node_engine(node: &NodeConfig, engine: Option<EngineLabel>) {
    if let Some(engine) = engine {
        eprintln!(
            "pipeline: node `{}` type={} engine={engine}",
            node.id, node.kind
        );
    }
}

/// Write each `(path, text)` into `record` as a string. The record is unchanged on the
/// first refusal, which becomes a stage error naming the node and the path.
pub(crate) fn write_strings<'a>(
    node: &str,
    record: &mut Record,
    writes: impl IntoIterator<Item = (&'a FieldPath, String)>,
) -> Result<(), StageError> {
    for (path, text) in writes {
        path.write(record, Value::String(text))
            .map_err(|e| StageError::new(format!("node `{node}`: cannot write `{path}`: {e}")))?;
    }
    Ok(())
}

/// A match error as the spec classifies it: every tripped limit is a drop with reason
/// `regex_limit`; anything else is a stage error.
pub(crate) fn match_failure(node: &str, error: MatchError) -> StageOutput {
    match error {
        MatchError::MatchLimit
        | MatchError::DepthLimit
        | MatchError::HeapLimit
        | MatchError::WorkLimit
        | MatchError::InputTooLarge { .. } => StageOutput::Drop(DropReason::RegexLimit),
        _ => StageOutput::Error(
            StageError::new(format!("node `{node}`: regex engine failed")).with_source(error),
        ),
    }
}
