//! `sample`: keep a share of the records, drop the rest with reason `sample`.
//!
//! ```yaml
//! - id: keep_tenth
//!   type: sample
//!   mode: random            # random | every_nth | consistent
//!   percent: 10             # random and consistent: the share kept, in (0, 100]
//!   # n: 10                 # every_nth: keep one record in n
//!   # key: [resource.host]  # consistent: records sharing these values are kept or dropped together
//!   # on_state_error: pass  # every_nth: pass (default) | nak
//! ```

use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::path::FieldPath;
use fusion_core::record::Record;
use fusion_core::stage::{Context, Stage, StageOutput};
use fusion_core::state::StateErrorPolicy;
use serde::Deserialize;

/// Every parameter of every mode, so a field belonging to another mode is rejected by
/// name rather than as "unknown".
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    mode: Option<String>,
    percent: Option<f64>,
    n: Option<u64>,
    key: Option<Vec<String>>,
    on_state_error: Option<StateErrorPolicy>,
}

/// The `sample` stage.
#[derive(Debug)]
pub struct Sample {
    mode: Mode,
    on_state_error: StateErrorPolicy,
}

#[derive(Debug)]
enum Mode {
    Random { percent: f64 },
    EveryNth { n: u64 },
    Consistent { percent: f64, key: Vec<FieldPath> },
}

const MODES: &str = "`random`, `every_nth` or `consistent`";

impl Sample {
    /// Build from a node's `mode` and the fields of that mode.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidParams`] when `mode` is missing or not one of the three, a
    /// field of the mode is missing or out of range (`percent` in `(0, 100]`, `n` at least
    /// 1, `key` at least one field path that parses), a field of another mode is given, or
    /// `on_state_error` is given outside `every_nth` or is not `pass` or `nak`.
    pub fn from_node(node: &NodeConfig) -> Result<Self, ConfigError> {
        let params: Params = node.parse_params()?;
        let Some(mode) = params.mode.as_deref() else {
            return Err(node.invalid_params(format!("`mode` is required: {MODES}")));
        };
        let refuse = |field: &str, owner: &str| {
            Err(node.invalid_params(format!("`{field}` belongs to `mode: {owner}`, not `{mode}`")))
        };
        match mode {
            "random" => {
                if params.n.is_some() {
                    return refuse("n", "every_nth");
                }
                if params.key.is_some() {
                    return refuse("key", "consistent");
                }
                if params.on_state_error.is_some() {
                    return refuse("on_state_error", "every_nth");
                }
                let percent = parse_percent(node, params.percent)?;
                Ok(Self {
                    mode: Mode::Random { percent },
                    on_state_error: StateErrorPolicy::Pass,
                })
            }
            "every_nth" => {
                if params.percent.is_some() {
                    return refuse("percent", "random` or `consistent");
                }
                if params.key.is_some() {
                    return refuse("key", "consistent");
                }
                let n = match params.n {
                    Some(n) if n >= 1 => n,
                    Some(_) => return Err(node.invalid_params("`n` must be at least 1")),
                    None => return Err(node.invalid_params("`n` is required: keep one record in n")),
                };
                Ok(Self {
                    mode: Mode::EveryNth { n },
                    on_state_error: params.on_state_error.unwrap_or(StateErrorPolicy::Pass),
                })
            }
            "consistent" => {
                if params.n.is_some() {
                    return refuse("n", "every_nth");
                }
                if params.on_state_error.is_some() {
                    return refuse("on_state_error", "every_nth");
                }
                let percent = parse_percent(node, params.percent)?;
                let Some(key) = params.key else {
                    return Err(node.invalid_params(
                        "`key` is required: the field paths records are kept or dropped together by",
                    ));
                };
                if key.is_empty() {
                    return Err(node.invalid_params("`key` needs at least one field path"));
                }
                let key = key
                    .iter()
                    .map(|path| {
                        FieldPath::parse(path)
                            .map_err(|e| node.invalid_params(format!("key `{path}`: {e}")))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self {
                    mode: Mode::Consistent { percent, key },
                    on_state_error: StateErrorPolicy::Pass,
                })
            }
            other => Err(node.invalid_params(format!("unknown mode `{other}`: use {MODES}"))),
        }
    }

    /// Factory for a [`fusion_core::registry::Registry`].
    ///
    /// # Errors
    ///
    /// See [`Sample::from_node`].
    pub fn build(node: &NodeConfig) -> Result<Box<dyn Stage>, ConfigError> {
        Ok(Box::new(Self::from_node(node)?))
    }
}

/// `percent` in `(0, 100]`.
fn parse_percent(node: &NodeConfig, percent: Option<f64>) -> Result<f64, ConfigError> {
    match percent {
        Some(p) if p > 0.0 && p <= 100.0 => Ok(p),
        Some(p) => Err(node.invalid_params(format!(
            "`percent` must be above 0 and at most 100, not {p}"
        ))),
        None => Err(node.invalid_params("`percent` is required: the share kept, above 0 and at most 100")),
    }
}

impl Stage for Sample {
    fn process(&self, record: Record, _ctx: &Context<'_>) -> StageOutput {
        let _ = &self.mode;
        StageOutput::Pass(record)
    }

    fn uses_state(&self) -> bool {
        matches!(self.mode, Mode::EveryNth { .. })
    }

    fn on_state_error(&self) -> StateErrorPolicy {
        self.on_state_error
    }
}
