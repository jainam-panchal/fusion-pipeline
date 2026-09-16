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
//!
//! `every_nth` counts deliveries on one shared sample count per tenant, `sample:count`
//! under the handle's prefix, one `incr` per record, and keeps the first of each `n`
//! (counts 1, n+1, 2n+1, ...), so the split is exact across every worker and every
//! replica. It is 1 in n of the deliveries that reach the node: a message NATS redelivers
//! is a new delivery and takes a new count, since remembering every record would cost a
//! store key per record. The count lives [`COUNT_TTL`], refreshed on every `incr`, so a
//! tenant quieter than that restarts at 1, and its first record back is kept.

use std::time::Duration;

use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::path::FieldPath;
use fusion_core::record::Record;
use fusion_core::stage::{Context, DropReason, Stage, StageOutput};
use fusion_core::state::StateErrorPolicy;
use serde::Deserialize;

use crate::key_hash::{fnv1a64, hash_key_fields, parse_key_fields};

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
}

#[derive(Debug)]
enum Mode {
    Random {
        share: Share,
        /// The node id hashed, mixed into the coin so two `random` nodes in series keep
        /// independent subsets rather than the same one twice. `Consistent` has no salt
        /// on purpose: the same key value must get the same verdict everywhere.
        salt: u64,
    },
    EveryNth {
        /// As `i64` because the store counts in `i64`; converted once at load.
        n: i64,
        /// The only mode that reaches the store, so the only one with a policy.
        on_state_error: StateErrorPolicy,
    },
    Consistent {
        share: Share,
        key: Vec<FieldPath>,
    },
}

/// The share kept, as the threshold a uniformly mixed 64-bit hash is compared against:
/// `percent` of the hash space lies below it. `percent: 100` keeps everything.
#[derive(Debug, Clone, Copy)]
struct Share {
    threshold: u64,
}

impl Share {
    fn from_percent(percent: f64) -> Self {
        // 2^64 * percent / 100. For `percent: 100` that is 2^64 itself, which the `as u64`
        // cast saturates to `u64::MAX`, so every hash is kept.
        Self {
            threshold: (2f64.powi(64) * percent / 100.0) as u64,
        }
    }

    /// Whether a value with this `hash` is inside the share. `hash` must already be mixed:
    /// the comparison assumes it is spread over the whole space.
    fn keeps(self, hash: u64) -> bool {
        hash <= self.threshold
    }
}

/// Each mode and the fields it takes. `mode` itself is common. The list of modes in every
/// error message and the refusal of a field under the wrong mode are both derived from
/// this table, so a mode's name and fields have one source; its construction is one arm
/// of the match in [`Sample::from_node`].
const MODE_FIELDS: [(&str, &[&str]); 3] = [
    ("random", &["percent"]),
    ("every_nth", &["n", "on_state_error"]),
    ("consistent", &["percent", "key"]),
];

/// The `every_nth` sample count under the handle's prefix: the purpose segment `sample`,
/// as `dedupe` has `dedupe`, then `count`. The stage's only key.
const COUNT_KEY: &str = "sample:count";

/// How long the `every_nth` sample count lives without a record, refreshed on every
/// `incr`. Long enough that no tenant with traffic ever sees it restart: a count that
/// expired with a short TTL would hand 1 to every record of a tenant quieter than the TTL
/// and keep them all.
const COUNT_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// The modes as an error message lists them: "`random`, `every_nth` or `consistent`".
fn modes() -> String {
    let last = MODE_FIELDS.len() - 1;
    MODE_FIELDS
        .iter()
        .enumerate()
        .map(|(i, (name, _))| match i {
            0 => format!("`{name}`"),
            i if i == last => format!(" or `{name}`"),
            _ => format!(", `{name}`"),
        })
        .collect()
}

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
            return Err(node.invalid_params(format!("`mode` is required: {}", modes())));
        };
        // A field present under a mode that does not take it is refused naming the modes
        // that do, so `n` under `random` is a caught mistake. This is the only place an
        // unknown mode is rejected.
        let Some((_, allowed)) = MODE_FIELDS.iter().find(|(name, _)| *name == mode) else {
            return Err(node.invalid_params(format!("unknown mode `{mode}`: use {}", modes())));
        };
        // Every field of `Params` except `mode`. A field added to `Params` and to
        // `MODE_FIELDS` must be added here too, or it is never refused under the wrong mode.
        let present = [
            ("percent", params.percent.is_some()),
            ("n", params.n.is_some()),
            ("key", params.key.is_some()),
            ("on_state_error", params.on_state_error.is_some()),
        ];
        if let Some((field, _)) = present
            .iter()
            .find(|(field, present)| *present && !allowed.contains(field))
        {
            let owners = MODE_FIELDS
                .iter()
                .filter(|(_, fields)| fields.contains(field))
                .map(|(name, _)| format!("`mode: {name}`"))
                .collect::<Vec<_>>()
                .join(" or ");
            return Err(
                node.invalid_params(format!("`{field}` belongs to {owners}, not `mode: {mode}`"))
            );
        }
        let mode = match mode {
            "random" => Mode::Random {
                share: parse_percent(node, params.percent)?,
                salt: fnv1a64(node.id.as_bytes()),
            },
            "every_nth" => Mode::EveryNth {
                n: match params.n {
                    Some(n) if n >= 1 => {
                        i64::try_from(n).map_err(|_| node.invalid_params("`n` is too large"))?
                    }
                    Some(_) => return Err(node.invalid_params("`n` must be at least 1")),
                    None => {
                        return Err(node.invalid_params("`n` is required: keep one record in n"));
                    }
                },
                on_state_error: params.on_state_error.unwrap_or(StateErrorPolicy::Pass),
            },
            "consistent" => {
                let share = parse_percent(node, params.percent)?;
                let Some(key) = params.key.as_deref() else {
                    return Err(node.invalid_params(
                        "`key` is required: the field paths records are kept or dropped together by",
                    ));
                };
                Mode::Consistent {
                    share,
                    key: parse_key_fields(node, key)?,
                }
            }
            // Reached only if a row is added to `MODE_FIELDS` without an arm here: a config
            // error naming the mode, not a panic at load.
            other => {
                return Err(node.invalid_params(format!(
                    "mode `{other}` is listed but not built; this is a bug in the sample stage"
                )));
            }
        };
        Ok(Self { mode })
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

/// `percent` in `(0, 100]`, as a [`Share`].
fn parse_percent(node: &NodeConfig, percent: Option<f64>) -> Result<Share, ConfigError> {
    match percent {
        Some(p) if p > 0.0 && p <= 100.0 => Ok(Share::from_percent(p)),
        Some(p) => Err(node.invalid_params(format!(
            "`percent` must be above 0 and at most 100, not {p}"
        ))),
        None => {
            Err(node
                .invalid_params("`percent` is required: the share kept, above 0 and at most 100"))
        }
    }
}

impl Stage for Sample {
    fn process(&self, record: Record, ctx: &Context<'_>) -> StageOutput {
        let keep = match &self.mode {
            // The coin is the record id: a redelivered record lands on the same side, as
            // every verdict in this pipeline is meant to.
            Mode::Random { share, salt } => share.keeps(mix(ctx.meta.record_id.0 ^ salt)),
            Mode::EveryNth { n, .. } => {
                let count = match ctx.state.incr(COUNT_KEY, 1, COUNT_TTL) {
                    Ok(count) => count,
                    Err(error) => return StageOutput::StateError { record, error },
                };
                // Counts 1, n+1, 2n+1, ...: the first of each n. A tenant with fewer than n
                // records still gets one through. `wrapping_sub` only so a store answering
                // `i64::MIN` cannot panic; `incr` never gets there.
                count.wrapping_sub(1).rem_euclid(*n) == 0
            }
            // No salt: the same key value must get the same answer on every node and every
            // pipeline, and a key kept at a lower percent is kept at any higher one.
            Mode::Consistent { share, key } => share.keeps(mix(hash_key_fields(key, &record))),
        };
        if keep {
            StageOutput::Pass(record)
        } else {
            StageOutput::Drop(DropReason::Sample)
        }
    }

    fn uses_state(&self) -> bool {
        matches!(self.mode, Mode::EveryNth { .. })
    }

    fn on_state_error(&self) -> StateErrorPolicy {
        match self.mode {
            Mode::EveryNth { on_state_error, .. } => on_state_error,
            Mode::Random { .. } | Mode::Consistent { .. } => StateErrorPolicy::Pass,
        }
    }
}

/// The splitmix64 finalizer: a bijection on `u64` that spreads nearby inputs (sequential
/// record ids, hashes of similar keys) evenly over the whole space, so a threshold on the
/// output keeps the configured share of any input population.
fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
