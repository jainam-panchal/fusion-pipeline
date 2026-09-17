//! The producer's plan: every message it will send, decided before the first is sent, so a
//! run is reproducible from its flags and the expectations are known up front.
//!
//! Originals go round-robin across the sets, each set cycling through its lines. After an
//! original, a deliberate duplicate (the same line under a new id) is scheduled with the
//! chance that makes duplicates `dup_percent` of all messages, and sent at most [`DUP_LAG`]
//! later, well inside the pipeline's dedupe window. Message `i` is sent at `i / rate`.
//!
//! The same body comes back in its set's next cycle. [`plan`] refuses a rate at which that
//! happens before the dedupe window, the duplicate lag and a margin have passed, since the
//! next cycle's original would then be dropped as a repeat of this cycle's.

use std::collections::BTreeMap;
use std::time::Duration;

/// The longest a duplicate trails its original.
pub const DUP_LAG: Duration = Duration::from_millis(500);

/// What [`plan`] adds to the dedupe window and [`DUP_LAG`] before a body may come back.
pub const REPEAT_MARGIN: Duration = Duration::from_secs(1);

/// Record ids are `base | i`, so a plan holds fewer than this many messages.
pub const MAX_COUNT: u64 = 1 << 22;

/// The producer's flags that shape the plan.
#[derive(Debug, Clone, PartialEq)]
pub struct PlanConfig {
    /// Messages per second.
    pub rate: f64,
    /// How many messages to send.
    pub count: u64,
    /// The share of messages that are deliberate duplicates, 0 to 50.
    pub dup_percent: u8,
    /// Decides which originals get a duplicate and how far behind it is.
    pub seed: u64,
    /// The pipeline's dedupe window, for the repeat check.
    pub dedupe_window: Duration,
}

impl Default for PlanConfig {
    /// The spec's run: 100k records over about 60s, 30% duplicates, against a 2s window.
    fn default() -> Self {
        Self {
            rate: 1667.0,
            count: 100_000,
            dup_percent: 30,
            seed: 1,
            dedupe_window: Duration::from_secs(2),
        }
    }
}

/// One message of the plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Planned {
    /// The record id, sent as `Fusion-Record-Id`.
    pub id: u64,
    /// The index of its set in the producer's set list.
    pub set: usize,
    /// The index of its line in that set's loaded lines.
    pub line: usize,
    /// How many times the set had been through all its lines before this line.
    pub cycle: u64,
    /// For a deliberate duplicate, the id of its original.
    pub dup_of: Option<u64>,
    /// When to send it, from the start of the run.
    pub at: Duration,
}

/// Flags [`plan`] refuses.
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum PlanError {
    /// A flag is out of range.
    #[error("{0}")]
    InvalidFlag(String),
    /// A body would come back before the dedupe window, the lag and the margin are over.
    #[error(
        "--rate is too high: a body comes back after {repeat:?}, but the dedupe window, the \
         duplicate lag and a margin need {needed:?}; lower --rate or --dup-percent"
    )]
    RateTooHigh {
        /// How soon the shortest set repeats a body.
        repeat: Duration,
        /// How long it must take at least.
        needed: Duration,
    },
}

/// The plan for `config` over sets with `lens` lines each, with record ids `base | i`.
///
/// # Errors
///
/// [`PlanError`] for a rate that is not positive, a duplicate share over 50%, a count of
/// [`MAX_COUNT`] or more, no sets or an empty one, a rate too low to fit a duplicate within
/// [`DUP_LAG`], or a rate that repeats a body too soon.
pub fn plan(config: &PlanConfig, lens: &[usize], base: u64) -> Result<Vec<Planned>, PlanError> {
    let invalid = |message: &str| Err(PlanError::InvalidFlag(message.to_owned()));
    if !(config.rate.is_finite() && config.rate > 0.0) {
        return invalid("--rate must be a positive number");
    }
    if config.dup_percent > 50 {
        return invalid("--dup-percent must be from 0 to 50");
    }
    if config.count >= MAX_COUNT {
        return Err(PlanError::InvalidFlag(format!(
            "--count must be below {MAX_COUNT}"
        )));
    }
    let Some(&shortest) = lens.iter().min() else {
        return invalid("--datasets names no set");
    };
    if shortest == 0 {
        return invalid("a set has no lines");
    }

    let share = f64::from(config.dup_percent) / 100.0;
    let originals_per_second = config.rate * (1.0 - share);
    let repeat =
        Duration::from_secs_f64(shortest as f64 * lens.len() as f64 / originals_per_second);
    let needed = config.dedupe_window + DUP_LAG + REPEAT_MARGIN;
    if repeat < needed {
        return Err(PlanError::RateTooHigh { repeat, needed });
    }
    // A duplicate is due at most `half` slots after its original and waits behind at most
    // `half` others, so it is sent within `2 * half` slots, which is within the lag.
    let lag_slots = (DUP_LAG.as_secs_f64() * config.rate).floor() as u64;
    let half = lag_slots / 2;
    if share > 0.0 && half == 0 {
        return invalid("--rate is too low to send a duplicate within 500ms; use at least 4");
    }
    let dup_chance = share / (1.0 - share);

    let mut rng = SplitMix64(config.seed);
    let mut sent_per_set = vec![0_u64; lens.len()];
    let mut originals = 0_usize;
    // Scheduled duplicates by (due slot, original's slot), each held as its original.
    let mut due: BTreeMap<(u64, u64), Planned> = BTreeMap::new();
    let mut messages = Vec::with_capacity(usize::try_from(config.count).unwrap_or(0));
    for i in 0..config.count {
        let id = base | i;
        let at = Duration::from_secs_f64(i as f64 / config.rate);
        if let Some(entry) = due.first_entry().filter(|entry| entry.key().0 <= i) {
            let original = entry.remove();
            messages.push(Planned {
                id,
                dup_of: Some(original.id),
                at,
                ..original
            });
            continue;
        }
        let set = originals % lens.len();
        originals += 1;
        let len = lens[set] as u64;
        let (line, cycle) = (sent_per_set[set] % len, sent_per_set[set] / len);
        sent_per_set[set] += 1;
        let line = usize::try_from(line).unwrap_or(usize::MAX);
        let original = Planned {
            id,
            set,
            line,
            cycle,
            dup_of: None,
            at,
        };
        if half > 0 && (due.len() as u64) < half && rng.unit() < dup_chance {
            let after = 1 + rng.next() % half;
            due.insert((i + after, i), original.clone());
        }
        messages.push(original);
    }
    Ok(messages)
}

/// A small, seedable generator, so a plan needs no dependency to be reproducible.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A number in `[0, 1)`.
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1_u64 << 53) as f64
    }
}
