//! `dedupe`: suppress repeats of a record, keyed on chosen fields, inside a window.
//!
//! ```yaml
//! - id: dedupe_body
//!   type: dedupe
//!   key: [body, resource.host]   # one or more field paths; a missing field is null
//!   window: 10s                  # ms | s | m | h; at least 1 ms
//!   on_state_error: pass         # pass (default) | nak
//! ```
//!
//! One state key per distinct content, `dedupe:{hash}` under the handle's prefix, holding
//! `"{record id} {ingestion time}"` with the window as its TTL. The window is measured in
//! the ingestion time on the record's [`Meta`](fusion_core::meta::Meta), fixed at intake
//! and unchanged by redelivery or by any stage rewriting the record's time fields, so the
//! decision for a record is the same whenever it reaches the stage: a crash between a state
//! write and the ack never turns a real record into a duplicate.

use std::time::Duration;

use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::meta::Meta;
use fusion_core::path::FieldPath;
use fusion_core::record::{Record, RecordId};
use fusion_core::stage::{Context, DropReason, Stage, StageOutput};
use fusion_core::state::StateErrorPolicy;
use serde::Deserialize;

use crate::key_hash::{hash_key_fields, parse_key_fields};

/// The purpose segment of this stage's keys: `{prefix}dedupe:{hash}`.
const PURPOSE: &str = "dedupe";

/// How many times a record past the window tries to take the key over before passing
/// without it. Each refusal means another worker wrote first; two is enough to survive one
/// such write, and the bound keeps a busy key from holding a worker in a loop.
const TAKEOVER_ATTEMPTS: usize = 2;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    key: Vec<String>,
    window: WindowText,
    #[serde(default = "default_policy")]
    on_state_error: StateErrorPolicy,
}

/// `window` as written: a string, or a bare number so the error can say a unit is missing.
#[derive(Deserialize)]
#[serde(untagged)]
enum WindowText {
    Text(String),
    Bare(u64),
}

impl WindowText {
    fn as_text(&self) -> String {
        match self {
            Self::Text(text) => text.clone(),
            Self::Bare(n) => n.to_string(),
        }
    }
}

const fn default_policy() -> StateErrorPolicy {
    StateErrorPolicy::Pass
}

/// The `dedupe` stage.
#[derive(Debug)]
pub struct Dedupe {
    key: Vec<FieldPath>,
    window: Duration,
    on_state_error: StateErrorPolicy,
}

impl Dedupe {
    /// Build from a node's `key`, `window` and `on_state_error`.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidParams`] when `key` is empty or a path in it does not parse,
    /// `window` is not `<integer><ms|s|m|h>` or is under 1 ms, or `on_state_error` is not
    /// `pass` or `nak`.
    pub fn from_node(node: &NodeConfig) -> Result<Self, ConfigError> {
        let params: Params = node.parse_params()?;
        let key = parse_key_fields(node, &params.key)?;
        let window_text = params.window.as_text();
        let window = parse_window(&window_text)
            .map_err(|e| node.invalid_params(format!("window `{window_text}`: {e}")))?;
        Ok(Self {
            key,
            window,
            on_state_error: params.on_state_error,
        })
    }

    /// Factory for a [`fusion_core::registry::Registry`].
    ///
    /// # Errors
    ///
    /// See [`Dedupe::from_node`].
    pub fn build(node: &NodeConfig) -> Result<Box<dyn Stage>, ConfigError> {
        Ok(Box::new(Self::from_node(node)?))
    }

    /// The state key for `record`'s content: `dedupe:` plus the hash of its key fields.
    fn state_key(&self, record: &Record, meta: &Meta) -> String {
        format!(
            "{PURPOSE}:{:016x}",
            hash_key_fields(&self.key, record, meta)
        )
    }
}

impl Stage for Dedupe {
    fn process(&self, record: Record, ctx: &Context<'_>) -> StageOutput {
        let incoming = Holder {
            id: ctx.meta.record_id,
            ingestion_time: ctx.meta.ingestion_time.unix_nanos(),
        };
        let key = self.state_key(&record, ctx.meta);
        let existing = match ctx.state.set_nx(&key, &incoming.to_bytes(), self.window) {
            Ok(existing) => existing,
            Err(error) => return StageOutput::StateError { record, error },
        };
        let Some(existing) = existing else {
            // Claimed: first sighting in this window.
            return StageOutput::Pass(record);
        };
        // The holder's window may be over in ingestion time while its key still lives on
        // the server clock. Then this record takes the key over, so the burst behind it
        // dedupes against a fresh window instead of passing until the old TTL runs out.
        // The takeover is conditional on the holder still being the one just read: another
        // worker may have taken the key over meanwhile, and its record, not the stale one
        // read here, decides. A refused takeover answers with the current holder, so the
        // verdict is re-run against it without another round trip; `WindowOver` again is
        // tried once more, and after that the record passes without holding the key: an
        // extra copy rather than a loop other workers' writes could keep alive.
        let mut holder = existing;
        for _ in 0..TAKEOVER_ATTEMPTS {
            let verdict = incoming.against_value(&holder, self.window);
            if verdict != Verdict::WindowOver {
                return verdict.settle(record);
            }
            let taken = ctx
                .state
                .compare_and_set(&key, &holder, &incoming.to_bytes(), self.window);
            match taken {
                Ok(None) => return StageOutput::Pass(record),
                Ok(Some(current)) => holder = current,
                Err(error) => return StageOutput::StateError { record, error },
            }
        }
        Verdict::WindowOver.settle(record)
    }

    fn uses_state(&self) -> bool {
        true
    }

    fn on_state_error(&self) -> StateErrorPolicy {
        self.on_state_error
    }
}

/// The bytes this stage stores for the record `id` ingested at `ingestion_time_unix_nano`.
/// For tests that play another worker writing a holder; the format is this stage's alone.
#[doc(hidden)]
#[must_use]
pub fn holder_value(id: u64, ingestion_time_unix_nano: u64) -> Vec<u8> {
    Holder {
        id: RecordId(id),
        ingestion_time: ingestion_time_unix_nano,
    }
    .to_bytes()
}

/// Who holds a dedupe state key: the record that claimed it and its ingestion time. The state
/// value is `"{id} {ingestion time}"`, written and read here only.
struct Holder {
    id: RecordId,
    ingestion_time: u64,
}

/// How an incoming record relates to the record holding its key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// The holder itself, redelivered.
    SameRecord,
    /// Ingested before the holder: an earlier record coming back.
    OlderThanHolder,
    /// A different record ingested inside the holder's window.
    Repeat,
    /// Ingested `window` or more after the holder: the window is over.
    WindowOver,
    /// The key holds something this stage did not write.
    Unreadable,
}

impl Verdict {
    /// The output for a verdict: only a repeat drops. The same record again (redelivery)
    /// and a record older than the holder (an earlier record coming back after its key
    /// expired) pass, and so does a record facing a value the stage cannot read, since
    /// passing is at worst one extra copy and never a lost record. `WindowOver` is settled
    /// only once the takeover attempts are spent, and passes for the same reason: an extra
    /// copy rather than a loop other workers' writes could keep alive.
    fn settle(self, record: Record) -> StageOutput {
        match self {
            Self::Repeat => StageOutput::Drop(DropReason::Dedupe),
            Self::SameRecord | Self::OlderThanHolder | Self::Unreadable | Self::WindowOver => {
                StageOutput::Pass(record)
            }
        }
    }
}

impl Holder {
    fn to_bytes(&self) -> Vec<u8> {
        format!("{} {}", self.id, self.ingestion_time).into_bytes()
    }

    fn parse(value: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(value).ok()?;
        let (id, ingestion_time) = text.split_once(' ')?;
        Some(Self {
            id: RecordId(id.parse().ok()?),
            ingestion_time: ingestion_time.parse().ok()?,
        })
    }

    /// The verdict against the stored `value`; `Unreadable` when it is not a value this
    /// stage wrote, so the stage never takes a key over from what it cannot read.
    fn against_value(&self, value: &[u8], window: Duration) -> Verdict {
        Self::parse(value).map_or(Verdict::Unreadable, |holder| self.against(&holder, window))
    }

    fn against(&self, holder: &Self, window: Duration) -> Verdict {
        if self.id == holder.id {
            Verdict::SameRecord
        } else if self.ingestion_time < holder.ingestion_time {
            Verdict::OlderThanHolder
        } else if self.ingestion_time - holder.ingestion_time < window_nanos(window) {
            Verdict::Repeat
        } else {
            Verdict::WindowOver
        }
    }
}

fn window_nanos(window: Duration) -> u64 {
    u64::try_from(window.as_nanos()).unwrap_or(u64::MAX)
}

/// `<integer><ms|s|m|h>`, at least 1 ms.
fn parse_window(text: &str) -> Result<Duration, String> {
    let digits_end = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (digits, unit) = text.split_at(digits_end);
    let count: u64 = digits
        .parse()
        .map_err(|_| "write `<integer><ms|s|m|h>`, e.g. `10s`".to_owned())?;
    let unit_millis: u64 = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "" => return Err("needs a unit: `ms`, `s`, `m` or `h`".to_owned()),
        other => return Err(format!("unknown unit `{other}`; use `ms`, `s`, `m` or `h`")),
    };
    let millis = count
        .checked_mul(unit_millis)
        .ok_or_else(|| "too large".to_owned())?;
    if millis < 1 {
        return Err("must be at least 1ms".to_owned());
    }
    Ok(Duration::from_millis(millis))
}
