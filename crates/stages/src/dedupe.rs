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
//! ingestion time (`observed_time_unix_nano`, then `time_unix_nano`, then the worker clock),
//! which a redelivered record carries unchanged, so the decision for a record is the same
//! whenever it reaches the stage: a crash between a state write and the ack never turns a
//! real record into a duplicate.

use std::fmt::Write as _;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::path::{FieldPath, FieldValue, Num};
use fusion_core::record::{Record, RecordId};
use fusion_core::stage::{Context, DropReason, Stage, StageOutput};
use fusion_core::state::StateErrorPolicy;
use serde::Deserialize;

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
        if params.key.is_empty() {
            return Err(node.invalid_params("`key` needs at least one field path"));
        }
        let key = params
            .key
            .iter()
            .map(|path| {
                FieldPath::parse(path)
                    .map_err(|e| node.invalid_params(format!("key `{path}`: {e}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
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
    fn state_key(&self, record: &Record) -> String {
        let mut canonical = String::from("[");
        for (i, path) in self.key.iter().enumerate() {
            if i > 0 {
                canonical.push(',');
            }
            write_canonical(&mut canonical, path.read(record));
        }
        canonical.push(']');
        format!("{PURPOSE}:{:016x}", fnv1a64(canonical.as_bytes()))
    }
}

impl Stage for Dedupe {
    fn process(&self, record: Record, ctx: &Context<'_>) -> StageOutput {
        let incoming = Holder {
            id: ctx.record_id,
            ingestion_time: ingestion_time(&record),
        };
        let key = self.state_key(&record);
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
        StageOutput::Pass(record)
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

/// Who holds a dedupe key: the record that claimed it and its ingestion time. The state
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
    /// The output for a verdict that ends the decision: only a repeat drops. The same
    /// record again (redelivery) and a record older than the holder (an earlier record
    /// coming back after its key expired) pass, and so does a record facing a value the
    /// stage cannot read, since passing is at worst one extra copy and never a lost record.
    /// `WindowOver` does not end the decision and is not settled here.
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

/// When the record entered: `observed_time_unix_nano`, else `time_unix_nano`, else now.
fn ingestion_time(record: &Record) -> u64 {
    record
        .observed_time_unix_nano
        .or(record.time_unix_nano)
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        })
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

/// One key field as canonical JSON, so equal values hash equal whatever their source.
fn write_canonical(out: &mut String, value: FieldValue<'_>) {
    match value {
        FieldValue::Null => out.push_str("null"),
        FieldValue::Bool(b) => out.push_str(if b { "true" } else { "false" }),
        FieldValue::Num(Num::Int(i)) => {
            let _ = write!(out, "{i}");
        }
        FieldValue::Num(Num::Float(f)) => {
            let _ = write!(out, "{f:?}");
        }
        FieldValue::Str(s) => out.push_str(&serde_json::to_string(s).unwrap_or_default()),
        FieldValue::Json(v) => out.push_str(&serde_json::to_string(v).unwrap_or_default()),
        _ => out.push_str("null"),
    }
}

/// FNV-1a, 64-bit: stable across builds and platforms, and cheap next to a store round trip.
fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    bytes
        .iter()
        .fold(OFFSET, |hash, &b| (hash ^ u64::from(b)).wrapping_mul(PRIME))
}
