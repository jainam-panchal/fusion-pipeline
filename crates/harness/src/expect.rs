//! What the producer expects of every message it sent, written as the expectations file (one
//! JSON object per line) for the verifier.
//!
//! What the pipeline does with a record is a property of `deploy/pipeline-poc.yaml`, written
//! down here by hand rather than computed by running the pipeline: the subjects a set reaches
//! are on its [`Set`], which lines the config's `sample` node keeps is [`sample_keeps`], and
//! what its `edit` and `lua` nodes write are [`Edits::poc`] and [`LuaWrites::poc`]. The
//! verifier checks the pipeline against them, and `crates/pipeline/tests/deploy_configs.rs`
//! checks that they and the config agree.

use std::collections::BTreeMap;

use fusion_core::hash::{fnv1a64, mix};
use fusion_core::stage::DropReason;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::loghub::{Line, MAIN, Set};

/// The name the POC config's `edit` node writes into `attributes.pipeline`.
pub const PIPELINE_NAME: &str = "poc";

/// The `percent` of the POC config's `sample` node.
pub const SAMPLE_PERCENT: f64 = 90.0;

/// Whether the POC config's `sample` node keeps line `line_id` in `cycle`.
///
/// The node runs in `consistent` mode on `[attributes."loghub.line_id",
/// attributes."loghub.cycle"]`, both integers, so every copy of a duplicate group gets one
/// verdict, and a line left out in one cycle is kept in others. The rule is the one the
/// mode documents: FNV-1a 64 over the key values as a canonical JSON array, the splitmix
/// finalizer, kept at or below `percent` of the hash space.
#[must_use]
pub fn sample_keeps(line_id: usize, cycle: u64) -> bool {
    let threshold = (2f64.powi(64) * SAMPLE_PERCENT / 100.0) as u64;
    mix(fnv1a64(format!("[{line_id},{cycle}]").as_bytes())) <= threshold
}

/// The expected outcome of one message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Expectation {
    /// The record id the message carried in `Fusion-Record-Id`.
    pub id: u64,
    /// Its set, which is its `resource."log.format"`.
    pub set: String,
    /// The tenant it was published under.
    pub tenant: String,
    /// The `LineId` of its line.
    pub line_id: usize,
    /// Its set's cycle.
    pub cycle: u64,
    /// For a deliberate duplicate, the id of its original.
    pub dup_of: Option<u64>,
    /// The subjects its duplicate group must reach, the main one first when `sample` keeps
    /// the line in this cycle; empty for a line of a set without an audit subject that it
    /// leaves out.
    pub subjects: Vec<String>,
    /// Why it may be dropped: `dedupe` for a deliberate duplicate, which may equally arrive.
    #[serde(with = "drop_reason")]
    pub drop: Option<DropReason>,
    /// The attributes extraction must lift: the line's CSV row, normalised, empty cells left
    /// out.
    pub attributes: BTreeMap<String, String>,
    /// What the config's `edit` node writes on the main subject.
    pub edits: Edits,
    /// What the config's `lua` node writes on the main subject.
    pub lua: LuaWrites,
}

/// What an `edit` node writes into a record's attributes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edits {
    /// Attributes set to a fixed value, by name.
    pub set: BTreeMap<String, String>,
    /// Attributes copied from another attribute, target to source. Absent when the source is.
    pub copied: BTreeMap<String, String>,
}

impl Edits {
    /// Whether `attributes` carry these writes: each set attribute with its value, and each
    /// copied attribute equal to its source in the same record, both absent together.
    #[must_use]
    pub fn written_in(&self, attributes: &Map<String, Value>) -> bool {
        let set = self
            .set
            .iter()
            .all(|(name, value)| attributes.get(name).and_then(Value::as_str) == Some(value));
        let copied = self
            .copied
            .iter()
            .all(|(target, source)| attributes.get(target) == attributes.get(source));
        set && copied
    }

    /// The POC config's `edit` node: `attributes.Component` copied to `attributes.service`,
    /// and `attributes.pipeline` set to [`PIPELINE_NAME`].
    #[must_use]
    pub fn poc() -> Self {
        Self {
            set: BTreeMap::from([("pipeline".to_owned(), PIPELINE_NAME.to_owned())]),
            copied: BTreeMap::from([("service".to_owned(), "Component".to_owned())]),
        }
    }
}

/// What the POC config's `lua` node writes into a record's attributes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LuaWrites {
    /// Attributes holding the byte length of another string attribute, target to source.
    /// Absent when the source is not a string.
    pub lengths: BTreeMap<String, String>,
}

impl LuaWrites {
    /// Whether `attributes` carry these writes: each length attribute an integer equal to the
    /// byte length of its string source in the same record, both absent together.
    #[must_use]
    pub fn written_in(&self, attributes: &Map<String, Value>) -> bool {
        self.lengths.iter().all(|(target, source)| {
            let expected = attributes
                .get(source)
                .and_then(Value::as_str)
                .map(|text| text.len() as u64);
            attributes.get(target).map(Value::as_u64) == expected.map(Some)
        })
    }

    /// The POC config's `lua` node: the byte length of `attributes.Content`, as extracted
    /// (trailing whitespace included), in `attributes.content_bytes`.
    #[must_use]
    pub fn poc() -> Self {
        Self {
            lengths: BTreeMap::from([("content_bytes".to_owned(), "Content".to_owned())]),
        }
    }
}

/// A duplicate group: the ids the producer sent for one line in one cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Group<'e> {
    /// The line's set.
    pub set: &'e str,
    /// The line's `LineId`.
    pub line_id: usize,
    /// The set's cycle.
    pub cycle: u64,
}

impl Expectation {
    /// The message as a report names it: its id, set and `LineId`.
    #[must_use]
    pub fn describe(&self) -> String {
        format!("id {} ({} LineId {})", self.id, self.set, self.line_id)
    }

    /// The subject `dedupe` is judged on: the first its group reaches, since `dedupe` runs
    /// before every sink. The main subject when `sample` keeps the line, else the Linux audit
    /// subject, else none.
    #[must_use]
    pub fn dedupe_subject(&self) -> Option<&str> {
        self.subjects.first().map(String::as_str)
    }

    /// The duplicate group the message belongs to.
    #[must_use]
    pub fn group(&self) -> Group<'_> {
        Group {
            set: &self.set,
            line_id: self.line_id,
            cycle: self.cycle,
        }
    }
}

/// The expectation for message `id`, which carries `line` of `set` in `cycle`, a deliberate
/// duplicate of `dup_of` when given. A line the `sample` node leaves out in `cycle` reaches
/// only the set's subjects that branch off before it: none but the Linux audit subject.
#[must_use]
pub fn expectation(
    id: u64,
    set: &Set,
    line: &Line,
    cycle: u64,
    dup_of: Option<u64>,
) -> Expectation {
    Expectation {
        id,
        set: set.name.to_owned(),
        tenant: set.tenant.to_owned(),
        line_id: line.line_id,
        cycle,
        dup_of,
        subjects: set
            .subjects
            .iter()
            .filter(|s| **s != MAIN || sample_keeps(line.line_id, cycle))
            .map(|s| (*s).to_owned())
            .collect(),
        drop: dup_of.map(|_| DropReason::Dedupe),
        attributes: line.attributes.clone(),
        edits: Edits::poc(),
        lua: LuaWrites::poc(),
    }
}

/// A drop reason in the expectations file: its name from the closed set, or `null`.
mod drop_reason {
    use fusion_core::stage::DropReason;
    use serde::{Deserialize, Deserializer, Serializer, de};

    #[expect(
        clippy::ref_option,
        reason = "serde's `with` passes the field by reference"
    )]
    pub fn serialize<S: Serializer>(
        reason: &Option<DropReason>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match reason {
            Some(reason) => serializer.serialize_str(reason.as_str()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<DropReason>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|name| {
                DropReason::parse(&name).ok_or_else(|| {
                    de::Error::custom(format!(
                        "unknown drop reason `{name}`; expected one of {}",
                        DropReason::ONE_OF
                    ))
                })
            })
            .transpose()
    }
}
