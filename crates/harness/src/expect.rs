//! What the producer expects of every message it sent, written as the expectations file (one
//! JSON object per line) for the verifier.
//!
//! What the pipeline does with a record is a property of `deploy/pipeline-poc.yaml`, written
//! down here by hand rather than computed by running the pipeline: the subjects a set reaches
//! are on its [`Set`], and what the config's `edit` node writes is [`Edits::poc`]. The
//! verifier checks the pipeline against them, and `crates/pipeline/tests/deploy_configs.rs`
//! checks that they and the config agree.

use std::collections::BTreeMap;

use fusion_core::stage::DropReason;
use serde::{Deserialize, Serialize};

use crate::loghub::{Line, Set};

/// The name the POC config's `edit` node writes into `attributes.pipeline`.
pub const PIPELINE_NAME: &str = "poc";

/// The expected outcome of one message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Expectation {
    /// The record id the message carried in `Fusion-Record-Id`.
    pub id: u64,
    /// Its set, which is its `resource.log.format`.
    pub set: String,
    /// The tenant it was published under.
    pub tenant: String,
    /// The `LineId` of its line.
    pub line_id: usize,
    /// Its set's cycle.
    pub cycle: u64,
    /// For a deliberate duplicate, the id of its original.
    pub dup_of: Option<u64>,
    /// The subjects its duplicate group must reach, the main one first.
    pub subjects: Vec<String>,
    /// Why it may be dropped: `dedupe` for a deliberate duplicate, which may equally arrive.
    #[serde(with = "drop_reason")]
    pub drop: Option<DropReason>,
    /// The attributes extraction must lift: the line's CSV row, normalised, empty cells left
    /// out.
    pub attributes: BTreeMap<String, String>,
    /// What the config's `edit` node writes on the main subject.
    pub edits: Edits,
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
/// duplicate of `dup_of` when given.
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
        subjects: set.subjects.iter().map(|s| (*s).to_owned()).collect(),
        drop: dup_of.map(|_| DropReason::Dedupe),
        attributes: line.attributes.clone(),
        edits: Edits::poc(),
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
