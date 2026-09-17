//! What the producer expects of every message it sent, written as the expectations file (one
//! JSON object per line) for the verifier.
//!
//! Where a record goes is a property of `deploy/pipeline-poc.yaml`, written down here by hand
//! rather than computed by running the pipeline: the verifier checks the pipeline against
//! this table, and `crates/pipeline/tests/deploy_configs.rs` checks that the table and the
//! config agree.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::loghub;

/// The subject every set's records reach, after extraction.
pub const MAIN: &str = "processed.loghub.main";

/// The subject Linux records also reach, straight from the route, before extraction.
pub const AUDIT: &str = "processed.loghub.audit";

/// The subjects a record of `set` reaches, `None` for a set the harness does not know.
#[must_use]
pub fn sinks(set: &str) -> Option<&'static [&'static str]> {
    match set {
        "Linux" => Some(&[MAIN, AUDIT]),
        "OpenSSH" | "Apache" | "Mac" => Some(&[MAIN]),
        _ => None,
    }
}

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
    /// The subjects its duplicate group must reach.
    pub sinks: Vec<String>,
    /// Why it may be dropped: `dedupe` for a deliberate duplicate, which may equally arrive.
    pub drop: Option<String>,
    /// The attributes extraction must lift: the line's CSV row, normalised, empty cells left
    /// out.
    pub attributes: BTreeMap<String, String>,
}

impl Expectation {
    /// The duplicate group: the ids the producer sent for one line in one cycle.
    #[must_use]
    pub fn group(&self) -> (&str, usize, u64) {
        (&self.set, self.line_id, self.cycle)
    }
}

/// The expectation for a message, `None` when `set` is not a vendored set.
#[must_use]
pub fn expectation(
    id: u64,
    set: &str,
    line_id: usize,
    cycle: u64,
    dup_of: Option<u64>,
    attributes: BTreeMap<String, String>,
) -> Option<Expectation> {
    let tenant = loghub::set(set)?.tenant;
    Some(Expectation {
        id,
        set: set.to_owned(),
        tenant: tenant.to_owned(),
        line_id,
        cycle,
        dup_of,
        sinks: sinks(set)?.iter().map(|s| (*s).to_owned()).collect(),
        drop: dup_of.map(|_| "dedupe".to_owned()),
        attributes,
    })
}
