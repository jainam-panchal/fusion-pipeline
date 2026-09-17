//! The verifier's verdict: the expectations against what the sinks wrote and the
//! dead-letter stream holds after a run.
//!
//! What reached each subject is judged per duplicate group, because `dedupe` keeps whichever
//! copy of a group it sees first and lets both through when they race: a group is missing
//! from a subject when no id of it arrived there, and a second id, or a second copy of one
//! id, is an extra copy, which at-least-once delivery allows. A message nobody expected on that
//! subject, under that tenant, or without a readable `Fusion-Record-Id` is unexpected. A
//! dead letter fails the run even when a duplicate of it arrived.
//!
//! `dedupe` is judged by the share of planned duplicates (expectations with `drop: dedupe`)
//! that never reached the main subject: under [`MIN_DUPLICATES_DROPPED_PERCENT`] fails the run, so a
//! `dedupe` that drops nothing, or only some, cannot pass, while the copies a race or a
//! paused state store lets through (chaos, #14: about 5s of a 60s run) can.
//!
//! Extraction and `edit` are judged once per group, on the first copy that reached the main
//! subject. Extraction compares the set's CSV columns, an absent attribute equal to an empty
//! cell and `Content` without trailing whitespace; it is reported, never gated. `edit` is
//! compared with what the record itself carries (a copied attribute equals its source, as
//! extracted), so a pattern's mistake is never counted as `edit`'s; a mismatch fails the run.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use serde_json::{Map, Value};

use crate::expect::{Expectation, Group};
use crate::loghub::{self, MAIN};

/// The share of planned duplicates, in percent, `dedupe` must drop for a run to pass.
pub const MIN_DUPLICATES_DROPPED_PERCENT: u64 = 80;

/// How many examples of each finding the summary lists.
const SHOWN: usize = 20;

/// How many mismatching `LineId`s the summary lists per set.
const SHOWN_LINE_IDS: usize = 100;

/// One message a sink wrote, read back off its subject.
#[derive(Debug, Clone)]
pub struct Written {
    /// The subject it was published on.
    pub subject: String,
    /// Its `Fusion-Record-Id` header, as written.
    pub record_id: Option<String>,
    /// Its `Fusion-Tenant` header.
    pub tenant: Option<String>,
    /// Its record's attributes.
    pub attributes: Map<String, Value>,
}

/// One message read from the dead-letter stream.
#[derive(Debug, Clone)]
pub struct DeadLetter {
    /// Its `Fusion-Record-Id` header, as written.
    pub record_id: Option<String>,
}

/// A kind of failure the summary gives examples of.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Finding {
    /// A group none of whose ids reached one of its subjects.
    Missing,
    /// A message or dead letter nobody expected.
    Unexpected,
    /// A published message in the dead-letter stream.
    DeadLettered,
    /// A main-subject copy whose `edit` attributes are not what the config writes.
    EditMismatch,
}

impl Finding {
    /// The finding's name in the summary.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Unexpected => "unexpected",
            Self::DeadLettered => "dead_lettered",
            Self::EditMismatch => "edit_mismatch",
        }
    }
}

/// Extraction for one set, whose name is its log format.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SetReport {
    /// Groups whose main-subject copy was compared.
    pub checked: u64,
    /// Of those, groups whose attributes differ from the CSV.
    pub mismatched: u64,
    /// The distinct `LineId`s that differ, in order: a line that differs in every cycle is
    /// listed once.
    pub mismatched_lines: BTreeSet<usize>,
}

impl SetReport {
    /// The share of compared groups that match, 0 when none was compared.
    #[must_use]
    pub fn accuracy(&self) -> f64 {
        if self.checked == 0 {
            return 0.0;
        }
        (self.checked - self.mismatched) as f64 / self.checked as f64
    }
}

/// The outcome of a run.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Report {
    /// Messages the producer published and got a `PubAck` for.
    pub published: u64,
    /// Distinct (record id, subject) pairs read back that were expected there.
    pub received: u64,
    /// (group, subject) pairs no id of the group reached.
    pub missing: u64,
    /// Messages and dead letters nobody expected.
    pub unexpected: u64,
    /// Messages beyond the first per (group, subject).
    pub extra_copies: u64,
    /// Published messages found in the dead-letter stream.
    pub dead_lettered: u64,
    /// Planned duplicates: expectations with `drop: dedupe`.
    pub duplicates_planned: u64,
    /// Of those, how many copies never reached the main subject.
    pub duplicates_dropped: u64,
    /// Groups whose main-subject copy does not carry what `edit` writes.
    pub edit_mismatch: u64,
    /// Extraction per set.
    pub sets: BTreeMap<String, SetReport>,
    /// Examples of each finding, for the summary.
    pub examples: BTreeMap<Finding, Vec<String>>,
}

impl Report {
    /// Whether nothing was lost, unexpected, dead-lettered or wrongly edited, and `dedupe`
    /// dropped enough of the planned duplicates.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.missing == 0
            && self.unexpected == 0
            && self.dead_lettered == 0
            && self.edit_mismatch == 0
            && self.dedupe_held()
    }

    /// Whether `dedupe` dropped at least [`MIN_DUPLICATES_DROPPED_PERCENT`] of the planned
    /// duplicates; true when none was planned.
    #[must_use]
    pub fn dedupe_held(&self) -> bool {
        u128::from(self.duplicates_dropped) * 100
            >= u128::from(self.duplicates_planned) * u128::from(MIN_DUPLICATES_DROPPED_PERCENT)
    }

    fn example(&mut self, finding: Finding, text: impl FnOnce() -> String) {
        let list = self.examples.entry(finding).or_default();
        if list.len() < SHOWN {
            list.push(text());
        }
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "published      {}", self.published)?;
        writeln!(f, "received       {}", self.received)?;
        writeln!(f, "missing        {}", self.missing)?;
        writeln!(f, "unexpected     {}", self.unexpected)?;
        writeln!(f, "dead_lettered  {}", self.dead_lettered)?;
        writeln!(f, "edit_mismatch  {}", self.edit_mismatch)?;
        writeln!(f, "extra_copies   {}", self.extra_copies)?;
        writeln!(
            f,
            "dedupe         dropped {} of {} planned duplicates (at least {}%: {})",
            self.duplicates_dropped,
            self.duplicates_planned,
            MIN_DUPLICATES_DROPPED_PERCENT,
            if self.dedupe_held() { "held" } else { "FAILED" }
        )?;
        writeln!(f, "extraction by set:")?;
        for (name, set) in &self.sets {
            writeln!(
                f,
                "  {name:<8} {:>7.3}% of {} groups, {} mismatched",
                set.accuracy() * 100.0,
                set.checked,
                set.mismatched
            )?;
            if !set.mismatched_lines.is_empty() {
                let shown: Vec<String> = set
                    .mismatched_lines
                    .iter()
                    .take(SHOWN_LINE_IDS)
                    .map(ToString::to_string)
                    .collect();
                let more = set.mismatched_lines.len().saturating_sub(shown.len());
                let tail = if more > 0 {
                    format!(" (+{more} more)")
                } else {
                    String::new()
                };
                writeln!(f, "    distinct LineIds: {}{tail}", shown.join(", "))?;
            }
        }
        for (finding, list) in &self.examples {
            writeln!(f, "{}:", finding.as_str())?;
            for line in list {
                writeln!(f, "  {line}")?;
            }
        }
        write!(
            f,
            "verdict: {}",
            if self.passed() { "PASS" } else { "FAIL" }
        )
    }
}

/// A `Fusion-Record-Id` as the pipeline writes it: decimal digits only.
fn parse_id(text: Option<&str>) -> Option<u64> {
    text.filter(|t| !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit()))?
        .parse()
        .ok()
}

/// The judgement of `written` and `dead` against `expectations`.
#[must_use]
pub fn judge(expectations: &[Expectation], written: &[Written], dead: &[DeadLetter]) -> Report {
    let mut report = Report {
        published: expectations.len() as u64,
        ..Report::default()
    };
    let by_id: HashMap<u64, &Expectation> = expectations.iter().map(|e| (e.id, e)).collect();
    for e in expectations {
        report.sets.entry(e.set.clone()).or_default();
    }

    let mut copies: HashMap<(u64, &str), u64> = HashMap::new();
    let mut reached: HashMap<(Group<'_>, &str), u64> = HashMap::new();
    let mut compared: BTreeSet<Group<'_>> = BTreeSet::new();
    for w in written {
        let id = parse_id(w.record_id.as_deref());
        let Some(e) = id.and_then(|id| by_id.get(&id)) else {
            report.unexpected += 1;
            report.example(Finding::Unexpected, || {
                format!("{:?} on {}: no published record id", w.record_id, w.subject)
            });
            continue;
        };
        if !e.subjects.contains(&w.subject) || w.tenant.as_deref() != Some(&e.tenant) {
            report.unexpected += 1;
            report.example(Finding::Unexpected, || {
                format!(
                    "id {} ({} LineId {}) on {} under tenant {:?}",
                    e.id, e.set, e.line_id, w.subject, w.tenant
                )
            });
            continue;
        }
        let seen = copies.entry((e.id, w.subject.as_str())).or_default();
        *seen += 1;
        if *seen > 1 {
            report.extra_copies += 1;
            continue;
        }
        let ids = reached.entry((e.group(), w.subject.as_str())).or_default();
        *ids += 1;
        if *ids > 1 {
            report.extra_copies += 1;
        }
        if w.subject == MAIN && compared.insert(e.group()) {
            let set = report.sets.entry(e.set.clone()).or_default();
            set.checked += 1;
            if !extraction_matches(e, &w.attributes) {
                set.mismatched += 1;
                set.mismatched_lines.insert(e.line_id);
            }
            if !edits_match(e, &w.attributes) {
                report.edit_mismatch += 1;
                report.example(Finding::EditMismatch, || {
                    format!("id {} ({} LineId {})", e.id, e.set, e.line_id)
                });
            }
        }
    }
    report.received = copies.len() as u64;

    // Per group: its first expectation, how many ids it has, how many were planned drops.
    let mut groups: BTreeMap<Group<'_>, (&Expectation, u64, u64)> = BTreeMap::new();
    for e in expectations {
        let (_, size, planned) = groups.entry(e.group()).or_insert((e, 0, 0));
        *size += 1;
        if e.drop.is_some() {
            *planned += 1;
        }
    }
    for (group, (e, size, planned)) in &groups {
        for subject in &e.subjects {
            if !reached.contains_key(&(*group, subject.as_str())) {
                report.missing += 1;
                report.example(Finding::Missing, || {
                    format!(
                        "{} LineId {} cycle {} never reached {subject}",
                        e.set, e.line_id, e.cycle
                    )
                });
            }
        }
        // One copy of a group is meant to arrive; every other copy that did not arrive is a
        // drop, whichever of them `dedupe` kept.
        let arrived = reached.get(&(*group, MAIN)).copied().unwrap_or(0).max(1);
        report.duplicates_planned += planned;
        report.duplicates_dropped += size.saturating_sub(arrived).min(*planned);
    }

    for letter in dead {
        match parse_id(letter.record_id.as_deref()).and_then(|id| by_id.get(&id)) {
            Some(e) => {
                report.dead_lettered += 1;
                report.example(Finding::DeadLettered, || {
                    format!("id {} ({} LineId {})", e.id, e.set, e.line_id)
                });
            }
            None => {
                report.unexpected += 1;
                report.example(Finding::Unexpected, || {
                    format!("dead letter {:?}: no published record id", letter.record_id)
                });
            }
        }
    }
    report
}

/// Whether `attributes` hold exactly the CSV's value, or nothing, for each of the set's
/// columns.
fn extraction_matches(e: &Expectation, attributes: &Map<String, Value>) -> bool {
    let columns = loghub::set(&e.set).map_or(&[][..], |set| set.columns);
    columns.iter().all(|column| {
        let got = attributes.get(*column).map(|value| match value {
            Value::String(text) => loghub::normalise(column, text),
            other => loghub::normalise(column, &other.to_string()),
        });
        got.as_ref() == e.attributes.get(*column)
    })
}

/// Whether `attributes` carry what `edit` writes: each set attribute with its value, and
/// each copied attribute equal to its source in the same record, both absent together.
fn edits_match(e: &Expectation, attributes: &Map<String, Value>) -> bool {
    let set = e
        .edits
        .set
        .iter()
        .all(|(name, value)| attributes.get(name).and_then(Value::as_str) == Some(value));
    let copied = e
        .edits
        .copied
        .iter()
        .all(|(target, source)| attributes.get(target) == attributes.get(source));
    set && copied
}
