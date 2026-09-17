//! The verifier's verdict: the expectations against what the sinks and the dead-letter stream
//! hold after a run.
//!
//! Delivery is judged per duplicate group and sink, because `dedupe` keeps whichever copy of
//! a group it sees first and lets both through when they race: a group is missing from a
//! sink when no id of it arrived there, and a second id, or a second copy of one id, is an
//! extra copy, which at-least-once delivery allows. A delivery nobody expected on that
//! subject, under that tenant, or without a readable `Fusion-Record-Id` is unexpected. A
//! dead letter fails the run even when a duplicate of it arrived.
//!
//! Extraction is judged once per group, on the first copy that reached the main sink: the
//! set's CSV columns, an absent attribute equal to an empty cell and `Content` compared
//! without trailing whitespace. It is reported, never gated.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use serde_json::{Map, Value};

use crate::expect::{Expectation, MAIN};
use crate::loghub;

/// How many examples of each failure the summary lists.
const SHOWN: usize = 20;

/// One message read from a sink subject.
#[derive(Debug, Clone)]
pub struct Delivery {
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

/// Extraction for one set.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct FormatReport {
    /// Groups whose main-sink copy was compared.
    pub checked: u64,
    /// Of those, groups whose attributes differ from the CSV.
    pub mismatched: u64,
    /// The `LineId`s that differ, in the order found.
    pub mismatched_lines: Vec<usize>,
}

impl FormatReport {
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
    /// Distinct (record id, subject) pairs read from the sinks.
    pub received: u64,
    /// (group, sink) pairs no id of the group reached.
    pub missing: u64,
    /// Deliveries and dead letters nobody expected.
    pub unexpected: u64,
    /// Deliveries beyond the first per (group, sink).
    pub extra_copies: u64,
    /// Published messages found in the dead-letter stream.
    pub dead_lettered: u64,
    /// Extraction per set.
    pub formats: BTreeMap<String, FormatReport>,
    /// Examples of each failure, for the summary.
    pub examples: BTreeMap<&'static str, Vec<String>>,
}

impl Report {
    /// Whether nothing was lost, nothing unexpected arrived and nothing was dead-lettered.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.missing == 0 && self.unexpected == 0 && self.dead_lettered == 0
    }

    fn example(&mut self, kind: &'static str, text: impl FnOnce() -> String) {
        let list = self.examples.entry(kind).or_default();
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
        writeln!(f, "extra_copies   {}", self.extra_copies)?;
        writeln!(f, "extraction:")?;
        for (set, format) in &self.formats {
            writeln!(
                f,
                "  {set:<8} {:>7.3}% of {} groups, {} mismatched",
                format.accuracy() * 100.0,
                format.checked,
                format.mismatched
            )?;
            if !format.mismatched_lines.is_empty() {
                let shown: Vec<String> = format
                    .mismatched_lines
                    .iter()
                    .take(SHOWN * 5)
                    .map(ToString::to_string)
                    .collect();
                let more = format.mismatched_lines.len().saturating_sub(shown.len());
                let tail = if more > 0 {
                    format!(" (+{more} more)")
                } else {
                    String::new()
                };
                writeln!(f, "    LineIds: {}{tail}", shown.join(", "))?;
            }
        }
        for (kind, list) in &self.examples {
            writeln!(f, "{kind}:")?;
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

/// The judgement of `deliveries` and `dead` against `expectations`.
#[must_use]
pub fn judge(expectations: &[Expectation], deliveries: &[Delivery], dead: &[DeadLetter]) -> Report {
    let mut report = Report {
        published: expectations.len() as u64,
        ..Report::default()
    };
    let by_id: HashMap<u64, &Expectation> = expectations.iter().map(|e| (e.id, e)).collect();
    for e in expectations {
        report.formats.entry(e.set.clone()).or_default();
    }

    let mut copies: HashMap<(u64, &str), u64> = HashMap::new();
    let mut reached: HashMap<((&str, usize, u64), &str), u64> = HashMap::new();
    let mut compared: BTreeSet<(&str, usize, u64)> = BTreeSet::new();
    for d in deliveries {
        let id = parse_id(d.record_id.as_deref());
        let Some(e) = id.and_then(|id| by_id.get(&id)) else {
            report.unexpected += 1;
            report.example("unexpected", || {
                format!("{:?} on {}: no published record id", d.record_id, d.subject)
            });
            continue;
        };
        if !e.sinks.contains(&d.subject) || d.tenant.as_deref() != Some(&e.tenant) {
            report.unexpected += 1;
            report.example("unexpected", || {
                format!(
                    "id {} ({} LineId {}) on {} under tenant {:?}",
                    e.id, e.set, e.line_id, d.subject, d.tenant
                )
            });
            continue;
        }
        let seen = copies.entry((e.id, d.subject.as_str())).or_default();
        *seen += 1;
        if *seen > 1 {
            report.extra_copies += 1;
            continue;
        }
        let ids = reached.entry((e.group(), d.subject.as_str())).or_default();
        *ids += 1;
        if *ids > 1 {
            report.extra_copies += 1;
        }
        if d.subject == MAIN && compared.insert(e.group()) {
            let format = report.formats.entry(e.set.clone()).or_default();
            format.checked += 1;
            if !extraction_matches(e, &d.attributes) {
                format.mismatched += 1;
                format.mismatched_lines.push(e.line_id);
            }
        }
    }
    report.received = copies.len() as u64;

    let mut groups: BTreeMap<(&str, usize, u64), &Expectation> = BTreeMap::new();
    for e in expectations {
        groups.entry(e.group()).or_insert(e);
    }
    for (group, e) in &groups {
        for sink in &e.sinks {
            if !reached.contains_key(&(*group, sink.as_str())) {
                report.missing += 1;
                report.example("missing", || {
                    format!(
                        "{} LineId {} cycle {} never reached {sink}",
                        e.set, e.line_id, e.cycle
                    )
                });
            }
        }
    }

    for letter in dead {
        match parse_id(letter.record_id.as_deref()).and_then(|id| by_id.get(&id)) {
            Some(e) => {
                report.dead_lettered += 1;
                report.example("dead_lettered", || {
                    format!("id {} ({} LineId {})", e.id, e.set, e.line_id)
                });
            }
            None => {
                report.unexpected += 1;
                report.example("unexpected", || {
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
        let got = attributes.get(*column).map(|value| {
            let text = match value {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            if *column == "Content" {
                text.trim_end().to_owned()
            } else {
                text
            }
        });
        got.as_ref() == e.attributes.get(*column)
    })
}
