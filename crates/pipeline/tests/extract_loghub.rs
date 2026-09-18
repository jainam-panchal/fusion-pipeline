//! Extraction accuracy against loghub ground truth: for every 20th line of a vendored
//! set, the attributes the set's `extract` node in `deploy/pipeline-poc.yaml` lifts equal that
//! line's row in the `_structured_corrected.csv`, under the normalisation rules in
//! `testdata/loghub/README.md`. The sets, their columns and the rules are the loghub
//! harness's (`fusion_harness::loghub`).

mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;

use fusion_core::memory::AckOutcome;
use fusion_core::record::Record;
use fusion_harness::loghub::{self, Set};
use serde_json::{Value, json};

use common::{Harness, WAIT, deploy_pattern, start};

fn set(name: &str) -> &'static Set {
    loghub::set(name).unwrap_or_else(|| panic!("{name} is a vendored set"))
}

/// The pattern of the set's extract node in the POC config.
fn pattern(set: &Set) -> String {
    deploy_pattern(
        "pipeline-poc.yaml",
        &format!("parse_{}", set.name.to_lowercase()),
    )
}

/// Every 20th `LineId`, so 100 of the 2,000 lines, the same ones every run.
const STRIDE: usize = 20;

fn testdata(set: &str, suffix: &str) -> PathBuf {
    loghub::testdata()
        .join(set)
        .join(format!("{set}_2k.log{suffix}"))
}

fn raw_lines(set: &str) -> Vec<String> {
    std::fs::read_to_string(testdata(set, ""))
        .expect("raw log is vendored")
        .lines()
        .map(str::to_owned)
        .collect()
}

/// `LineId` to the expected attributes for that line, normalised per the README.
fn ground_truth(set: &Set) -> BTreeMap<usize, BTreeMap<String, String>> {
    let mut reader = csv::Reader::from_path(testdata(set.name, "_structured_corrected.csv"))
        .expect("structured CSV is vendored");
    let headers = reader.headers().expect("CSV has a header").clone();
    let mut rows = BTreeMap::new();
    for row in reader.records() {
        let row = row.expect("CSV row parses");
        let cell = |name: &str| {
            let i = headers
                .iter()
                .position(|h| h == name)
                .unwrap_or_else(|| panic!("column {name}"));
            row.get(i).unwrap_or("")
        };
        let line_id: usize = cell("LineId").parse().expect("LineId is an integer");
        let expected = set
            .columns
            .iter()
            .filter_map(|column| {
                let text = loghub::normalise(column, cell(column));
                (!text.is_empty()).then(|| ((*column).to_owned(), text))
            })
            .collect();
        rows.insert(line_id, expected);
    }
    rows
}

fn extracted(h: &Harness, line_id: usize) -> BTreeMap<String, String> {
    let record = h
        .sinks
        .records("out")
        .into_iter()
        .find(|r| r.value()["id"].as_u64().map(|i| i as usize) == Some(line_id))
        .unwrap_or_else(|| panic!("line {line_id} reached the sink"));
    let attributes = record.value()["attributes"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    attributes
        .into_iter()
        .filter_map(|(k, v)| match v {
            Value::String(s) if k == "Content" => Some((k, s.trim_end().to_owned())),
            Value::String(s) => Some((k, s)),
            _ => None,
        })
        .collect()
}

fn assert_set_extracts_its_ground_truth(set: &Set) {
    let lines = raw_lines(set.name).len();
    let sampled: Vec<usize> = (1..=lines).step_by(STRIDE).collect();
    assert_eq!(sampled.len(), 100, "{}: 100 lines sampled", set.name);
    assert_lines_extract_their_ground_truth(set, &sampled);
}

fn assert_lines_extract_their_ground_truth(set: &Set, sampled: &[usize]) {
    let yaml = format!(
        r#"
nodes:
  - id: parse
    type: extract
    field: body
    pattern: '{}'
  - id: out
    type: sink.memory
"#,
        pattern(set)
    );
    let h = start(&yaml, 4);
    let lines = raw_lines(set.name);
    let truth = ground_truth(set);

    let probes: Vec<_> = sampled
        .iter()
        .map(|&line_id| {
            let record = Record::from_json(
                &json!({
                    "id": line_id,
                    "body": lines[line_id - 1],
                    "resource": {"log.format": set.name},
                    "attributes": {"loghub.line_id": line_id},
                })
                .to_string(),
            )
            .expect("record parses");
            (line_id, h.push(record))
        })
        .collect();
    for (line_id, probe) in &probes {
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "line {line_id}");
    }

    let mut mismatches = Vec::new();
    for &line_id in sampled {
        let mut got = extracted(&h, line_id);
        got.remove("loghub.line_id");
        let want = &truth[&line_id];
        if &got != want {
            mismatches.push(format!(
                "LineId {line_id}\n  line: {}\n  want: {want:?}\n  got:  {got:?}",
                lines[line_id - 1]
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "{}: {} of {} sampled lines mismatch\n{}",
        set.name,
        mismatches.len(),
        sampled.len(),
        mismatches.join("\n")
    );
    h.finish();
}

#[test]
fn linux_lines_extract_to_the_structured_csv_columns() {
    assert_set_extracts_its_ground_truth(set("Linux"));
}

/// Linux lines with more than one space before the component (`combo  -- root[2421]:`) or
/// after the colon (`kernel:   HighMem zone: ...`): the CSV's `Component` and `Content` start
/// at the first non-space. Found by the loghub harness (issue #13).
#[test]
fn linux_lines_with_extra_spaces_extract_without_them() {
    assert_lines_extract_their_ground_truth(
        set("Linux"),
        &[899, 1913, 1914, 1915, 1916, 1917, 1923, 1924, 1926],
    );
}

#[test]
fn apache_lines_extract_to_the_structured_csv_columns() {
    assert_set_extracts_its_ground_truth(set("Apache"));
}

#[test]
fn openssh_lines_extract_to_the_structured_csv_columns() {
    assert_set_extracts_its_ground_truth(set("OpenSSH"));
}

#[test]
fn mac_lines_extract_to_the_structured_csv_columns() {
    assert_set_extracts_its_ground_truth(set("Mac"));
}
