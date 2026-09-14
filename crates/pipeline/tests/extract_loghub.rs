//! Extraction accuracy against loghub ground truth: for every 20th line of a vendored
//! set, the attributes `pcre2_extract` lifts equal that line's row in the
//! `_structured_corrected.csv`, under the normalisation rules in `testdata/loghub/README.md`.

mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;

use fusion_core::memory::AckOutcome;
use fusion_core::record::Record;
use serde_json::{Value, json};

use common::{Harness, WAIT, start};

/// One vendored set: its name, the pattern for it and the CSV columns it must lift.
struct Set {
    name: &'static str,
    pattern: &'static str,
    columns: &'static [&'static str],
}

const LINUX: Set = Set {
    name: "Linux",
    pattern: r"^(?<Month>[A-Z][a-z]{2}) +(?<Date>\d{1,2}) (?<Time>\d{2}:\d{2}:\d{2}) (?<Level>\S+) (?<Component>[^\[:]+)(?:\[(?<PID>\d+)\])?: (?<Content>.*)$",
    columns: &[
        "Month",
        "Date",
        "Time",
        "Level",
        "Component",
        "PID",
        "Content",
    ],
};

const APACHE: Set = Set {
    name: "Apache",
    pattern: r"^\[(?<Time>[^\]]+)\] \[(?<Level>\w+)\] (?<Content>.*)$",
    columns: &["Time", "Level", "Content"],
};

const OPENSSH: Set = Set {
    name: "OpenSSH",
    pattern: r"^(?<Date>[A-Z][a-z]{2}) +(?<Day>\d{1,2}) (?<Time>\d{2}:\d{2}:\d{2}) (?<Component>\S+) sshd\[(?<Pid>\d+)\]: (?<Content>.*)$",
    columns: &["Date", "Day", "Time", "Component", "Pid", "Content"],
};

/// Every 20th `LineId`, so 100 of the 2,000 lines, the same ones every run.
const STRIDE: usize = 20;

fn testdata(set: &str, suffix: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../testdata/loghub")
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
                let text = normalise(cell(column));
                (!text.is_empty()).then(|| ((*column).to_owned(), text))
            })
            .collect();
        rows.insert(line_id, expected);
    }
    rows
}

/// README rules 1 and 3: a pandas float `N.0` is the integer `N`; trailing whitespace goes.
fn normalise(cell: &str) -> String {
    let trimmed = cell.trim_end();
    match trimmed.strip_suffix(".0") {
        Some(digits) if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) => {
            digits.to_owned()
        }
        _ => trimmed.to_owned(),
    }
}

fn extracted(h: &Harness, line_id: usize) -> BTreeMap<String, String> {
    h.sinks
        .records("out")
        .into_iter()
        .find(|r| r.id.map(|i| i.0 as usize) == Some(line_id))
        .unwrap_or_else(|| panic!("line {line_id} reached the sink"))
        .attributes
        .into_iter()
        .filter_map(|(k, v)| match v {
            Value::String(s) => Some((k, s.trim_end().to_owned())),
            _ => None,
        })
        .collect()
}

fn assert_set_extracts_its_ground_truth(set: &Set) {
    let yaml = format!(
        r#"
nodes:
  - id: parse
    type: pcre2_extract
    field: body
    pattern: '{}'
  - id: out
    type: sink.memory
"#,
        set.pattern
    );
    let h = start(&yaml, 4);
    let lines = raw_lines(set.name);
    let truth = ground_truth(set);
    let sampled: Vec<usize> = (1..=lines.len()).step_by(STRIDE).collect();
    assert_eq!(sampled.len(), 100, "{}: 100 lines sampled", set.name);

    let probes: Vec<_> = sampled
        .iter()
        .map(|&line_id| {
            let record = Record::from_json(
                &json!({
                    "id": line_id,
                    "body": lines[line_id - 1],
                    "resource": {"tenant.id": set.name, "log.format": set.name},
                    "attributes": {"loghub.line_id": line_id},
                })
                .to_string(),
            )
            .expect("record parses");
            (line_id, h.source.push(record))
        })
        .collect();
    for (line_id, probe) in &probes {
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "line {line_id}");
    }

    let mut mismatches = Vec::new();
    for &line_id in &sampled {
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
    assert_set_extracts_its_ground_truth(&LINUX);
}

#[test]
fn apache_lines_extract_to_the_structured_csv_columns() {
    assert_set_extracts_its_ground_truth(&APACHE);
}

#[test]
fn openssh_lines_extract_to_the_structured_csv_columns() {
    assert_set_extracts_its_ground_truth(&OPENSSH);
}
