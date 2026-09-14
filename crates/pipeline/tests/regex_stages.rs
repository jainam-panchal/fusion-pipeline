//! `pcre2_extract` and `redact` through the trait boundary: YAML config in, envelopes
//! through the in-memory source, assertions on what reached the in-memory sink, how each
//! ack settled and what the recorder saw.

mod common;

use fusion_core::memory::AckOutcome;
use fusion_core::metrics::Metric;
use fusion_core::record::Record;
use serde_json::{Value, json};

use common::{Harness, WAIT, for_each_worker_count, start};

/// The spec's Linux syslog pattern, lifting the structured CSV's columns.
const LINUX_PATTERN: &str = r"^(?<Month>[A-Z][a-z]{2}) +(?<Date>\d{1,2}) (?<Time>\d{2}:\d{2}:\d{2}) (?<Level>\S+) (?<Component>[^\[:]+)(?:\[(?<PID>\d+)\])?: (?<Content>.*)$";

fn extract_yaml(pattern: &str) -> String {
    format!(
        r#"
nodes:
  - id: parse_linux
    type: pcre2_extract
    field: body
    pattern: '{pattern}'
  - id: out
    type: sink.memory
"#
    )
}

fn record(id: u64, body: &str) -> Record {
    Record::from_json(
        &json!({"id": id, "body": body, "resource": {"tenant.id": "acme"}}).to_string(),
    )
    .expect("record parses")
}

fn attributes(h: &Harness, sink: &str, id: u64) -> serde_json::Map<String, Value> {
    h.sinks
        .records(sink)
        .into_iter()
        .find(|r| r.id.map(|i| i.0) == Some(id))
        .unwrap_or_else(|| panic!("record {id} reached `{sink}`"))
        .attributes
}

#[test]
fn named_groups_become_attributes_and_the_body_is_kept() {
    for_each_worker_count(|workers| {
        let h = start(&extract_yaml(LINUX_PATTERN), workers);
        let line = "Jun 14 15:16:01 combo sshd(pam_unix)[19939]: authentication failure; logname= uid=0 euid=0 tty=NODEVssh ruser= rhost=218.188.2.4 ";

        let probe = h.source.push(record(1, line));
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");

        let attrs = attributes(&h, "out", 1);
        let expected = json!({
            "Month": "Jun",
            "Date": "14",
            "Time": "15:16:01",
            "Level": "combo",
            "Component": "sshd(pam_unix)",
            "PID": "19939",
            "Content": "authentication failure; logname= uid=0 euid=0 tty=NODEVssh ruser= rhost=218.188.2.4 ",
        });
        assert_eq!(Value::Object(attrs), expected, "workers={workers}");
        let body = h.sinks.records("out")[0].body.clone();
        assert_eq!(body, Some(Value::String(line.to_owned())));
        h.finish();
    });
}

#[test]
fn a_non_matching_line_passes_unchanged_and_is_counted() {
    for_each_worker_count(|workers| {
        let h = start(&extract_yaml(LINUX_PATTERN), workers);

        let probe = h.source.push(record(7, "not a syslog line"));
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");

        let out = h.sinks.records("out");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], record(7, "not a syslog line"), "unchanged");
        let labels = [
            ("tenant", "acme"),
            ("stage", "parse_linux"),
            ("engine", "linear"),
        ];
        assert_eq!(
            h.counter(Metric::RegexNonmatch, &labels),
            1,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(Metric::RecordsOut, &labels),
            1,
            "workers={workers}"
        );
        h.finish();
    });
}

#[test]
fn regex_node_metrics_carry_the_engine_label_and_other_nodes_do_not() {
    let h = start(&extract_yaml(LINUX_PATTERN), 1);
    assert_eq!(
        h.source.push(record(1, "x")).wait(WAIT),
        Some(AckOutcome::Ack)
    );
    let linear = [
        ("tenant", "acme"),
        ("stage", "parse_linux"),
        ("engine", "linear"),
    ];
    assert_eq!(h.counter(Metric::RecordsIn, &linear), 1);
    assert_eq!(h.samples(Metric::StageDuration, &linear).len(), 1);
    assert_eq!(
        h.counter(Metric::RecordsIn, &linear[..2]),
        0,
        "no series without the label"
    );
    assert_eq!(
        h.counter(Metric::RecordsIn, &[("tenant", "acme"), ("stage", "out")]),
        1
    );
    h.finish();

    // Lookbehind keeps the pattern off the linear engine.
    let h = start(&extract_yaml(r"(?<=id=)(?<Id>\d+)"), 1);
    assert_eq!(
        h.source.push(record(2, "id=42")).wait(WAIT),
        Some(AckOutcome::Ack)
    );
    let backtracking = [
        ("tenant", "acme"),
        ("stage", "parse_linux"),
        ("engine", "backtracking"),
    ];
    assert_eq!(h.counter(Metric::RecordsIn, &backtracking), 1);
    assert_eq!(
        attributes(&h, "out", 2),
        json!({"Id": "42"}).as_object().cloned().expect("object")
    );
    h.finish();
}
