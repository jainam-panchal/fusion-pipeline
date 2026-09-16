//! `extract` and `redact` through the trait boundary: YAML config in, envelopes
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
    type: extract
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

fn extract_yaml_with(pattern: &str, extra: &str) -> String {
    extract_yaml(pattern).replace("    pattern:", &format!("{extra}\n    pattern:"))
}

fn load_error(yaml: &str) -> String {
    let registry = common::registry(&fusion_core::memory::MemorySinks::new());
    fusion_core::pipeline::Pipeline::from_yaml(yaml, &registry)
        .err()
        .map(|e| e.to_string())
        .expect("config rejected")
}

#[test]
fn on_redos_risk_reject_refuses_a_pattern_the_lint_flags() {
    // Nested unbounded quantifiers: the textbook shape, caught by the lint on any engine.
    let message = load_error(&extract_yaml(r"(?<a>(a+)+)$"));
    assert!(message.contains("parse_linux"), "{message}");
    assert!(message.contains("ReDoS"), "{message}");
}

#[test]
fn on_redos_risk_reject_refuses_a_pattern_the_canary_trips() {
    // Passes the lint, lands on PCRE2 (lookahead) and is O(n²) unanchored: only the canary
    // sees it. The default policy is `reject`.
    let message = load_error(&extract_yaml(r"(?<x>(?:a|b)*)(?=c)"));
    assert!(message.contains("parse_linux"), "{message}");
    assert!(message.contains("canary"), "{message}");
}

#[test]
fn on_redos_risk_warn_loads_the_pattern_and_serves_records() {
    let yaml = extract_yaml_with(r"(?<x>(?:a|b)*)(?=c)", "    on_redos_risk: warn");
    let h = start(&yaml, 1);
    assert_eq!(
        h.source.push(record(1, "abc")).wait(WAIT),
        Some(AckOutcome::Ack)
    );
    assert_eq!(
        attributes(&h, "out", 1),
        json!({"x": "ab"}).as_object().cloned().expect("object")
    );
    h.finish();
}

#[test]
fn a_record_over_input_bytes_is_dropped_with_reason_regex_limit() {
    for_each_worker_count(|workers| {
        let yaml = extract_yaml_with(LINUX_PATTERN, "    limits: { input_bytes: 16 }");
        let h = start(&yaml, workers);

        let big = h.source.push(record(1, &"x".repeat(17)));
        let fits = h.source.push(record(2, &"x".repeat(16)));
        assert_eq!(big.wait(WAIT), Some(AckOutcome::Ack), "a drop acks");
        assert_eq!(fits.wait(WAIT), Some(AckOutcome::Ack));

        assert_eq!(h.ids("out"), vec![2], "workers={workers}");
        let dropped = [
            ("tenant", "acme"),
            ("stage", "parse_linux"),
            ("engine", "linear"),
            ("reason", "regex_limit"),
        ];
        assert_eq!(
            h.counter(Metric::RecordsDropped, &dropped),
            1,
            "workers={workers}"
        );
        assert_eq!(h.counter(Metric::RecordsErrored, &dropped[..3]), 0);
        h.finish();
    });
}

#[test]
fn a_tripped_match_limit_drops_the_record_and_the_stage_keeps_serving() {
    // Exponential on PCRE2; `warn` gets it past the lint and the canary so the runtime
    // limit is what stops it.
    let yaml = extract_yaml_with(
        r"^(?=a)(?<run>(a+)+)$",
        "    on_redos_risk: warn\n    limits: { match: 1000 }",
    );
    let h = start(&yaml, 1);
    let mut adversarial = "a".repeat(30);
    adversarial.push('!');

    assert_eq!(
        h.source.push(record(1, &adversarial)).wait(WAIT),
        Some(AckOutcome::Ack)
    );
    assert_eq!(
        h.source.push(record(2, "aaaa")).wait(WAIT),
        Some(AckOutcome::Ack)
    );

    assert_eq!(h.ids("out"), vec![2]);
    assert_eq!(
        attributes(&h, "out", 2),
        json!({"run": "aaaa"}).as_object().cloned().expect("object")
    );
    let dropped = [
        ("tenant", "acme"),
        ("stage", "parse_linux"),
        ("engine", "backtracking"),
        ("reason", "regex_limit"),
    ];
    assert_eq!(h.counter(Metric::RecordsDropped, &dropped), 1);
    h.finish();
}

const REDACT: &str = r#"
nodes:
  - id: mask_phones
    type: redact
    fields: [body, attributes.msg]
    pattern: '\d{3}-\d{4}'
    replace: '[phone]'
  - id: out
    type: sink.memory
"#;

fn redact_record(id: u64, body: &str, msg: Value, other: &str) -> Record {
    Record::from_json(
        &json!({
            "id": id,
            "body": body,
            "attributes": {"msg": msg, "other": other},
            "resource": {"tenant.id": "acme"},
        })
        .to_string(),
    )
    .expect("record parses")
}

#[test]
fn redact_replaces_every_match_in_each_listed_field_and_nothing_else() {
    for_each_worker_count(|workers| {
        let h = start(REDACT, workers);
        let probe = h.source.push(redact_record(
            1,
            "call 555-1234 or 555-9876",
            json!("cell 555-0000"),
            "555-1111 stays",
        ));
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");

        let out = &h.sinks.records("out")[0];
        assert_eq!(out.body, Some(json!("call [phone] or [phone]")));
        assert_eq!(out.attributes["msg"], json!("cell [phone]"));
        assert_eq!(out.attributes["other"], json!("555-1111 stays"));
        let labels = [
            ("tenant", "acme"),
            ("stage", "mask_phones"),
            ("engine", "linear"),
        ];
        assert_eq!(h.counter(Metric::RegexNonmatch, &labels), 0);
        h.finish();
    });
}

#[test]
fn redact_counts_a_record_where_no_listed_field_matched_once_and_skips_non_strings() {
    let h = start(REDACT, 1);
    let probe = h
        .source
        .push(redact_record(1, "no phone", json!(42), "555-1111 stays"));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));

    let out = &h.sinks.records("out")[0];
    assert_eq!(out.body, Some(json!("no phone")));
    assert_eq!(out.attributes["msg"], json!(42));
    let labels = [
        ("tenant", "acme"),
        ("stage", "mask_phones"),
        ("engine", "linear"),
    ];
    assert_eq!(h.counter(Metric::RegexNonmatch, &labels), 1);
    h.finish();
}

#[test]
fn redact_over_input_bytes_drops_with_reason_regex_limit_and_nothing_reaches_the_sink() {
    let yaml = REDACT.replace(
        "    replace:",
        "    limits: { input_bytes: 8 }\n    replace:",
    );
    let h = start(&yaml, 1);
    let probe = h
        .source
        .push(redact_record(1, "555-1234 too long", json!("x"), "y"));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));

    assert!(h.ids("out").is_empty());
    let dropped = [
        ("tenant", "acme"),
        ("stage", "mask_phones"),
        ("engine", "linear"),
        ("reason", "regex_limit"),
    ];
    assert_eq!(h.counter(Metric::RecordsDropped, &dropped), 1);
    h.finish();
}

#[test]
fn redact_rejects_fields_that_take_no_string_and_malformed_fields_at_load_naming_the_node() {
    for field in ["id", "kind", "severity_number"] {
        let message = load_error(&REDACT.replace("[body, attributes.msg]", &format!("[{field}]")));
        assert!(
            message.contains("mask_phones")
                && message.contains(field)
                && message.contains("string"),
            "{message}"
        );
    }

    let message = load_error(&REDACT.replace("[body, attributes.msg]", "[attributes]"));
    assert!(
        message.contains("mask_phones") && message.contains("attributes"),
        "{message}"
    );

    let message = load_error(&REDACT.replace("[body, attributes.msg]", "[]"));
    assert!(
        message.contains("mask_phones") && message.contains("fields"),
        "{message}"
    );
}
