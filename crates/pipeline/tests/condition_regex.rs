//! `=~` and `!~` in `filter` and `route` conditions go through the regex facade with the
//! same limits and ReDoS policy as the regex stages, and are observable the same way: a
//! tripped limit is a `regex_limit` drop and the node carries the `engine` label.

mod common;

use fusion_core::memory::{AckOutcome, MemorySinks};
use fusion_core::metrics::Metric;
use fusion_core::pipeline::Pipeline;

use common::{WAIT, acme_record, for_each_worker_count, start};

const KEEP_DISK: &str = r#"
nodes:
  - id: keep_disk
    type: filter
    condition: body =~ "disk (full|failing)"
    action: keep
  - id: out
    type: sink.memory
"#;

fn load_error(yaml: &str) -> String {
    let registry = common::registry(&MemorySinks::new());
    Pipeline::from_yaml(yaml, &registry)
        .err()
        .map(|e| e.to_string())
        .expect("config rejected")
}

#[test]
fn filter_keeps_records_whose_field_matches_and_drops_the_rest() {
    for_each_worker_count(|workers| {
        let h = start(KEEP_DISK, workers);
        let probes = [
            h.source.push(acme_record(1, "disk full on /var")),
            h.source.push(acme_record(2, "disk fine")),
            h.source.push(acme_record(3, "disk failing")),
        ];
        for probe in &probes {
            assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");
        }
        assert_eq!(h.ids("out"), vec![1, 3], "workers={workers}");
        let labels = [
            ("tenant", "acme"),
            ("stage", "keep_disk"),
            ("engine", "linear"),
        ];
        assert_eq!(
            h.counter(Metric::RecordsIn, &labels),
            3,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(
                Metric::RecordsDropped,
                &[
                    ("tenant", "acme"),
                    ("stage", "keep_disk"),
                    ("engine", "linear"),
                    ("reason", "filter")
                ]
            ),
            1
        );
        h.finish();
    });
}

#[test]
fn not_match_is_the_negation_and_a_non_string_field_never_matches() {
    let yaml = KEEP_DISK.replace("=~", "!~");
    let h = start(&yaml, 1);
    let probes = [
        h.source.push(acme_record(1, "disk full")),
        h.source.push(acme_record(2, "all good")),
        h.source.push(
            fusion_core::record::Record::from_json(
                r#"{"id": 3, "body": 7, "resource": {"tenant.id": "acme"}}"#,
            )
            .expect("record parses"),
        ),
    ];
    for probe in &probes {
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    }
    // 2 does not match, so `!~` keeps it; 3 is not a string, so `=~` is false and `!~`
    // is true, as `!=` is for a type mismatch.
    assert_eq!(h.ids("out"), vec![2, 3]);
    h.finish();
}

#[test]
fn a_field_over_input_bytes_is_dropped_with_reason_regex_limit_by_the_filter() {
    // `disk full` is 9 bytes and matches; anything longer trips `input_bytes`.
    let yaml = KEEP_DISK.replace(
        "    action: keep",
        "    action: keep\n    limits: { input_bytes: 9 }",
    );
    let h = start(&yaml, 1);
    assert_eq!(
        h.source
            .push(acme_record(1, "disk full on /var"))
            .wait(WAIT),
        Some(AckOutcome::Ack)
    );
    assert_eq!(
        h.source.push(acme_record(2, "disk full")).wait(WAIT),
        Some(AckOutcome::Ack)
    );

    assert_eq!(h.ids("out"), vec![2]);
    let dropped = [
        ("tenant", "acme"),
        ("stage", "keep_disk"),
        ("engine", "linear"),
        ("reason", "regex_limit"),
    ];
    assert_eq!(h.counter(Metric::RecordsDropped, &dropped), 1);
    h.finish();
}

#[test]
fn route_conditions_use_the_facade_and_the_node_is_labelled_by_its_worst_engine() {
    let yaml = r#"
nodes:
  - id: by_body
    type: route
    routes:
      ssh: body =~ "(?<=sshd)\\[\\d+\\]"
      disk: body =~ "disk"
    default: other
    on_redos_risk: warn
  - id: ssh_out
    type: sink.memory
    from: by_body.ssh
  - id: disk_out
    type: sink.memory
    from: by_body.disk
  - id: other_out
    type: sink.memory
    from: by_body.other
"#;
    let h = start(yaml, 1);
    let probes = [
        h.source.push(acme_record(1, "sshd[19939]: check pass")),
        h.source.push(acme_record(2, "disk full")),
        h.source.push(acme_record(3, "hello")),
    ];
    for probe in &probes {
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    }
    assert_eq!(h.ids("ssh_out"), vec![1]);
    assert_eq!(h.ids("disk_out"), vec![2]);
    assert_eq!(h.ids("other_out"), vec![3]);
    // One leaf needs PCRE2 (lookbehind), one is linear: the node reports backtracking.
    let labels = [
        ("tenant", "acme"),
        ("stage", "by_body"),
        ("engine", "backtracking"),
    ];
    assert_eq!(h.counter(Metric::RecordsIn, &labels), 3);
    h.finish();
}

#[test]
fn a_condition_without_regex_operators_carries_no_engine_label() {
    let yaml = KEEP_DISK.replace(r#"body =~ "disk (full|failing)""#, r#"body == "disk full""#);
    let h = start(&yaml, 1);
    assert_eq!(
        h.source.push(acme_record(1, "disk full")).wait(WAIT),
        Some(AckOutcome::Ack)
    );
    assert_eq!(
        h.counter(
            Metric::RecordsIn,
            &[("tenant", "acme"), ("stage", "keep_disk")]
        ),
        1
    );
    h.finish();
}

#[test]
fn a_regex_operator_needs_a_string_literal_and_a_pattern_that_passes_the_checks() {
    let message = load_error(&KEEP_DISK.replace(r#""disk (full|failing)""#, "42"));
    assert!(
        message.contains("keep_disk") && message.contains("=~"),
        "{message}"
    );

    let message = load_error(&KEEP_DISK.replace("disk (full|failing)", "(a+)+$"));
    assert!(
        message.contains("keep_disk") && message.contains("ReDoS"),
        "{message}"
    );

    let message = load_error(&KEEP_DISK.replace("disk (full|failing)", "disk ("));
    assert!(
        message.contains("keep_disk") && message.contains("offset"),
        "{message}"
    );
}
