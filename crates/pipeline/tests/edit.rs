//! The `edit` node through the trait boundary: YAML config in, envelopes pushed through
//! the in-memory source, assertions on what the in-memory sink received, how each ack
//! handle settled and what the recorder counted.

mod common;

use common::{WAIT, for_each_worker_count, start};
use fusion_core::memory::AckOutcome;
use fusion_core::metrics::Metric;
use fusion_core::record::Record;
use serde_json::{Value, json};

/// The issue's example, behind a filter, into one sink.
const EXAMPLE: &str = r#"
name: ingest
nodes:
  - id: keep_errors
    type: filter
    condition: severity_text == "ERROR"
    action: keep
  - id: normalise
    type: edit
    ops:
      - set:    { field: resource.env, value: prod }
      - rename: { from: attributes.http.path, to: attributes.http.route }
      - copy:   { from: body, to: attributes.raw }
      - hash:   { field: attributes.user.email }
      - delete: { fields: [attributes.debug] }
  - id: out
    type: sink.memory
"#;

/// SHA-256 of `alice@example.com`, computed outside this codebase.
const ALICE_SHA256: &str = "ff8d9819fc0e12bf0d24892e45987e249a28dce836a85cad60e28eaaa8c6d976";
/// SHA-256 of the text `42`.
const FORTY_TWO_SHA256: &str = "73475cb40a568e8da8a045ced110137e159f890ac4da883b6b17dc651b3a8049";
/// SHA-256 of the text `true`.
const TRUE_SHA256: &str = "b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b";
/// SHA-256 of the text `1.5`.
const ONE_POINT_FIVE_SHA256: &str =
    "9f29a130438b81170b92a42650f9a94291ecad60bd47af2a3886e75f7f728725";

const STAGE: [(&str, &str); 2] = [("tenant", "acme"), ("stage", "normalise")];

/// An `edit` node with `ops` (and an optional node line) into one sink, no filter.
fn config(node_lines: &str, ops: &str) -> String {
    format!(
        "name: ingest\nnodes:\n  - id: normalise\n    type: edit\n{node_lines}    ops:\n{ops}  - id: out\n    type: sink.memory\n"
    )
}

/// A tenant `acme` record from a JSON object, with `id` and `resource.tenant.id` filled in.
fn record(id: u64, mut json: Value) -> Record {
    json["id"] = json!(id);
    json["resource"]["tenant.id"] = json!("acme");
    Record::from_json(&json.to_string()).expect("record parses")
}

/// Push `records` through `yaml` at `workers`, wait for every ack, return what `out`
/// received in id order plus the harness for counter assertions.
fn run(yaml: &str, workers: usize, records: Vec<Record>) -> (Vec<Record>, common::Harness) {
    let h = start(yaml, workers);
    let probes: Vec<_> = records.into_iter().map(|r| h.source.push(r)).collect();
    for (i, probe) in probes.iter().enumerate() {
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "record {i}");
    }
    let mut out = h.sinks.records("out");
    out.sort_by_key(|r| r.id.map(|id| id.0));
    (out, h)
}

/// The labels of one unapplied op by `normalise` for tenant `acme`.
fn unapplied<'a>(op: &'a str, field: &'a str, cause: &'a str) -> [(&'a str, &'a str); 5] {
    [
        ("tenant", "acme"),
        ("stage", "normalise"),
        ("op", op),
        ("field", field),
        ("cause", cause),
    ]
}

#[test]
fn filter_then_edit_then_sink_acks_every_record_and_the_sink_sees_the_edits() {
    for_each_worker_count(|workers| {
        let h = start(EXAMPLE, workers);
        let probes: Vec<_> = (1..=100)
            .map(|id| {
                h.source.push(record(
                    id,
                    json!({
                        "severity_text": if id % 2 == 0 { "ERROR" } else { "INFO" },
                        "body": "GET /users/42",
                        "attributes": {
                            "http.path": "/users/42",
                            "user.email": "alice@example.com",
                            "debug": true
                        }
                    }),
                ))
            })
            .collect();
        for (i, probe) in probes.iter().enumerate() {
            assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "record {i}");
        }
        let out = h.sinks.records("out");
        assert_eq!(
            out.len(),
            50,
            "workers {workers}: every ERROR record reached the sink"
        );
        for r in &out {
            assert_eq!(r.resource.get("env"), Some(&json!("prod")));
            assert_eq!(r.attributes.get("http.route"), Some(&json!("/users/42")));
            assert_eq!(r.attributes.get("http.path"), None);
            assert_eq!(r.attributes.get("raw"), Some(&json!("GET /users/42")));
            assert_eq!(r.body, Some(json!("GET /users/42")));
            assert_eq!(r.attributes.get("user.email"), Some(&json!(ALICE_SHA256)));
            assert_eq!(r.attributes.get("debug"), None);
        }
        assert_eq!(h.counter(Metric::RecordsIn, &STAGE), 50);
        assert_eq!(h.counter(Metric::RecordsOut, &STAGE), 50);
        assert_eq!(h.counter(Metric::RecordsErrored, &STAGE), 0);
        h.finish();
    });
}

#[test]
fn each_op_produces_the_expected_record() {
    let cases: [(&str, Value, Value); 5] = [
        (
            "      - set: { field: severity_number, value: 17 }\n",
            json!({"body": "x"}),
            json!({"body": "x", "severity_number": 17}),
        ),
        (
            "      - rename: { from: attributes.a, to: attributes.b }\n",
            json!({"attributes": {"a": 1}}),
            json!({"attributes": {"b": 1}}),
        ),
        (
            "      - copy: { from: severity_text, to: attributes.level }\n",
            json!({"severity_text": "WARN"}),
            json!({"severity_text": "WARN", "attributes": {"level": "WARN"}}),
        ),
        (
            "      - hash: { field: attributes.user.email }\n",
            json!({"attributes": {"user.email": "alice@example.com"}}),
            json!({"attributes": {"user.email": ALICE_SHA256}}),
        ),
        (
            "      - delete: { fields: [attributes.a, severity_text, attributes.missing] }\n",
            json!({"severity_text": "WARN", "attributes": {"a": 1, "keep": 2}}),
            json!({"attributes": {"keep": 2}}),
        ),
    ];
    for (ops, before, after) in cases {
        let (out, h) = run(&config("", ops), 1, vec![record(1, before)]);
        assert_eq!(out, vec![record(1, after)], "{ops}");
        assert_eq!(h.counter(Metric::RecordsErrored, &STAGE), 0);
        h.finish();
    }
}

#[test]
fn ops_run_in_order_on_the_same_record() {
    // rename then set on the new name: the set wins.
    let yaml = config(
        "",
        "      - rename: { from: attributes.a, to: attributes.b }\n      - set: { field: attributes.b, value: set }\n",
    );
    let (out, h) = run(
        &yaml,
        1,
        vec![record(1, json!({"attributes": {"a": "moved"}}))],
    );
    assert_eq!(out[0].attributes.get("b"), Some(&json!("set")));
    assert_eq!(out[0].attributes.get("a"), None);
    h.finish();

    // set then rename of the old name: the value just set moves.
    let yaml = config(
        "",
        "      - set: { field: attributes.a, value: set }\n      - rename: { from: attributes.a, to: attributes.b }\n",
    );
    let (out, h) = run(
        &yaml,
        1,
        vec![record(1, json!({"attributes": {"a": "old"}}))],
    );
    assert_eq!(out[0].attributes.get("b"), Some(&json!("set")));
    assert_eq!(out[0].attributes.get("a"), None);
    h.finish();

    // set the new name, then rename an absent old name onto it: the set value stays and
    // the rename is unapplied.
    let yaml = config(
        "",
        "      - set: { field: attributes.b, value: set }\n      - rename: { from: attributes.a, to: attributes.b }\n",
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({"attributes": {}}))]);
    assert_eq!(out[0].attributes.get("b"), Some(&json!("set")));
    assert_eq!(
        h.counter(
            Metric::EditUnapplied,
            &unapplied("rename", "attributes.a", "absent")
        ),
        1
    );
    h.finish();
}

#[test]
fn rename_and_copy_overwrite_an_existing_to() {
    let yaml = config(
        "",
        "      - rename: { from: attributes.a, to: attributes.b }\n      - copy: { from: body, to: attributes.c }\n",
    );
    let (out, h) = run(
        &yaml,
        1,
        vec![record(
            1,
            json!({"body": "new", "attributes": {"a": "from a", "b": "old b", "c": "old c"}}),
        )],
    );
    assert_eq!(
        out[0].attributes,
        json!({"b": "from a", "c": "new"})
            .as_object()
            .cloned()
            .expect("object")
    );
    h.finish();
}

#[test]
fn an_absent_source_leaves_the_record_and_counts_absent_for_that_op_and_field() {
    for_each_worker_count(|workers| {
        let yaml = config(
            "",
            "      - rename: { from: 'attributes.\"http.path\"', to: attributes.http.route }\n      - copy: { from: attributes.nothing, to: attributes.copy }\n      - hash: { field: attributes.nil }\n      - set: { field: attributes.after, value: ran }\n",
        );
        let before = json!({"body": "x", "attributes": {"nil": null}});
        let (out, h) = run(&yaml, workers, vec![record(1, before)]);
        assert_eq!(
            out[0].attributes,
            json!({"nil": null, "after": "ran"})
                .as_object()
                .cloned()
                .expect("object"),
            "workers {workers}: nothing written, the op after the unapplied ones still ran"
        );
        assert_eq!(
            h.counter(
                Metric::EditUnapplied,
                &unapplied("rename", "attributes.http.path", "absent")
            ),
            1
        );
        assert_eq!(
            h.counter(
                Metric::EditUnapplied,
                &unapplied("copy", "attributes.nothing", "absent")
            ),
            1
        );
        assert_eq!(
            h.counter(
                Metric::EditUnapplied,
                &unapplied("hash", "attributes.nil", "absent")
            ),
            1
        );
        assert_eq!(h.counter(Metric::RecordsOut, &STAGE), 1);
        assert_eq!(h.counter(Metric::RecordsDropped, &STAGE), 0);
        h.finish();
    });
}

#[test]
fn a_target_that_refuses_the_value_leaves_the_record_and_counts_type() {
    let yaml = config(
        "",
        "      - copy: { from: body, to: severity_number }\n      - rename: { from: body, to: attributes.raw }\n      - hash: { field: attributes.list }\n",
    );
    let before = json!({"body": {"nested": true}, "attributes": {"list": [1, 2]}});
    let (out, h) = run(&yaml, 1, vec![record(1, before.clone())]);
    assert_eq!(out, vec![record(1, before)], "record unchanged");
    assert_eq!(
        h.counter(Metric::EditUnapplied, &unapplied("copy", "body", "type")),
        1,
        "a composite cannot go into severity_number"
    );
    assert_eq!(
        h.counter(Metric::EditUnapplied, &unapplied("rename", "body", "type")),
        1,
        "a composite cannot go under a map key, and body is still there"
    );
    assert_eq!(
        h.counter(
            Metric::EditUnapplied,
            &unapplied("hash", "attributes.list", "type")
        ),
        1
    );
    assert_eq!(h.counter(Metric::RecordsErrored, &STAGE), 0);
    h.finish();
}

#[test]
fn hash_takes_a_number_or_bool_as_its_canonical_text() {
    let yaml = config(
        "",
        "      - hash: { field: attributes.n }\n      - hash: { field: attributes.f }\n      - hash: { field: attributes.b }\n      - hash: { field: severity_text }\n",
    );
    let before = json!({"severity_text": "42", "attributes": {"n": 42, "f": 1.5, "b": true}});
    let (out, h) = run(&yaml, 1, vec![record(1, before)]);
    assert_eq!(out[0].attributes.get("n"), Some(&json!(FORTY_TWO_SHA256)));
    assert_eq!(
        out[0].attributes.get("f"),
        Some(&json!(ONE_POINT_FIVE_SHA256))
    );
    assert_eq!(out[0].attributes.get("b"), Some(&json!(TRUE_SHA256)));
    assert_eq!(
        out[0].severity_text.as_deref(),
        Some(FORTY_TWO_SHA256),
        "the string `42` and the number 42 hash the same"
    );
    h.finish();
}

#[test]
fn on_unapplied_drop_drops_with_reason_edit_unapplied_and_acks() {
    for_each_worker_count(|workers| {
        let yaml = config(
            "    on_unapplied: drop\n",
            "      - rename: { from: attributes.a, to: attributes.b }\n      - set: { field: attributes.after, value: ran }\n",
        );
        let (out, h) = run(
            &yaml,
            workers,
            vec![
                record(1, json!({"attributes": {"a": 1}})),
                record(2, json!({"attributes": {}})),
            ],
        );
        assert_eq!(out.len(), 1, "workers {workers}");
        assert_eq!(out[0].id.map(|id| id.0), Some(1));
        assert_eq!(out[0].attributes.get("after"), Some(&json!("ran")));
        assert_eq!(
            h.counter(
                Metric::RecordsDropped,
                &[
                    ("tenant", "acme"),
                    ("stage", "normalise"),
                    ("reason", "edit_unapplied")
                ]
            ),
            1
        );
        assert_eq!(
            h.counter(
                Metric::EditUnapplied,
                &unapplied("rename", "attributes.a", "absent")
            ),
            1
        );
        assert_eq!(h.counter(Metric::RecordsOut, &STAGE), 1);
        assert_eq!(h.counter(Metric::RecordsErrored, &STAGE), 0);
        h.finish();
    });
}

#[test]
fn edit_never_errors_whatever_the_record_holds() {
    let mixed = vec![
        record(1, json!({})),
        record(2, json!({"body": null})),
        record(
            3,
            json!({"body": [1, [2]], "attributes": {"http.path": {"deep": 1}}}),
        ),
        record(
            4,
            json!({"severity_number": 3, "attributes": {"user.email": 1e300}}),
        ),
        record(
            5,
            json!({"body": "", "attributes": {"http.path": "", "debug": null}}),
        ),
    ];
    let yaml = EXAMPLE.replace(
        "condition: severity_text == \"ERROR\"\n    action: keep",
        "condition: id > 0\n    action: keep",
    );
    let (out, h) = run(&yaml, 4, mixed);
    assert_eq!(out.len(), 5);
    assert_eq!(h.counter(Metric::RecordsErrored, &STAGE), 0);
    assert_eq!(h.counter(Metric::RecordsDropped, &STAGE), 0);
    assert_eq!(h.counter(Metric::RecordsOut, &STAGE), 5);
    h.finish();
}

const RETENANT_THEN_DEDUPE: &str = r#"
name: ingest
nodes:
  - id: normalise
    type: edit
    ops:
      - set: { field: resource.tenant.id, value: other }
  - id: dedupe_body
    type: dedupe
    from: normalise
    key: [body]
    window: 10s
  - id: out
    type: sink.memory
    from: dedupe_body
"#;

#[test]
fn a_tenant_rewritten_by_edit_is_payload_and_labels_and_state_keys_keep_the_arrival_tenant() {
    for_each_worker_count(|workers| {
        let (out, h) = run(
            RETENANT_THEN_DEDUPE,
            workers,
            vec![record(1, json!({"body": "x"}))],
        );
        assert_eq!(out[0].resource.get("tenant.id"), Some(&json!("other")));
        assert_eq!(
            h.counter(Metric::RecordsOut, &STAGE),
            1,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(Metric::RecordsOut, &[("tenant", "acme"), ("stage", "out")]),
            1,
            "workers={workers}"
        );
        let keys = h.state.keys();
        assert_eq!(keys.len(), 1, "workers={workers}");
        assert!(
            keys[0].starts_with("ingest:acme:dedupe_body:"),
            "{keys:?} workers={workers}"
        );
        h.finish();
    });
}
