//! The record is any JSON, and paths address all of it (issue #79, ADR 0008).
//!
//! One test per line of the issue's "Done when" list, driven through the trait boundary: a
//! YAML config, records pushed through the in-memory source, and what reached the in-memory
//! sink.

mod common;

use fusion_core::memory::AckOutcome;
use fusion_core::meta::Arrival;
use fusion_core::record::{Record, RecordId};
use serde_json::{Value, json};

use common::{TENANT, WAIT, arrival_as, start};

fn record(value: Value) -> Record {
    Record::new(value)
}

/// Run `yaml` over `records` and return what the sink `out` was given.
///
/// The record id comes from the arrival, as a transport header gives it (ADR 0007), not from
/// a payload key: these records need no `id` field, which is the point.
fn run(yaml: &str, records: Vec<Record>) -> Vec<Value> {
    let h = start(yaml, 1);
    let probes: Vec<_> = records
        .into_iter()
        .enumerate()
        .map(|(i, record)| {
            let arrival = Arrival {
                record_id: Some(RecordId(i as u64 + 1)),
                ..arrival_as(TENANT)
            };
            h.source.push_arrival(record, arrival)
        })
        .collect();
    for probe in &probes {
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "every record acks");
    }
    let out = h
        .sinks
        .records("out")
        .iter()
        .map(|r| r.value().clone())
        .collect();
    h.finish();
    out
}

/// A config that only writes what it is given to the sink.
const PASSTHROUGH: &str = r#"
nodes:
  - id: out
    type: sink.memory
"#;

#[test]
fn a_record_is_kept_as_sent_and_written_back_as_left() {
    let sent =
        json!({"level": "error", "msg": "auth failure", "rhost": "10.0.0.1", "host": "web-1"});
    let out = run(PASSTHROUGH, vec![record(sent.clone())]);
    assert_eq!(out, vec![sent], "nothing dropped, nothing added");
}

#[test]
fn an_unknown_key_works_in_a_condition_a_key_and_an_op() {
    let yaml = r#"
nodes:
  - id: errors_only
    type: filter
    condition: level == "error"
    action: keep
  - id: tag
    type: edit
    from: errors_only
    ops:
      - copy: {from: rhost, to: source_ip}
      - set: {field: host, value: web-1-renamed}
  - id: out
    type: sink.memory
    from: tag
"#;
    let out = run(
        yaml,
        vec![
            record(
                json!({"level": "error", "msg": "auth failure", "rhost": "10.0.0.1", "host": "web-1"}),
            ),
            record(json!({"level": "info", "msg": "started"})),
        ],
    );
    assert_eq!(
        out,
        vec![json!({
            "level": "error",
            "msg": "auth failure",
            "rhost": "10.0.0.1",
            "host": "web-1-renamed",
            "source_ip": "10.0.0.1",
        })],
        "the info record was filtered out on a key no field list knows"
    );
}

#[test]
fn a_nested_key_can_be_copied_to_another_nested_key() {
    let yaml = r#"
nodes:
  - id: move
    type: edit
    ops:
      - copy: {from: test2.key2, to: test2.key1}
  - id: out
    type: sink.memory
    from: move
"#;
    let out = run(
        yaml,
        vec![record(
            json!({"test": 12, "test2": {"key1": "ans1", "key2": 123}}),
        )],
    );
    assert_eq!(
        out,
        vec![json!({"test": 12, "test2": {"key1": 123, "key2": 123}})]
    );
}

#[test]
fn a_list_position_is_addressable() {
    let yaml = r#"
nodes:
  - id: postgres_only
    type: filter
    condition: attributes.0.value.intValue == 5432
    action: keep
  - id: out
    type: sink.memory
    from: postgres_only
"#;
    let otlp = json!({"attributes": [
        {"key": "db.port", "value": {"intValue": 5432}},
        {"key": "db.name", "value": {"stringValue": "orders"}},
    ]});
    let other = json!({"attributes": [{"key": "db.port", "value": {"intValue": 1521}}]});
    let out = run(yaml, vec![record(otlp.clone()), record(other)]);
    assert_eq!(out, vec![otlp]);
}

#[test]
fn a_key_with_a_dot_is_addressable_in_quotes() {
    let yaml = r#"
nodes:
  - id: linux_only
    type: filter
    condition: resource."log.format" == "Linux"
    action: keep
  - id: out
    type: sink.memory
    from: linux_only
"#;
    let flat = json!({"resource": {"log.format": "Linux"}});
    // The same text without the quotes would name this one instead.
    let nested = json!({"resource": {"log": {"format": "Linux"}}});
    let out = run(yaml, vec![record(flat.clone()), record(nested)]);
    assert_eq!(out, vec![flat]);
}

#[test]
fn a_payload_with_a_wrong_type_for_an_old_field_is_processed_not_dead_lettered() {
    // `id` as a UUID and `severity_number` as text both failed to decode before, so the
    // message was nakked and dead-lettered after `max_deliver`, with no stage reading either.
    let sent = json!({"id": "3f2a-not-a-number", "severity_number": "high", "body": "still a log"});
    let out = run(PASSTHROUGH, vec![record(sent.clone())]);
    assert_eq!(out, vec![sent], "acked and written, types and all");
}

#[test]
fn a_record_that_is_not_an_object_goes_through_whole() {
    // What a `codec: text` source produces: the line itself.
    let line = json!("Jun 14 15:16:01 combo sshd[19939]: authentication failure");
    let out = run(PASSTHROUGH, vec![record(line.clone())]);
    assert_eq!(out, vec![line]);
}

#[test]
fn the_whole_record_is_addressable_as_a_dot() {
    let yaml = r#"
nodes:
  - id: keep_the_line
    type: edit
    ops:
      - copy: {from: ., to: body}
  - id: out
    type: sink.memory
    from: keep_the_line
"#;
    let out = run(yaml, vec![record(json!("a raw line"))]);
    assert_eq!(
        out,
        vec![json!({"body": "a raw line"})],
        "a text record becomes an object holding the line"
    );
}

#[test]
fn meta_is_read_only_and_never_enters_the_payload_by_itself() {
    let yaml = r#"
nodes:
  - id: tag
    type: edit
    ops:
      - copy: {from: meta.tenant, to: tenant_seen}
  - id: out
    type: sink.memory
    from: tag
"#;
    let out = run(yaml, vec![record(json!({"body": "x"}))]);
    assert_eq!(
        out,
        vec![json!({"body": "x", "tenant_seen": TENANT})],
        "the one way a pipeline value enters a record is an explicit copy"
    );
}

#[test]
fn a_payload_key_spelled_meta_is_reached_with_a_leading_dot() {
    let yaml = r#"
nodes:
  - id: keep
    type: filter
    condition: .meta.id == "the producer's own"
    action: keep
  - id: out
    type: sink.memory
    from: keep
"#;
    let sent = json!({"meta": {"id": "the producer's own"}});
    let out = run(yaml, vec![record(sent.clone())]);
    assert_eq!(out, vec![sent]);
}

#[test]
fn extract_writes_its_groups_under_into() {
    let yaml = r#"
nodes:
  - id: parse
    type: extract
    field: body
    into: parsed
    pattern: '^(?<level>[A-Z]+): (?<message>.+)$'
  - id: out
    type: sink.memory
    from: parse
"#;
    let out = run(yaml, vec![record(json!({"body": "ERROR: disk full"}))]);
    assert_eq!(
        out,
        vec![json!({
            "body": "ERROR: disk full",
            "parsed": {"level": "ERROR", "message": "disk full"},
        })]
    );
}
