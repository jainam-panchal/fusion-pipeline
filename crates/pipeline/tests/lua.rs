//! The `lua` node through the trait boundary: YAML config with an inline script in,
//! envelopes pushed through the in-memory source, assertions on what the in-memory sink
//! received, how each ack handle settled and what the recorder counted.

mod common;

use common::{WAIT, for_each_worker_count, start};
use fusion_core::memory::AckOutcome;
use fusion_core::metrics::Metric;
use fusion_core::record::Record;
use serde_json::{Value, json};

const STAGE: [(&str, &str); 2] = [("tenant", "acme"), ("stage", "script")];

/// A `lua` node with inline `source` (and optional extra node lines) into one sink.
fn config(node_lines: &str, source: &str) -> String {
    let indented: String = source
        .lines()
        .map(|l| format!("      {l}\n"))
        .collect();
    format!(
        "name: ingest\nnodes:\n  - id: script\n    type: lua\n{node_lines}    source: |\n{indented}  - id: out\n    type: sink.memory\n"
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

#[test]
fn a_script_returning_a_mutated_record_reaches_the_sink_with_the_mutation() {
    for_each_worker_count(|workers| {
        let yaml = config(
            "",
            r#"function process(record)
  record.attributes["http.route"] = record.attributes["http.path"] .. "!"
  record.attributes["http.path"] = nil
  record.severity_text = "WARN"
  record.body = string.upper(record.body)
  return record
end"#,
        );
        let (out, h) = run(
            &yaml,
            workers,
            vec![record(
                7,
                json!({"body": "disk full", "attributes": {"http.path": "/users"}}),
            )],
        );
        assert_eq!(out.len(), 1, "workers {workers}");
        let r = &out[0];
        assert_eq!(r.attributes.get("http.route"), Some(&json!("/users!")));
        assert_eq!(r.attributes.get("http.path"), None);
        assert_eq!(r.severity_text.as_deref(), Some("WARN"));
        assert_eq!(r.body, Some(json!("DISK FULL")));
        assert_eq!(r.resource.get("tenant.id"), Some(&json!("acme")));
        h.finish();
    });
}

#[test]
fn a_script_returning_nil_drops_the_record_with_reason_lua_drop_and_acks() {
    for_each_worker_count(|workers| {
        let yaml = config(
            "",
            r#"function process(record)
  if record.severity_text == "DEBUG" then return nil end
  return record
end"#,
        );
        let (out, h) = run(
            &yaml,
            workers,
            vec![
                record(1, json!({"severity_text": "DEBUG"})),
                record(2, json!({"severity_text": "ERROR"})),
            ],
        );
        assert_eq!(out.iter().map(|r| r.id.map(|i| i.0)).collect::<Vec<_>>(), vec![Some(2)]);
        assert_eq!(
            h.counter(
                Metric::RecordsDropped,
                &[("tenant", "acme"), ("stage", "script"), ("reason", "lua_drop")]
            ),
            1,
            "workers {workers}"
        );
        assert_eq!(h.counter(Metric::RecordsErrored, &STAGE), 0);
        h.finish();
    });
}

#[test]
fn a_script_returning_a_list_splits_the_record_and_the_ack_fires_once() {
    for_each_worker_count(|workers| {
        let yaml = config(
            "",
            r#"function process(record)
  local first = { id = record.id, kind = record.kind, body = "a", resource = record.resource }
  local second = { id = record.id, kind = record.kind, body = "b", resource = record.resource }
  return { first, second }
end"#,
        );
        let h = start(&yaml, workers);
        let probe = h.source.push(record(9, json!({"body": "a\nb"})));
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
        let mut bodies: Vec<_> = h
            .sinks
            .records("out")
            .iter()
            .map(|r| (r.id.map(|i| i.0), r.body.clone()))
            .collect();
        bodies.sort_by(|a, b| a.1.as_ref().map(Value::to_string).cmp(&b.1.as_ref().map(Value::to_string)));
        assert_eq!(
            bodies,
            vec![(Some(9), Some(json!("a"))), (Some(9), Some(json!("b")))],
            "workers {workers}"
        );
        assert_eq!(h.counter(Metric::RecordsIn, &STAGE), 1);
        assert_eq!(h.counter(Metric::RecordsOut, &STAGE), 2);
        assert_eq!(h.counter(Metric::RecordsOut, &[("tenant", "acme"), ("stage", "out")]), 2);
        h.finish();
    });
}

/// The labels of one `lua_errors_total` count by `script` for tenant `acme`.
fn lua_error(kind: &str) -> [(&str, &str); 3] {
    [("tenant", "acme"), ("stage", "script"), ("kind", kind)]
}

const LOOPS: &str = "function process(record)\n  while true do end\nend";

#[test]
fn an_infinite_loop_is_stopped_by_the_instruction_budget_and_passes_by_default() {
    for_each_worker_count(|workers| {
        let yaml = config("    limits: { instructions: 10000 }\n", LOOPS);
        let (out, h) = run(&yaml, workers, vec![record(1, json!({"body": "x"}))]);
        assert_eq!(out.len(), 1, "workers {workers}: `pass` forwards the record unchanged");
        assert_eq!(out[0].body, Some(json!("x")));
        assert_eq!(h.counter(Metric::LuaErrors, &lua_error("instructions")), 1);
        assert_eq!(h.counter(Metric::RecordsErrored, &STAGE), 0);
        assert_eq!(h.counter(Metric::RecordsOut, &STAGE), 1);
        h.finish();
    });
}

#[test]
fn on_error_drop_drops_with_reason_lua_error_and_acks() {
    let yaml = config("    limits: { instructions: 10000 }\n    on_error: drop\n", LOOPS);
    let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
    assert!(out.is_empty());
    assert_eq!(
        h.counter(
            Metric::RecordsDropped,
            &[("tenant", "acme"), ("stage", "script"), ("reason", "lua_error")]
        ),
        1
    );
    assert_eq!(h.counter(Metric::LuaErrors, &lua_error("instructions")), 1);
    h.finish();
}

#[test]
fn on_error_nak_fails_the_record_so_the_source_message_is_nakked() {
    let yaml = config("    limits: { instructions: 10000 }\n    on_error: nak\n", LOOPS);
    let h = start(&yaml, 1);
    let probe = h.source.push(record(1, json!({"body": "x"})));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));
    assert!(h.sinks.records("out").is_empty());
    assert_eq!(h.counter(Metric::RecordsErrored, &STAGE), 1);
    assert_eq!(h.counter(Metric::LuaErrors, &lua_error("instructions")), 1);
    assert_eq!(h.counter(Metric::SourceNaks, &[("tenant", "acme")]), 1);
    h.finish();
}

#[test]
fn the_budget_is_per_record_so_a_worker_keeps_serving_after_a_trip() {
    let yaml = config(
        "    limits: { instructions: 10000 }\n    on_error: drop\n",
        "function process(record)\n  if record.body == \"loop\" then while true do end end\n  return record\nend",
    );
    let (out, h) = run(
        &yaml,
        1,
        vec![
            record(1, json!({"body": "loop"})),
            record(2, json!({"body": "fine"})),
            record(3, json!({"body": "fine"})),
        ],
    );
    assert_eq!(out.iter().map(|r| r.id.map(|i| i.0)).collect::<Vec<_>>(), vec![Some(2), Some(3)]);
    assert_eq!(h.counter(Metric::LuaErrors, &lua_error("instructions")), 1);
    h.finish();
}
