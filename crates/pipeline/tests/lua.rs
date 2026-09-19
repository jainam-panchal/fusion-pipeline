//! The `lua` node through the trait boundary: YAML config with a script in,
//! envelopes pushed through the in-memory source, assertions on what the in-memory sink
//! received, how each ack handle settled and what the recorder counted.

mod common;

use common::{TENANT, WAIT, arrival_as, deploy_config, for_each_worker_count, start};
use fusion_core::config::Config;
use fusion_core::memory::AckOutcome;
use fusion_core::meta::{Arrival, IngestionTime, unix_nanos_now};
use fusion_core::metrics::CounterMetric;
use fusion_core::record::{Record, RecordId};
use serde_json::{Value, json};

const STAGE: [(&str, &str); 2] = [("tenant", "acme"), ("stage", "script")];

/// A `lua` node with inline `source` (and optional extra node lines) into one sink.
fn config(node_lines: &str, source: &str) -> String {
    let indented: String = source.lines().map(|l| format!("      {l}\n")).collect();
    format!(
        "name: ingest\nnodes:\n  - id: script\n    type: lua\n{node_lines}    source: |\n{indented}  - id: out\n    type: sink.memory\n"
    )
}

/// A record from a JSON object, with `id` filled in. The harness pushes it as tenant `acme`.
fn record(id: u64, mut json: Value) -> Record {
    json["id"] = json!(id);
    Record::from_json(&json.to_string()).expect("record parses")
}

/// Push `records` through `yaml` at `workers`, wait for every ack, return what `out`
/// received in id order plus the harness for counter assertions.
fn run(yaml: &str, workers: usize, records: Vec<Record>) -> (Vec<Record>, common::Harness) {
    let h = start(yaml, workers);
    let probes: Vec<_> = records.into_iter().map(|r| h.push(r)).collect();
    for (i, probe) in probes.iter().enumerate() {
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "record {i}");
    }
    let mut out = h.sinks.records("out");
    out.sort_by_key(|r| r.value()["id"].as_u64());
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
        assert_eq!(
            r.value()["attributes"].get("http.route"),
            Some(&json!("/users!"))
        );
        assert_eq!(r.value()["attributes"].get("http.path"), None);
        assert_eq!(r.value()["severity_text"].as_str(), Some("WARN"));
        assert_eq!(r.value()["body"], json!("DISK FULL"));
        assert_eq!(
            r.value()["resource"].get("tenant.id"),
            None,
            "the pipeline adds no tenant"
        );
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
        assert_eq!(
            out.iter()
                .map(|r| r.value()["id"].as_u64())
                .collect::<Vec<_>>(),
            vec![Some(2)]
        );
        assert_eq!(
            h.counter(
                CounterMetric::RecordsDropped,
                &[
                    ("tenant", "acme"),
                    ("stage", "script"),
                    ("reason", "lua_drop")
                ]
            ),
            1,
            "workers {workers}"
        );
        assert_eq!(h.counter(CounterMetric::RecordsErrored, &STAGE), 0);
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
        let probe = h.push(record(9, json!({"body": "a\nb"})));
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
        let mut bodies: Vec<_> = h
            .sinks
            .records("out")
            .iter()
            .map(|r| (r.value()["id"].as_u64(), r.value()["body"].clone()))
            .collect();
        bodies.sort_by_key(|(_, body)| body.to_string());
        assert_eq!(
            bodies,
            vec![(Some(9), json!("a")), (Some(9), json!("b"))],
            "workers {workers}"
        );
        assert_eq!(h.counter(CounterMetric::RecordsIn, &STAGE), 1);
        assert_eq!(h.counter(CounterMetric::RecordsOut, &STAGE), 2);
        assert_eq!(
            h.counter(
                CounterMetric::RecordsOut,
                &[("tenant", "acme"), ("stage", "out")]
            ),
            2
        );
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
        assert_eq!(
            out.len(),
            1,
            "workers {workers}: `pass` forwards the record unchanged"
        );
        assert_eq!(out[0].value()["body"], json!("x"));
        assert_eq!(
            h.counter(CounterMetric::LuaErrors, &lua_error("instructions")),
            1
        );
        assert_eq!(h.counter(CounterMetric::RecordsErrored, &STAGE), 0);
        assert_eq!(h.counter(CounterMetric::RecordsOut, &STAGE), 1);
        h.finish();
    });
}

#[test]
fn on_error_drop_drops_with_reason_lua_error_and_acks() {
    let yaml = config(
        "    limits: { instructions: 10000 }\n    on_error: drop\n",
        LOOPS,
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
    assert!(out.is_empty());
    assert_eq!(
        h.counter(
            CounterMetric::RecordsDropped,
            &[
                ("tenant", "acme"),
                ("stage", "script"),
                ("reason", "lua_error")
            ]
        ),
        1
    );
    assert_eq!(
        h.counter(CounterMetric::LuaErrors, &lua_error("instructions")),
        1
    );
    h.finish();
}

#[test]
fn on_error_nak_fails_the_record_so_the_source_message_is_nakked() {
    let yaml = config(
        "    limits: { instructions: 10000 }\n    on_error: nak\n",
        LOOPS,
    );
    let h = start(&yaml, 1);
    let probe = h.push(record(1, json!({"body": "x"})));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));
    assert!(h.sinks.records("out").is_empty());
    assert_eq!(h.counter(CounterMetric::RecordsErrored, &STAGE), 1);
    assert_eq!(
        h.counter(CounterMetric::LuaErrors, &lua_error("instructions")),
        1
    );
    assert_eq!(
        h.counter(CounterMetric::SourceNaks, &[("tenant", "acme")]),
        1
    );
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
    assert_eq!(
        out.iter()
            .map(|r| r.value()["id"].as_u64())
            .collect::<Vec<_>>(),
        vec![Some(2), Some(3)]
    );
    assert_eq!(
        h.counter(CounterMetric::LuaErrors, &lua_error("instructions")),
        1
    );
    h.finish();
}

#[test]
fn unbounded_table_growth_is_stopped_by_the_memory_cap() {
    for_each_worker_count(|workers| {
        let yaml = config(
            "    limits: { instructions: 100000000, memory_kib: 256 }\n    on_error: drop\n",
            "function process(record)\n  local t = {}\n  while true do t[#t + 1] = string.rep(\"x\", 1024) end\nend",
        );
        let (out, h) = run(
            &yaml,
            workers,
            vec![
                record(1, json!({"body": "x"})),
                record(2, json!({"body": "y"})),
            ],
        );
        assert!(out.is_empty(), "workers {workers}");
        assert_eq!(h.counter(CounterMetric::LuaErrors, &lua_error("memory")), 2);
        assert_eq!(
            h.counter(CounterMetric::LuaErrors, &lua_error("instructions")),
            0
        );
        h.finish();
    });
}

#[test]
fn a_script_that_raises_counts_as_a_runtime_error() {
    let yaml = config(
        "    on_error: drop\n",
        "function process(record)\n  local x = nil\n  return x.field\nend",
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
    assert!(out.is_empty());
    assert_eq!(
        h.counter(CounterMetric::LuaErrors, &lua_error("runtime")),
        1
    );
    h.finish();
}

#[test]
fn a_returned_value_the_stage_refuses_counts_as_an_output_error() {
    // No key and no type is a record's any more, so what is left to refuse is a value with
    // no JSON form at all, the output cap, and the two shapes that are no record: an empty
    // table and a boolean, which is how a script means to drop one.
    let cases: [(&str, &str); 5] = [
        (
            "a value with no JSON form",
            "record.body = function() end\n  return record",
        ),
        (
            "a non-finite number",
            "record.body = 1 / 0\n  return record",
        ),
        (
            "oversized body",
            "record.body = string.rep(\"x\", 2048)\n  return record",
        ),
        ("an empty table", "return {}"),
        ("a boolean", "return true"),
    ];
    for (name, body) in cases {
        let yaml = config(
            "    limits: { output_kib: 1 }\n    on_error: drop\n",
            &format!("function process(record)\n  {body}\nend"),
        );
        let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
        assert!(out.is_empty(), "{name}: refused");
        assert_eq!(
            h.counter(CounterMetric::LuaErrors, &lua_error("output")),
            1,
            "{name}"
        );
        h.finish();
    }
}

#[test]
fn state_set_nx_from_lua_writes_through_the_same_state_store_under_the_node_prefix() {
    let yaml = config(
        "",
        r#"function process(record)
  record.attributes = record.attributes or {}
  local claimed, holder = state.set_nx("seen:" .. record.body, tostring(record.id), 60000)
  if not claimed then
    record.attributes["first_seen_by"] = holder
  end
  return record
end"#,
    );
    let (out, h) = run(
        &yaml,
        1,
        vec![
            record(1, json!({"body": "disk full"})),
            record(2, json!({"body": "disk full"})),
        ],
    );
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].value()["attributes"].get("first_seen_by"), None);
    assert_eq!(
        out[1].value()["attributes"].get("first_seen_by"),
        Some(&json!("1"))
    );
    assert_eq!(
        h.state.keys(),
        vec!["ingest:acme:script:seen:disk full".to_owned()]
    );
    assert_eq!(h.counter(CounterMetric::StateOps, &STAGE), 2);
    h.finish();
}

#[test]
fn state_get_incr_and_del_reach_the_store() {
    let yaml = config(
        "",
        r#"function process(record)
  record.attributes = record.attributes or {}
  local n = state.incr("count", 1, 60000)
  record.attributes["n"] = n
  record.attributes["seen"] = state.get("count")
  if n == 2 then state.del("count") end
  return record
end"#,
    );
    let (out, h) = run(&yaml, 1, (1..=3).map(|id| record(id, json!({}))).collect());
    let ns: Vec<_> = out
        .iter()
        .map(|r| r.value()["attributes"].get("n").cloned())
        .collect();
    assert_eq!(ns, vec![Some(json!(1)), Some(json!(2)), Some(json!(1))]);
    assert_eq!(out[1].value()["attributes"].get("seen"), Some(&json!("2")));
    h.finish();
}

#[test]
fn a_state_error_is_handled_by_on_state_error_not_by_on_error() {
    let source = "function process(record)\n  state.incr(\"count\", 1, 1000)\n  return record\nend";
    // Default: nak, the safe choice for a stage that may produce data from state.
    let h = start(&config("    on_error: drop\n", source), 1);
    h.state.fail_all(true);
    let probe = h.push(record(1, json!({})));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));
    assert_eq!(h.counter(CounterMetric::StateErrors, &STAGE), 1);
    assert_eq!(
        h.counter(CounterMetric::LuaErrors, &lua_error("runtime")),
        0
    );
    assert_eq!(
        h.counter(
            CounterMetric::RecordsDropped,
            &[
                ("tenant", "acme"),
                ("stage", "script"),
                ("reason", "lua_error")
            ]
        ),
        0
    );
    h.finish();
    // `pass` forwards the record as it came in.
    let h = start(&config("    on_state_error: pass\n", source), 1);
    h.state.fail_all(true);
    let probe = h.push(record(1, json!({"body": "x"})));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    assert_eq!(h.sinks.records("out").len(), 1);
    h.finish();
}

#[test]
fn an_upvalue_counter_persists_across_records_on_one_worker() {
    let yaml = config(
        "",
        "local seen = 0\nfunction process(record)\n  seen = seen + 1\n  record.attributes = record.attributes or {}\n  record.attributes[\"seen\"] = seen\n  return record\nend",
    );
    let (out, h) = run(&yaml, 1, (1..=3).map(|id| record(id, json!({}))).collect());
    let seen: Vec<_> = out
        .iter()
        .map(|r| r.value()["attributes"].get("seen").cloned())
        .collect();
    assert_eq!(seen, vec![Some(json!(1)), Some(json!(2)), Some(json!(3))]);
    h.finish();
}

#[test]
fn each_worker_counts_in_its_own_vm() {
    const RECORDS: u64 = 200;
    // The busy loop is what makes the split happen rather than be hoped for: a record costs
    // enough that the pushes outrun one worker, the intake backs up, and the idle workers
    // are woken. It is far under the default budget.
    let yaml = config(
        "",
        "local seen = 0\nfunction process(record)\n  local n = 0\n  for i = 1, 20000 do n = n + 1 end\n  seen = seen + 1\n  record.attributes = record.attributes or {}\n  record.attributes[\"seen\"] = seen\n  return record\nend",
    );
    let (out, h) = run(
        &yaml,
        4,
        (1..=RECORDS).map(|id| record(id, json!({}))).collect(),
    );
    let mut counts: Vec<u64> = out
        .iter()
        .map(|r| r.value()["attributes"]["seen"].as_u64().expect("a count"))
        .collect();
    counts.sort_unstable();
    // One VM per worker means the counters are independent, so no worker sees every record
    // and the counts repeat: four ones, four twos, and so on. Which worker takes which
    // record is the channel's business, so only the shape is asserted, not the split.
    assert_eq!(counts.len() as u64, RECORDS);
    assert!(
        *counts.last().expect("a count") < RECORDS,
        "no worker counted every record: highest was {:?}",
        counts.last()
    );
    assert!(
        counts.iter().filter(|&&c| c == 1).count() > 1,
        "several workers started their own counter at 1"
    );
    h.finish();
}

#[test]
fn now_ns_reads_the_clock_and_log_info_and_warn_are_callable() {
    let before = unix_nanos_now();
    let yaml = config(
        "",
        r#"function process(record)
  log.info("handling " .. record.body)
  log.warn("nearly done")
  record.attributes = record.attributes or {}
  record.attributes["stamped_at"] = now_ns()
  return record
end"#,
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
    let after = unix_nanos_now();
    let stamped = out[0].value()["attributes"]["stamped_at"]
        .as_u64()
        .expect("now_ns() was written");
    assert!(
        (before..=after).contains(&stamped),
        "now_ns() is the wall clock: {stamped} outside {before}..={after}"
    );
    assert_eq!(
        h.counter(CounterMetric::LuaErrors, &lua_error("runtime")),
        0,
        "log.* do not raise"
    );
    h.finish();
}

#[test]
fn a_script_may_drop_or_change_the_id_and_the_kind_and_split_records_keep_the_records_meta() {
    let yaml = config(
        "",
        r#"function process(record)
  local other = {body = "second", id = 99, kind = "span"}
  record.id = nil
  record.kind = "metric"
  return {record, other}
end"#,
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].value().get("id"), None);
    assert_eq!(out[0].value()["kind"], json!("metric"));
    assert_eq!(out[1].value()["id"].as_u64(), Some(99));
    assert_eq!(out[1].value()["kind"], json!("span"));
    assert_eq!(h.counter(CounterMetric::LuaErrors, &lua_error("output")), 0);
    assert_eq!(
        h.counter(
            CounterMetric::RecordsOut,
            &[("tenant", "acme"), ("stage", "out")]
        ),
        2
    );
    h.finish();
}

#[test]
fn a_script_may_rewrite_the_tenant_and_labels_keep_the_meta_tenant() {
    let yaml = config(
        "",
        "function process(record)\n  record.resource = record.resource or {}\n  record.resource[\"tenant.id\"] = \"other\"\n  return record\nend",
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0].value()["resource"].get("tenant.id"),
        Some(&json!("other"))
    );
    assert_eq!(h.counter(CounterMetric::LuaErrors, &lua_error("output")), 0);
    assert_eq!(
        h.counter(
            CounterMetric::RecordsOut,
            &[("tenant", "acme"), ("stage", "out")]
        ),
        1
    );
    h.finish();
}

const STAMP_THEN_DEDUPE: &str = r#"
name: ingest
nodes:
  - id: script
    type: lua
    source: |
      function process(record)
        record.observed_time_unix_nano = now_ns()
        return record
      end
  - id: dedupe_body
    type: dedupe
    from: script
    key: [body]
    window: 10s
  - id: out
    type: sink.memory
    from: dedupe_body
"#;

#[test]
fn a_script_stamping_the_clock_into_a_time_field_does_not_move_a_downstream_window() {
    for_each_worker_count(|workers| {
        let h = start(STAMP_THEN_DEDUPE, workers);
        let before = unix_nanos_now();

        // Ingested 20 s apart, past the 10 s window, then stamped a few microseconds apart
        // by the script. The window is the ingestion time's, so both pass; the third was
        // ingested 5 s after the second and is its repeat. Redelivering the first, with a
        // later clock reading this time, still passes as its own holder's record.
        let sends = [
            (101, 1_000, 1),
            (102, 1_020, 1),
            (103, 1_025, 1),
            (101, 1_000, 2),
        ];
        for (id, ingested_s, delivery_count) in sends {
            let r = record(id, json!({"body": "disk full"}));
            let arrival = Arrival {
                tenant: Some("acme".to_owned()),
                ingestion_time: Some(IngestionTime::Reported(ingested_s * 1_000_000_000)),
                delivery_count,
                ..Arrival::default()
            };
            let probe = h.push_as_producer(r, arrival);
            assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");
        }

        assert_eq!(h.ids("out"), vec![101, 101, 102], "workers={workers}");
        for written in h.sinks.records("out") {
            let stamped = written.value()["observed_time_unix_nano"]
                .as_u64()
                .expect("stamped");
            assert!(stamped >= before, "the sink writes the script's stamp");
        }
        h.finish();
    });
}

#[test]
fn a_script_read_from_a_file_runs_over_records() {
    let dir = std::env::temp_dir().join(format!("fusion-lua-harness-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join("stamp.lua");
    std::fs::write(
        &path,
        "function process(record)\n  record.attributes = record.attributes or {}\n  record.attributes[\"from\"] = \"file\"\n  return record\nend",
    )
    .expect("write the script");
    let yaml = format!(
        "name: ingest\nnodes:\n  - id: script\n    type: lua\n    script: {}\n  - id: out\n    type: sink.memory\n",
        path.display()
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0].value()["attributes"].get("from"),
        Some(&json!("file"))
    );
    h.finish();
    std::fs::remove_dir_all(&dir).ok();
}

/// The issue's demo, as the compose pipeline ships it in `deploy/pipeline.yaml`: what
/// `edit` cannot do. Split a multi-line body into one record per line and derive
/// `http.status_class` from `http.status`. Read from the shipped file, so the script an
/// operator copies is the one this test drives.
fn shipped_split_lines() -> String {
    let config =
        Config::from_yaml(&deploy_config("pipeline.yaml")).expect("the compose config parses");
    let node = config
        .nodes
        .iter()
        .find(|node| node.id == "split_lines")
        .expect("the compose config has `split_lines`");
    let params: Value = node.parse_params().expect("params parse");
    params["source"]
        .as_str()
        .expect("`split_lines` has an inline source")
        .to_owned()
}

#[test]
fn the_shipped_split_script_splits_lines_and_derives_the_status_class() {
    let script = shipped_split_lines();
    for_each_worker_count(|workers| {
        let yaml = config("", &script);
        let (out, h) = run(
            &yaml,
            workers,
            vec![
                record(
                    1,
                    json!({"body": "one\ntwo\nthree", "attributes": {"http.status": 503}}),
                ),
                record(
                    2,
                    json!({"body": "single", "attributes": {"http.status": 200}}),
                ),
                record(3, json!({"body": "no status"})),
                record(
                    4,
                    json!({"body": "null status", "attributes": {"http.status": null}}),
                ),
                record(5, json!({"body": "\n\n"})),
            ],
        );
        let lines: Vec<_> = out
            .iter()
            .filter(|r| r.value()["id"] == 1)
            .map(|r| {
                (
                    r.value()["body"].clone(),
                    r.value()["attributes"].get("http.status_class").cloned(),
                )
            })
            .collect();
        assert_eq!(lines.len(), 3, "workers {workers}");
        for (body, class) in &lines {
            assert!(matches!(body, Value::String(_)));
            assert_eq!(class, &Some(json!("5xx")));
        }
        let single = out.iter().find(|r| r.value()["id"] == 2).expect("record 2");
        assert_eq!(
            single.value()["attributes"].get("http.status_class"),
            Some(&json!("2xx"))
        );
        let none = out.iter().find(|r| r.value()["id"] == 3).expect("record 3");
        assert_eq!(none.value()["attributes"].get("http.status_class"), None);
        let null = out.iter().find(|r| r.value()["id"] == 4).expect("record 4");
        assert_eq!(null.value()["attributes"].get("http.status_class"), None);
        assert_eq!(
            null.value()["attributes"].get("http.status"),
            Some(&Value::Null)
        );
        let blank = out
            .iter()
            .find(|r| r.value()["id"] == 5)
            .expect("record 5, only newlines, passes unchanged");
        assert_eq!(blank.value()["body"], json!("\n\n"));
        assert_eq!(h.counter(CounterMetric::RecordsOut, &STAGE), 7);
        // Without the script's newline-only guard, record 5 still arrives under
        // `on_error: pass`; only the `output` count shows the empty split was refused.
        for kind in ["output", "runtime"] {
            assert_eq!(h.counter(CounterMetric::LuaErrors, &lua_error(kind)), 0);
        }
        h.finish();
    });
}

#[test]
fn pcall_cannot_swallow_the_instruction_budget() {
    let yaml = config(
        "    limits: { instructions: 10000 }\n    on_error: drop\n",
        "function process(record)\n  while true do pcall(function() while true do end end) end\nend",
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({}))]);
    assert!(out.is_empty());
    assert_eq!(
        h.counter(CounterMetric::LuaErrors, &lua_error("instructions")),
        1
    );
    h.finish();
}

#[test]
fn pcall_cannot_swallow_the_memory_cap() {
    let yaml = config(
        "    limits: { instructions: 100000000, memory_kib: 256 }\n    on_error: drop\n",
        "function process(record)\n  local t = {}\n  while true do\n    local ok = pcall(function() t[#t + 1] = string.rep(\"x\", 4096) end)\n    if not ok then t = {} end\n  end\nend",
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({}))]);
    assert!(out.is_empty());
    assert_eq!(h.counter(CounterMetric::LuaErrors, &lua_error("memory")), 1);
    h.finish();
}

#[test]
fn pcall_cannot_swallow_a_state_error_so_on_state_error_still_applies() {
    let yaml = config(
        "    on_error: pass\n",
        "function process(record)\n  local ok = pcall(state.incr, \"count\", 1, 1000)\n  record.attributes[\"ok\"] = ok\n  return record\nend",
    );
    let h = start(&yaml, 1);
    h.state.fail_all(true);
    let probe = h.push(record(1, json!({})));
    assert_eq!(
        probe.wait(WAIT),
        Some(AckOutcome::Nak(None)),
        "default on_state_error is nak"
    );
    assert!(h.sinks.records("out").is_empty());
    h.finish();
}

#[test]
fn pcall_and_xpcall_still_catch_the_scripts_own_errors() {
    let yaml = config(
        "",
        r#"function process(record)
  record.attributes = record.attributes or {}
  local ok, err = pcall(error, "boom")
  record.attributes["pcall"] = tostring(ok) .. ":" .. err
  local ok2, handled = xpcall(function() return nil + 1 end, function(m) return "handled" end)
  record.attributes["xpcall"] = tostring(ok2) .. ":" .. handled
  local ok3, a, b = pcall(function() return 1, 2 end)
  record.attributes["values"] = tostring(ok3) .. ":" .. a .. b
  return record
end"#,
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({}))]);
    assert_eq!(out.len(), 1);
    let caught = out[0].value()["attributes"]["pcall"]
        .as_str()
        .expect("a string");
    assert!(caught.starts_with("false:"), "{caught}");
    assert!(caught.contains("boom"), "{caught}");
    assert_eq!(
        out[0].value()["attributes"].get("xpcall"),
        Some(&json!("false:handled"))
    );
    assert_eq!(
        out[0].value()["attributes"].get("values"),
        Some(&json!("true:12"))
    );
    assert_eq!(
        h.counter(CounterMetric::LuaErrors, &lua_error("runtime")),
        0
    );
    h.finish();
}

#[test]
fn a_memory_error_rebuilds_the_worker_vm_so_a_leaky_upvalue_does_not_poison_every_record() {
    // `string.rep` builds its result in a buffer and then copies it into the string, so one
    // call peaks at twice the chunk: 1600 KiB of a 2048 KiB cap passes on a fresh VM, and
    // with 800 KiB already kept in the upvalue the next call cannot fit.
    let yaml = config(
        "    limits: { instructions: 100000000, memory_kib: 2048 }\n    on_error: drop\n",
        "local kept = {}\nfunction process(record)\n  kept[#kept + 1] = string.rep(\"x\", 800 * 1024)\n  record.attributes = record.attributes or {}\n  record.attributes[\"kept\"] = #kept\n  return record\nend",
    );
    let (out, h) = run(&yaml, 1, (1..=5).map(|id| record(id, json!({}))).collect());
    let ids: Vec<_> = out.iter().map(|r| r.value()["id"].as_u64()).collect();
    assert_eq!(
        ids,
        vec![Some(1), Some(3), Some(5)],
        "every second record trips the cap and the next runs on a fresh VM"
    );
    for r in &out {
        assert_eq!(
            r.value()["attributes"].get("kept"),
            Some(&json!(1)),
            "upvalues start over after a rebuild"
        );
    }
    assert_eq!(h.counter(CounterMetric::LuaErrors, &lua_error("memory")), 2);
    h.finish();
}

#[test]
fn an_integral_float_comes_back_as_the_float_it_is() {
    // Lua's `/` always yields a float, and nothing types a record's keys any more, so the
    // float is written as the script made it: `18 / 2` is `9.0`, not `9`.
    let yaml = config(
        "",
        r#"function process(record)
  return { id = record.id, body = record.body, severity_number = 18 / 2, time_unix_nano = 10 / 5 }
end"#,
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].value()["severity_number"], json!(9.0));
    assert_eq!(out[0].value()["severity_number"].as_u64(), None);
    assert_eq!(out[0].value()["time_unix_nano"], json!(2.0));
    assert_eq!(h.counter(CounterMetric::LuaErrors, &lua_error("output")), 0);
    h.finish();
}

#[test]
fn a_map_value_that_arrived_composite_round_trips_through_an_untouched_script() {
    let yaml = config("", "function process(record) return record end");
    let (out, h) = run(
        &yaml,
        1,
        vec![record(
            1,
            json!({"attributes": {"tags": ["a", "b"], "meta": {"k": 1}}}),
        )],
    );
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0].value()["attributes"].get("tags"),
        Some(&json!(["a", "b"]))
    );
    assert_eq!(
        out[0].value()["attributes"].get("meta"),
        Some(&json!({"k": 1}))
    );
    assert_eq!(h.counter(CounterMetric::LuaErrors, &lua_error("output")), 0);
    h.finish();
}

#[test]
fn a_record_id_above_two_to_the_63_crosses_as_text_on_meta() {
    // The record id the transport named lives on `meta`, where an id too wide for a Lua
    // integer crosses as its decimal text. The record's own `id` is payload like any other
    // key: a number that wide is the float Lua can hold, and a string stays a string.
    let yaml = config(
        "",
        r#"function process(record, meta)
  assert(type(meta.id) == "string", "an id above 2^63 is its decimal text")
  record.attributes = { meta_id = meta.id }
  local copy = { id = "7", body = "set as text" }
  return { record, copy }
end"#,
    );
    let big = u64::MAX - 1;
    let (out, h) = run(&yaml, 1, vec![record(big, json!({"body": "x"}))]);
    assert_eq!(out.len(), 2);
    assert_eq!(
        out[0].value()["attributes"].get("meta_id"),
        Some(&json!(big.to_string()))
    );
    assert_eq!(out[0].value()["id"].as_f64(), Some(big as f64));
    assert_eq!(out[1].value()["id"], json!("7"));
    assert_eq!(h.counter(CounterMetric::LuaErrors, &lua_error("output")), 0);
    h.finish();
}

#[test]
fn meta_is_a_second_argument_that_refuses_a_write_and_record_meta_is_an_ordinary_key() {
    // `meta` never enters the record on its own: a script copies a value out of it, and a
    // write to it raises. `record.meta` names nothing but a payload key.
    let yaml = config(
        "",
        r#"function process(record, meta)
  record.meta = { tenant = meta.tenant, delivery = meta.delivery_count }
  record.write_raised = not pcall(function() meta.tenant = "other" end)
  return record
end"#,
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0].value()["meta"],
        json!({"tenant": "acme", "delivery": 1})
    );
    assert_eq!(out[0].value()["write_raised"], json!(true));
    assert_eq!(h.counter(CounterMetric::LuaErrors, &lua_error("output")), 0);
    assert_eq!(
        h.counter(CounterMetric::LuaErrors, &lua_error("runtime")),
        0
    );
    h.finish();
}

#[test]
fn record_copy_is_a_deep_copy_so_a_split_record_changes_alone() {
    let yaml = config(
        "",
        r#"function process(record)
  local copy = record:copy()
  copy.body = "second"
  copy.attributes["only"] = "copy"
  local again = copy:copy()
  again.body = "third"
  return { record, copy, again }
end"#,
    );
    let (out, h) = run(
        &yaml,
        1,
        vec![record(
            1,
            json!({"body": "first", "time_unix_nano": 5, "trace_id": "ab", "attributes": {"a": 1}, "resource": {"service.name": "api"}}),
        )],
    );
    let bodies: Vec<_> = out.iter().map(|r| r.value()["body"].clone()).collect();
    assert_eq!(
        bodies,
        vec![json!("first"), json!("second"), json!("third")]
    );
    assert_eq!(
        out[0].value()["attributes"].get("only"),
        None,
        "the original is untouched"
    );
    for r in &out[1..] {
        assert_eq!(r.value()["attributes"].get("only"), Some(&json!("copy")));
        assert_eq!(
            r.value()["time_unix_nano"].as_u64(),
            Some(5),
            "every field is copied"
        );
        assert_eq!(r.value()["trace_id"].as_str(), Some("ab"));
        assert_eq!(
            r.value()["resource"].get("service.name"),
            Some(&json!("api"))
        );
    }
    assert_eq!(h.counter(CounterMetric::LuaErrors, &lua_error("output")), 0);
    h.finish();
}

#[test]
fn a_returned_value_that_contains_itself_is_an_output_error_not_a_crash() {
    let yaml = config(
        "    on_error: drop\n",
        r#"function process(record)
  local loop = {}
  loop.self = loop
  record.body = loop
  return record
end"#,
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
    assert!(out.is_empty());
    assert_eq!(h.counter(CounterMetric::LuaErrors, &lua_error("output")), 1);
    // The worker is still serving.
    let probe = h.push(record(2, json!({"body": "x"})));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    h.finish();
}

#[test]
fn a_script_cannot_reach_or_replace_the_record_metatable() {
    let yaml = config(
        "",
        r#"function process(record)
  record.attributes = record.attributes or {}
  record.attributes["mt"] = getmetatable(record)
  record.attributes["locked"] = not pcall(setmetatable, record, nil)
  return record
end"#,
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
    assert_eq!(
        out[0].value()["attributes"].get("mt"),
        Some(&json!("record"))
    );
    assert_eq!(
        out[0].value()["attributes"].get("locked"),
        Some(&json!(true))
    );
    h.finish();
}

#[test]
fn the_deepest_body_a_record_can_decode_comes_back_unchanged() {
    let yaml = config("", "function process(record) return record end");
    let nested = |depth| {
        let mut body = json!("leaf");
        for _ in 0..depth {
            body = json!({ "n": body });
        }
        body
    };
    // Found rather than hard-coded, so the test follows the decoder's limit.
    let parses =
        |depth| Record::from_json(&json!({ "id": 1, "body": nested(depth) }).to_string()).is_ok();
    let deepest = (1..)
        .find(|&depth| !parses(depth))
        .expect("the decoder has a depth limit")
        - 1;
    let body = nested(deepest);
    let (out, h) = run(&yaml, 1, vec![record(1, json!({ "body": body.clone() }))]);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].value()["body"], body);
    assert_eq!(h.counter(CounterMetric::LuaErrors, &lua_error("output")), 0);
    h.finish();
}

#[test]
fn a_returned_value_may_nest_127_tables_below_the_record_and_no_deeper() {
    // The reader allows 128 tables, and the record table is the first of them, so 127 may
    // nest below it.
    let yaml = config(
        "    on_error: drop\n",
        r#"function process(record)
  local body = "leaf"
  for _ = 1, record.attributes.depth do body = { n = body } end
  record.body = body
  return record
end"#,
    );
    let (out, h) = run(
        &yaml,
        1,
        vec![
            record(1, json!({ "attributes": { "depth": 127 } })),
            record(2, json!({ "attributes": { "depth": 128 } })),
        ],
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].value()["id"].as_u64(), Some(1));
    assert_eq!(h.counter(CounterMetric::LuaErrors, &lua_error("output")), 1);
    h.finish();
}

#[test]
fn a_large_integral_float_comes_back_as_the_float_it_stands_for() {
    // A float wide enough to hold a nanosecond timestamp keeps its value exactly, and
    // stays a float: no key is coerced to an integer for standing for one.
    let yaml = config(
        "",
        "function process(record)\n  record.time_unix_nano = 1789000000000 * 1e6\n  return record\nend",
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
    let stamped = out[0].value()["time_unix_nano"].as_f64().expect("a number");
    assert_eq!(stamped, 1_789_000_000_000_000_000f64);
    assert_eq!(out[0].value()["time_unix_nano"].as_u64(), None);
    h.finish();
}

/// Composite values of every shape a record can decode into, `[]` and `null` included.
fn composites() -> Value {
    json!({
        "body": [[], null, {"k": null}],
        "attributes": {
            "empty": [],
            "holes": [1, null, 2],
            "trailing": [null],
            "nothing": null,
            "nested": {"a": [], "b": null}
        }
    })
}

#[test]
fn an_untouched_or_copied_record_comes_back_with_its_lists_and_nulls() {
    for script in [
        "function process(record) return record end",
        "function process(record) return record:copy() end",
    ] {
        let yaml = config("", script);
        let sent = record(1, composites());
        let (out, h) = run(&yaml, 1, vec![sent.clone()]);
        assert_eq!(out, vec![sent], "{script}");
        h.finish();
    }
}

#[test]
fn a_script_sees_json_null_inside_lists_and_maps_and_may_write_it() {
    let yaml = config(
        "",
        r#"function process(record)
  local a = record.attributes
  a.length = #a.holes
  a.hole_is_null = a.holes[2] == json.null
  a.nothing_is_null = a.nothing == json.null
  a.written = json.null
  table.insert(a.empty, "x")
  return record
end"#,
    );
    let (out, h) = run(&yaml, 1, vec![record(1, composites())]);
    let a = &out[0].value()["attributes"];
    assert_eq!(a.get("length"), Some(&json!(3)));
    assert_eq!(a.get("hole_is_null"), Some(&json!(true)));
    assert_eq!(a.get("nothing_is_null"), Some(&json!(true)));
    assert_eq!(a.get("written"), Some(&Value::Null));
    assert_eq!(a.get("empty"), Some(&json!(["x"])));
    h.finish();
}

#[test]
fn a_list_given_a_key_that_is_not_its_position_is_an_output_error() {
    let yaml = config(
        "    on_error: drop\n",
        r#"function process(record)
  record.attributes.empty.name = "x"
  return record
end"#,
    );
    let (out, h) = run(&yaml, 1, vec![record(1, composites())]);
    assert!(out.is_empty());
    assert_eq!(h.counter(CounterMetric::LuaErrors, &lua_error("output")), 1);
    h.finish();
}

#[test]
fn a_script_cannot_unmark_or_change_the_list_metatable() {
    let yaml = config(
        "",
        r#"function process(record)
  local list = record.attributes.empty
  record.attributes.mt = getmetatable(list)
  record.attributes.locked = not pcall(setmetatable, list, nil)
  return record
end"#,
    );
    let (out, h) = run(&yaml, 1, vec![record(1, composites())]);
    assert_eq!(out[0].value()["attributes"].get("mt"), Some(&json!("list")));
    assert_eq!(
        out[0].value()["attributes"].get("locked"),
        Some(&json!(true))
    );
    assert_eq!(out[0].value()["attributes"].get("empty"), Some(&json!([])));
    h.finish();
}

#[test]
fn a_list_with_a_nil_hole_is_an_output_error_naming_the_hole() {
    let yaml = config(
        "    on_error: drop\n",
        r#"function process(record)
  record.attributes.holes[2] = nil
  return record
end"#,
    );
    let (out, h) = run(&yaml, 1, vec![record(1, composites())]);
    assert!(out.is_empty());
    assert_eq!(h.counter(CounterMetric::LuaErrors, &lua_error("output")), 1);
    h.finish();
}

#[test]
fn a_key_set_to_json_null_is_kept_as_null_and_nil_removes_it() {
    // A record is any JSON, so a top-level `null` is a value like any other: `json.null`
    // writes it and `nil` is what takes a key out of the record.
    let yaml = config(
        "",
        r#"function process(record)
  record.severity_text = json.null
  record.kind = json.null
  record.body = nil
  return record
end"#,
    );
    let (out, h) = run(
        &yaml,
        1,
        vec![record(1, json!({"severity_text": "WARN", "body": "x"}))],
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].value().get("severity_text"), Some(&Value::Null));
    assert_eq!(out[0].value().get("kind"), Some(&Value::Null));
    assert_eq!(out[0].value().get("body"), None);
    h.finish();
}

#[test]
fn json_list_makes_a_list_that_stays_one_when_empty() {
    let yaml = config(
        "",
        r#"function process(record)
  record.attributes = record.attributes or {}
  record.attributes.fresh = json.list()
  record.attributes.given = json.list({})
  record.attributes.filled = json.list({ "a", json.null })
  return record
end"#,
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({}))]);
    let a = &out[0].value()["attributes"];
    assert_eq!(a.get("fresh"), Some(&json!([])));
    assert_eq!(a.get("given"), Some(&json!([])));
    assert_eq!(a.get("filled"), Some(&json!(["a", null])));
    h.finish();
}

#[test]
fn a_script_cannot_change_json() {
    let yaml = config(
        "",
        r#"function process(record)
  record.attributes.refused = not pcall(function() json.null = 1 end)
  record.attributes.mt = getmetatable(json)
  record.attributes.still_null = record.attributes.nothing == json.null
  return record
end"#,
    );
    let (out, h) = run(&yaml, 1, vec![record(1, composites())]);
    let a = &out[0].value()["attributes"];
    assert_eq!(a.get("refused"), Some(&json!(true)));
    assert_eq!(a.get("mt"), Some(&json!("json")));
    assert_eq!(a.get("still_null"), Some(&json!(true)));
    h.finish();
}

#[test]
fn a_returned_split_list_may_hold_only_its_positions() {
    for script in [
        "function process(record)\n  return { record, extra = record:copy() }\nend",
        "function process(record)\n  return { record, nil, record:copy() }\nend",
    ] {
        let yaml = config("    on_error: drop\n", script);
        let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
        assert!(out.is_empty(), "{script}");
        assert_eq!(
            h.counter(CounterMetric::LuaErrors, &lua_error("output")),
            1,
            "{script}"
        );
        h.finish();
    }
}

#[test]
fn what_a_script_does_to_json_is_gone_by_the_next_record() {
    let yaml = config(
        "",
        r#"function process(record)
  record.attributes.null_ok = record.attributes.nothing == json.null
  record.attributes.list_ok = type(json.list) == "function"
  rawset(json, "null", 1)
  json = nil
  return record
end"#,
    );
    let (out, h) = run(
        &yaml,
        1,
        vec![record(1, composites()), record(2, composites())],
    );
    assert_eq!(out.len(), 2);
    for r in &out {
        assert_eq!(
            r.value()["attributes"].get("null_ok"),
            Some(&json!(true)),
            "{:?}",
            r.value()["id"]
        );
        assert_eq!(
            r.value()["attributes"].get("list_ok"),
            Some(&json!(true)),
            "{:?}",
            r.value()["id"]
        );
    }
    h.finish();
}

#[test]
fn json_is_there_at_the_top_level_and_list_keeps_a_list() {
    let yaml = config(
        "",
        r#"local empty = json.list()
function process(record)
  record.attributes.same = json.list(empty) == empty
  record.attributes.kept = json.list(record.attributes.holes) == record.attributes.holes
  record.attributes.empty = empty
  return record
end"#,
    );
    let (out, h) = run(&yaml, 1, vec![record(1, composites())]);
    let a = &out[0].value()["attributes"];
    assert_eq!(a.get("same"), Some(&json!(true)));
    assert_eq!(a.get("kept"), Some(&json!(true)));
    assert_eq!(a.get("empty"), Some(&json!([])));
    h.finish();
}

#[test]
fn a_returned_list_with_no_records_is_an_output_error() {
    for script in [
        "function process(record)\n  return json.list()\nend",
        "function process(record)\n  return json.list({ name = record })\nend",
    ] {
        let yaml = config("    on_error: drop\n", script);
        let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
        assert!(out.is_empty(), "{script}");
        assert_eq!(
            h.counter(CounterMetric::LuaErrors, &lua_error("output")),
            1,
            "{script}"
        );
        h.finish();
    }
}

/// A script that returns the record it was given returns it unchanged, whatever JSON shape
/// the record has. The empty list is the case that used to come back as an empty object: the
/// record table's metatable carries the shape, so the way back does not have to guess.
#[test]
fn an_identity_script_returns_every_shape_unchanged() {
    const IDENTITY: &str = r#"
nodes:
  - id: same
    type: lua
    source: |
      function process(record)
        return record
      end
  - id: out
    type: sink.memory
    from: same
"#;
    for shape in [
        json!([]),
        json!([1, 2]),
        json!({}),
        json!({"a": 1}),
        json!({"a": [], "b": {}}),
        json!("a raw line"),
        json!(7),
    ] {
        let h = start(IDENTITY, 1);
        let probe = h.source.push_arrival(
            Record::new(shape.clone()),
            Arrival {
                record_id: Some(RecordId(1)),
                ..arrival_as(TENANT)
            },
        );
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "{shape}");
        assert_eq!(h.sinks.records("out")[0].value(), &shape, "{shape}");
        h.finish();
    }
}

/// `record:copy()` keeps the shape too, so a split of a list record is a list of list
/// records rather than of empty objects.
#[test]
fn a_copy_of_a_list_record_is_still_a_list() {
    const SPLIT: &str = r#"
nodes:
  - id: twice
    type: lua
    source: |
      function process(record)
        return {record:copy(), record:copy()}
      end
  - id: out
    type: sink.memory
    from: twice
"#;
    let h = start(SPLIT, 1);
    let probe = h.source.push_arrival(
        Record::new(json!([])),
        Arrival {
            record_id: Some(RecordId(1)),
            ..arrival_as(TENANT)
        },
    );
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    let out: Vec<_> = h
        .sinks
        .records("out")
        .iter()
        .map(|r| r.value().clone())
        .collect();
    assert_eq!(out, vec![json!([]), json!([])]);
    h.finish();
}

/// A record that is a boolean must be able to come back. The guard that refuses a returned
/// boolean exists for a script meaning to drop the record, and applies only when the record
/// did not arrive as one — otherwise an identity script on a boolean record counted an
/// `output` error, discarded whatever the script did, and nakked under `on_error: nak`.
#[test]
fn a_boolean_record_survives_a_script_but_a_stray_boolean_is_still_refused() {
    let identity = |on_error: &str| {
        format!(
            "nodes:\n  - id: same\n    type: lua\n{on_error}    source: |\n      function process(record)\n        return record\n      end\n  - id: out\n    type: sink.memory\n    from: same\n"
        )
    };
    let errors = [("tenant", TENANT), ("stage", "same"), ("kind", "output")];

    for on_error in ["", "    on_error: nak\n"] {
        let h = start(&identity(on_error), 1);
        let probe = h.source.push_arrival(
            Record::new(json!(true)),
            Arrival {
                record_id: Some(RecordId(1)),
                ..arrival_as(TENANT)
            },
        );
        assert_eq!(
            probe.wait(WAIT),
            Some(AckOutcome::Ack),
            "on_error {on_error:?}"
        );
        assert_eq!(h.sinks.records("out")[0].value(), &json!(true));
        assert_eq!(h.counter(CounterMetric::LuaErrors, &errors), 0);
        h.finish();
    }

    // A script may transform it, too.
    let yaml = "nodes:\n  - id: same\n    type: lua\n    source: |\n      function process(record)\n        return not record\n      end\n  - id: out\n    type: sink.memory\n    from: same\n";
    let h = start(yaml, 1);
    let probe = h.source.push_arrival(
        Record::new(json!(true)),
        Arrival {
            record_id: Some(RecordId(1)),
            ..arrival_as(TENANT)
        },
    );
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    assert_eq!(h.sinks.records("out")[0].value(), &json!(false));
    h.finish();

    // But `return false` from a script working on anything else is still the footgun it was.
    let h = start(yaml, 1);
    let probe = h.source.push_arrival(
        Record::new(json!({"a": 1})),
        Arrival {
            record_id: Some(RecordId(1)),
            ..arrival_as(TENANT)
        },
    );
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    assert_eq!(h.counter(CounterMetric::LuaErrors, &errors), 1, "refused");
    h.finish();
}

/// A record table nested inside a returned value keeps its shape too, so the fix for
/// `return record` is not special to the top level.
#[test]
fn a_record_table_nested_in_a_returned_value_keeps_its_shape() {
    const NEST: &str = r#"
nodes:
  - id: wrap
    type: lua
    source: |
      function process(record)
        return {a = record, b = record:copy()}
      end
  - id: out
    type: sink.memory
    from: wrap
"#;
    for shape in [json!([]), json!([1]), json!({})] {
        let h = start(NEST, 1);
        let probe = h.source.push_arrival(
            Record::new(shape.clone()),
            Arrival {
                record_id: Some(RecordId(1)),
                ..arrival_as(TENANT)
            },
        );
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "{shape}");
        assert_eq!(
            h.sinks.records("out")[0].value(),
            &json!({"a": shape, "b": shape}),
            "{shape}"
        );
        h.finish();
    }
}
