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
    let indented: String = source.lines().map(|l| format!("      {l}\n")).collect();
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
        assert_eq!(
            out.iter().map(|r| r.id.map(|i| i.0)).collect::<Vec<_>>(),
            vec![Some(2)]
        );
        assert_eq!(
            h.counter(
                Metric::RecordsDropped,
                &[
                    ("tenant", "acme"),
                    ("stage", "script"),
                    ("reason", "lua_drop")
                ]
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
        bodies.sort_by(|a, b| {
            a.1.as_ref()
                .map(Value::to_string)
                .cmp(&b.1.as_ref().map(Value::to_string))
        });
        assert_eq!(
            bodies,
            vec![(Some(9), Some(json!("a"))), (Some(9), Some(json!("b")))],
            "workers {workers}"
        );
        assert_eq!(h.counter(Metric::RecordsIn, &STAGE), 1);
        assert_eq!(h.counter(Metric::RecordsOut, &STAGE), 2);
        assert_eq!(
            h.counter(Metric::RecordsOut, &[("tenant", "acme"), ("stage", "out")]),
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
        assert_eq!(out[0].body, Some(json!("x")));
        assert_eq!(h.counter(Metric::LuaErrors, &lua_error("instructions")), 1);
        assert_eq!(h.counter(Metric::RecordsErrored, &STAGE), 0);
        assert_eq!(h.counter(Metric::RecordsOut, &STAGE), 1);
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
            Metric::RecordsDropped,
            &[
                ("tenant", "acme"),
                ("stage", "script"),
                ("reason", "lua_error")
            ]
        ),
        1
    );
    assert_eq!(h.counter(Metric::LuaErrors, &lua_error("instructions")), 1);
    h.finish();
}

#[test]
fn on_error_nak_fails_the_record_so_the_source_message_is_nakked() {
    let yaml = config(
        "    limits: { instructions: 10000 }\n    on_error: nak\n",
        LOOPS,
    );
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
    assert_eq!(
        out.iter().map(|r| r.id.map(|i| i.0)).collect::<Vec<_>>(),
        vec![Some(2), Some(3)]
    );
    assert_eq!(h.counter(Metric::LuaErrors, &lua_error("instructions")), 1);
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
        assert_eq!(h.counter(Metric::LuaErrors, &lua_error("memory")), 2);
        assert_eq!(h.counter(Metric::LuaErrors, &lua_error("instructions")), 0);
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
    assert_eq!(h.counter(Metric::LuaErrors, &lua_error("runtime")), 1);
    h.finish();
}

#[test]
fn a_returned_record_the_stage_refuses_counts_as_an_output_error() {
    let cases: [(&str, &str); 6] = [
        ("missing id", "record.id = nil\n  return record"),
        ("changed id", "record.id = record.id + 1\n  return record"),
        (
            "wrong type",
            "record.severity_number = \"high\"\n  return record",
        ),
        (
            "oversized body",
            "record.body = string.rep(\"x\", 2048)\n  return record",
        ),
        (
            "changed tenant",
            "record.resource[\"tenant.id\"] = \"other\"\n  return record",
        ),
        ("not a record", "return 42"),
    ];
    for (name, body) in cases {
        let yaml = config(
            "    limits: { output_kib: 1 }\n    on_error: drop\n",
            &format!("function process(record)\n  {body}\nend"),
        );
        let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
        assert!(out.is_empty(), "{name}: refused");
        assert_eq!(
            h.counter(Metric::LuaErrors, &lua_error("output")),
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
    assert_eq!(out[0].attributes.get("first_seen_by"), None);
    assert_eq!(out[1].attributes.get("first_seen_by"), Some(&json!("1")));
    assert_eq!(
        h.state.keys(),
        vec!["ingest:acme:script:seen:disk full".to_owned()]
    );
    assert_eq!(h.counter(Metric::StateOps, &STAGE), 2);
    h.finish();
}

#[test]
fn state_get_incr_and_del_reach_the_store() {
    let yaml = config(
        "",
        r#"function process(record)
  local n = state.incr("count", 1, 60000)
  record.attributes["n"] = n
  record.attributes["seen"] = state.get("count")
  if n == 2 then state.del("count") end
  return record
end"#,
    );
    let (out, h) = run(&yaml, 1, (1..=3).map(|id| record(id, json!({}))).collect());
    let ns: Vec<_> = out.iter().map(|r| r.attributes.get("n").cloned()).collect();
    assert_eq!(ns, vec![Some(json!(1)), Some(json!(2)), Some(json!(1))]);
    assert_eq!(out[1].attributes.get("seen"), Some(&json!("2")));
    h.finish();
}

#[test]
fn a_state_error_is_handled_by_on_state_error_not_by_on_error() {
    let source = "function process(record)\n  state.incr(\"count\", 1, 1000)\n  return record\nend";
    // Default: nak, the safe choice for a stage that may produce data from state.
    let h = start(&config("    on_error: drop\n", source), 1);
    h.state.fail_all(true);
    let probe = h.source.push(record(1, json!({})));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));
    assert_eq!(h.counter(Metric::StateErrors, &STAGE), 1);
    assert_eq!(h.counter(Metric::LuaErrors, &lua_error("runtime")), 0);
    assert_eq!(
        h.counter(
            Metric::RecordsDropped,
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
    let probe = h.source.push(record(1, json!({"body": "x"})));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    assert_eq!(h.sinks.records("out").len(), 1);
    h.finish();
}

#[test]
fn an_upvalue_counter_persists_across_records_on_one_worker() {
    let yaml = config(
        "",
        "local seen = 0\nfunction process(record)\n  seen = seen + 1\n  record.attributes[\"seen\"] = seen\n  return record\nend",
    );
    let (out, _h) = run(&yaml, 1, (1..=3).map(|id| record(id, json!({}))).collect());
    let seen: Vec<_> = out
        .iter()
        .map(|r| r.attributes.get("seen").cloned())
        .collect();
    assert_eq!(seen, vec![Some(json!(1)), Some(json!(2)), Some(json!(3))]);
}

/// The issue's demo: what `edit` cannot do. Split a multi-line body into one record per
/// line and derive `http.status_class` from `http.status`.
const DEMO: &str = r#"local function class_of(status)
  if status == nil then return nil end
  return string.format("%dxx", status // 100)
end

function process(record)
  local class = class_of(record.attributes["http.status"])
  if class then record.attributes["http.status_class"] = class end
  if type(record.body) ~= "string" or not record.body:find("\n") then
    return record
  end
  local out = {}
  for line in record.body:gmatch("[^\n]+") do
    out[#out + 1] = {
      id = record.id,
      kind = record.kind,
      body = line,
      severity_text = record.severity_text,
      attributes = record.attributes,
      resource = record.resource,
      scope = record.scope,
    }
  end
  return out
end"#;

#[test]
fn the_demo_script_splits_lines_and_derives_the_status_class() {
    for_each_worker_count(|workers| {
        let yaml = config("", DEMO);
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
            ],
        );
        let lines: Vec<_> = out
            .iter()
            .filter(|r| r.id == Some(fusion_core::record::RecordId(1)))
            .map(|r| {
                (
                    r.body.clone(),
                    r.attributes.get("http.status_class").cloned(),
                )
            })
            .collect();
        assert_eq!(lines.len(), 3, "workers {workers}");
        for (body, class) in &lines {
            assert!(matches!(body, Some(Value::String(_))));
            assert_eq!(class, &Some(json!("5xx")));
        }
        let single = out
            .iter()
            .find(|r| r.id == Some(fusion_core::record::RecordId(2)))
            .expect("record 2");
        assert_eq!(
            single.attributes.get("http.status_class"),
            Some(&json!("2xx"))
        );
        let none = out
            .iter()
            .find(|r| r.id == Some(fusion_core::record::RecordId(3)))
            .expect("record 3");
        assert_eq!(none.attributes.get("http.status_class"), None);
        assert_eq!(h.counter(Metric::RecordsOut, &STAGE), 5);
        assert_eq!(h.counter(Metric::LuaErrors, &lua_error("output")), 0);
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
    assert_eq!(h.counter(Metric::LuaErrors, &lua_error("instructions")), 1);
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
    assert_eq!(h.counter(Metric::LuaErrors, &lua_error("memory")), 1);
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
    let probe = h.source.push(record(1, json!({})));
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
    let caught = out[0].attributes["pcall"].as_str().expect("a string");
    assert!(caught.starts_with("false:"), "{caught}");
    assert!(caught.contains("boom"), "{caught}");
    assert_eq!(
        out[0].attributes.get("xpcall"),
        Some(&json!("false:handled"))
    );
    assert_eq!(out[0].attributes.get("values"), Some(&json!("true:12")));
    assert_eq!(h.counter(Metric::LuaErrors, &lua_error("runtime")), 0);
    h.finish();
}

#[test]
fn a_memory_fault_rebuilds_the_worker_vm_so_a_leaky_upvalue_does_not_poison_every_record() {
    // `string.rep` builds its result in a buffer and then copies it into the string, so one
    // call peaks at twice the chunk: 1600 KiB of a 2048 KiB cap passes on a fresh VM, and
    // with 800 KiB already kept in the upvalue the next call cannot fit.
    let yaml = config(
        "    limits: { instructions: 100000000, memory_kib: 2048 }\n    on_error: drop\n",
        "local kept = {}\nfunction process(record)\n  kept[#kept + 1] = string.rep(\"x\", 800 * 1024)\n  record.attributes[\"kept\"] = #kept\n  return record\nend",
    );
    let (out, h) = run(&yaml, 1, (1..=5).map(|id| record(id, json!({}))).collect());
    let ids: Vec<_> = out.iter().map(|r| r.id.map(|i| i.0)).collect();
    assert_eq!(
        ids,
        vec![Some(1), Some(3), Some(5)],
        "every second record trips the cap and the next runs on a fresh VM"
    );
    for r in &out {
        assert_eq!(
            r.attributes.get("kept"),
            Some(&json!(1)),
            "upvalues start over after a rebuild"
        );
    }
    assert_eq!(h.counter(Metric::LuaErrors, &lua_error("memory")), 2);
    h.finish();
}

#[test]
fn kind_is_filled_in_and_integral_floats_are_accepted_in_typed_fields() {
    let yaml = config(
        "",
        r#"function process(record)
  return { id = record.id, body = record.body, severity_number = 18 / 2, time_unix_nano = 10 / 5, resource = record.resource }
end"#,
    );
    let (out, h) = run(&yaml, 1, vec![record(1, json!({"body": "x"}))]);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].kind, fusion_core::record::Kind::Log);
    assert_eq!(out[0].severity_number, Some(9));
    assert_eq!(out[0].time_unix_nano, Some(2));
    assert_eq!(h.counter(Metric::LuaErrors, &lua_error("output")), 0);
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
    assert_eq!(out[0].attributes.get("tags"), Some(&json!(["a", "b"])));
    assert_eq!(out[0].attributes.get("meta"), Some(&json!({"k": 1})));
    assert_eq!(h.counter(Metric::LuaErrors, &lua_error("output")), 0);
    h.finish();
}
