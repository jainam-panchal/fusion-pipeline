//! The record's `Meta` through the trait boundary: envelopes pushed with and without an
//! `Arrival`, a test stage that writes what its context says into the record, and
//! assertions on what the in-memory sink received and what the recorder counted.

mod common;

use common::{WAIT, registry, start_with};
use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::io::FailureKind;
use fusion_core::memory::{AckOutcome, MemorySinks};
use fusion_core::meta::{Arrival, IngestionTime, unix_nanos_now};
use fusion_core::metrics::{CounterMetric, HistogramMetric};
use fusion_core::record::{Kind, Record, RecordId};
use fusion_core::stage::{Context, Stage, StageOutput};
use serde_json::{Value, json};

/// Writes the context's `Meta` into `attributes.meta.*`, so the sink shows what a stage saw.
struct Reveal;

impl Stage for Reveal {
    fn process(&self, mut record: Record, ctx: &Context<'_>) -> StageOutput {
        let meta = ctx.meta;
        for (key, value) in [
            ("meta.record_id", json!(meta.record_id.0)),
            ("meta.tenant", json!(&*meta.tenant)),
            (
                "meta.ingestion_time",
                json!(meta.ingestion_time.unix_nanos()),
            ),
            ("meta.delivery_count", json!(meta.delivery_count)),
        ] {
            record.attributes.insert(key.to_owned(), value);
        }
        StageOutput::Pass(record)
    }
}

fn build_reveal(_: &NodeConfig) -> Result<Box<dyn Stage>, ConfigError> {
    Ok(Box::new(Reveal))
}

const REVEAL: &str = r#"
name: ingest
nodes:
  - id: reveal
    type: reveal
  - id: out
    type: sink.memory
"#;

const PASS_THROUGH: &str = r#"
name: ingest
nodes:
  - id: out
    type: sink.memory
"#;

const SPLIT_THEN_REVEAL: &str = r#"
name: ingest
nodes:
  - id: split
    type: lua
    source: |
      function process(record)
        local copy = {}
        for k, v in pairs(record) do copy[k] = v end
        copy.body = "second"
        copy.resource = { ["tenant.id"] = "minted" }
        copy.observed_time_unix_nano = 1
        return { record, copy }
      end
  - id: reveal
    type: reveal
    from: split
  - id: out
    type: sink.memory
    from: reveal
"#;

const REWRITE_ID_THEN_SPLIT_THEN_REVEAL: &str = r#"
name: ingest
nodes:
  - id: rewrite
    type: edit
    ops:
      - set: { field: id, value: 99 }
  - id: split
    type: lua
    from: rewrite
    source: |
      function process(record)
        local copy = {}
        for k, v in pairs(record) do copy[k] = v end
        copy.id = 100
        return { record, copy }
      end
  - id: reveal
    type: reveal
    from: split
  - id: out
    type: sink.memory
    from: reveal
"#;

/// Start `yaml` with the `reveal` stage registered, push `record` with `arrival`, wait for
/// the ack and return what `out` received.
fn reveal(yaml: &str, record: Record, arrival: Arrival) -> (Vec<Record>, common::Harness) {
    let sinks = MemorySinks::new();
    let mut registry = registry(&sinks);
    registry.register_stage("reveal", build_reveal);
    let h = start_with(yaml, 1, sinks, registry);
    let probe = h.source.push_arrival(record, arrival);
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    (h.sinks.records("out"), h)
}

fn record(json: &Value) -> Record {
    Record::from_json(&json.to_string()).expect("record parses")
}

/// What a transport says about a first delivery of record `id`, and nothing else.
fn with_id(id: u64) -> Arrival {
    Arrival {
        record_id: Some(RecordId(id)),
        ..Arrival::default()
    }
}

fn meta_of(record: &Record, key: &str) -> Value {
    record.attributes[&format!("meta.{key}")].clone()
}

#[test]
fn the_tenant_and_the_time_are_the_transports_and_the_records_are_never_read() {
    let (out, h) = reveal(
        REVEAL,
        record(&json!({
            "id": 7,
            "observed_time_unix_nano": 5_000_000_000_u64,
            "resource": {"tenant.id": "beta"}
        })),
        Arrival {
            tenant: Some("acme".to_owned()),
            ingestion_time: Some(IngestionTime::Reported(9_000_000_000)),
            delivery_count: 3,
            ..with_id(7)
        },
    );
    assert_eq!(meta_of(&out[0], "record_id"), json!(7));
    assert_eq!(meta_of(&out[0], "tenant"), json!("acme"));
    assert_eq!(meta_of(&out[0], "ingestion_time"), json!(9_000_000_000_u64));
    assert_eq!(meta_of(&out[0], "delivery_count"), json!(3));
    assert_eq!(
        out[0].resource.get("tenant.id"),
        Some(&json!("beta")),
        "the producer's tenant stays in the payload"
    );
    assert_eq!(
        out[0].observed_time_unix_nano,
        Some(5_000_000_000),
        "and so does its time"
    );
    assert_eq!(
        h.counter(
            CounterMetric::RecordsOut,
            &[("tenant", "acme"), ("stage", "reveal")]
        ),
        1
    );
    assert_eq!(
        h.counter(
            CounterMetric::RecordsOut,
            &[("tenant", "beta"), ("stage", "reveal")]
        ),
        0
    );
    h.finish();
}

#[test]
fn the_record_id_and_the_kind_are_the_transports_and_the_payloads_are_never_read() {
    let sent = record(&json!({"id": 5, "kind": "metric", "body": "disk full"}));
    let (out, h) = reveal(REVEAL, sent, with_id(9));
    assert_eq!(meta_of(&out[0], "record_id"), json!(9));
    assert_eq!(out[0].id, Some(RecordId(5)), "the payload keeps its id");
    assert_eq!(
        out[0].kind,
        Kind::Metric,
        "and its kind, walked all the same"
    );
    assert_eq!(h.sinks.outgoing("out")[0].meta.record_id, RecordId(9));
    h.finish();
}

#[test]
fn a_message_the_transport_says_is_not_a_log_is_dropped_whatever_the_payload_says() {
    let sinks = MemorySinks::new();
    let h = start_with(PASS_THROUGH, 1, sinks.clone(), registry(&sinks));
    for kind in [Kind::Metric, Kind::Span] {
        let probe = h.source.push_arrival(
            record(&json!({"id": 7, "kind": "log"})),
            Arrival {
                kind: Some(kind),
                ..with_id(7)
            },
        );
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "{kind}");
    }
    assert!(h.sinks.records("out").is_empty());
    assert_eq!(
        h.counter(
            CounterMetric::RecordsDropped,
            &[
                ("tenant", "unknown"),
                ("stage", "source"),
                ("reason", "invalid_record")
            ]
        ),
        2
    );
    h.finish();
}

#[test]
fn a_message_without_a_record_id_is_nakked_whatever_the_payload_carries() {
    let sinks = MemorySinks::new();
    let h = start_with(PASS_THROUGH, 1, sinks.clone(), registry(&sinks));
    let probe = h
        .source
        .push_arrival(record(&json!({"id": 7, "body": "x"})), Arrival::default());
    assert!(matches!(probe.wait(WAIT), Some(AckOutcome::Nak(_))));
    let failure = probe.failure().expect("a nak carries its failure");
    assert_eq!(failure.kind, FailureKind::MissingId);
    assert_eq!(failure.record_id, None);
    assert!(h.sinks.records("out").is_empty());
    h.finish();
}

#[test]
fn a_message_that_is_not_a_log_is_dropped_even_without_a_record_id() {
    let sinks = MemorySinks::new();
    let h = start_with(PASS_THROUGH, 1, sinks.clone(), registry(&sinks));
    let probe = h.source.push_arrival(
        record(&json!({"body": "x"})),
        Arrival {
            kind: Some(Kind::Span),
            ..Arrival::default()
        },
    );
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    let dropped = |reason| {
        h.counter(
            CounterMetric::RecordsDropped,
            &[
                ("tenant", "unknown"),
                ("stage", "source"),
                ("reason", reason),
            ],
        )
    };
    assert_eq!(dropped("invalid_record"), 1);
    assert_eq!(dropped("missing_id"), 0);
    h.finish();
}

#[test]
fn a_stage_rewriting_the_payload_id_moves_no_decision_and_nothing_else_in_the_payload() {
    let sent = record(&json!({"id": 7, "body": "disk full", "severity_text": "ERROR"}));
    let (out, h) = reveal(REWRITE_ID_THEN_SPLIT_THEN_REVEAL, sent.clone(), with_id(7));
    assert_eq!(out.len(), 2);
    let ids: Vec<_> = out.iter().map(|r| r.id).collect();
    assert_eq!(
        ids,
        [Some(RecordId(99)), Some(RecordId(100))],
        "the payload ids are what the stages wrote"
    );
    for r in &out {
        assert_eq!(meta_of(r, "record_id"), json!(7), "every later stage saw 7");
        let mut untouched = r.clone();
        untouched.id = sent.id;
        untouched
            .attributes
            .retain(|key, _| !key.starts_with("meta."));
        assert_eq!(untouched, sent, "nothing but the id changed");
    }
    for written in h.sinks.outgoing("out") {
        assert_eq!(
            written.meta.record_id,
            RecordId(7),
            "the sink's Meta, so its header"
        );
    }
    h.finish();
}

#[test]
fn a_record_without_a_tenant_or_a_time_leaves_without_them() {
    let (out, h) = reveal(
        REVEAL,
        record(&json!({"id": 7})),
        Arrival {
            tenant: Some("acme".to_owned()),
            ingestion_time: Some(IngestionTime::Reported(9_000_000_000)),
            ..with_id(7)
        },
    );
    assert_eq!(meta_of(&out[0], "tenant"), json!("acme"));
    assert_eq!(meta_of(&out[0], "ingestion_time"), json!(9_000_000_000_u64));
    assert_eq!(out[0].observed_time_unix_nano, None, "nothing is stamped");
    assert_eq!(out[0].resource.get("tenant.id"), None, "nothing is stamped");
    h.finish();
}

#[test]
fn the_sink_receives_the_meta_beside_the_record_it_never_entered() {
    let sinks = MemorySinks::new();
    let h = start_with(PASS_THROUGH, 1, sinks.clone(), registry(&sinks));
    let sent = record(&json!({"id": 7, "body": "disk full"}));
    let probe = h.source.push_arrival(
        sent.clone(),
        Arrival {
            tenant: Some("acme".to_owned()),
            ingestion_time: Some(IngestionTime::Reported(9_000_000_000)),
            delivery_count: 2,
            ..with_id(7)
        },
    );
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    let written = h.sinks.outgoing("out");
    assert_eq!(written.len(), 1);
    assert_eq!(
        written[0].record, sent,
        "the record leaves exactly as it came"
    );
    assert_eq!(&*written[0].meta.tenant, "acme");
    assert_eq!(
        written[0].meta.ingestion_time,
        IngestionTime::Reported(9_000_000_000)
    );
    assert_eq!(written[0].meta.delivery_count, 2);
    assert_eq!(written[0].meta.record_id.0, 7);
    h.finish();
}

#[test]
fn a_clock_time_an_upstream_pipeline_passed_on_stays_a_clock_time() {
    let sinks = MemorySinks::new();
    let h = start_with(PASS_THROUGH, 1, sinks.clone(), registry(&sinks));
    let probe = h.source.push_arrival(
        record(&json!({"id": 7, "observed_time_unix_nano": 5_000_000_000_u64})),
        Arrival {
            ingestion_time: Some(IngestionTime::Clock(9_000_000_000)),
            ..with_id(7)
        },
    );
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    assert_eq!(
        h.sinks.outgoing("out")[0].meta.ingestion_time,
        IngestionTime::Clock(9_000_000_000),
        "not replaced by the record's time, and not marked reported"
    );
    assert!(
        h.samples(HistogramMetric::EndToEnd, &[("tenant", "unknown")])
            .is_empty(),
        "end to end is not measured from a clock reading"
    );
    h.finish();
}

#[test]
fn a_redelivery_is_counted_under_the_tenant_every_other_metric_of_the_record_carries() {
    let (_, h) = reveal(
        REVEAL,
        record(&json!({"id": 7, "resource": {"tenant.id": "beta"}})),
        Arrival {
            tenant: Some("acme".to_owned()),
            delivery_count: 2,
            ..with_id(7)
        },
    );
    assert_eq!(
        h.counter(CounterMetric::SourceRedeliveries, &[("tenant", "acme")]),
        1
    );
    assert_eq!(
        h.counter(CounterMetric::SourceRedeliveries, &[("tenant", "beta")]),
        0
    );
    h.finish();
}

#[test]
fn a_first_delivery_is_not_a_redelivery() {
    let (_, h) = reveal(REVEAL, record(&json!({"id": 7})), with_id(7));
    assert_eq!(
        h.counter(CounterMetric::SourceRedeliveries, &[("tenant", "unknown")]),
        0
    );
    h.finish();
}

#[test]
fn a_source_that_names_nothing_gives_unknown_and_the_clock_whatever_the_record_carries() {
    let before = unix_nanos_now();
    let (out, h) = reveal(
        REVEAL,
        record(&json!({
            "id": 7,
            "time_unix_nano": 4_000_000_000_u64,
            "observed_time_unix_nano": 5_000_000_000_u64,
            "resource": {"tenant.id": "acme"}
        })),
        with_id(7),
    );
    let after = unix_nanos_now();
    assert_eq!(meta_of(&out[0], "tenant"), json!("unknown"));
    let ingested = meta_of(&out[0], "ingestion_time")
        .as_u64()
        .expect("a number");
    assert!(
        (before..=after).contains(&ingested),
        "the worker clock, not the record's 4 or 5 s: {ingested}"
    );
    assert_eq!(meta_of(&out[0], "delivery_count"), json!(1));
    assert!(
        h.samples(HistogramMetric::EndToEnd, &[("tenant", "unknown")])
            .is_empty(),
        "a clock time is not measured end to end"
    );
    h.finish();
}

#[test]
fn a_record_with_no_tenant_and_no_time_gets_unknown_and_the_worker_clock() {
    let before = unix_nanos_now();
    let (out, h) = reveal(REVEAL, record(&json!({"id": 7})), with_id(7));
    let after = unix_nanos_now();
    assert_eq!(meta_of(&out[0], "tenant"), json!("unknown"));
    let ingested = meta_of(&out[0], "ingestion_time")
        .as_u64()
        .expect("a number");
    assert!(
        (before..=after).contains(&ingested),
        "{ingested} outside {before}..={after}"
    );
    h.finish();
}

#[test]
fn every_record_a_split_emits_continues_under_its_parents_meta() {
    let (out, h) = reveal(
        SPLIT_THEN_REVEAL,
        record(&json!({"id": 7, "body": "first"})),
        Arrival {
            tenant: Some("acme".to_owned()),
            ingestion_time: Some(IngestionTime::Reported(5_000_000_000)),
            delivery_count: 2,
            ..with_id(7)
        },
    );
    assert_eq!(out.len(), 2);
    for r in &out {
        assert_eq!(meta_of(r, "record_id"), json!(7));
        assert_eq!(meta_of(r, "tenant"), json!("acme"));
        assert_eq!(meta_of(r, "ingestion_time"), json!(5_000_000_000_u64));
        assert_eq!(meta_of(r, "delivery_count"), json!(2));
    }
    assert_eq!(out[1].resource.get("tenant.id"), Some(&json!("minted")));
    assert_eq!(
        h.counter(
            CounterMetric::RecordsOut,
            &[("tenant", "acme"), ("stage", "out")]
        ),
        2
    );
    h.finish();
}

/// Push `record` with `arrival` through `yaml` (no test stages), wait for the ack, return the
/// harness.
fn push_through(yaml: &str, record: Record, arrival: Arrival) -> common::Harness {
    let sinks = MemorySinks::new();
    let h = start_with(yaml, 1, sinks.clone(), registry(&sinks));
    let probe = h.source.push_arrival(record, arrival);
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    h
}

/// A second delivery of record `id` for tenant `acme`, ingested at 9 s.
fn acme_arrival(id: u64) -> Arrival {
    Arrival {
        tenant: Some(common::TENANT.to_owned()),
        ingestion_time: Some(IngestionTime::Reported(9_000_000_000)),
        delivery_count: 2,
        ..with_id(id)
    }
}

#[test]
fn a_route_on_meta_tenant_follows_the_pipelines_tenant_not_the_payloads() {
    let yaml = r#"
name: ingest
nodes:
  - id: by_tenant
    type: route
    routes:
      acme: meta.tenant == "acme"
    default: other
  - id: acme_out
    type: sink.memory
    from: by_tenant.acme
  - id: other_out
    type: sink.memory
    from: by_tenant.other
"#;
    let h = push_through(
        yaml,
        record(&json!({"id": 7, "resource": {"tenant.id": "beta"}})),
        acme_arrival(7),
    );
    assert_eq!(h.sinks.records("acme_out").len(), 1);
    assert!(h.sinks.records("other_out").is_empty());
    h.finish();
}

#[test]
fn edit_copy_from_meta_is_the_one_way_a_pipeline_value_enters_the_record() {
    let yaml = r#"
name: ingest
nodes:
  - id: stamp
    type: edit
    ops:
      - copy: { from: meta.tenant, to: resource.tenant.id }
      - copy: { from: meta.ingestion_time, to: observed_time_unix_nano }
      - copy: { from: meta.delivery_count, to: attributes.delivery }
      - copy: { from: meta.id, to: attributes.arrived_as }
  - id: out
    type: sink.memory
"#;
    let h = push_through(
        yaml,
        record(&json!({"id": 7, "body": "x"})),
        acme_arrival(7),
    );
    let out = h.sinks.records("out");
    assert_eq!(out[0].resource.get("tenant.id"), Some(&json!("acme")));
    assert_eq!(out[0].observed_time_unix_nano, Some(9_000_000_000));
    assert_eq!(out[0].attributes.get("delivery"), Some(&json!(2)));
    assert_eq!(out[0].attributes.get("arrived_as"), Some(&json!(7)));
    h.finish();
}

#[test]
fn a_dedupe_key_on_meta_tenant_reads_the_pipelines_tenant() {
    let yaml = r#"
name: ingest
nodes:
  - id: once_per_tenant
    type: dedupe
    key: [meta.tenant]
    window: 10s
  - id: out
    type: sink.memory
"#;
    let sinks = MemorySinks::new();
    let h = start_with(yaml, 1, sinks.clone(), registry(&sinks));
    // Two different payload tenants, one pipeline tenant: the second is a repeat.
    for (id, payload_tenant) in [(1, "beta"), (2, "gamma")] {
        let probe = h.source.push_arrival(
            record(&json!({"id": id, "resource": {"tenant.id": payload_tenant}})),
            acme_arrival(id),
        );
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    }
    assert_eq!(h.sinks.records("out").len(), 1);
    h.finish();
}

#[test]
fn a_script_reads_meta_as_its_second_argument() {
    let yaml = r#"
name: ingest
nodes:
  - id: script
    type: lua
    source: |
      function process(record, meta)
        local seen = {}
        for k, v in pairs(meta) do seen[#seen + 1] = k end
        table.sort(seen)
        record.attributes["meta.keys"] = table.concat(seen, ",")
        record.attributes["meta.id"] = meta.id
        record.attributes["meta.tenant"] = meta.tenant
        record.attributes["meta.ingestion_time"] = meta.ingestion_time
        record.attributes["meta.delivery_count"] = meta.delivery_count
        record.attributes["meta.other"] = meta.other == nil
        return record
      end
  - id: out
    type: sink.memory
"#;
    let h = push_through(
        yaml,
        record(&json!({"id": 7, "resource": {"tenant.id": "beta"}})),
        acme_arrival(7),
    );
    let out = h.sinks.records("out");
    assert_eq!(
        meta_of(&out[0], "keys"),
        json!("delivery_count,id,ingestion_time,tenant")
    );
    assert_eq!(meta_of(&out[0], "id"), json!(7));
    assert_eq!(meta_of(&out[0], "tenant"), json!("acme"));
    assert_eq!(meta_of(&out[0], "ingestion_time"), json!(9_000_000_000_u64));
    assert_eq!(meta_of(&out[0], "delivery_count"), json!(2));
    assert_eq!(meta_of(&out[0], "other"), json!(true));
    h.finish();
}

#[test]
fn a_script_writing_to_meta_is_a_runtime_error_and_changes_nothing() {
    let yaml = r#"
name: ingest
nodes:
  - id: script
    type: lua
    on_error: pass
    source: |
      function process(record, meta)
        meta.tenant = "beta"
        return record
      end
  - id: out
    type: sink.memory
"#;
    let h = push_through(yaml, record(&json!({"id": 7})), acme_arrival(7));
    assert_eq!(
        h.counter(
            CounterMetric::LuaErrors,
            &[("tenant", "acme"), ("stage", "script"), ("kind", "runtime")]
        ),
        1
    );
    let written = h.sinks.outgoing("out");
    assert_eq!(&*written[0].meta.tenant, "acme");
    h.finish();
}

#[test]
fn a_script_cannot_unlock_meta_or_carry_a_raw_write_to_the_next_record() {
    let yaml = r#"
name: ingest
nodes:
  - id: script
    type: lua
    source: |
      function process(record, meta)
        record.attributes["locked"] = getmetatable(meta)
        record.attributes["reset"] = not pcall(setmetatable, meta, nil)
        record.attributes["tenant"] = meta.tenant
        rawset(meta, "tenant", "beta")
        return record
      end
  - id: out
    type: sink.memory
"#;
    let sinks = MemorySinks::new();
    let h = start_with(yaml, 1, sinks.clone(), registry(&sinks));
    for id in [1, 2] {
        let probe = h
            .source
            .push_arrival(record(&json!({"id": id})), acme_arrival(id));
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    }
    for r in h.sinks.records("out") {
        assert_eq!(r.attributes.get("locked"), Some(&json!("meta")));
        assert_eq!(r.attributes.get("reset"), Some(&json!(true)));
        assert_eq!(
            r.attributes.get("tenant"),
            Some(&json!("acme")),
            "a raw write on one record's meta does not reach the next"
        );
    }
    h.finish();
}

#[test]
fn a_transport_tenant_that_is_empty_or_holds_a_control_character_is_unknown() {
    for bad in ["", "a\nb", "a\tb"] {
        let (out, h) = reveal(
            REVEAL,
            record(&json!({"id": 7, "resource": {"tenant.id": "acme"}})),
            Arrival {
                tenant: Some(bad.to_owned()),
                ..with_id(7)
            },
        );
        assert_eq!(
            meta_of(&out[0], "tenant"),
            json!("unknown"),
            "{bad:?}, and the record's tenant is never a fallback"
        );
        h.finish();
    }
}
