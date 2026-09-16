//! The record's `Meta` through the trait boundary: envelopes pushed with and without an
//! `Arrival`, a test stage that writes what its context says into the record, and
//! assertions on what the in-memory sink received and what the recorder counted.

mod common;

use common::{WAIT, registry, start_with};
use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::memory::{AckOutcome, MemorySinks};
use fusion_core::meta::{Arrival, unix_nanos_now};
use fusion_core::metrics::Metric;
use fusion_core::record::Record;
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

fn meta_of(record: &Record, key: &str) -> Value {
    record.attributes[&format!("meta.{key}")].clone()
}

#[test]
fn the_records_own_tenant_and_time_come_before_what_the_transport_says() {
    let (out, h) = reveal(
        REVEAL,
        record(&json!({
            "id": 7,
            "observed_time_unix_nano": 5_000_000_000_u64,
            "resource": {"tenant.id": "acme"}
        })),
        Arrival {
            tenant: Some("from-subject".to_owned()),
            ingestion_time: Some(9_000_000_000),
            delivery_count: 3,
        },
    );
    assert_eq!(meta_of(&out[0], "record_id"), json!(7));
    assert_eq!(meta_of(&out[0], "tenant"), json!("acme"));
    assert_eq!(meta_of(&out[0], "ingestion_time"), json!(5_000_000_000_u64));
    assert_eq!(meta_of(&out[0], "delivery_count"), json!(3));
    assert_eq!(
        h.counter(
            Metric::RecordsOut,
            &[("tenant", "acme"), ("stage", "reveal")]
        ),
        1
    );
    h.finish();
}

#[test]
fn a_record_without_a_tenant_or_a_time_takes_the_transports() {
    let (out, h) = reveal(
        REVEAL,
        record(&json!({"id": 7})),
        Arrival {
            tenant: Some("from-subject".to_owned()),
            ingestion_time: Some(9_000_000_000),
            delivery_count: 1,
        },
    );
    assert_eq!(meta_of(&out[0], "tenant"), json!("from-subject"));
    assert_eq!(meta_of(&out[0], "ingestion_time"), json!(9_000_000_000_u64));
    assert_eq!(
        out[0].observed_time_unix_nano, None,
        "the payload is untouched"
    );
    assert_eq!(
        h.counter(
            Metric::RecordsOut,
            &[("tenant", "from-subject"), ("stage", "reveal")]
        ),
        1
    );
    h.finish();
}

#[test]
fn a_tenant_field_that_is_not_a_string_is_no_tenant_and_the_transports_stands() {
    let (out, h) = reveal(
        REVEAL,
        record(&json!({"id": 7, "resource": {"tenant.id": 42}})),
        Arrival {
            tenant: Some("acme".to_owned()),
            ..Arrival::default()
        },
    );
    assert_eq!(meta_of(&out[0], "tenant"), json!("acme"));
    h.finish();
}

#[test]
fn a_redelivery_is_counted_under_the_tenant_every_other_metric_of_the_record_carries() {
    let (_, h) = reveal(
        REVEAL,
        record(&json!({"id": 7, "resource": {"tenant.id": "beta"}})),
        Arrival {
            tenant: Some("acme".to_owned()),
            ingestion_time: None,
            delivery_count: 2,
        },
    );
    assert_eq!(
        h.counter(Metric::SourceRedeliveries, &[("tenant", "beta")]),
        1
    );
    assert_eq!(
        h.counter(Metric::SourceRedeliveries, &[("tenant", "acme")]),
        0
    );
    h.finish();
}

#[test]
fn a_first_delivery_is_not_a_redelivery() {
    let (_, h) = reveal(REVEAL, record(&json!({"id": 7})), Arrival::default());
    assert_eq!(
        h.counter(Metric::SourceRedeliveries, &[("tenant", "unknown")]),
        0
    );
    h.finish();
}

#[test]
fn a_source_that_says_nothing_leaves_the_engine_to_read_the_record_at_intake() {
    let (out, h) = reveal(
        REVEAL,
        record(&json!({
            "id": 7,
            "time_unix_nano": 4_000_000_000_u64,
            "resource": {"tenant.id": "acme"}
        })),
        Arrival::default(),
    );
    assert_eq!(meta_of(&out[0], "tenant"), json!("acme"));
    assert_eq!(meta_of(&out[0], "ingestion_time"), json!(4_000_000_000_u64));
    assert_eq!(meta_of(&out[0], "delivery_count"), json!(1));
    h.finish();
}

#[test]
fn a_record_with_no_tenant_and_no_time_gets_unknown_and_the_worker_clock() {
    let before = unix_nanos_now();
    let (out, h) = reveal(REVEAL, record(&json!({"id": 7})), Arrival::default());
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
        record(&json!({
            "id": 7,
            "body": "first",
            "observed_time_unix_nano": 5_000_000_000_u64,
            "resource": {"tenant.id": "acme"}
        })),
        Arrival {
            delivery_count: 2,
            ..Arrival::default()
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
        h.counter(Metric::RecordsOut, &[("tenant", "acme"), ("stage", "out")]),
        2
    );
    h.finish();
}
