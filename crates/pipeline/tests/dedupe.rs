//! The `dedupe` node through the trait boundary: YAML config in, envelopes pushed through
//! the in-memory source, assertions on which records reached the in-memory sink, how each
//! ack handle settled, what the state store holds and what the recorder counted.

mod common;

use std::time::Duration;

use fusion_core::memory::AckOutcome;
use fusion_core::metrics::Metric;
use fusion_core::record::Record;

use common::{
    DEDUPE_DROP, WAIT, acme_record as record, acme_record_observed_at, for_each_worker_count, start,
};

const DEDUPE_BODY: &str = r#"
name: ingest
nodes:
  - id: dedupe_body
    type: dedupe
    key: [body]
    window: 10s
  - id: out
    type: sink.memory
"#;

#[test]
fn two_different_records_with_the_same_key_inside_the_window_pass_once_and_drop_once() {
    for_each_worker_count(|workers| {
        let h = start(DEDUPE_BODY, workers);

        let first = h.source.push(record(101, "disk full"));
        assert_eq!(first.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");
        let second = h.source.push(record(102, "disk full"));
        assert_eq!(
            second.wait(WAIT),
            Some(AckOutcome::Ack),
            "workers={workers}"
        );

        assert_eq!(h.ids("out"), vec![101], "workers={workers}");
        assert_eq!(
            h.counter(Metric::RecordsDropped, &DEDUPE_DROP),
            1,
            "workers={workers}"
        );
        h.finish();
    });
}

/// A record with an explicit ingestion time, in seconds.
fn record_at(id: u64, body: &str, observed_s: u64) -> Record {
    acme_record_observed_at(id, body, observed_s * 1_000_000_000)
}

#[test]
fn the_same_record_pushed_twice_passes_twice_because_redelivery_is_not_a_duplicate() {
    for_each_worker_count(|workers| {
        let h = start(DEDUPE_BODY, workers);

        let first = h.source.push(record(101, "disk full"));
        assert_eq!(first.wait(WAIT), Some(AckOutcome::Ack));
        let again = h.source.push(record(101, "disk full"));
        assert_eq!(again.wait(WAIT), Some(AckOutcome::Ack));

        assert_eq!(h.ids("out"), vec![101, 101], "workers={workers}");
        assert_eq!(
            h.counter(Metric::RecordsDropped, &DEDUPE_DROP),
            0,
            "workers={workers}"
        );
        h.finish();
    });
}

#[test]
fn a_repeat_after_the_window_passes_again() {
    let h = start(DEDUPE_BODY, 1);

    assert_eq!(
        h.source.push(record_at(101, "disk full", 0)).wait(WAIT),
        Some(AckOutcome::Ack)
    );
    h.state.advance(Duration::from_secs(10));
    assert_eq!(
        h.source.push(record_at(102, "disk full", 11)).wait(WAIT),
        Some(AckOutcome::Ack)
    );

    assert_eq!(h.ids("out"), vec![101, 102]);
    h.finish();
}

/// The crash case: 101 passes and its key expires while the pipeline is down; a real
/// duplicate 102 claims the key; 101 is redelivered after that, carrying its original
/// ingestion time. Older than the owner, so it passes. A later repeat inside 102's window
/// still drops.
#[test]
fn a_record_redelivered_after_its_window_expired_and_a_newer_duplicate_took_the_key_still_passes() {
    let h = start(DEDUPE_BODY, 1);

    assert_eq!(
        h.source.push(record_at(101, "disk full", 0)).wait(WAIT),
        Some(AckOutcome::Ack)
    );
    h.state.advance(Duration::from_secs(15));
    assert_eq!(
        h.source.push(record_at(102, "disk full", 15)).wait(WAIT),
        Some(AckOutcome::Ack),
        "new window"
    );
    assert_eq!(
        h.source.push(record_at(101, "disk full", 0)).wait(WAIT),
        Some(AckOutcome::Ack),
        "redelivery of 101"
    );
    assert_eq!(
        h.source.push(record_at(103, "disk full", 18)).wait(WAIT),
        Some(AckOutcome::Ack),
        "repeat inside 102's window"
    );

    assert_eq!(h.ids("out"), vec![101, 101, 102]);
    assert_eq!(h.counter(Metric::RecordsDropped, &DEDUPE_DROP), 1);
    h.finish();
}

/// The backlog-replay case: a burst arrives within wall-clock seconds whose ingestion times
/// span several windows. The first record past the holder's window opens a new one, so the
/// records after it inside that new window drop; passing them all until the old key's
/// wall-clock TTL ran out would let a whole burst through.
#[test]
fn a_record_past_the_window_opens_a_new_one_so_the_burst_behind_it_still_dedupes() {
    let h = start(DEDUPE_BODY, 1);

    assert_eq!(
        h.source.push(record_at(101, "disk full", 0)).wait(WAIT),
        Some(AckOutcome::Ack)
    );
    for (id, s) in [(102, 10), (103, 11), (104, 15), (105, 20), (106, 29)] {
        assert_eq!(
            h.source.push(record_at(id, "disk full", s)).wait(WAIT),
            Some(AckOutcome::Ack),
            "record {id}"
        );
    }

    // 101 opens [0, 10); 102 at 10 opens [10, 20) and 103, 104 drop; 105 at 20 opens
    // [20, 30) and 106 drops.
    assert_eq!(h.ids("out"), vec![101, 102, 105]);
    assert_eq!(h.counter(Metric::RecordsDropped, &DEDUPE_DROP), 3);
    h.finish();
}

#[test]
fn a_repeat_whose_ingestion_time_is_past_the_window_passes_even_while_the_key_is_alive() {
    let h = start(DEDUPE_BODY, 1);

    assert_eq!(
        h.source.push(record_at(101, "disk full", 0)).wait(WAIT),
        Some(AckOutcome::Ack)
    );
    // The key is still alive on the store's clock, but in ingestion time the window is over.
    assert_eq!(
        h.source.push(record_at(102, "disk full", 10)).wait(WAIT),
        Some(AckOutcome::Ack)
    );

    assert_eq!(h.ids("out"), vec![101, 102]);
    h.finish();
}

#[test]
fn with_on_state_error_pass_a_failing_store_forwards_the_record_and_counts_the_error() {
    for_each_worker_count(|workers| {
        let h = start(DEDUPE_BODY, workers);
        h.state.fail_all(true);

        let probe = h.source.push(record(101, "disk full"));

        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");
        assert_eq!(h.ids("out"), vec![101], "workers={workers}");
        let stage = [("tenant", "acme"), ("stage", "dedupe_body")];
        assert_eq!(
            h.counter(Metric::StateErrors, &stage),
            1,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(Metric::RecordsErrored, &stage),
            0,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(Metric::RecordsOut, &stage),
            1,
            "workers={workers}"
        );
        h.finish();
    });
}

#[test]
fn with_on_state_error_nak_a_failing_store_naks_the_record_and_counts_the_error() {
    for_each_worker_count(|workers| {
        let yaml = DEDUPE_BODY.replace("window: 10s", "window: 10s\n    on_state_error: nak");
        let h = start(&yaml, workers);
        h.state.fail_all(true);

        let probe = h.source.push(record(101, "disk full"));

        assert!(
            matches!(probe.wait(WAIT), Some(AckOutcome::Nak(_))),
            "workers={workers}"
        );
        assert!(h.ids("out").is_empty(), "workers={workers}");
        let stage = [("tenant", "acme"), ("stage", "dedupe_body")];
        assert_eq!(
            h.counter(Metric::StateErrors, &stage),
            1,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(Metric::RecordsErrored, &stage),
            1,
            "workers={workers}"
        );
        assert_eq!(h.counter(Metric::SourceNaks, &[("tenant", "acme")]), 1);
        h.finish();
    });
}

#[test]
fn state_keys_are_namespaced_by_pipeline_tenant_and_node() {
    let h = start(DEDUPE_BODY, 1);

    assert_eq!(
        h.source.push(record(101, "disk full")).wait(WAIT),
        Some(AckOutcome::Ack)
    );
    let other_tenant = Record::from_json(
        r#"{"id": 201, "body": "disk full", "resource": {"tenant.id": "globex"}}"#,
    )
    .expect("record parses");
    assert_eq!(
        h.source.push(other_tenant).wait(WAIT),
        Some(AckOutcome::Ack)
    );

    let keys = h.state.keys();
    assert_eq!(keys.len(), 2, "{keys:?}");
    assert!(
        keys.iter()
            .any(|k| k.starts_with("ingest:acme:dedupe_body:dedupe:")),
        "{keys:?}"
    );
    assert!(
        keys.iter()
            .any(|k| k.starts_with("ingest:globex:dedupe_body:dedupe:")),
        "{keys:?}"
    );
    assert_eq!(h.ids("out"), vec![101, 201], "tenants never share a window");
    h.finish();
}

#[test]
fn a_tenant_containing_the_separator_cannot_escape_its_segment() {
    let h = start(DEDUPE_BODY, 1);
    let tricky = Record::from_json(
        r#"{"id": 301, "body": "x", "resource": {"tenant.id": "acme:dedupe_body"}}"#,
    )
    .expect("record parses");

    assert_eq!(h.source.push(tricky).wait(WAIT), Some(AckOutcome::Ack));

    let keys = h.state.keys();
    assert!(
        keys[0].starts_with("ingest:acme%3Adedupe_body:dedupe_body:dedupe:"),
        "{keys:?}"
    );
    h.finish();
}

#[test]
fn every_store_operation_is_counted_and_timed_for_the_tenant_and_node() {
    let h = start(DEDUPE_BODY, 1);

    assert_eq!(
        h.source.push(record(101, "disk full")).wait(WAIT),
        Some(AckOutcome::Ack)
    );
    assert_eq!(
        h.source.push(record(102, "disk full")).wait(WAIT),
        Some(AckOutcome::Ack)
    );

    let stage = [("tenant", "acme"), ("stage", "dedupe_body")];
    assert_eq!(
        h.counter(Metric::StateOps, &stage),
        2,
        "one set_nx per record"
    );
    assert_eq!(h.samples(Metric::StateOpDuration, &stage).len(), 2);
    assert_eq!(h.counter(Metric::StateErrors, &stage), 0);
    h.finish();
}

#[test]
fn a_missing_key_field_counts_as_null_so_records_without_it_dedupe_together() {
    let yaml = DEDUPE_BODY.replace("key: [body]", "key: [attributes.host]");
    let h = start(&yaml, 1);

    assert_eq!(
        h.source.push(record(101, "a")).wait(WAIT),
        Some(AckOutcome::Ack)
    );
    assert_eq!(
        h.source.push(record(102, "b")).wait(WAIT),
        Some(AckOutcome::Ack)
    );

    assert_eq!(h.ids("out"), vec![101]);
    h.finish();
}

#[test]
fn the_key_is_the_combination_of_every_listed_field() {
    let yaml = DEDUPE_BODY.replace("key: [body]", "key: [body, resource.host]");
    let h = start(&yaml, 1);
    let on = |id: u64, host: &str| {
        Record::from_json(&format!(
            r#"{{"id": {id}, "body": "disk full", "resource": {{"tenant.id": "acme", "host": "{host}"}}}}"#
        ))
        .expect("record parses")
    };

    assert_eq!(
        h.source.push(on(1, "web-0")).wait(WAIT),
        Some(AckOutcome::Ack)
    );
    assert_eq!(
        h.source.push(on(2, "web-1")).wait(WAIT),
        Some(AckOutcome::Ack)
    );
    assert_eq!(
        h.source.push(on(3, "web-0")).wait(WAIT),
        Some(AckOutcome::Ack)
    );

    assert_eq!(h.ids("out"), vec![1, 2]);
    h.finish();
}
