//! The `dedupe` node through the trait boundary: YAML config in, envelopes pushed through
//! the in-memory source, assertions on which records reached the in-memory sink, how each
//! ack handle settled, what the state store holds and what the recorder counted.

mod common;

use std::time::Duration;

use fusion_core::memory::AckOutcome;
use fusion_core::metrics::{CounterMetric, HistogramMetric};
use fusion_core::record::Record;
use fusion_core::state::StateStore as _;
use fusion_stages::dedupe as dedupe_stage;

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
            h.counter(CounterMetric::RecordsDropped, &DEDUPE_DROP),
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
            h.counter(CounterMetric::RecordsDropped, &DEDUPE_DROP),
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
    assert_eq!(h.counter(CounterMetric::RecordsDropped, &DEDUPE_DROP), 1);
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
    assert_eq!(h.counter(CounterMetric::RecordsDropped, &DEDUPE_DROP), 3);
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

/// The value the stage stores for a record ingested at `observed_s` seconds.
fn holder(id: u64, observed_s: u64) -> Vec<u8> {
    dedupe_stage::holder_value(id, observed_s * 1_000_000_000)
}

/// The holder of the one dedupe state key in the store.
fn stored_holder(h: &common::Harness) -> Vec<u8> {
    let keys = h.state.keys();
    assert_eq!(keys.len(), 1, "{keys:?}");
    h.state
        .get(&keys[0])
        .expect("store answers")
        .expect("key is live")
}

/// Two workers each hold a record past 101's window with the same content. Both read 101
/// as the holder; worker B takes the key over with 102 first; worker A, with 103, finds 102
/// there, is inside its window, and drops. Exactly one of the pair passes.
#[test]
fn two_records_past_the_window_racing_on_two_workers_pass_exactly_once() {
    for_each_worker_count(|workers| {
        let h = start(DEDUPE_BODY, workers);
        assert_eq!(
            h.source.push(record_at(101, "disk full", 0)).wait(WAIT),
            Some(AckOutcome::Ack)
        );
        h.state.after_next_holder_reply(|data, key| {
            data.set(key, &holder(102, 10), Duration::from_secs(10))
                .expect("worker B takes the key over");
        });

        assert_eq!(
            h.source.push(record_at(103, "disk full", 11)).wait(WAIT),
            Some(AckOutcome::Ack)
        );

        assert_eq!(h.ids("out"), vec![101], "103 is a repeat of 102");
        assert_eq!(h.counter(CounterMetric::RecordsDropped, &DEDUPE_DROP), 1);
        assert_eq!(stored_holder(&h), holder(102, 10), "102 keeps the key");
        h.finish();
    });
}

/// A slow worker read 101 as the holder for its record 102 (ingested at 10 s); by the time
/// it takes over, 105 (ingested at 20 s) holds the key. 102 is older than 105, so it
/// passes, and the key must stay with 105: a repeat at 25 s is inside 105's window and
/// drops. A plain write would have moved the window back to 10 s and let it through.
#[test]
fn a_stale_takeover_against_a_newer_holder_does_not_move_the_window_back() {
    for_each_worker_count(|workers| {
        let h = start(DEDUPE_BODY, workers);
        assert_eq!(
            h.source.push(record_at(101, "disk full", 0)).wait(WAIT),
            Some(AckOutcome::Ack)
        );
        h.state.after_next_holder_reply(|data, key| {
            data.set(key, &holder(105, 20), Duration::from_secs(10))
                .expect("a faster worker took the key over with a newer record");
        });

        assert_eq!(
            h.source.push(record_at(102, "disk full", 10)).wait(WAIT),
            Some(AckOutcome::Ack),
            "102 is older than the holder 105: an extra copy, never a loss"
        );
        assert_eq!(
            h.source.push(record_at(106, "disk full", 25)).wait(WAIT),
            Some(AckOutcome::Ack),
            "106 is inside 105's window"
        );

        assert_eq!(h.ids("out"), vec![101, 102]);
        assert_eq!(h.counter(CounterMetric::RecordsDropped, &DEDUPE_DROP), 1);
        assert_eq!(
            stored_holder(&h),
            holder(105, 20),
            "the window stayed at 20 s"
        );
        h.finish();
    });
}

/// The key expired on the store's clock between the claim and the takeover. Nothing holds
/// it, so the takeover claims it and the record passes.
#[test]
fn a_takeover_of_a_key_that_expired_since_the_claim_still_claims_it() {
    for_each_worker_count(|workers| {
        let h = start(DEDUPE_BODY, workers);
        assert_eq!(
            h.source.push(record_at(101, "disk full", 0)).wait(WAIT),
            Some(AckOutcome::Ack)
        );
        h.state
            .after_next_holder_reply(|data, _| data.advance(Duration::from_secs(10)));

        assert_eq!(
            h.source.push(record_at(102, "disk full", 10)).wait(WAIT),
            Some(AckOutcome::Ack)
        );
        assert_eq!(
            h.source.push(record_at(103, "disk full", 11)).wait(WAIT),
            Some(AckOutcome::Ack),
            "inside 102's window"
        );

        assert_eq!(h.ids("out"), vec![101, 102]);
        assert_eq!(h.counter(CounterMetric::RecordsDropped, &DEDUPE_DROP), 1);
        h.finish();
    });
}

/// The holder moved between the claim and the takeover, but to a record that is itself a
/// full window older than this one (an old record claimed the expired key during a
/// replay). The first takeover is refused; the verdict against the new holder is
/// `WindowOver` again, so the stage tries once more against it and wins. The burst behind
/// then dedupes against this record, not the old one.
#[test]
fn a_takeover_refused_by_a_holder_that_is_also_past_the_window_is_retried_once() {
    for_each_worker_count(|workers| {
        let h = start(DEDUPE_BODY, workers);
        assert_eq!(
            h.source.push(record_at(101, "disk full", 0)).wait(WAIT),
            Some(AckOutcome::Ack)
        );
        h.state.after_next_holder_reply(|data, key| {
            data.set(key, &holder(99, 0), Duration::from_secs(10))
                .expect("an old record claimed the key meanwhile");
        });

        assert_eq!(
            h.source.push(record_at(102, "disk full", 10)).wait(WAIT),
            Some(AckOutcome::Ack)
        );
        assert_eq!(
            h.source.push(record_at(103, "disk full", 11)).wait(WAIT),
            Some(AckOutcome::Ack),
            "inside 102's window"
        );

        assert_eq!(h.ids("out"), vec![101, 102], "workers={workers}");
        assert_eq!(h.counter(CounterMetric::RecordsDropped, &DEDUPE_DROP), 1);
        assert_eq!(stored_holder(&h), holder(102, 10), "102 took the key over");
        h.finish();
    });
}

/// Refused twice, the stage stops: the record passes without holding the key, an extra
/// copy rather than a loop another worker's writes could keep alive. The next record with
/// this content takes the key over from what it then reads.
#[test]
fn a_takeover_refused_twice_passes_without_a_third_attempt() {
    for_each_worker_count(|workers| {
        let h = start(DEDUPE_BODY, workers);
        assert_eq!(
            h.source.push(record_at(101, "disk full", 0)).wait(WAIT),
            Some(AckOutcome::Ack)
        );
        // After the claim is refused, and again after the first takeover is refused.
        h.state.after_next_holder_reply(|data, key| {
            data.set(key, &holder(99, 0), Duration::from_secs(10))
                .expect("writes");
        });
        h.state.after_next_holder_reply(|data, key| {
            data.set(key, &holder(98, 0), Duration::from_secs(10))
                .expect("writes");
        });

        assert_eq!(
            h.source.push(record_at(102, "disk full", 10)).wait(WAIT),
            Some(AckOutcome::Ack)
        );

        assert_eq!(h.ids("out"), vec![101, 102], "workers={workers}");
        assert_eq!(stored_holder(&h), holder(98, 0), "102 gave up the takeover");
        let stage = [("tenant", "acme"), ("stage", "dedupe_body")];
        assert_eq!(
            h.counter(CounterMetric::StateOps, &stage),
            1 + 3,
            "101's claim, then 102's claim and two takeovers"
        );
        h.finish();
    });
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
            h.counter(CounterMetric::StateErrors, &stage),
            1,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(CounterMetric::RecordsErrored, &stage),
            0,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(CounterMetric::RecordsOut, &stage),
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
            h.counter(CounterMetric::StateErrors, &stage),
            1,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(CounterMetric::RecordsErrored, &stage),
            1,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(CounterMetric::SourceNaks, &[("tenant", "acme")]),
            1
        );
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
        h.counter(CounterMetric::StateOps, &stage),
        2,
        "one set_nx per record"
    );
    assert_eq!(h.samples(HistogramMetric::StateOpDuration, &stage).len(), 2);
    assert_eq!(h.counter(CounterMetric::StateErrors, &stage), 0);
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

/// A stage that reaches the state store without declaring `uses_state`. The handle refuses
/// it naming the node, the policy applies, and the store metrics stay untouched, since no
/// store was ever contacted.
mod undeclared {
    use fusion_core::config::{ConfigError, NodeConfig};
    use fusion_core::record::Record;
    use fusion_core::stage::{Context, Stage, StageOutput};
    use fusion_core::state::StateErrorPolicy;
    use std::time::Duration;

    pub struct Liar(pub StateErrorPolicy);

    impl Stage for Liar {
        fn process(&self, record: Record, ctx: &Context<'_>) -> StageOutput {
            match ctx.state.set_nx("k", b"v", Duration::from_secs(1)) {
                Ok(_) => StageOutput::Pass(record),
                Err(error) => StageOutput::StateError { record, error },
            }
        }
        // `uses_state` deliberately left at its default of `false`.
        fn on_state_error(&self) -> StateErrorPolicy {
            self.0
        }
    }

    pub fn build(
        policy: StateErrorPolicy,
    ) -> impl Fn(&NodeConfig) -> Result<Box<dyn Stage>, ConfigError> {
        move |_: &NodeConfig| Ok(Box::new(Liar(policy)) as Box<dyn Stage>)
    }
}

#[test]
fn a_stage_that_uses_state_without_declaring_it_gets_an_error_naming_the_node_and_no_store_metrics()
{
    use fusion_core::memory::MemorySinks;
    use fusion_core::state::StateErrorPolicy;

    let yaml = "nodes:\n  - id: liar\n    type: liar\n  - id: out\n    type: sink.memory\n";
    let sinks = MemorySinks::new();
    let mut registry = common::registry(&sinks);
    registry.register_stage("liar", undeclared::build(StateErrorPolicy::Nak));
    let h = common::start_with(yaml, 1, sinks.clone(), registry);

    let outcome = h.source.push(record(1, "x")).wait(WAIT);

    assert!(matches!(outcome, Some(AckOutcome::Nak(_))), "{outcome:?}");
    let stage = [("tenant", "acme"), ("stage", "liar")];
    assert_eq!(
        h.counter(CounterMetric::StateOps, &stage),
        0,
        "no store was contacted"
    );
    assert_eq!(
        h.counter(CounterMetric::StateErrors, &stage),
        0,
        "not a store error"
    );
    assert_eq!(
        h.counter(CounterMetric::RecordsErrored, &stage),
        1,
        "policy still applied"
    );
    h.finish();
}

#[test]
fn an_undeclared_stage_under_pass_forwards_the_record() {
    use fusion_core::memory::MemorySinks;
    use fusion_core::state::StateErrorPolicy;

    let yaml = "nodes:\n  - id: liar\n    type: liar\n  - id: out\n    type: sink.memory\n";
    let sinks = MemorySinks::new();
    let mut registry = common::registry(&sinks);
    registry.register_stage("liar", undeclared::build(StateErrorPolicy::Pass));
    let h = common::start_with(yaml, 1, sinks.clone(), registry);

    assert_eq!(
        h.source.push(record(1, "x")).wait(WAIT),
        Some(AckOutcome::Ack)
    );

    assert_eq!(h.ids("out"), vec![1]);
    assert_eq!(
        h.counter(
            CounterMetric::StateOps,
            &[("tenant", "acme"), ("stage", "liar")]
        ),
        0
    );
    h.finish();
}

const REWRITE_THEN_DEDUPE: &str = r#"
name: ingest
nodes:
  - id: restamp
    type: edit
    ops:
      - set: { field: observed_time_unix_nano, value: 5000000000 }
  - id: dedupe_body
    type: dedupe
    from: restamp
    key: [body]
    window: 10s
  - id: out
    type: sink.memory
    from: dedupe_body
"#;

#[test]
fn a_time_field_rewritten_upstream_does_not_move_the_window_of_the_records_ingestion_time() {
    for_each_worker_count(|workers| {
        let h = start(REWRITE_THEN_DEDUPE, workers);

        // Ingested 20 s apart, past the 10 s window, then stamped with one time by
        // `restamp`. The window is measured in ingestion time, so both pass; the third was
        // ingested 5 s after the second and is its repeat.
        for (id, ingested_s) in [(101, 1_000), (102, 1_020), (103, 1_025)] {
            let pushed = h.source.push(record_at(id, "disk full", ingested_s));
            assert_eq!(
                pushed.wait(WAIT),
                Some(AckOutcome::Ack),
                "workers={workers}"
            );
        }

        assert_eq!(h.ids("out"), vec![101, 102], "workers={workers}");
        let written: Vec<_> = h
            .sinks
            .records("out")
            .iter()
            .map(|r| r.observed_time_unix_nano)
            .collect();
        assert_eq!(
            written,
            vec![Some(5_000_000_000); 2],
            "the sink writes the payload"
        );
        h.finish();
    });
}

#[test]
fn the_window_follows_the_transports_time_not_the_producers_clock() {
    use fusion_core::meta::{Arrival, IngestionTime};

    for_each_worker_count(|workers| {
        let h = start(DEDUPE_BODY, workers);

        // The producer's clock says all three happened at the same instant; the transport
        // says they entered 20 s and then 5 s apart. The transport decides: the first two
        // are past the 10 s window, the third is the second's repeat.
        for (id, entered_s) in [(101, 1_000), (102, 1_020), (103, 1_025)] {
            let pushed = h.source.push_arrival(
                record_at(id, "disk full", 7),
                Arrival {
                    ingestion_time: Some(IngestionTime::Reported(entered_s * 1_000_000_000)),
                    ..Arrival::default()
                },
            );
            assert_eq!(
                pushed.wait(WAIT),
                Some(AckOutcome::Ack),
                "workers={workers}"
            );
        }

        assert_eq!(h.ids("out"), vec![101, 102], "workers={workers}");
        assert_eq!(stored_holder(&h), holder(102, 1_020), "workers={workers}");
        h.finish();
    });
}
