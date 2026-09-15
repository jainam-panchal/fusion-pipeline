//! The `sample` node through the trait boundary: YAML config in, envelopes pushed through
//! the in-memory source, assertions on which records reached the in-memory sink, how each
//! ack handle settled, what the state store holds and what the recorder counted.

mod common;

use std::time::Duration;

use common::{WAIT, acme_record as record, for_each_worker_count, start};
use fusion_core::memory::{AckOutcome, AckProbe};
use fusion_core::metrics::Metric;
use fusion_core::record::Record;
use fusion_core::state::StateStore as _;

const RANDOM_TENTH: &str = r#"
name: ingest
nodes:
  - id: keep_some
    type: sample
    mode: random
    percent: 10
  - id: out
    type: sink.memory
"#;

/// The labels of a `sample` drop by the node `keep_some` for tenant `acme`.
const SAMPLE_DROP: [(&str, &str); 3] = [
    ("tenant", "acme"),
    ("stage", "keep_some"),
    ("reason", "sample"),
];
const STAGE: [(&str, &str); 2] = [("tenant", "acme"), ("stage", "keep_some")];

/// Every probe settled as `expected`.
fn assert_all(probes: &[AckProbe], expected: AckOutcome) {
    for (i, probe) in probes.iter().enumerate() {
        assert_eq!(probe.wait(WAIT), Some(expected), "record {i}");
    }
}

#[test]
fn random_at_ten_percent_keeps_between_nine_and_eleven_percent_of_100k_records() {
    let h = start(RANDOM_TENTH, 4);
    let probes: Vec<AckProbe> = (1..=100_000)
        .map(|id| h.source.push(record(id, "disk full")))
        .collect();
    assert_all(&probes, AckOutcome::Ack);

    let kept = h.ids("out").len() as u64;
    assert!((9_000..=11_000).contains(&kept), "kept {kept}");
    assert_eq!(h.counter(Metric::RecordsDropped, &SAMPLE_DROP), 100_000 - kept);
    assert_eq!(h.counter(Metric::StateOps, &STAGE), 0, "random needs no state");
    h.finish();
}

#[test]
fn random_gives_a_redelivered_record_the_same_verdict() {
    let h = start(RANDOM_TENTH, 1);
    let ids: Vec<u64> = (1..=200).collect();
    for &id in &ids {
        assert_eq!(h.source.push(record(id, "x")).wait(WAIT), Some(AckOutcome::Ack));
    }
    let first_pass = h.ids("out");
    assert!(!first_pass.is_empty() && first_pass.len() < ids.len(), "{first_pass:?}");

    for &id in &ids {
        assert_eq!(h.source.push(record(id, "x")).wait(WAIT), Some(AckOutcome::Ack));
    }

    let mut twice = first_pass.clone();
    twice.extend(first_pass.iter().copied());
    twice.sort_unstable();
    assert_eq!(h.ids("out"), twice, "every kept id kept again, every dropped id dropped again");
    h.finish();
}

const EVERY_TENTH: &str = r#"
name: ingest
nodes:
  - id: keep_some
    type: sample
    mode: every_nth
    n: 10
  - id: out
    type: sink.memory
"#;

#[test]
fn every_nth_at_ten_keeps_exactly_one_thousand_of_ten_thousand_records_across_four_workers() {
    let h = start(EVERY_TENTH, 4);
    let probes: Vec<AckProbe> = (1..=10_000)
        .map(|id| h.source.push(record(id, "disk full")))
        .collect();
    assert_all(&probes, AckOutcome::Ack);

    assert_eq!(h.ids("out").len(), 1_000);
    assert_eq!(h.counter(Metric::RecordsDropped, &SAMPLE_DROP), 9_000);
    h.finish();
}

#[test]
fn every_nth_keeps_the_first_record_of_each_n_so_a_small_tenant_still_gets_one_through() {
    let h = start(EVERY_TENTH, 1);
    for id in 1..=12 {
        assert_eq!(h.source.push(record(id, "x")).wait(WAIT), Some(AckOutcome::Ack));
    }

    assert_eq!(h.ids("out"), vec![1, 11]);
    h.finish();
}
