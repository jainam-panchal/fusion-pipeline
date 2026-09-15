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
