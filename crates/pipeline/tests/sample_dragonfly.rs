//! The `every_nth` acceptance criteria through the engine on a real Dragonfly
//! (`DRAGONFLY_URL`): the in-memory source and sink as in `sample.rs`, the store as in
//! deploy. Needs `deploy/compose.yaml` up and is ignored by default.

mod common;

use std::sync::Arc;

use common::{WAIT, acme_record as record, registry, start_with_state};
use fusion_core::memory::{AckOutcome, AckProbe, MemorySinks};
use fusion_core::metrics::Metric;
use fusion_state::Dragonfly;

/// A pipeline name no other run shares, so counts from one test run never see another's.
fn yaml() -> String {
    format!(
        "name: t{}\nnodes:\n  - id: keep_some\n    type: sample\n    mode: every_nth\n    n: 10\n  - id: out\n    type: sink.memory\n",
        std::process::id()
    )
}

#[test]
#[ignore = "needs Dragonfly at DRAGONFLY_URL"]
fn on_dragonfly_every_nth_keeps_one_thousand_of_ten_thousand_across_four_workers_and_counts_a_redelivery_again()
 {
    let sinks = MemorySinks::new();
    let state = Arc::new(Dragonfly::from_env().expect("url parses"));
    let h = start_with_state(&yaml(), 4, sinks.clone(), registry(&sinks), state);

    let probes: Vec<AckProbe> = (1..=10_000)
        .map(|id| h.source.push(record(id, "x")))
        .collect();
    for probe in &probes {
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    }
    assert_eq!(h.ids("out").len(), 1_000);

    // A redelivery is a new delivery: count 10001 for record 1, the first of the next ten.
    assert_eq!(
        h.source.push(record(1, "x")).wait(WAIT),
        Some(AckOutcome::Ack)
    );
    assert_eq!(h.ids("out").len(), 1_001, "count 10001 is kept");

    let stage = [("tenant", "acme"), ("stage", "keep_some")];
    let drop = [
        ("tenant", "acme"),
        ("stage", "keep_some"),
        ("reason", "sample"),
    ];
    assert_eq!(h.counter(Metric::RecordsDropped, &drop), 9_000);
    assert_eq!(
        h.counter(Metric::StateOps, &stage),
        10_001,
        "one incr per delivery"
    );
    assert_eq!(h.counter(Metric::StateErrors, &stage), 0);
    h.finish();
}
