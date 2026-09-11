//! The dedupe acceptance criteria through the engine on a real Dragonfly (`DRAGONFLY_URL`):
//! the in-memory source and sink as in `dedupe.rs`, the store as in deploy. Needs
//! `deploy/compose.yaml` up; ignored by default.

mod common;

use std::sync::Arc;

use common::{DEDUPE_DROP, WAIT, acme_record as record, registry, start_with_state};
use fusion_core::memory::{AckOutcome, MemorySinks};
use fusion_core::metrics::Metric;
use fusion_core::record::Record;
use fusion_state::Dragonfly;

fn yaml(pipeline_name: &str) -> String {
    format!(
        "name: {pipeline_name}\nnodes:\n  - id: dedupe_body\n    type: dedupe\n    key: [body]\n    window: 10s\n  - id: out\n    type: sink.memory\n"
    )
}

/// A pipeline name no other run shares, so keys from one test run never see another's.
fn unique_name() -> String {
    format!("t{}", std::process::id())
}

#[test]
#[ignore = "needs Dragonfly at DRAGONFLY_URL"]
fn on_dragonfly_a_repeat_drops_a_redelivery_passes_and_workers_share_the_window() {
    let sinks = MemorySinks::new();
    let state = Arc::new(Dragonfly::from_env().expect("url parses"));
    let h = start_with_state(
        &yaml(&unique_name()),
        4,
        sinks.clone(),
        registry(&sinks),
        state,
    );

    assert_eq!(
        h.source.push(record(101, "disk full")).wait(WAIT),
        Some(AckOutcome::Ack)
    );
    assert_eq!(
        h.source.push(record(102, "disk full")).wait(WAIT),
        Some(AckOutcome::Ack),
        "a different record with the same key"
    );
    assert_eq!(
        h.source.push(record(101, "disk full")).wait(WAIT),
        Some(AckOutcome::Ack),
        "the first record again"
    );
    assert_eq!(
        h.source.push(record(103, "other")).wait(WAIT),
        Some(AckOutcome::Ack)
    );

    assert_eq!(h.ids("out"), vec![101, 101, 103]);
    let stage = [("tenant", "acme"), ("stage", "dedupe_body")];
    assert_eq!(h.counter(Metric::RecordsDropped, &DEDUPE_DROP), 1);
    assert_eq!(h.counter(Metric::StateOps, &stage), 4);
    assert_eq!(h.counter(Metric::StateErrors, &stage), 0);
    h.finish();
}

#[test]
#[ignore = "needs Dragonfly at DRAGONFLY_URL"]
fn on_dragonfly_a_repeat_after_the_window_passes_again() {
    let sinks = MemorySinks::new();
    let state = Arc::new(Dragonfly::from_env().expect("url parses"));
    let short = yaml(&unique_name()).replace("window: 10s", "window: 300ms");
    let h = start_with_state(&short, 1, sinks.clone(), registry(&sinks), state);
    let at = |id: u64, ms: u64| {
        Record::from_json(&format!(
            r#"{{"id": {id}, "body": "x", "observed_time_unix_nano": {}, "resource": {{"tenant.id": "acme"}}}}"#,
            ms * 1_000_000
        ))
        .expect("record parses")
    };

    assert_eq!(h.source.push(at(1, 0)).wait(WAIT), Some(AckOutcome::Ack));
    assert_eq!(h.source.push(at(2, 100)).wait(WAIT), Some(AckOutcome::Ack));
    std::thread::sleep(std::time::Duration::from_millis(500));
    assert_eq!(h.source.push(at(3, 600)).wait(WAIT), Some(AckOutcome::Ack));

    assert_eq!(h.ids("out"), vec![1, 3]);
    h.finish();
}
