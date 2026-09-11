//! The duration histograms through the trait boundary: one sample per stage run, one per
//! sink write, one per record settled.

mod common;

use fusion_core::memory::AckOutcome;
use fusion_core::metrics::Metric;
use fusion_core::record::Record;

use common::{WAIT, start};

const KEEP_ERRORS: &str = r#"
nodes:
  - id: keep_errors
    type: filter
    condition: severity_text == "ERROR"
    action: keep
  - id: out
    type: sink.memory
"#;

fn record(id: u64, observed_nanos_ago: u64) -> Record {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock is after the epoch")
        .as_nanos() as u64;
    Record::from_json(&format!(
        r#"{{"id": {id}, "severity_text": "ERROR", "body": "disk full",
             "observed_time_unix_nano": {},
             "resource": {{"tenant.id": "acme"}}}}"#,
        now - observed_nanos_ago
    ))
    .expect("record parses")
}

#[test]
fn every_stage_run_leaves_one_non_negative_duration_sample() {
    let h = start(KEEP_ERRORS, 1);

    for id in 1..=3 {
        assert_eq!(
            h.source.push(record(id, 0)).wait(WAIT),
            Some(AckOutcome::Ack)
        );
    }

    let samples = h.samples(
        Metric::StageDuration,
        &[("tenant", "acme"), ("stage", "keep_errors")],
    );
    assert_eq!(samples.len(), 3);
    assert!(samples.iter().all(|s| *s >= 0.0), "{samples:?}");
    h.finish();
}

#[test]
fn every_sink_write_leaves_one_publish_duration_sample() {
    let h = start(KEEP_ERRORS, 1);

    assert_eq!(
        h.source.push(record(1, 0)).wait(WAIT),
        Some(AckOutcome::Ack)
    );

    assert_eq!(
        h.samples(
            Metric::SinkPublishDuration,
            &[("tenant", "acme"), ("stage", "out")]
        )
        .len(),
        1
    );
    h.finish();
}

#[test]
fn a_settled_record_leaves_one_end_to_end_sample_measured_from_its_observed_time() {
    let h = start(KEEP_ERRORS, 1);

    // Observed a full second before it entered the pipeline.
    assert_eq!(
        h.source.push(record(1, 1_000_000_000)).wait(WAIT),
        Some(AckOutcome::Ack)
    );

    let samples = h.samples(Metric::EndToEnd, &[("tenant", "acme")]);
    assert_eq!(samples.len(), 1);
    assert!(samples[0] >= 1.0, "{samples:?}");
    h.finish();
}

#[test]
fn a_record_without_an_observed_time_leaves_no_end_to_end_sample() {
    let h = start(KEEP_ERRORS, 1);

    let bare = Record::from_json(
        r#"{"id": 1, "severity_text": "ERROR", "body": "x", "resource": {"tenant.id": "acme"}}"#,
    )
    .expect("record parses");
    assert_eq!(h.source.push(bare).wait(WAIT), Some(AckOutcome::Ack));

    assert!(
        h.samples(Metric::EndToEnd, &[("tenant", "acme")])
            .is_empty()
    );
    h.finish();
}
