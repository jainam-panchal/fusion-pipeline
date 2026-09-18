//! The duration histograms through the trait boundary: one sample per stage run, one per
//! sink write, one per record settled.

mod common;

use fusion_core::memory::AckOutcome;
use fusion_core::metrics::HistogramMetric;
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

fn record(id: u64) -> Record {
    Record::from_json(&format!(
        r#"{{"id": {id}, "severity_text": "ERROR", "body": "disk full"}}"#
    ))
    .expect("record parses")
}

/// The transport time of a message published `nanos_ago` before now.
fn published_ago(nanos_ago: u64) -> u64 {
    fusion_core::meta::unix_nanos_now() - nanos_ago
}

#[test]
fn every_stage_run_leaves_one_non_negative_duration_sample() {
    let h = start(KEEP_ERRORS, 1);

    for id in 1..=3 {
        assert_eq!(
            h.push_at(record(id), published_ago(0)).wait(WAIT),
            Some(AckOutcome::Ack)
        );
    }

    let samples = h.samples(
        HistogramMetric::StageDuration,
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
        h.push_at(record(1), published_ago(0)).wait(WAIT),
        Some(AckOutcome::Ack)
    );

    assert_eq!(
        h.samples(
            HistogramMetric::SinkPublishDuration,
            &[("tenant", "acme"), ("stage", "out")]
        )
        .len(),
        1
    );
    h.finish();
}

#[test]
fn a_settled_record_leaves_one_end_to_end_sample_measured_from_its_ingestion_time() {
    let h = start(KEEP_ERRORS, 1);

    // Published a full second before the pipeline took it.
    assert_eq!(
        h.push_at(record(1), published_ago(1_000_000_000))
            .wait(WAIT),
        Some(AckOutcome::Ack)
    );

    let samples = h.samples(HistogramMetric::EndToEnd, &[("tenant", "acme")]);
    assert_eq!(samples.len(), 1);
    assert!(samples[0] >= 1.0, "{samples:?}");
    h.finish();
}

#[test]
fn a_record_whose_source_gives_no_time_leaves_no_end_to_end_sample() {
    let h = start(KEEP_ERRORS, 1);

    // The record says when it was observed; only a transport's time counts, and there is
    // none, so the ingestion time is the worker clock's and end to end is not measured.
    let mut observed = record(1);
    observed.0["observed_time_unix_nano"] = published_ago(1_000_000_000).into();
    assert_eq!(h.push(observed).wait(WAIT), Some(AckOutcome::Ack));

    assert!(
        h.samples(HistogramMetric::EndToEnd, &[("tenant", "acme")])
            .is_empty()
    );
    h.finish();
}

#[test]
fn a_nakked_record_leaves_no_end_to_end_sample() {
    let h = start(KEEP_ERRORS, 1);
    h.sinks.fail_writes_to("out");

    assert_eq!(
        h.push_at(record(1), published_ago(1_000_000_000))
            .wait(WAIT),
        Some(AckOutcome::Nak(None))
    );

    assert!(
        h.samples(HistogramMetric::EndToEnd, &[("tenant", "acme")])
            .is_empty()
    );
    h.finish();
}
