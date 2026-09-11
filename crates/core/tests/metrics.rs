//! The metric vocabulary, through the recorder boundary: the closed set of metric names the
//! spec's Telemetry section declares, the closed set of drop reasons that label
//! `records_dropped_total`, and what the in-memory recorder observes when either is emitted.

use fusion_core::metrics::{InMemoryRecorder, Labels, Metric, MetricKind, Metrics, Recorder};
use fusion_core::stage::DropReason;

/// Spec, Telemetry: the drop reasons are exactly this set, in this spelling.
const SPEC_DROP_REASONS: [&str; 10] = [
    "filter",
    "route_default_drop",
    "sample",
    "dedupe",
    "lua_drop",
    "lua_error",
    "regex_limit",
    "state_error",
    "invalid_record",
    "missing_id",
];

/// Spec, Telemetry: every metric the pipeline exports, in this spelling.
const SPEC_METRICS: [&str; 15] = [
    "records_in_total",
    "records_out_total",
    "records_dropped_total",
    "records_errored_total",
    "stage_duration_seconds",
    "state_ops_total",
    "state_op_duration_seconds",
    "state_errors_total",
    "lua_errors_total",
    "source_naks_total",
    "source_redeliveries_total",
    "dlq_total",
    "sink_publish_duration_seconds",
    "sink_publish_errors_total",
    "pipeline_end_to_end_seconds",
];

#[test]
fn drop_reason_labels_are_exactly_the_spec_closed_set() {
    let mut labels: Vec<&str> = DropReason::ALL.iter().map(|r| r.as_str()).collect();
    let mut spec = SPEC_DROP_REASONS.to_vec();
    labels.sort_unstable();
    spec.sort_unstable();
    assert_eq!(labels, spec);
}

#[test]
fn metric_names_are_exactly_the_spec_set() {
    let mut names: Vec<&str> = Metric::ALL.iter().map(|m| m.as_str()).collect();
    let mut spec = SPEC_METRICS.to_vec();
    names.sort_unstable();
    spec.sort_unstable();
    assert_eq!(names, spec);
}

#[test]
fn every_total_is_a_counter_and_every_seconds_is_a_histogram() {
    for metric in Metric::ALL {
        let expected = if metric.as_str().ends_with("_total") {
            MetricKind::Counter
        } else {
            MetricKind::Histogram
        };
        assert_eq!(metric.kind(), expected, "{}", metric.as_str());
    }
}

#[test]
fn a_counted_metric_is_observed_under_its_labels() {
    let recorder = InMemoryRecorder::new();
    let metrics = Metrics::new(recorder.clone());

    metrics.dropped("acme", "keep_errors", DropReason::Filter);
    metrics.dropped("acme", "keep_errors", DropReason::Filter);
    metrics.dropped("acme", "keep_errors", DropReason::Sample);

    assert_eq!(
        recorder.counter(
            Metric::RecordsDropped,
            &[
                ("tenant", "acme"),
                ("stage", "keep_errors"),
                ("reason", "filter")
            ],
        ),
        2
    );
    assert_eq!(
        recorder.counter(
            Metric::RecordsDropped,
            &[
                ("tenant", "acme"),
                ("stage", "keep_errors"),
                ("reason", "sample")
            ],
        ),
        1
    );
}

#[test]
fn an_observed_metric_keeps_every_sample_under_its_labels() {
    let recorder = InMemoryRecorder::new();

    recorder.observe(
        Metric::StageDuration,
        &Labels::new("acme", "keep_errors"),
        0.25,
    );
    recorder.observe(
        Metric::StageDuration,
        &Labels::new("acme", "keep_errors"),
        0.75,
    );

    assert_eq!(
        recorder.samples(
            Metric::StageDuration,
            &[("tenant", "acme"), ("stage", "keep_errors")]
        ),
        [0.25, 0.75]
    );
}

#[test]
fn a_record_without_a_tenant_is_counted_under_unknown() {
    let recorder = InMemoryRecorder::new();
    let metrics = Metrics::new(recorder.clone());

    metrics.records_in(Metrics::UNKNOWN_TENANT, "out");

    assert_eq!(
        recorder.counter(
            Metric::RecordsIn,
            &[("tenant", "unknown"), ("stage", "out")]
        ),
        1
    );
}
