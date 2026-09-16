//! The two closed sets the spec's Telemetry section fixes: the metric names and the drop
//! reasons that label `records_dropped_total`. Nothing else is observable below the trait
//! boundary; what the engine emits is tested through it in the pipeline crate.

use fusion_core::metrics::{EditCause, EditOp, Metric, MetricKind};
use fusion_core::stage::DropReason;

/// Spec, Telemetry: the drop reasons are exactly this set, in this spelling.
const SPEC_DROP_REASONS: [&str; 11] = [
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
    "edit_unapplied",
];

/// Spec, Telemetry: every metric the pipeline exports, in this spelling.
const SPEC_METRICS: [&str; 17] = [
    "records_in_total",
    "records_out_total",
    "records_dropped_total",
    "records_errored_total",
    "stage_duration_seconds",
    "state_ops_total",
    "state_op_duration_seconds",
    "state_errors_total",
    "lua_errors_total",
    "regex_nonmatch_total",
    "edit_unapplied_total",
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

/// Spec, Stages (`edit`): the `op` label is the op set, the `cause` label the two ways an
/// op is unapplied, in this spelling.
#[test]
fn edit_label_values_are_exactly_the_spec_sets() {
    let ops: Vec<&str> = EditOp::ALL.iter().map(|o| o.as_str()).collect();
    assert_eq!(ops, ["set", "rename", "copy", "hash", "delete"]);
    let causes: Vec<&str> = EditCause::ALL.iter().map(|c| c.as_str()).collect();
    assert_eq!(causes, ["absent", "type"]);
}
