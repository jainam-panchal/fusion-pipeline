//! The closed sets the spec fixes: the metric names, the drop reasons that label
//! `records_dropped_total`, and the `edit`, `lua` and `engine` label values. Each set's `ALL` is
//! complete by construction (`closed_set!`), so these pin its names to the spec. Nothing else
//! is observable below the trait boundary; what the engine emits is tested through it in the
//! pipeline crate.

use fusion_core::metrics::{
    CounterMetric, EditCause, EditOp, EngineLabel, HistogramMetric, LuaErrorKind, Metric,
};
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
const SPEC_METRICS: [&str; 18] = [
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
    "source_invalid_headers_total",
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
    for counter in CounterMetric::ALL {
        assert!(counter.as_str().ends_with("_total"), "{counter}");
    }
    for histogram in HistogramMetric::ALL {
        assert!(histogram.as_str().ends_with("_seconds"), "{histogram}");
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

#[test]
fn lua_error_kinds_are_exactly_the_spec_set() {
    let kinds: Vec<&str> = LuaErrorKind::ALL.iter().map(|k| k.as_str()).collect();
    assert_eq!(kinds, ["instructions", "memory", "runtime", "output"]);
}

/// Spec, regex facade: the `engine` label is `linear` or `backtracking`.
#[test]
fn engine_label_values_are_exactly_the_spec_set() {
    let engines: Vec<&str> = EngineLabel::ALL.iter().map(|e| e.as_str()).collect();
    assert_eq!(engines, ["linear", "backtracking"]);
    assert_eq!(EngineLabel::Backtracking.to_string(), "backtracking");
}

/// The counters and the histograms together are every metric, each once, under the same
/// name.
#[test]
fn the_typed_subsets_partition_the_metrics_by_instrument() {
    let mut split: Vec<Metric> = CounterMetric::ALL
        .iter()
        .map(|c| c.metric())
        .chain(HistogramMetric::ALL.iter().map(|h| h.metric()))
        .collect();
    let mut all = Metric::ALL.to_vec();
    split.sort_unstable();
    all.sort_unstable();
    assert_eq!(split, all);
    for counter in CounterMetric::ALL {
        assert_eq!(counter.as_str(), counter.metric().as_str());
    }
    for histogram in HistogramMetric::ALL {
        assert_eq!(histogram.as_str(), histogram.metric().as_str());
    }
}
