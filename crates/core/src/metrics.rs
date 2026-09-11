//! The metric vocabulary and the recorder boundary.
//!
//! The spec's Telemetry section names every metric the pipeline exports; [`Metric`] is that
//! list as a closed set, so a name that is not in the spec cannot be emitted. The engine
//! emits through [`Metrics`], whose typed methods fix the label set of each metric, and a
//! [`Recorder`] is the seam an exporter implements: two methods, one per instrument kind.
//! [`InMemoryRecorder`] is the fake for tests, next to the in-memory source and sinks in
//! [`crate::memory`]; the OTLP recorder lives in the telemetry crate.
//!
//! Labels are `tenant` on everything, `stage` (the node id, or the reserved `source` for
//! decisions the engine takes before any node runs) where the spec gives one, `reason` on
//! `records_dropped_total` and `kind` on `lua_errors_total`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::record::Record;
use crate::stage::DropReason;

/// Every metric the pipeline exports. Closed set; the spelling is [`Metric::as_str`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Metric {
    /// `records_in_total{tenant, stage}`: records handed to a node.
    RecordsIn,
    /// `records_out_total{tenant, stage}`: records a node passed on, or a sink accepted.
    RecordsOut,
    /// `records_dropped_total{tenant, stage, reason}`: intentional drops. The spine metric.
    RecordsDropped,
    /// `records_errored_total{tenant, stage}`: stage errors and sink failures.
    RecordsErrored,
    /// `stage_duration_seconds{tenant, stage}`: one stage run.
    StageDuration,
    /// `state_ops_total{tenant, stage}`: state store operations. Emitted by stateful stages.
    StateOps,
    /// `state_op_duration_seconds{tenant, stage}`: one state store operation.
    StateOpDuration,
    /// `state_errors_total{tenant, stage}`: state store failures.
    StateErrors,
    /// `lua_errors_total{tenant, stage, kind}`: Lua stage errors by kind.
    LuaErrors,
    /// `source_naks_total{tenant}`: messages the engine negatively acknowledged.
    SourceNaks,
    /// `source_redeliveries_total{tenant}`: messages the source saw more than once.
    SourceRedeliveries,
    /// `dlq_total{tenant}`: messages sent to the dead-letter queue.
    Dlq,
    /// `sink_publish_duration_seconds{tenant, stage}`: one sink write, until durable acceptance.
    SinkPublishDuration,
    /// `sink_publish_errors_total{tenant, stage}`: sink writes without durable acceptance.
    SinkPublishErrors,
    /// `pipeline_end_to_end_seconds{tenant}`: observed time to settlement.
    EndToEnd,
}

/// Which instrument a metric is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    /// A monotonic count.
    Counter,
    /// A distribution of durations, in seconds.
    Histogram,
}

impl Metric {
    /// Every metric, for an exporter that creates its instruments up front.
    pub const ALL: [Self; 15] = [
        Self::RecordsIn,
        Self::RecordsOut,
        Self::RecordsDropped,
        Self::RecordsErrored,
        Self::StageDuration,
        Self::StateOps,
        Self::StateOpDuration,
        Self::StateErrors,
        Self::LuaErrors,
        Self::SourceNaks,
        Self::SourceRedeliveries,
        Self::Dlq,
        Self::SinkPublishDuration,
        Self::SinkPublishErrors,
        Self::EndToEnd,
    ];

    /// The exported name, as the spec spells it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RecordsIn => "records_in_total",
            Self::RecordsOut => "records_out_total",
            Self::RecordsDropped => "records_dropped_total",
            Self::RecordsErrored => "records_errored_total",
            Self::StageDuration => "stage_duration_seconds",
            Self::StateOps => "state_ops_total",
            Self::StateOpDuration => "state_op_duration_seconds",
            Self::StateErrors => "state_errors_total",
            Self::LuaErrors => "lua_errors_total",
            Self::SourceNaks => "source_naks_total",
            Self::SourceRedeliveries => "source_redeliveries_total",
            Self::Dlq => "dlq_total",
            Self::SinkPublishDuration => "sink_publish_duration_seconds",
            Self::SinkPublishErrors => "sink_publish_errors_total",
            Self::EndToEnd => "pipeline_end_to_end_seconds",
        }
    }

    /// Counter or histogram.
    #[must_use]
    pub const fn kind(self) -> MetricKind {
        match self {
            Self::RecordsIn
            | Self::RecordsOut
            | Self::RecordsDropped
            | Self::RecordsErrored
            | Self::StateOps
            | Self::StateErrors
            | Self::LuaErrors
            | Self::SourceNaks
            | Self::SourceRedeliveries
            | Self::Dlq
            | Self::SinkPublishErrors => MetricKind::Counter,
            Self::StageDuration
            | Self::StateOpDuration
            | Self::SinkPublishDuration
            | Self::EndToEnd => MetricKind::Histogram,
        }
    }
}

/// The labels of one measurement. `tenant` is always set; the rest as the metric requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Labels<'a> {
    tenant: &'a str,
    stage: Option<&'a str>,
    reason: Option<DropReason>,
    kind: Option<&'a str>,
}

impl<'a> Labels<'a> {
    /// Labels for a per-stage metric.
    #[must_use]
    pub const fn new(tenant: &'a str, stage: &'a str) -> Self {
        Self {
            tenant,
            stage: Some(stage),
            reason: None,
            kind: None,
        }
    }

    /// Labels for a metric that is only per tenant.
    #[must_use]
    pub const fn for_tenant(tenant: &'a str) -> Self {
        Self {
            tenant,
            stage: None,
            reason: None,
            kind: None,
        }
    }

    /// Add the `reason` label.
    #[must_use]
    pub const fn with_reason(mut self, reason: DropReason) -> Self {
        self.reason = Some(reason);
        self
    }

    /// Add the `kind` label.
    #[must_use]
    pub const fn with_kind(mut self, kind: &'a str) -> Self {
        self.kind = Some(kind);
        self
    }

    /// The set labels as `(name, value)` pairs, in a fixed order.
    pub fn pairs(&self) -> impl Iterator<Item = (&'static str, &'a str)> {
        [
            Some(("tenant", self.tenant)),
            self.stage.map(|s| ("stage", s)),
            self.reason.map(|r| ("reason", r.as_str())),
            self.kind.map(|k| ("kind", k)),
        ]
        .into_iter()
        .flatten()
    }
}

/// Where measurements go. Implemented by exporters; shared across worker threads.
pub trait Recorder: Send + Sync {
    /// Add `by` to a counter.
    fn count(&self, metric: Metric, labels: &Labels<'_>, by: u64);
    /// Record one histogram sample.
    fn observe(&self, metric: Metric, labels: &Labels<'_>, value: f64);
}

/// The engine's handle on a recorder. Each method is one spec metric with its label set.
#[derive(Clone)]
pub struct Metrics {
    recorder: Arc<dyn Recorder>,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}

impl Metrics {
    /// The `tenant` label of a record that carries no `resource.tenant.id`.
    pub const UNKNOWN_TENANT: &'static str = "unknown";

    /// Emit through `recorder`.
    pub fn new(recorder: impl Recorder + 'static) -> Self {
        Self {
            recorder: Arc::new(recorder),
        }
    }

    /// Emit nowhere.
    #[must_use]
    pub fn noop() -> Self {
        Self::new(Noop)
    }

    /// The `tenant` label for `record`: its `resource.tenant.id`, or
    /// [`Metrics::UNKNOWN_TENANT`].
    #[must_use]
    pub fn tenant_of(record: &Record) -> &str {
        record
            .resource
            .get("tenant.id")
            .and_then(|v| v.as_str())
            .unwrap_or(Self::UNKNOWN_TENANT)
    }

    /// `records_in_total`.
    pub fn records_in(&self, tenant: &str, stage: &str) {
        self.recorder
            .count(Metric::RecordsIn, &Labels::new(tenant, stage), 1);
    }

    /// `records_out_total`, by `count` records.
    pub fn records_out(&self, tenant: &str, stage: &str, count: u64) {
        self.recorder
            .count(Metric::RecordsOut, &Labels::new(tenant, stage), count);
    }

    /// `records_dropped_total`.
    pub fn dropped(&self, tenant: &str, stage: &str, reason: DropReason) {
        self.recorder.count(
            Metric::RecordsDropped,
            &Labels::new(tenant, stage).with_reason(reason),
            1,
        );
    }

    /// `records_errored_total`.
    pub fn errored(&self, tenant: &str, stage: &str) {
        self.recorder
            .count(Metric::RecordsErrored, &Labels::new(tenant, stage), 1);
    }

    /// `stage_duration_seconds`.
    pub fn stage_duration(&self, tenant: &str, stage: &str, elapsed: Duration) {
        self.recorder.observe(
            Metric::StageDuration,
            &Labels::new(tenant, stage),
            elapsed.as_secs_f64(),
        );
    }

    /// `sink_publish_duration_seconds`.
    pub fn sink_publish_duration(&self, tenant: &str, stage: &str, elapsed: Duration) {
        self.recorder.observe(
            Metric::SinkPublishDuration,
            &Labels::new(tenant, stage),
            elapsed.as_secs_f64(),
        );
    }

    /// `sink_publish_errors_total`.
    pub fn sink_publish_error(&self, tenant: &str, stage: &str) {
        self.recorder
            .count(Metric::SinkPublishErrors, &Labels::new(tenant, stage), 1);
    }

    /// `source_naks_total`.
    pub fn source_nak(&self, tenant: &str) {
        self.recorder
            .count(Metric::SourceNaks, &Labels::for_tenant(tenant), 1);
    }

    /// `source_redeliveries_total`.
    pub fn source_redelivery(&self, tenant: &str) {
        self.recorder
            .count(Metric::SourceRedeliveries, &Labels::for_tenant(tenant), 1);
    }

    /// `pipeline_end_to_end_seconds`.
    pub fn end_to_end(&self, tenant: &str, elapsed: Duration) {
        self.recorder.observe(
            Metric::EndToEnd,
            &Labels::for_tenant(tenant),
            elapsed.as_secs_f64(),
        );
    }
}

struct Noop;

impl Recorder for Noop {
    fn count(&self, _: Metric, _: &Labels<'_>, _: u64) {}
    fn observe(&self, _: Metric, _: &Labels<'_>, _: f64) {}
}

type Series = (Metric, Vec<(&'static str, String)>);

#[derive(Default)]
struct Observed {
    counters: BTreeMap<Series, u64>,
    samples: BTreeMap<Series, Vec<f64>>,
}

/// A [`Recorder`] that keeps every measurement for a test to read back.
#[derive(Clone, Default)]
pub struct InMemoryRecorder {
    observed: Arc<Mutex<Observed>>,
}

impl InMemoryRecorder {
    /// An empty recorder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The value of the counter `metric` under exactly `labels`, zero if never counted.
    #[must_use]
    pub fn counter(&self, metric: Metric, labels: &[(&str, &str)]) -> u64 {
        lock_unpoisoned(&self.observed)
            .counters
            .get(&series(metric, labels))
            .copied()
            .unwrap_or(0)
    }

    /// Every sample of the histogram `metric` under exactly `labels`, in recording order.
    #[must_use]
    pub fn samples(&self, metric: Metric, labels: &[(&str, &str)]) -> Vec<f64> {
        lock_unpoisoned(&self.observed)
            .samples
            .get(&series(metric, labels))
            .cloned()
            .unwrap_or_default()
    }
}

/// The lookup key: the metric plus its label pairs, `name` interned to the fixed label set.
fn series(metric: Metric, labels: &[(&str, &str)]) -> Series {
    let labels = labels
        .iter()
        .map(|(name, value)| (intern(name), (*value).to_owned()))
        .collect();
    (metric, labels)
}

fn intern(name: &str) -> &'static str {
    match name {
        "tenant" => "tenant",
        "stage" => "stage",
        "reason" => "reason",
        "kind" => "kind",
        other => panic!("`{other}` is not a label any metric carries"),
    }
}

impl Recorder for InMemoryRecorder {
    fn count(&self, metric: Metric, labels: &Labels<'_>, by: u64) {
        let key = (
            metric,
            labels.pairs().map(|(n, v)| (n, v.to_owned())).collect(),
        );
        *lock_unpoisoned(&self.observed)
            .counters
            .entry(key)
            .or_insert(0) += by;
    }

    fn observe(&self, metric: Metric, labels: &Labels<'_>, value: f64) {
        let key = (
            metric,
            labels.pairs().map(|(n, v)| (n, v.to_owned())).collect(),
        );
        lock_unpoisoned(&self.observed)
            .samples
            .entry(key)
            .or_default()
            .push(value);
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
