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
//! `records_dropped_total`, `kind` on `lua_errors_total`, and `op`, `field` and `cause` on
//! `edit_unapplied_total`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::closed_set::closed_set;
use crate::memory::lock_unpoisoned;
use crate::stage::DropReason;

closed_set! {
    /// Every metric the pipeline exports. Closed set: adding one is a spec amendment, and
    /// [`Metric::ALL`] lists them all; an exporter creates its instruments from it up front.
    /// The spelling is [`Metric::as_str`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum Metric {
        /// `records_in_total{tenant, stage}`: records handed to a node.
        RecordsIn = "records_in_total",
        /// `records_out_total{tenant, stage}`: records a node passed on, or a sink accepted.
        RecordsOut = "records_out_total",
        /// `records_dropped_total{tenant, stage, reason}`: intentional drops. The spine metric.
        RecordsDropped = "records_dropped_total",
        /// `records_errored_total{tenant, stage}`: stage errors and sink failures.
        RecordsErrored = "records_errored_total",
        /// `stage_duration_seconds{tenant, stage}`: one stage run.
        StageDuration = "stage_duration_seconds",
        /// `state_ops_total{tenant, stage}`: state store operations. Emitted by stateful stages.
        StateOps = "state_ops_total",
        /// `state_op_duration_seconds{tenant, stage}`: one state store operation.
        StateOpDuration = "state_op_duration_seconds",
        /// `state_errors_total{tenant, stage}`: state store failures.
        StateErrors = "state_errors_total",
        /// `lua_errors_total{tenant, stage, kind}`: Lua stage errors by kind.
        LuaErrors = "lua_errors_total",
        /// `regex_nonmatch_total{tenant, stage, engine}`: records a regex stage's pattern did not
        /// match, passed on unchanged. Emitted by the regex stages.
        RegexNonmatch = "regex_nonmatch_total",
        /// `edit_unapplied_total{tenant, stage, op, field, cause}`: `edit` ops that could not
        /// apply to a record. Emitted by the edit stage.
        EditUnapplied = "edit_unapplied_total",
        /// `source_naks_total{tenant}`: messages the engine negatively acknowledged.
        SourceNaks = "source_naks_total",
        /// `source_redeliveries_total{tenant}`: messages the source saw more than once.
        SourceRedeliveries = "source_redeliveries_total",
        /// `dlq_total{tenant}`: messages sent to the dead-letter queue.
        Dlq = "dlq_total",
        /// `sink_publish_duration_seconds{tenant, stage}`: one sink write, until durable
        /// acceptance.
        SinkPublishDuration = "sink_publish_duration_seconds",
        /// `sink_publish_errors_total{tenant, stage}`: sink writes without durable acceptance.
        SinkPublishErrors = "sink_publish_errors_total",
        /// `pipeline_end_to_end_seconds{tenant}`: ingestion time to settlement.
        EndToEnd = "pipeline_end_to_end_seconds",
    }
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
            | Self::RegexNonmatch
            | Self::EditUnapplied
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

/// The `engine` label: the facade's classification of a node's pattern. A closed set, so
/// the label value cannot drift from the two engines the facade is defined as; core does
/// not depend on the regex crate, so the stages crate maps the facade's engine onto it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineLabel {
    /// The Rust `regex` crate: linear time, cannot backtrack.
    Linear,
    /// PCRE2 under the runtime limits.
    Backtracking,
}

impl EngineLabel {
    /// The label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Linear => "linear",
            Self::Backtracking => "backtracking",
        }
    }
}

impl std::fmt::Display for EngineLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

closed_set! {
    /// The `op` label of `edit_unapplied_total`: the kind of an `edit` op. Closed set, so the
    /// label cannot drift from the five ops the spec defines; the stages crate maps its own op
    /// onto it. [`EditOp::ALL`] lists them in the spec's order.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum EditOp {
        /// `set {field, value}`.
        Set = "set",
        /// `rename {from, to}`.
        Rename = "rename",
        /// `copy {from, to}`.
        Copy = "copy",
        /// `hash {field}`.
        Hash = "hash",
        /// `delete {fields}`.
        Delete = "delete",
    }
}

closed_set! {
    /// The `cause` label of `edit_unapplied_total`: why an `edit` op could not apply. Closed
    /// set; [`EditCause::ALL`] lists both.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum EditCause {
        /// The source field read as null: absent, or JSON `null`.
        Absent = "absent",
        /// The target refused the value: a composite into a map key, a string into a typed
        /// field.
        Type = "type",
    }
}

closed_set! {
    /// The `kind` label of `lua_errors_total`: what stopped a run of a Lua script. Closed set;
    /// [`LuaErrorKind::ALL`] lists them all. Load-time failures (a script that does not parse,
    /// defines no `process` or names a forbidden global) reject the config and never count.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum LuaErrorKind {
        /// The instruction budget tripped.
        Instructions = "instructions",
        /// The memory cap tripped.
        Memory = "memory",
        /// The script raised, or indexed something it should not have.
        Runtime = "runtime",
        /// The returned record was refused: a mistyped field, a key that is not a record
        /// field, an oversized output, or a table that is neither a record nor a list.
        Output = "output",
    }
}

/// The labels of one measurement. `tenant` is always set; the rest as the metric requires.
/// `engine` is set on every per-node metric of a node whose stage runs a regex, and on no
/// other node, so `sum by (stage)` is unchanged and a regex node can be split by engine.
/// The three `edit` labels are set together or not at all, through [`Labels::with_edit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Labels<'a> {
    tenant: &'a str,
    stage: Option<&'a str>,
    engine: Option<EngineLabel>,
    reason: Option<DropReason>,
    kind: Option<&'a str>,
    edit: Option<EditLabels<'a>>,
}

/// The three labels of `edit_unapplied_total`, set together through [`Labels::with_edit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EditLabels<'a> {
    op: EditOp,
    field: &'a str,
    cause: EditCause,
}

impl<'a> Labels<'a> {
    /// Labels for a per-stage metric.
    #[must_use]
    pub const fn new(tenant: &'a str, stage: &'a str) -> Self {
        Self {
            tenant,
            stage: Some(stage),
            engine: None,
            reason: None,
            kind: None,
            edit: None,
        }
    }

    /// Labels for a metric that is only per tenant.
    #[must_use]
    pub const fn for_tenant(tenant: &'a str) -> Self {
        Self {
            tenant,
            stage: None,
            engine: None,
            reason: None,
            kind: None,
            edit: None,
        }
    }

    /// The `stage` label, if set.
    #[must_use]
    pub const fn stage(&self) -> Option<&'a str> {
        self.stage
    }

    /// Add the `engine` label, or leave it off for `None`.
    #[must_use]
    pub const fn with_engine(mut self, engine: Option<EngineLabel>) -> Self {
        self.engine = engine;
        self
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

    /// Add the `op`, `field` and `cause` labels of `edit_unapplied_total`, as one, so a
    /// series with an `op` and no `cause` cannot be built. `field` is the op's source path
    /// (for `set`, the field it writes), in canonical form.
    #[must_use]
    pub const fn with_edit(mut self, op: EditOp, field: &'a str, cause: EditCause) -> Self {
        self.edit = Some(EditLabels { op, field, cause });
        self
    }

    /// The set labels as `(name, value)` pairs, in a fixed order.
    pub fn pairs(&self) -> impl Iterator<Item = (&'static str, &'a str)> {
        [
            Some(("tenant", self.tenant)),
            self.stage.map(|s| ("stage", s)),
            self.engine.map(|e| ("engine", e.as_str())),
            self.reason.map(|r| ("reason", r.as_str())),
            self.kind.map(|k| ("kind", k)),
            self.edit.map(|e| ("op", e.op.as_str())),
            self.edit.map(|e| ("field", e.field)),
            self.edit.map(|e| ("cause", e.cause.as_str())),
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

    /// `records_in_total`. `labels` is the node's [`Labels::new`], with the engine label
    /// when its stage declares one.
    pub fn records_in(&self, labels: &Labels<'_>) {
        self.recorder.count(Metric::RecordsIn, labels, 1);
    }

    /// `records_out_total`, by `count` records.
    pub fn records_out(&self, labels: &Labels<'_>, count: u64) {
        self.recorder.count(Metric::RecordsOut, labels, count);
    }

    /// `records_dropped_total`.
    pub fn dropped(&self, labels: &Labels<'_>, reason: DropReason) {
        self.recorder
            .count(Metric::RecordsDropped, &labels.with_reason(reason), 1);
    }

    /// `records_errored_total`.
    pub fn errored(&self, labels: &Labels<'_>) {
        self.recorder.count(Metric::RecordsErrored, labels, 1);
    }

    /// `stage_duration_seconds`.
    pub fn stage_duration(&self, labels: &Labels<'_>, elapsed: Duration) {
        self.recorder
            .observe(Metric::StageDuration, labels, elapsed.as_secs_f64());
    }

    /// `sink_publish_duration_seconds`.
    pub fn sink_publish_duration(&self, labels: &Labels<'_>, elapsed: Duration) {
        self.recorder
            .observe(Metric::SinkPublishDuration, labels, elapsed.as_secs_f64());
    }

    /// `sink_publish_errors_total`.
    pub fn sink_publish_error(&self, labels: &Labels<'_>) {
        self.recorder.count(Metric::SinkPublishErrors, labels, 1);
    }

    /// `state_ops_total` and `state_op_duration_seconds`: one state store operation.
    pub fn state_op(&self, labels: &Labels<'_>, elapsed: Duration) {
        self.recorder.count(Metric::StateOps, labels, 1);
        self.recorder
            .observe(Metric::StateOpDuration, labels, elapsed.as_secs_f64());
    }

    /// `state_errors_total`.
    pub fn state_error(&self, labels: &Labels<'_>) {
        self.recorder.count(Metric::StateErrors, labels, 1);
    }

    /// `regex_nonmatch_total`.
    pub fn regex_nonmatch(&self, labels: &Labels<'_>) {
        self.recorder.count(Metric::RegexNonmatch, labels, 1);
    }

    /// `edit_unapplied_total`. `labels` carries the node's labels plus [`Labels::with_edit`].
    pub fn edit_unapplied(&self, labels: &Labels<'_>) {
        self.recorder.count(Metric::EditUnapplied, labels, 1);
    }

    /// `lua_errors_total`. `labels` carries the node's labels plus [`Labels::with_kind`].
    pub fn lua_error(&self, labels: &Labels<'_>) {
        self.recorder.count(Metric::LuaErrors, labels, 1);
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
        "engine" => "engine",
        "reason" => "reason",
        "kind" => "kind",
        "op" => "op",
        "field" => "field",
        "cause" => "cause",
        other => panic!("`{other}` is not a label any metric carries"),
    }
}

/// The key a measurement is stored under: the metric plus its labels, owned.
fn key(metric: Metric, labels: &Labels<'_>) -> Series {
    (
        metric,
        labels.pairs().map(|(n, v)| (n, v.to_owned())).collect(),
    )
}

impl Recorder for InMemoryRecorder {
    fn count(&self, metric: Metric, labels: &Labels<'_>, by: u64) {
        *lock_unpoisoned(&self.observed)
            .counters
            .entry(key(metric, labels))
            .or_insert(0) += by;
    }

    fn observe(&self, metric: Metric, labels: &Labels<'_>, value: f64) {
        lock_unpoisoned(&self.observed)
            .samples
            .entry(key(metric, labels))
            .or_default()
            .push(value);
    }
}
