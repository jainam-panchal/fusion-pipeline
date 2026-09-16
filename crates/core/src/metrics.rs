//! The metric vocabulary and the recorder boundary.
//!
//! The spec's Telemetry section names every metric the pipeline exports; [`Metric`] is that
//! list as a closed set, so a name that is not in the spec cannot be emitted. The engine
//! emits through [`Metrics`], whose typed methods fix the label set of each metric, and a
//! [`Recorder`] is the seam an exporter implements: two methods, one per instrument kind,
//! each taking only the metrics of its kind.
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

/// Declares [`Metric`] from one list in which every metric names its instrument, and from the
/// same list the two typed subsets, [`CounterMetric`] and [`HistogramMetric`]. All three are
/// closed sets, so none can miss a metric, and [`Recorder::count`] takes only a counter and
/// [`Recorder::observe`] only a histogram, so a metric recorded through the wrong instrument
/// does not compile.
macro_rules! metrics {
    ($($(#[doc = $doc:literal])* $kind:ident $variant:ident = $wire:literal,)+) => {
        closed_set! {
            /// Every metric the pipeline exports. Closed set: adding one is a spec amendment,
            /// and [`Metric::ALL`] lists them all. The spelling is [`Metric::as_str`].
            #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub enum Metric {
                $($(#[doc = $doc])* $variant = $wire,)+
            }
        }

        metrics!(@subset counter CounterMetric
            "The metrics that are counters, recorded with [`Recorder::count`]. An exporter \
             creates one counter per value of [`CounterMetric::ALL`]."
            [] $($(#[doc = $doc])* $kind $variant = $wire,)+);
        metrics!(@subset histogram HistogramMetric
            "The metrics that are histograms of durations in seconds, recorded with \
             [`Recorder::observe`]. An exporter creates one histogram per value of \
             [`HistogramMetric::ALL`]."
            [] $($(#[doc = $doc])* $kind $variant = $wire,)+);
    };
    // The subset `$name` of the metrics tagged `$want`, as a fold: `@subset` takes the next
    // metric and hands it to `@pick`, which sends it to `@keep` (joins the accumulator) when
    // its instrument is `$want` and otherwise drops it. Once the list is empty the
    // accumulator is the subset, declared as a closed set with each metric's own docs.
    (@subset $want:ident $name:ident $about:literal
        [$($(#[doc = $d:literal])* $v:ident = $w:literal,)*]) => {
        closed_set! {
            #[doc = $about]
            #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub enum $name {
                $($(#[doc = $d])* $v = $w,)*
            }
        }

        impl $name {
            /// The metric this value is.
            #[must_use]
            pub const fn metric(self) -> Metric {
                match self {
                    $(Self::$v => Metric::$v,)*
                }
            }
        }
    };
    (@subset $want:ident $name:ident $about:literal [$($acc:tt)*]
        $(#[doc = $d:literal])* $kind:ident $v:ident = $w:literal, $($rest:tt)*) => {
        metrics!(@pick $want $kind $name $about [$($acc)*] [$(#[doc = $d])* $v = $w,] $($rest)*);
    };
    (@pick counter counter $($t:tt)*) => { metrics!(@keep counter $($t)*); };
    (@pick histogram histogram $($t:tt)*) => { metrics!(@keep histogram $($t)*); };
    // Any other instrument: the metric in `[$($item)*]` is not in this subset and is dropped.
    (@pick $want:ident $other:ident $name:ident $about:literal [$($acc:tt)*] [$($item:tt)*]
        $($rest:tt)*) => {
        metrics!(@subset $want $name $about [$($acc)*] $($rest)*);
    };
    (@keep $want:ident $name:ident $about:literal [$($acc:tt)*] [$($item:tt)*] $($rest:tt)*) => {
        metrics!(@subset $want $name $about [$($acc)* $($item)*] $($rest)*);
    };
}

metrics! {
    /// `records_in_total{tenant, stage}`: records handed to a node.
    counter RecordsIn = "records_in_total",
    /// `records_out_total{tenant, stage}`: records a node passed on, or a sink accepted.
    counter RecordsOut = "records_out_total",
    /// `records_dropped_total{tenant, stage, reason}`: intentional drops. The spine metric.
    counter RecordsDropped = "records_dropped_total",
    /// `records_errored_total{tenant, stage}`: stage errors and sink failures.
    counter RecordsErrored = "records_errored_total",
    /// `stage_duration_seconds{tenant, stage}`: one stage run.
    histogram StageDuration = "stage_duration_seconds",
    /// `state_ops_total{tenant, stage}`: state store operations. Emitted by stateful stages.
    counter StateOps = "state_ops_total",
    /// `state_op_duration_seconds{tenant, stage}`: one state store operation.
    histogram StateOpDuration = "state_op_duration_seconds",
    /// `state_errors_total{tenant, stage}`: state store failures.
    counter StateErrors = "state_errors_total",
    /// `lua_errors_total{tenant, stage, kind}`: Lua stage errors by kind.
    counter LuaErrors = "lua_errors_total",
    /// `regex_nonmatch_total{tenant, stage, engine}`: records a regex stage's pattern did not
    /// match, passed on unchanged. Emitted by the regex stages.
    counter RegexNonmatch = "regex_nonmatch_total",
    /// `edit_unapplied_total{tenant, stage, op, field, cause}`: `edit` ops that could not
    /// apply to a record. Emitted by the edit stage.
    counter EditUnapplied = "edit_unapplied_total",
    /// `source_naks_total{tenant}`: messages the engine negatively acknowledged.
    counter SourceNaks = "source_naks_total",
    /// `source_redeliveries_total{tenant}`: messages the source saw more than once.
    counter SourceRedeliveries = "source_redeliveries_total",
    /// `source_invalid_headers_total{tenant}`: pipeline headers a source ignored because
    /// they did not parse.
    counter SourceInvalidHeaders = "source_invalid_headers_total",
    /// `dlq_total{tenant}`: messages sent to the dead-letter queue.
    counter Dlq = "dlq_total",
    /// `sink_publish_duration_seconds{tenant, stage}`: one sink write, until durable
    /// acceptance.
    histogram SinkPublishDuration = "sink_publish_duration_seconds",
    /// `sink_publish_errors_total{tenant, stage}`: sink writes without durable acceptance.
    counter SinkPublishErrors = "sink_publish_errors_total",
    /// `pipeline_end_to_end_seconds{tenant}`: ingestion time to settlement.
    histogram EndToEnd = "pipeline_end_to_end_seconds",
}

closed_set! {
    /// The `engine` label: the facade's classification of a node's pattern. A closed set, so
    /// the label value cannot drift from the two engines the facade is defined as; core does
    /// not depend on the regex crate, so the stages crate maps the facade's engine onto it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum EngineLabel {
        /// The Rust `regex` crate: linear time, cannot backtrack.
        Linear = "linear",
        /// PCRE2 under the runtime limits.
        Backtracking = "backtracking",
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

/// Where measurements go. Implemented by exporters; shared across worker threads. Each call
/// takes only metrics of its instrument:
///
/// ```
/// use fusion_core::metrics::{CounterMetric, HistogramMetric, InMemoryRecorder, Labels, Recorder};
///
/// let recorder = InMemoryRecorder::new();
/// let labels = Labels::new("acme", "keep_errors");
/// recorder.count(CounterMetric::RecordsIn, &labels, 1);
/// recorder.observe(HistogramMetric::StageDuration, &labels, 0.5);
/// ```
///
/// so a histogram cannot be counted:
///
/// ```compile_fail
/// use fusion_core::metrics::{HistogramMetric, InMemoryRecorder, Labels, Recorder};
///
/// let recorder = InMemoryRecorder::new();
/// let labels = Labels::new("acme", "keep_errors");
/// recorder.count(HistogramMetric::StageDuration, &labels, 1);
/// ```
///
/// and a counter cannot be observed:
///
/// ```compile_fail
/// use fusion_core::metrics::{CounterMetric, InMemoryRecorder, Labels, Recorder};
///
/// let recorder = InMemoryRecorder::new();
/// let labels = Labels::new("acme", "keep_errors");
/// recorder.observe(CounterMetric::RecordsIn, &labels, 0.5);
/// ```
pub trait Recorder: Send + Sync {
    /// Add `by` to a counter.
    fn count(&self, metric: CounterMetric, labels: &Labels<'_>, by: u64);
    /// Record one histogram sample, in seconds.
    fn observe(&self, metric: HistogramMetric, labels: &Labels<'_>, value: f64);
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
        self.recorder.count(CounterMetric::RecordsIn, labels, 1);
    }

    /// `records_out_total`, by `count` records.
    pub fn records_out(&self, labels: &Labels<'_>, count: u64) {
        self.recorder
            .count(CounterMetric::RecordsOut, labels, count);
    }

    /// `records_dropped_total`.
    pub fn dropped(&self, labels: &Labels<'_>, reason: DropReason) {
        self.recorder.count(
            CounterMetric::RecordsDropped,
            &labels.with_reason(reason),
            1,
        );
    }

    /// `records_errored_total`.
    pub fn errored(&self, labels: &Labels<'_>) {
        self.recorder
            .count(CounterMetric::RecordsErrored, labels, 1);
    }

    /// `stage_duration_seconds`.
    pub fn stage_duration(&self, labels: &Labels<'_>, elapsed: Duration) {
        self.recorder.observe(
            HistogramMetric::StageDuration,
            labels,
            elapsed.as_secs_f64(),
        );
    }

    /// `sink_publish_duration_seconds`.
    pub fn sink_publish_duration(&self, labels: &Labels<'_>, elapsed: Duration) {
        self.recorder.observe(
            HistogramMetric::SinkPublishDuration,
            labels,
            elapsed.as_secs_f64(),
        );
    }

    /// `sink_publish_errors_total`.
    pub fn sink_publish_error(&self, labels: &Labels<'_>) {
        self.recorder
            .count(CounterMetric::SinkPublishErrors, labels, 1);
    }

    /// `state_ops_total` and `state_op_duration_seconds`: one state store operation.
    pub fn state_op(&self, labels: &Labels<'_>, elapsed: Duration) {
        self.recorder.count(CounterMetric::StateOps, labels, 1);
        self.recorder.observe(
            HistogramMetric::StateOpDuration,
            labels,
            elapsed.as_secs_f64(),
        );
    }

    /// `state_errors_total`.
    pub fn state_error(&self, labels: &Labels<'_>) {
        self.recorder.count(CounterMetric::StateErrors, labels, 1);
    }

    /// `regex_nonmatch_total`.
    pub fn regex_nonmatch(&self, labels: &Labels<'_>) {
        self.recorder.count(CounterMetric::RegexNonmatch, labels, 1);
    }

    /// `edit_unapplied_total`. `labels` carries the node's labels plus [`Labels::with_edit`].
    pub fn edit_unapplied(&self, labels: &Labels<'_>) {
        self.recorder.count(CounterMetric::EditUnapplied, labels, 1);
    }

    /// `lua_errors_total`. `labels` carries the node's labels plus [`Labels::with_kind`].
    pub fn lua_error(&self, labels: &Labels<'_>) {
        self.recorder.count(CounterMetric::LuaErrors, labels, 1);
    }

    /// `source_naks_total`.
    pub fn source_nak(&self, tenant: &str) {
        self.recorder
            .count(CounterMetric::SourceNaks, &Labels::for_tenant(tenant), 1);
    }

    /// `source_redeliveries_total`.
    pub fn source_redelivery(&self, tenant: &str) {
        self.recorder.count(
            CounterMetric::SourceRedeliveries,
            &Labels::for_tenant(tenant),
            1,
        );
    }

    /// `source_invalid_headers_total`.
    pub fn source_invalid_header(&self, tenant: &str) {
        self.recorder.count(
            CounterMetric::SourceInvalidHeaders,
            &Labels::for_tenant(tenant),
            1,
        );
    }

    /// `pipeline_end_to_end_seconds`.
    pub fn end_to_end(&self, tenant: &str, elapsed: Duration) {
        self.recorder.observe(
            HistogramMetric::EndToEnd,
            &Labels::for_tenant(tenant),
            elapsed.as_secs_f64(),
        );
    }
}

struct Noop;

impl Recorder for Noop {
    fn count(&self, _: CounterMetric, _: &Labels<'_>, _: u64) {}
    fn observe(&self, _: HistogramMetric, _: &Labels<'_>, _: f64) {}
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
    pub fn counter(&self, metric: CounterMetric, labels: &[(&str, &str)]) -> u64 {
        lock_unpoisoned(&self.observed)
            .counters
            .get(&series(metric.metric(), labels))
            .copied()
            .unwrap_or(0)
    }

    /// Every sample of the histogram `metric` under exactly `labels`, in recording order.
    #[must_use]
    pub fn samples(&self, metric: HistogramMetric, labels: &[(&str, &str)]) -> Vec<f64> {
        lock_unpoisoned(&self.observed)
            .samples
            .get(&series(metric.metric(), labels))
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
    fn count(&self, metric: CounterMetric, labels: &Labels<'_>, by: u64) {
        *lock_unpoisoned(&self.observed)
            .counters
            .entry(key(metric.metric(), labels))
            .or_insert(0) += by;
    }

    fn observe(&self, metric: HistogramMetric, labels: &Labels<'_>, value: f64) {
        lock_unpoisoned(&self.observed)
            .samples
            .entry(key(metric.metric(), labels))
            .or_default()
            .push(value);
    }
}
