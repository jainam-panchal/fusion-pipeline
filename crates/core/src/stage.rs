//! The stage contract: one record in, one [`StageOutput`] out.
//!
//! A stateful stage reaches the state store through [`State`], the handle on its
//! [`Context`]. The handle prefixes every key with `{pipeline}:{tenant}:{node}:` so no stage
//! can share state across tenants or pipelines, and counts every operation on
//! `state_ops_total`, `state_op_duration_seconds` and `state_errors_total`.
//!
//! Only core builds a [`Context`]: each worker holds a stage environment and derives every
//! node's context from it, the record's [`Meta`] and the node, so the handle's tenant, the
//! metric labels and the `Meta` a stage reads are the same tenant by construction.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::closed_set::closed_set;
use crate::meta::Meta;
use crate::metrics::{EditCause, EditOp, EngineLabel, Labels, LuaErrorKind, Metrics};
use crate::record::Record;
use crate::state::{StateError, StateErrorPolicy, StateStore};

closed_set! {
    /// Why a record was intentionally dropped. Closed set: adding one is a spec amendment, and
    /// [`DropReason::ALL`] lists them all. It is the `reason` label on `records_dropped_total`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum DropReason {
        /// A `filter` node dropped it.
        Filter = "filter",
        /// A `route` node's default was `drop`.
        RouteDefaultDrop = "route_default_drop",
        /// A `sample` node did not select it.
        Sample = "sample",
        /// A `dedupe` node saw it before.
        Dedupe = "dedupe",
        /// A Lua script returned `nil`.
        LuaDrop = "lua_drop",
        /// A Lua script errored with `on_error: drop`.
        LuaError = "lua_error",
        /// A regex limit tripped.
        RegexLimit = "regex_limit",
        /// The state store failed and the node's policy is `drop`.
        StateError = "state_error",
        /// The record is malformed or of an unsupported kind.
        InvalidRecord = "invalid_record",
        /// The record carries no `id`.
        MissingId = "missing_id",
        /// An `edit` op could not apply and the node's `on_unapplied` is `drop`.
        EditUnapplied = "edit_unapplied",
    }
}

/// A stage failure. The engine negatively acknowledges the record's source message.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct StageError {
    message: String,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl StageError {
    /// A stage error with a message and no underlying cause.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    /// Attach the underlying cause.
    #[must_use]
    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }
}

/// What a stage did with a record.
#[derive(Debug)]
#[non_exhaustive]
pub enum StageOutput {
    /// Continue downstream with this record.
    Pass(Record),
    /// Stop here; the record counts as handled and is acknowledged.
    Drop(DropReason),
    /// Continue only to nodes subscribed to `label` on this node.
    Routed(String, Record),
    /// Continue downstream with each of these records.
    Split(Vec<Record>),
    /// The stage failed; the source message is negatively acknowledged.
    Error(StageError),
    /// The state store could not answer. The stage hands the record back unchanged and the
    /// engine applies the node's [`StateErrorPolicy`]: forward it, or fail it.
    StateError {
        /// The record, unchanged.
        record: Record,
        /// What the store said.
        error: StateError,
    },
}

/// A stage's handle on the state store for one record: every key it names is prefixed with
/// `{pipeline}:{tenant}:{node}:`, and every operation is timed and counted for that tenant
/// and node. Owned, so a stage that needs a `'static` handle (a Lua VM) can keep it.
#[derive(Clone)]
pub struct State {
    store: Arc<dyn StateStore>,
    metrics: Metrics,
    prefix: String,
    tenant: Arc<str>,
    node: String,
    /// The node's `engine` label, so its state-store series carry it like every other
    /// per-node metric.
    engine: Option<EngineLabel>,
    /// Whether the node's stage declared [`Stage::uses_state`]. When it did not, no
    /// connection was opened for it and every operation is refused before the store, and
    /// before the metrics, so the store counters stay about the store.
    declared: bool,
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("State")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl State {
    /// A handle for one record: `store` is the worker's connection, `pipeline`, `tenant` and
    /// `node` form the key prefix, `metrics` receives the counts under the node's labels
    /// (`engine` is the stage's [`Stage::engine_label`]), and `declared` is the node's
    /// [`Stage::uses_state`]. `tenant` and `node` are taken apart rather than as a
    /// [`Labels`] so a handle without a node id is unrepresentable: the key prefix is an
    /// invariant, not a convention. Built only by [`StageEnvironment::context`].
    #[must_use]
    fn new(
        store: Arc<dyn StateStore>,
        metrics: Metrics,
        pipeline: &str,
        tenant: Arc<str>,
        node: &str,
        engine: Option<EngineLabel>,
        declared: bool,
    ) -> Self {
        Self {
            store,
            metrics,
            prefix: format!("{pipeline}:{}:{node}:", escape_segment(&tenant)),
            tenant,
            node: node.to_owned(),
            engine,
            declared,
        }
    }

    /// The prefix every key gets: `{pipeline}:{tenant}:{node}:`.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    fn timed<T>(&self, op: impl FnOnce() -> Result<T, StateError>) -> Result<T, StateError> {
        if !self.declared {
            return Err(StateError::new(format!(
                "node `{}` used the state store without declaring `uses_state`",
                self.node
            )));
        }
        let started = Instant::now();
        let result = op();
        let labels = Labels::new(&self.tenant, &self.node).with_engine(self.engine);
        self.metrics.state_op(&labels, started.elapsed());
        if result.is_err() {
            self.metrics.state_error(&labels);
        }
        result
    }

    /// `key` under this handle's prefix.
    fn key(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }

    /// [`StateStore::set_nx`] under this handle's prefix.
    ///
    /// # Errors
    ///
    /// The store's [`StateError`], already counted.
    pub fn set_nx(
        &self,
        key: &str,
        value: &[u8],
        ttl: Duration,
    ) -> Result<Option<Vec<u8>>, StateError> {
        let key = self.key(key);
        self.timed(|| self.store.set_nx(&key, value, ttl))
    }

    /// [`StateStore::set`] under this handle's prefix.
    ///
    /// # Errors
    ///
    /// The store's [`StateError`], already counted.
    pub fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<(), StateError> {
        let key = self.key(key);
        self.timed(|| self.store.set(&key, value, ttl))
    }

    /// [`StateStore::compare_and_set`] under this handle's prefix.
    ///
    /// # Errors
    ///
    /// The store's [`StateError`], already counted.
    pub fn compare_and_set(
        &self,
        key: &str,
        expected: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> Result<Option<Vec<u8>>, StateError> {
        let key = self.key(key);
        self.timed(|| self.store.compare_and_set(&key, expected, value, ttl))
    }

    /// [`StateStore::get`] under this handle's prefix.
    ///
    /// # Errors
    ///
    /// The store's [`StateError`], already counted.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StateError> {
        let key = self.key(key);
        self.timed(|| self.store.get(&key))
    }

    /// [`StateStore::incr`] under this handle's prefix.
    ///
    /// # Errors
    ///
    /// The store's [`StateError`], already counted.
    pub fn incr(&self, key: &str, by: i64, ttl: Duration) -> Result<i64, StateError> {
        let key = self.key(key);
        self.timed(|| self.store.incr(&key, by, ttl))
    }

    /// [`StateStore::del`] under this handle's prefix.
    ///
    /// # Errors
    ///
    /// The store's [`StateError`], already counted.
    pub fn del(&self, key: &str) -> Result<(), StateError> {
        let key = self.key(key);
        self.timed(|| self.store.del(&key))
    }
}

/// A key segment with `:` and `%` escaped, so a tenant containing the separator cannot
/// change which node or pipeline a key belongs to. Node ids and pipeline names are checked
/// at load instead.
fn escape_segment(segment: &str) -> String {
    if !segment.contains([':', '%']) {
        return segment.to_owned();
    }
    let mut out = String::with_capacity(segment.len() + 4);
    for c in segment.chars() {
        match c {
            ':' => out.push_str("%3A"),
            '%' => out.push_str("%25"),
            other => out.push(other),
        }
    }
    out
}

/// A stage's handle on the metrics for one record: the node's labels are fixed, and only
/// the metrics a stage emits for itself are reachable, so the per-node series the engine
/// owns (`records_in_total` and the rest) stay the engine's.
#[derive(Debug, Clone, Copy)]
pub struct StageMetrics<'a> {
    metrics: &'a Metrics,
    labels: Labels<'a>,
}

impl<'a> StageMetrics<'a> {
    /// A handle emitting through `metrics` under `labels`, the node's labels as the engine
    /// built them. Built only by [`StageEnvironment::context`].
    #[must_use]
    const fn new(metrics: &'a Metrics, labels: Labels<'a>) -> Self {
        Self { metrics, labels }
    }

    /// `regex_nonmatch_total`: the stage's pattern did not match this record.
    pub fn regex_nonmatch(&self) {
        self.metrics.regex_nonmatch(&self.labels);
    }

    /// `edit_unapplied_total`: the `op` reading `field` could not apply to this record
    /// because of `cause`.
    pub fn edit_unapplied(&self, op: EditOp, field: &str, cause: EditCause) {
        self.metrics
            .edit_unapplied(&self.labels.with_edit(op, field, cause));
    }

    /// `lua_errors_total`: a run of the stage's script was stopped for `kind`.
    pub fn lua_error(&self, kind: LuaErrorKind) {
        self.metrics
            .lua_error(&self.labels.with_kind(kind.as_str()));
    }
}

/// Per-record context handed to a stage alongside the record. Built only by core, from the
/// worker's stage environment, the record's [`Meta`] and the node.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Context<'a> {
    /// Id of the node being run.
    pub node_id: &'a str,
    /// The pipeline's view of the record being processed: its id, tenant, ingestion time
    /// and delivery count. Read-only; every decision reads these, never the payload.
    pub meta: &'a Meta,
    /// The state handle: the worker's store connection, scoped to this pipeline, tenant
    /// and node.
    pub state: State,
    /// The metrics a stage emits for itself, under the node's labels.
    pub metrics: StageMetrics<'a>,
}

/// What a worker holds for every stage it runs: the pipeline's name, the worker's state-store
/// connection and the metrics handle. Built once per worker at engine start.
pub(crate) struct StageEnvironment {
    pipeline: String,
    store: Arc<dyn StateStore>,
    metrics: Metrics,
}

impl StageEnvironment {
    pub(crate) const fn new(
        pipeline: String,
        store: Arc<dyn StateStore>,
        metrics: Metrics,
    ) -> Self {
        Self {
            pipeline,
            store,
            metrics,
        }
    }

    /// The context for `stage`, the node `node_id`, running over the record whose `Meta` is
    /// `meta`: the state handle and the metric labels both take the tenant from `meta`.
    pub(crate) fn context<'a>(
        &'a self,
        meta: &'a Meta,
        node_id: &'a str,
        stage: &dyn Stage,
    ) -> Context<'a> {
        let engine = stage.engine_label();
        Context {
            node_id,
            meta,
            state: State::new(
                Arc::clone(&self.store),
                self.metrics.clone(),
                &self.pipeline,
                Arc::clone(&meta.tenant),
                node_id,
                engine,
                stage.uses_state(),
            ),
            metrics: StageMetrics::new(
                &self.metrics,
                Labels::new(&meta.tenant, node_id).with_engine(engine),
            ),
        }
    }
}

/// A pipeline stage. Shared across worker threads, so it must be `Send + Sync`; per-worker
/// resources are a later concern.
pub trait Stage: Send + Sync {
    /// Process one record.
    fn process(&self, record: Record, ctx: &Context<'_>) -> StageOutput;

    /// The `engine` label for this node's metrics: set for a stage that runs a regex,
    /// `None` for every other stage. Fixed at load, read by the engine once per record.
    fn engine_label(&self) -> Option<EngineLabel> {
        None
    }

    /// Whether this stage reaches the state store. The engine opens one connection per
    /// worker only when some node does.
    fn uses_state(&self) -> bool {
        false
    }

    /// What the engine does with a record this stage returned as
    /// [`StageOutput::StateError`]. Read from the node's `on_state_error` by the stage that
    /// parsed it; the default is to fail the record, the safe choice for any stage that
    /// would produce data from state.
    fn on_state_error(&self) -> StateErrorPolicy {
        StateErrorPolicy::Nak
    }
}
