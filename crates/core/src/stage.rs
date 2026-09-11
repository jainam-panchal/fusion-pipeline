//! The stage contract: one record in, one [`StageOutput`] out.
//!
//! A stateful stage reaches the state store through [`State`], the handle on its
//! [`Context`]. The handle prefixes every key with `{pipeline}:{tenant}:{node}:` so no stage
//! can share state across tenants or pipelines, and counts every operation on
//! `state_ops_total`, `state_op_duration_seconds` and `state_errors_total`.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::metrics::Metrics;
use crate::record::{Record, RecordId};
use crate::state::{StateError, StateErrorPolicy, StateStore};

/// Why a record was intentionally dropped. Closed set: adding one is a spec amendment, and
/// [`DropReason::ALL`] lists them all. It is the `reason` label on `records_dropped_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DropReason {
    /// A `filter` node dropped it.
    Filter,
    /// A `route` node's default was `drop`.
    RouteDefaultDrop,
    /// A `sample` node did not select it.
    Sample,
    /// A `dedupe` node saw it before.
    Dedupe,
    /// A Lua script returned `nil`.
    LuaDrop,
    /// A Lua script errored with `on_error: drop`.
    LuaError,
    /// A regex limit tripped.
    RegexLimit,
    /// The state store failed and the node's policy is `drop`.
    StateError,
    /// The record is malformed or of an unsupported kind.
    InvalidRecord,
    /// The record carries no `id`.
    MissingId,
}

impl DropReason {
    /// Every reason, for checks against the spec's closed set.
    pub const ALL: [Self; 10] = [
        Self::Filter,
        Self::RouteDefaultDrop,
        Self::Sample,
        Self::Dedupe,
        Self::LuaDrop,
        Self::LuaError,
        Self::RegexLimit,
        Self::StateError,
        Self::InvalidRecord,
        Self::MissingId,
    ];

    /// The metric label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Filter => "filter",
            Self::RouteDefaultDrop => "route_default_drop",
            Self::Sample => "sample",
            Self::Dedupe => "dedupe",
            Self::LuaDrop => "lua_drop",
            Self::LuaError => "lua_error",
            Self::RegexLimit => "regex_limit",
            Self::StateError => "state_error",
            Self::InvalidRecord => "invalid_record",
            Self::MissingId => "missing_id",
        }
    }
}

impl fmt::Display for DropReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
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
    tenant: String,
    node: String,
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
    /// `node` form the key prefix, and `metrics` receives the counts.
    #[must_use]
    pub fn new(
        store: Arc<dyn StateStore>,
        metrics: Metrics,
        pipeline: &str,
        tenant: &str,
        node: &str,
    ) -> Self {
        Self {
            store,
            metrics,
            prefix: format!("{pipeline}:{}:{node}:", escape_segment(tenant)),
            tenant: tenant.to_owned(),
            node: node.to_owned(),
        }
    }

    /// The prefix every key gets: `{pipeline}:{tenant}:{node}:`.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    fn timed<T>(&self, op: impl FnOnce() -> Result<T, StateError>) -> Result<T, StateError> {
        let started = Instant::now();
        let result = op();
        self.metrics
            .state_op(&self.tenant, &self.node, started.elapsed());
        if result.is_err() {
            self.metrics.state_error(&self.tenant, &self.node);
        }
        result
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
        let key = format!("{}{key}", self.prefix);
        self.timed(|| self.store.set_nx(&key, value, ttl))
    }

    /// [`StateStore::get`] under this handle's prefix.
    ///
    /// # Errors
    ///
    /// The store's [`StateError`], already counted.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StateError> {
        let key = format!("{}{key}", self.prefix);
        self.timed(|| self.store.get(&key))
    }

    /// [`StateStore::incr`] under this handle's prefix.
    ///
    /// # Errors
    ///
    /// The store's [`StateError`], already counted.
    pub fn incr(&self, key: &str, by: i64, ttl: Duration) -> Result<i64, StateError> {
        let key = format!("{}{key}", self.prefix);
        self.timed(|| self.store.incr(&key, by, ttl))
    }

    /// [`StateStore::del`] under this handle's prefix.
    ///
    /// # Errors
    ///
    /// The store's [`StateError`], already counted.
    pub fn del(&self, key: &str) -> Result<(), StateError> {
        let key = format!("{}{key}", self.prefix);
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

/// Per-record context handed to a stage alongside the record.
#[derive(Debug, Clone)]
pub struct Context<'a> {
    /// Id of the node being run.
    pub node_id: &'a str,
    /// Id of the record being processed.
    pub record_id: RecordId,
    /// The state store, scoped to this pipeline, tenant and node.
    pub state: State,
}

impl<'a> Context<'a> {
    /// A context over a fresh in-memory store and no metrics, for testing a stage on its own.
    #[must_use]
    pub fn in_memory(node_id: &'a str, record_id: RecordId) -> Self {
        Self {
            node_id,
            record_id,
            state: State::new(
                Arc::new(crate::memory::MemoryStateStore::new()),
                Metrics::noop(),
                crate::config::DEFAULT_NAME,
                Metrics::UNKNOWN_TENANT,
                node_id,
            ),
        }
    }
}

/// A pipeline stage. Shared across worker threads, so it must be `Send + Sync`; per-worker
/// resources are a later concern.
pub trait Stage: Send + Sync {
    /// Process one record.
    fn process(&self, record: Record, ctx: &Context<'_>) -> StageOutput;

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
