//! The stage contract: one record in, one [`StageOutput`] out.

use std::fmt;

use crate::record::{Record, RecordId};

/// Why a record was intentionally dropped. Closed set; it is the `reason` label on
/// `records_dropped_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
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
}

/// Per-record context handed to a stage alongside the record.
#[derive(Debug, Clone, Copy)]
pub struct Context<'a> {
    /// Id of the node being run.
    pub node_id: &'a str,
    /// Id of the record being processed.
    pub record_id: RecordId,
}

/// A pipeline stage. Shared across worker threads, so it must be `Send + Sync`; per-worker
/// resources are a later concern.
pub trait Stage: Send + Sync {
    /// Process one record.
    fn process(&self, record: Record, ctx: &Context<'_>) -> StageOutput;
}
