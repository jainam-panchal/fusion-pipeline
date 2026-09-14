//! The state store contract: the external key-value service behind stateful stages.
//!
//! [`StateStore`] is one worker's connection to the shared store. The data is shared by
//! every worker and every replica of a pipeline; only the connection is per worker, which is
//! why a [`StateStoreFactory`] opens one per worker thread at engine start and an unreachable
//! store fails startup. Dragonfly implements both in the state crate; the in-memory
//! implementation lives in [`crate::memory`].
//!
//! Stages never hold a store. They reach it through the [`crate::stage::Context`], whose
//! handle namespaces every key by pipeline, tenant and node and counts every operation.

use std::fmt;
use std::time::Duration;

use serde::Deserialize;

/// A state store operation failed: the store is unreachable, timed out, or refused the
/// command. The engine applies the node's [`StateErrorPolicy`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("state store: {message}")]
pub struct StateError {
    message: String,
}

impl StateError {
    /// An error with `message`.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// What went wrong.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// One worker's connection to the shared state store: the four operations the spec names
/// plus `set`, the unconditional write, and `compare_and_set`, the write a stage uses to
/// take over a key it has decided is stale without racing another worker for it. `Sync` because the handle a stage receives shares the worker's connection behind an
/// `Arc`; implementations keep their connection behind a mutex.
pub trait StateStore: Send + Sync {
    /// Set `key` to `value` with `ttl` only if it does not exist. `None` when this call
    /// claimed the key; `Some(existing)` with the value already there when it did not. One
    /// atomic step, so a caller never sees the key vanish between a failed claim and a read.
    ///
    /// # Errors
    ///
    /// [`StateError`] when the store cannot answer.
    fn set_nx(&self, key: &str, value: &[u8], ttl: Duration)
    -> Result<Option<Vec<u8>>, StateError>;

    /// Set `key` to `value` with `ttl` whether or not it exists. Last writer wins; use
    /// [`StateStore::set_nx`] to claim.
    ///
    /// # Errors
    ///
    /// [`StateError`] when the store cannot answer.
    fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<(), StateError>;

    /// Set `key` to `value` with `ttl` only if it still holds `expected`, or holds nothing.
    /// `None` when this call wrote; `Some(current)` with the value there instead when it did
    /// not, so the caller can decide against the holder that beat it without another round
    /// trip. One atomic step: the read and the write cannot be split by another worker.
    ///
    /// # Errors
    ///
    /// [`StateError`] when the store cannot answer.
    fn compare_and_set(
        &self,
        key: &str,
        expected: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> Result<Option<Vec<u8>>, StateError>;

    /// The value of `key`, or `None` when it does not exist or has expired.
    ///
    /// # Errors
    ///
    /// [`StateError`] when the store cannot answer.
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StateError>;

    /// Add `by` to the integer at `key` (zero when absent) and return the new value. The
    /// key's `ttl` is set on every call.
    ///
    /// # Errors
    ///
    /// [`StateError`] when the store cannot answer or the value is not an integer.
    fn incr(&self, key: &str, by: i64, ttl: Duration) -> Result<i64, StateError>;

    /// Remove `key`. Removing an absent key is not an error.
    ///
    /// # Errors
    ///
    /// [`StateError`] when the store cannot answer.
    fn del(&self, key: &str) -> Result<(), StateError>;
}

/// Opens one [`StateStore`] connection per worker. Shared by the engine across workers.
pub trait StateStoreFactory: Send + Sync {
    /// Open a connection. Called once per worker at engine start; failure is a startup
    /// error.
    ///
    /// # Errors
    ///
    /// [`StateError`] when the store is unreachable or does not answer a ping.
    fn open(&self) -> Result<Box<dyn StateStore>, StateError>;
}

/// What the engine does with a record when its stage could not reach the state store.
/// Read from the node's `on_state_error`; each stage kind supplies its own default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StateErrorPolicy {
    /// Forward the record unchanged, as if the node were not there.
    Pass,
    /// Fail the record so its source message is negatively acknowledged.
    Nak,
}

impl fmt::Display for StateErrorPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Pass => "pass",
            Self::Nak => "nak",
        })
    }
}
