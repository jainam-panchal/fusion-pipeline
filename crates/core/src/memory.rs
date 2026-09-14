//! In-memory source and sinks for tests and local runs.
//!
//! [`MemorySource`] forwards records pushed through a [`MemoryInput`]; every push returns an
//! [`AckProbe`] that observes how the engine settled the record. [`MemorySinks`] is a
//! [`SinkFactory`] whose sinks collect records per node id for later inspection.
//! [`MemoryStateStore`] is the state store: one shared map behind every connection it
//! opens, a clock the test advances by hand, and errors on demand.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::config::{ConfigError, NodeConfig};
use crate::io::{AckHandle, Envelope, Intake, Sink, SinkError, Source, SourceError};
use crate::record::Record;
use crate::registry::SinkFactory;
use crate::state::{StateError, StateStore, StateStoreFactory};

/// How the engine settled a record's source message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    /// Acknowledged.
    Ack,
    /// Negatively acknowledged, with the requested redelivery delay.
    Nak(Option<Duration>),
}

#[derive(Default)]
struct AckState {
    outcome: Mutex<Option<AckOutcome>>,
    settled: Condvar,
}

/// Observes the ack outcome of one pushed record.
#[derive(Clone)]
pub struct AckProbe {
    state: Arc<AckState>,
}

impl AckProbe {
    /// Wait up to `timeout` for the record to be settled.
    ///
    /// Returns `None` if the engine has not settled it in time.
    #[must_use]
    pub fn wait(&self, timeout: Duration) -> Option<AckOutcome> {
        let deadline = Instant::now() + timeout;
        let mut outcome = lock_unpoisoned(&self.state.outcome);
        while outcome.is_none() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let (guard, _) = self
                .state
                .settled
                .wait_timeout(outcome, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            outcome = guard;
        }
        *outcome
    }

    /// The outcome if already settled, without waiting.
    #[must_use]
    pub fn outcome(&self) -> Option<AckOutcome> {
        *lock_unpoisoned(&self.state.outcome)
    }
}

struct MemoryAck {
    state: Arc<AckState>,
}

impl MemoryAck {
    fn settle(&self, outcome: AckOutcome) {
        *lock_unpoisoned(&self.state.outcome) = Some(outcome);
        self.state.settled.notify_all();
    }
}

impl AckHandle for MemoryAck {
    fn ack(self: Box<Self>) {
        self.settle(AckOutcome::Ack);
    }

    fn nak(self: Box<Self>, delay: Option<Duration>) {
        self.settle(AckOutcome::Nak(delay));
    }
}

/// Producer side of a [`MemorySource`]. Clone freely; drop every clone to end the source.
#[derive(Debug, Clone)]
pub struct MemoryInput {
    tx: crossbeam_channel::Sender<Envelope>,
}

impl MemoryInput {
    /// Queue a record for the engine and return a probe on its ack outcome.
    ///
    /// # Panics
    ///
    /// Panics if the source has already finished, which only happens after the engine that
    /// owns it was joined.
    pub fn push(&self, record: Record) -> AckProbe {
        let state = Arc::new(AckState::default());
        let ack = Box::new(MemoryAck {
            state: Arc::clone(&state),
        });
        self.tx
            .send(Envelope { record, ack })
            .expect("memory source is running while its input is alive");
        AckProbe { state }
    }
}

/// A [`Source`] fed from a [`MemoryInput`].
#[derive(Debug)]
pub struct MemorySource {
    rx: crossbeam_channel::Receiver<Envelope>,
}

impl MemorySource {
    /// Create a source and its input handle.
    #[must_use]
    pub fn new() -> (Self, MemoryInput) {
        let (tx, rx) = crossbeam_channel::unbounded();
        (Self { rx }, MemoryInput { tx })
    }
}

impl Source for MemorySource {
    fn run(self: Box<Self>, intake: Intake) -> Result<(), SourceError> {
        for envelope in self.rx {
            intake.send(envelope)?;
        }
        Ok(())
    }
}

/// Collects records per sink node id. Register it under a `sink.*` type name.
///
/// Any sink can be made to fail on demand with [`MemorySinks::fail_writes_to`], to exercise
/// the engine's nak path.
#[derive(Debug, Clone, Default)]
pub struct MemorySinks {
    records: Arc<Mutex<BTreeMap<String, Vec<Record>>>>,
    failing: Arc<Mutex<BTreeSet<String>>>,
}

/// The error a failing memory sink returns.
#[derive(Debug, thiserror::Error)]
#[error("memory sink `{0}` is set to fail")]
pub struct InjectedSinkFailure(String);

impl MemorySinks {
    /// An empty collector.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Make every write to the sink node `node_id` fail.
    pub fn fail_writes_to(&self, node_id: &str) {
        lock_unpoisoned(&self.failing).insert(node_id.to_owned());
    }

    /// Records written to the sink node `node_id`, in arrival order.
    #[must_use]
    pub fn records(&self, node_id: &str) -> Vec<Record> {
        lock_unpoisoned(&self.records)
            .get(node_id)
            .cloned()
            .unwrap_or_default()
    }
}

impl SinkFactory for MemorySinks {
    fn build(&self, node: &NodeConfig) -> Result<Box<dyn Sink>, ConfigError> {
        Ok(Box::new(MemorySink {
            node_id: node.id.clone(),
            records: Arc::clone(&self.records),
            failing: Arc::clone(&self.failing),
        }))
    }
}

struct MemorySink {
    node_id: String,
    records: Arc<Mutex<BTreeMap<String, Vec<Record>>>>,
    failing: Arc<Mutex<BTreeSet<String>>>,
}

impl Sink for MemorySink {
    fn write(&self, records: &[Record]) -> Result<(), SinkError> {
        if lock_unpoisoned(&self.failing).contains(&self.node_id) {
            return Err(SinkError::new(InjectedSinkFailure(self.node_id.clone())));
        }
        lock_unpoisoned(&self.records)
            .entry(self.node_id.clone())
            .or_default()
            .extend_from_slice(records);
        Ok(())
    }
}

/// One stored value and when it expires on the fake clock.
struct Entry {
    value: Vec<u8>,
    expires_at: Duration,
}

#[derive(Default)]
struct StateData {
    entries: BTreeMap<String, Entry>,
    /// The fake clock: time since the store was created, moved only by
    /// [`MemoryStateStore::advance`].
    now: Duration,
    failing: bool,
}

impl StateData {
    fn live(&mut self, key: &str) -> Option<&mut Entry> {
        let now = self.now;
        if self.entries.get(key).is_some_and(|e| e.expires_at <= now) {
            self.entries.remove(key);
        }
        self.entries.get_mut(key)
    }

    /// Write `value` at `key`, its ttl starting now.
    fn write(&mut self, key: &str, value: &[u8], ttl: Duration) {
        let expires_at = self.now + ttl;
        self.entries.insert(
            key.to_owned(),
            Entry {
                value: value.to_vec(),
                expires_at,
            },
        );
    }

    fn check(&self) -> Result<(), StateError> {
        if self.failing {
            Err(StateError::new(INJECTED_STATE_FAILURE))
        } else {
            Ok(())
        }
    }
}

/// The message every operation fails with after [`MemoryStateStore::fail_all`].
pub const INJECTED_STATE_FAILURE: &str = "memory state store is set to fail";

/// A [`StateStore`] and [`StateStoreFactory`] over one shared map. Every connection it
/// opens, and the store itself, read and write the same data, as workers on one Dragonfly
/// do. Time is a fake clock that only [`MemoryStateStore::advance`] moves, so expiry is
/// deterministic. [`MemoryStateStore::fail_all`] makes every operation fail, to exercise the
/// engine's failure policy.
#[derive(Clone, Default)]
pub struct MemoryStateStore {
    data: Arc<Mutex<StateData>>,
    /// Closures to run, one per refused claim or takeover, in the gap between the store
    /// answering and the caller reading the answer. See
    /// [`MemoryStateStore::after_next_holder_reply`].
    races: Arc<Mutex<VecDeque<Race>>>,
}

/// A test acting as another worker: gets the store and the key just refused.
type Race = Box<dyn FnOnce(&MemoryStateStore, &str) + Send>;

impl std::fmt::Debug for MemoryStateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStateStore")
            .field("data", &self.data)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for StateData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateData")
            .field("keys", &self.entries.keys().collect::<Vec<_>>())
            .field("now", &self.now)
            .field("failing", &self.failing)
            .finish()
    }
}

impl MemoryStateStore {
    /// An empty store at time zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Move the fake clock forward, expiring whatever `ttl` has run out.
    pub fn advance(&self, by: Duration) {
        lock_unpoisoned(&self.data).now += by;
    }

    /// Make every operation fail (`true`) or succeed again (`false`). Opening a connection
    /// still succeeds: an outage after startup is a different fault from a store that was
    /// never reachable.
    pub fn fail_all(&self, failing: bool) {
        lock_unpoisoned(&self.data).failing = failing;
    }

    /// Run `race` once, after the next claim or takeover the store refuses and before its
    /// reply (the holder) reaches the caller. That gap is where another worker's write lands
    /// in production, so the test plays that worker: write a different holder, expire the
    /// key, whatever the scenario needs. Several calls queue up, one per refusal, in order.
    /// The queue is one per store, not per key: the next refusal of any key runs the next
    /// race, so a test that touches several keys must queue with that order in mind.
    pub fn after_next_holder_reply(
        &self,
        race: impl FnOnce(&MemoryStateStore, &str) + Send + 'static,
    ) {
        lock_unpoisoned(&self.races).push_back(Box::new(race));
    }

    /// Run the next queued race for `key`, with no lock held.
    fn holder_replied(&self, key: &str) {
        let race = lock_unpoisoned(&self.races).pop_front();
        if let Some(race) = race {
            race(self, key);
        }
    }

    /// Every live key, sorted.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        let mut data = lock_unpoisoned(&self.data);
        let now = data.now;
        data.entries.retain(|_, e| e.expires_at > now);
        data.entries.keys().cloned().collect()
    }
}

impl StateStore for MemoryStateStore {
    fn set_nx(
        &self,
        key: &str,
        value: &[u8],
        ttl: Duration,
    ) -> Result<Option<Vec<u8>>, StateError> {
        let mut data = lock_unpoisoned(&self.data);
        data.check()?;
        if let Some(existing) = data.live(key) {
            let existing = existing.value.clone();
            drop(data);
            self.holder_replied(key);
            return Ok(Some(existing));
        }
        data.write(key, value, ttl);
        Ok(None)
    }

    fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<(), StateError> {
        let mut data = lock_unpoisoned(&self.data);
        data.check()?;
        data.write(key, value, ttl);
        Ok(())
    }

    fn compare_and_set(
        &self,
        key: &str,
        expected: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> Result<Option<Vec<u8>>, StateError> {
        let mut data = lock_unpoisoned(&self.data);
        data.check()?;
        if let Some(current) = data.live(key) {
            if current.value != expected {
                let current = current.value.clone();
                drop(data);
                self.holder_replied(key);
                return Ok(Some(current));
            }
        }
        data.write(key, value, ttl);
        Ok(None)
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StateError> {
        let mut data = lock_unpoisoned(&self.data);
        data.check()?;
        Ok(data.live(key).map(|e| e.value.clone()))
    }

    fn incr(&self, key: &str, by: i64, ttl: Duration) -> Result<i64, StateError> {
        let mut data = lock_unpoisoned(&self.data);
        data.check()?;
        let current = match data.live(key) {
            None => 0,
            Some(entry) => std::str::from_utf8(&entry.value)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or_else(|| StateError::new(format!("key `{key}` is not an integer")))?,
        };
        let next = current
            .checked_add(by)
            .ok_or_else(|| StateError::new(format!("key `{key}` would overflow")))?;
        let expires_at = data.now + ttl;
        data.entries.insert(
            key.to_owned(),
            Entry {
                value: next.to_string().into_bytes(),
                expires_at,
            },
        );
        Ok(next)
    }

    fn del(&self, key: &str) -> Result<(), StateError> {
        let mut data = lock_unpoisoned(&self.data);
        data.check()?;
        data.entries.remove(key);
        Ok(())
    }
}

impl StateStoreFactory for MemoryStateStore {
    fn open(&self) -> Result<Box<dyn StateStore>, StateError> {
        Ok(Box::new(self.clone()))
    }
}

pub(crate) fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
