//! The Dragonfly state store: [`fusion_core::state::StateStore`] over the Redis protocol.
//!
//! [`Dragonfly`] is the factory the binary hands the engine; it opens one synchronous
//! connection per worker at start (connect, timeouts, `PING`), so an unreachable store fails
//! startup. The URL comes from [`URL_ENV`] (`DRAGONFLY_URL`), else [`DEFAULT_URL`].
//!
//! Each [`DragonflyStore`] keeps its connection behind a mutex and reopens it on the next
//! call after any I/O error or timeout. Reopening matters: a reply that arrives after a read
//! timeout would otherwise be read as the answer to the *next* command. Every operation is
//! one server-side atomic step: `SET NX PX GET` for the claim (an `EVAL` with the same
//! meaning when the server refuses the combination), `SET PX` for the plain write,
//! `MULTI INCRBY PEXPIRE EXEC` for the counter.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fusion_core::state::{StateError, StateStore, StateStoreFactory};
use redis::{Client, Connection, ErrorKind, RedisError, Value};

/// Environment variable naming the store.
pub const URL_ENV: &str = "DRAGONFLY_URL";
/// The store URL when the environment gives none: the compose stack's published port.
pub const DEFAULT_URL: &str = "redis://127.0.0.1:6379";

/// How long a connection attempt may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// How long one command may take to send or answer before the connection is dropped and
/// the operation fails. Short next to `ack_wait`, so a paused store becomes a `StateError`
/// the engine can act on rather than a stalled worker.
const OP_TIMEOUT: Duration = Duration::from_secs(2);

/// The claim as one `EVAL`, for servers that refuse `SET ... NX ... GET`: the existing value
/// if there is one, else set with the ttl and return nil.
const CLAIM_SCRIPT: &str = "local v = redis.call('GET', KEYS[1]) \
if v then return v end \
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2]) \
return false";

/// Pick the store URL: a non-empty `env` value wins, else [`DEFAULT_URL`].
#[must_use]
pub fn resolve_url(env: Option<&str>) -> String {
    env.filter(|url| !url.is_empty())
        .unwrap_or(DEFAULT_URL)
        .to_owned()
}

/// [`resolve_url`] against the process environment.
#[must_use]
pub fn url_from_env() -> String {
    let env = std::env::var(URL_ENV).ok();
    resolve_url(env.as_deref())
}

/// The factory: knows the URL, opens one connection per worker.
#[derive(Debug, Clone)]
pub struct Dragonfly {
    client: Client,
    url: String,
    /// Start every connection on the `EVAL` claim instead of `SET NX GET`, so the fallback
    /// can be tested against a server that accepts both.
    eval_claim: bool,
}

impl Dragonfly {
    /// A factory for the store at `url`. Nothing is contacted until [`Dragonfly::open`].
    ///
    /// # Errors
    ///
    /// [`StateError`] when `url` is not a Redis URL.
    pub fn new(url: &str) -> Result<Self, StateError> {
        let client = Client::open(url)
            .map_err(|e| StateError::new(format!("invalid state store url `{url}`: {e}")))?;
        Ok(Self {
            client,
            url: url.to_owned(),
            eval_claim: false,
        })
    }

    /// Every connection opened from this factory claims with the `EVAL` script from the
    /// start, as if the server had refused `SET ... NX ... GET`. For testing the fallback.
    #[doc(hidden)]
    #[must_use]
    pub fn with_eval_claim(mut self) -> Self {
        self.eval_claim = true;
        self
    }

    /// A factory for the store [`url_from_env`] names.
    ///
    /// # Errors
    ///
    /// See [`Dragonfly::new`].
    pub fn from_env() -> Result<Self, StateError> {
        Self::new(&url_from_env())
    }

    /// The URL this factory connects to.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    fn connect(&self) -> Result<Connection, StateError> {
        let unreachable = |e: &RedisError| {
            StateError::new(format!(
                "could not connect to the state store at {}: {e}",
                self.url
            ))
        };
        let mut connection = self
            .client
            .get_connection_with_timeout(CONNECT_TIMEOUT)
            .map_err(|e| unreachable(&e))?;
        connection
            .set_read_timeout(Some(OP_TIMEOUT))
            .and_then(|()| connection.set_write_timeout(Some(OP_TIMEOUT)))
            .map_err(|e| unreachable(&e))?;
        redis::cmd("PING")
            .query::<String>(&mut connection)
            .map_err(|e| unreachable(&e))?;
        Ok(connection)
    }
}

impl StateStoreFactory for Dragonfly {
    fn open(&self) -> Result<Box<dyn StateStore>, StateError> {
        let connection = self.connect()?;
        Ok(Box::new(DragonflyStore {
            factory: self.clone(),
            connection: Mutex::new(Some(connection)),
            claim_with_set_get: AtomicBool::new(!self.eval_claim),
        }))
    }
}

/// One worker's connection. Reconnects on the next call after an I/O error.
pub struct DragonflyStore {
    factory: Dragonfly,
    connection: Mutex<Option<Connection>>,
    /// Whether the server accepts `SET ... NX ... GET`; flipped off on the first refusal.
    claim_with_set_get: AtomicBool,
}

impl std::fmt::Debug for DragonflyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DragonflyStore")
            .field("url", &self.factory.url)
            .finish_non_exhaustive()
    }
}

impl DragonflyStore {
    /// Run `op` on the connection, reopening it first if the last call dropped it. Any I/O
    /// error or timeout drops the connection so the next call starts clean.
    fn with_connection<T>(
        &self,
        op: impl FnOnce(&mut Connection) -> Result<T, RedisError>,
    ) -> Result<T, StateError> {
        let mut slot = self
            .connection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if slot.is_none() {
            *slot = Some(self.factory.connect()?);
        }
        let connection = slot.as_mut().expect("connection was just opened");
        match op(connection) {
            Ok(value) => Ok(value),
            Err(err) => {
                if is_connection_fault(&err) {
                    *slot = None;
                }
                Err(StateError::new(format!(
                    "state store at {}: {err}",
                    self.factory.url
                )))
            }
        }
    }

    /// Whether `err` is the server refusing the command's syntax (it does not know
    /// `SET ... NX ... GET`) rather than failing to run it, so the caller may try another
    /// spelling. Any other server error (out of memory, loading, read-only) is reported as
    /// is and changes nothing.
    fn refused_syntax(err: &RedisError) -> bool {
        matches!(err.kind(), ErrorKind::Server(_))
            && err.to_string().to_ascii_lowercase().contains("syntax")
    }
}

/// An error after which the connection cannot be trusted: the socket is gone, or a reply
/// may still be in flight and would answer the next command.
fn is_connection_fault(err: &RedisError) -> bool {
    err.is_io_error() || err.is_timeout() || err.is_connection_dropped()
}

fn ttl_millis(ttl: Duration) -> i64 {
    i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX).max(1)
}

/// The value a claim answered with: nil means claimed, bytes mean the current holder.
fn existing_from(value: Value) -> Result<Option<Vec<u8>>, RedisError> {
    match value {
        Value::Nil => Ok(None),
        // `SET ... GET` on a key that existed answers with its value.
        other => Ok(redis::from_redis_value::<Option<Vec<u8>>>(other)?),
    }
}

impl StateStore for DragonflyStore {
    fn set_nx(
        &self,
        key: &str,
        value: &[u8],
        ttl: Duration,
    ) -> Result<Option<Vec<u8>>, StateError> {
        let millis = ttl_millis(ttl);
        self.with_connection(|connection| {
            if self.claim_with_set_get.load(Ordering::Relaxed) {
                let reply = redis::cmd("SET")
                    .arg(key)
                    .arg(value)
                    .arg("NX")
                    .arg("PX")
                    .arg(millis)
                    .arg("GET")
                    .query::<Value>(connection);
                match reply {
                    Ok(reply) => return existing_from(reply),
                    Err(err) if Self::refused_syntax(&err) => {
                        self.claim_with_set_get.store(false, Ordering::Relaxed);
                    }
                    Err(err) => return Err(err),
                }
            }
            let reply = redis::cmd("EVAL")
                .arg(CLAIM_SCRIPT)
                .arg(1)
                .arg(key)
                .arg(value)
                .arg(millis)
                .query::<Value>(connection)?;
            existing_from(reply)
        })
    }

    fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<(), StateError> {
        let millis = ttl_millis(ttl);
        self.with_connection(|connection| {
            redis::cmd("SET")
                .arg(key)
                .arg(value)
                .arg("PX")
                .arg(millis)
                .query::<String>(connection)?;
            Ok(())
        })
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StateError> {
        self.with_connection(|connection| {
            redis::cmd("GET")
                .arg(key)
                .query::<Option<Vec<u8>>>(connection)
        })
    }

    fn incr(&self, key: &str, by: i64, ttl: Duration) -> Result<i64, StateError> {
        let millis = ttl_millis(ttl);
        self.with_connection(|connection| {
            let (value, _expired): (i64, i64) = redis::pipe()
                .atomic()
                .cmd("INCRBY")
                .arg(key)
                .arg(by)
                .cmd("PEXPIRE")
                .arg(key)
                .arg(millis)
                .query(connection)?;
            Ok(value)
        })
    }

    fn del(&self, key: &str) -> Result<(), StateError> {
        self.with_connection(|connection| {
            redis::cmd("DEL").arg(key).query::<i64>(connection)?;
            Ok(())
        })
    }
}
