//! `source: {type: nats}`: a JetStream pull consumer feeding the engine.
//!
//! ```yaml
//! source:
//!   type: nats
//!   url: nats://127.0.0.1:4222   # overridden by NATS_URL
//!   stream: LOGS
//!   consumer: pipeline           # pull, explicit ack, ack_wait 30s, max_deliver 5
//! ```
//!
//! Each message is decoded as one JSON record, stamped with the tenant from its subject when
//! the record carries none, and handed to the engine with an ack handle that acks or naks the
//! JetStream message. A payload that is not a record is nak'd like any other failure and
//! reported on stderr; it runs out `max_deliver` the same way a record without an id does,
//! which is where the dead-letter ticket picks it up.
//!
//! Naks carry a delay. When the engine gives none, [`nak_delay`] derives one from the
//! message's delivery count: 1s on the first failure, doubling to [`MAX_NAK_DELAY`], so a
//! sink that is down does not burn through `max_deliver` in milliseconds.

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::{AckKind, message::Acker};
use fusion_core::io::{AckHandle, Envelope, Intake, Source, SourceError};
use fusion_core::record::Record;
use futures::StreamExt;
use tokio::runtime::Runtime;
use tokio::sync::watch;

use crate::subject::{stamp_tenant, tenant_from_subject};

/// Longest redelivery delay [`nak_delay`] asks for.
pub const MAX_NAK_DELAY: Duration = Duration::from_secs(30);

/// Redelivery delay for a nak on the `delivered`-th delivery of a message: 1s, 2s, 4s, ...,
/// capped at [`MAX_NAK_DELAY`]. A count below one (not reported) is treated as the first.
#[must_use]
pub fn nak_delay(delivered: u64) -> Duration {
    // 2^5 s already exceeds the cap, so the exponent never needs to go higher.
    let exponent = u32::try_from(delivered.saturating_sub(1))
        .unwrap_or(u32::MAX)
        .min(5);
    Duration::from_secs(1 << exponent).min(MAX_NAK_DELAY)
}

/// A JetStream source. Build one through [`crate::Nats::source`].
#[derive(Debug)]
pub struct NatsSource {
    runtime: Arc<Runtime>,
    consumer: PullConsumer,
    shutdown: watch::Receiver<bool>,
}

impl NatsSource {
    pub(crate) fn new(
        runtime: Arc<Runtime>,
        consumer: PullConsumer,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            runtime,
            consumer,
            shutdown,
        }
    }

    async fn pump(mut self, intake: Intake) -> Result<(), SourceError> {
        let mut messages = self
            .consumer
            .messages()
            .await
            .map_err(|e| SourceError::Other(Box::new(e)))?;
        loop {
            let next = tokio::select! {
                biased;
                _ = self.shutdown.wait_for(|stop| *stop) => return Ok(()),
                next = messages.next() => next,
            };
            let message = match next {
                Some(Ok(message)) => message,
                Some(Err(err)) => {
                    // Heartbeat and pull errors are transient: the stream re-issues the pull.
                    eprintln!("nats source: {err}");
                    continue;
                }
                None => return Ok(()),
            };
            let delivered = message
                .info()
                .ok()
                .and_then(|info| u64::try_from(info.delivered).ok())
                .unwrap_or(1);
            let (message, acker) = message.split();
            let record = match decode(&message.subject, &message.payload) {
                Ok(record) => record,
                Err(err) => {
                    eprintln!(
                        "nats source: nak of undecodable message on `{}` (delivery {delivered}): {err}",
                        message.subject
                    );
                    // Settled here, on the runtime: `NatsAck` blocks on the runtime and
                    // cannot be used from inside it.
                    let nak = AckKind::Nak(Some(nak_delay(delivered)));
                    if let Err(err) = acker.ack_with(nak).await {
                        eprintln!("nats source: could not nak message: {err}");
                    }
                    continue;
                }
            };
            let ack = Box::new(NatsAck {
                runtime: Arc::clone(&self.runtime),
                acker,
                delivered,
            });
            // On a closed intake the envelope, and its ack handle, are dropped unsettled; the
            // message redelivers after `ack_wait`.
            intake.send(Envelope { record, ack })?;
        }
    }
}

fn decode(subject: &str, payload: &[u8]) -> Result<Record, serde_json::Error> {
    let mut record: Record = serde_json::from_slice(payload)?;
    if let Some(tenant) = tenant_from_subject(subject) {
        stamp_tenant(&mut record, tenant);
    }
    Ok(record)
}

impl Source for NatsSource {
    fn run(self: Box<Self>, intake: Intake) -> Result<(), SourceError> {
        let runtime = Arc::clone(&self.runtime);
        runtime.block_on(self.pump(intake))
    }
}

/// Settles one JetStream message from whichever engine thread finishes the record. Engine
/// threads are not runtime threads, so blocking on the runtime here is safe; the source
/// loop itself must never use this type.
struct NatsAck {
    runtime: Arc<Runtime>,
    acker: Acker,
    /// How many times JetStream has delivered this message, this one included.
    delivered: u64,
}

impl NatsAck {
    fn settle(&self, kind: AckKind) {
        if let Err(err) = self.runtime.block_on(self.acker.ack_with(kind)) {
            // A lost ack redelivers after `ack_wait`; a lost nak redelivers the same way.
            eprintln!("nats source: could not settle message: {err}");
        }
    }
}

impl AckHandle for NatsAck {
    fn ack(self: Box<Self>) {
        self.settle(AckKind::Ack);
    }

    fn nak(self: Box<Self>, delay: Option<Duration>) {
        self.settle(AckKind::Nak(Some(
            delay.unwrap_or_else(|| nak_delay(self.delivered)),
        )));
    }
}
