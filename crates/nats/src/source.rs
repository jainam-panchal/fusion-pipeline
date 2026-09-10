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
//! JetStream message. A payload that is not a record cannot succeed on redelivery, so it is
//! terminated instead of nak'd and reported on stderr.
//!
//! A nak without a delay from the engine is sent with [`DEFAULT_NAK_DELAY`], so a sink that
//! is down does not burn through `max_deliver` in milliseconds.

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

/// Redelivery delay used when the engine naks without one.
pub const DEFAULT_NAK_DELAY: Duration = Duration::from_secs(1);

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
            let (message, acker) = message.split();
            let ack = Box::new(NatsAck {
                runtime: Arc::clone(&self.runtime),
                acker,
            });
            let record = match decode(&message.subject, &message.payload) {
                Ok(record) => record,
                Err(err) => {
                    eprintln!(
                        "nats source: terminating undecodable message on `{}`: {err}",
                        message.subject
                    );
                    ack.terminate();
                    continue;
                }
            };
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

/// Settles one JetStream message from whichever engine thread finishes the record.
struct NatsAck {
    runtime: Arc<Runtime>,
    acker: Acker,
}

impl NatsAck {
    fn settle(&self, kind: AckKind) {
        if let Err(err) = self.runtime.block_on(self.acker.ack_with(kind)) {
            // A lost ack redelivers after `ack_wait`; a lost nak redelivers the same way.
            eprintln!("nats source: could not settle message: {err}");
        }
    }

    fn terminate(self: Box<Self>) {
        self.settle(AckKind::Term);
    }
}

impl AckHandle for NatsAck {
    fn ack(self: Box<Self>) {
        self.settle(AckKind::Ack);
    }

    fn nak(self: Box<Self>, delay: Option<Duration>) {
        self.settle(AckKind::Nak(Some(delay.unwrap_or(DEFAULT_NAK_DELAY))));
    }
}
