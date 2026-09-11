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
//! the record carries none and with the JetStream publish time as `observed_time_unix_nano`
//! when it carries no timestamp at all, and handed to the engine with an ack handle that acks
//! or naks the JetStream message. The publish time is the server's and does not change on
//! redelivery, so stateful stages that measure windows in ingestion time see the same value
//! every time the record comes back.
//!
//! A payload that is not a record is nak'd like any other failure and reported on stderr; it
//! runs out `max_deliver` the same way a record without an id does, which is where the
//! dead-letter ticket picks it up.
//!
//! Naks carry a delay. When the engine gives none, [`nak_delay`] derives one from the
//! message's delivery count: 1s on the first failure, doubling to [`MAX_NAK_DELAY`], so a
//! sink that is down does not burn through `max_deliver` in milliseconds.
//!
//! A message delivered more than once counts on `source_redeliveries_total`, and a nak the
//! source issues itself (an undecodable payload) on `source_naks_total`, both under the
//! tenant the subject names.

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::{AckKind, message::Acker};
use fusion_core::io::{AckHandle, Envelope, Intake, Source, SourceError};
use fusion_core::metrics::Metrics;
use fusion_core::record::Record;
use futures::StreamExt;
use tokio::runtime::Runtime;
use tokio::sync::watch;

use crate::subject::{stamp_tenant, tenant_from_subject};

/// Longest redelivery delay [`nak_delay`] asks for. A consumer with `max_deliver` 5, as the
/// compose stack creates, never reaches it (1s, 2s, 4s, 8s, then the final delivery); the cap
/// guards consumers configured with more attempts.
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
    metrics: Metrics,
}

impl NatsSource {
    pub(crate) fn new(
        runtime: Arc<Runtime>,
        consumer: PullConsumer,
        shutdown: watch::Receiver<bool>,
        metrics: Metrics,
    ) -> Self {
        Self {
            runtime,
            consumer,
            shutdown,
            metrics,
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
            let info = message.info().ok();
            let delivered = info
                .as_ref()
                .and_then(|info| u64::try_from(info.delivered).ok())
                .unwrap_or(1);
            let published = info
                .as_ref()
                .and_then(|info| u64::try_from(info.published.unix_timestamp_nanos()).ok());
            let (message, acker) = message.split();
            let subject_tenant = tenant_from_subject(&message.subject);
            let tenant = subject_tenant.unwrap_or(Metrics::UNKNOWN_TENANT);
            if delivered > 1 {
                self.metrics.source_redelivery(tenant);
            }
            let record = match decode(subject_tenant, published, &message.payload) {
                Ok(record) => record,
                Err(err) => {
                    eprintln!(
                        "nats source: nak of undecodable message on `{}` (delivery {delivered}): {err}",
                        message.subject
                    );
                    self.metrics.source_nak(tenant);
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

/// Decode one record, stamping `tenant` (from the subject) when the record carries none and
/// `published` (the JetStream publish time) when it carries no timestamp.
fn decode(
    tenant: Option<&str>,
    published: Option<u64>,
    payload: &[u8],
) -> Result<Record, serde_json::Error> {
    let mut record: Record = serde_json::from_slice(payload)?;
    if let Some(tenant) = tenant {
        stamp_tenant(&mut record, tenant);
    }
    if let Some(published) = published {
        stamp_ingestion_time(&mut record, published);
    }
    Ok(record)
}

/// Set `observed_time_unix_nano` to `published_unix_nanos` when the record has neither
/// `observed_time_unix_nano` nor `time_unix_nano`. A record that says when it was observed
/// or when it happened keeps its own word; only a record with no notion of time gets the
/// server's.
pub fn stamp_ingestion_time(record: &mut Record, published_unix_nanos: u64) {
    if record.observed_time_unix_nano.is_none() && record.time_unix_nano.is_none() {
        record.observed_time_unix_nano = Some(published_unix_nanos);
    }
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
