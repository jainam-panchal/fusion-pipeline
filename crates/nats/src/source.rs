//! `source: {type: nats}`: a JetStream pull consumer feeding the engine.
//!
//! ```yaml
//! source:
//!   type: nats
//!   url: nats://127.0.0.1:4222   # overridden by NATS_URL
//!   stream: LOGS
//!   consumer: pipeline           # pull, explicit ack, ack_wait 30s, max_deliver 5
//!   tenant_prefix: logs          # {tenant_prefix}.{tenant}.> names the tenant; the default
//! ```
//!
//! Each message is decoded as one JSON record and handed to the engine, untouched, with an
//! ack handle that acks or naks the JetStream message. What the transport says about the
//! message goes beside the record as its [`fusion_core::meta::Arrival`], built by
//! [`crate::headers::arrival`]: the subject's tenant (`{tenant_prefix}.{tenant}.>`), else an
//! upstream pipeline's `Fusion-Tenant`; an upstream pipeline's `Fusion-Ingestion-Time`, else
//! the JetStream publish time; and the delivery count. The engine resolves the record's
//! `Meta` from it alone (ADR 0005). Nothing is read from or written into the record. The
//! publish time is the server's and does not change on redelivery, so stateful stages that
//! measure windows in ingestion time see the same value every time the record comes back.
//!
//! A pipeline header that does not parse is ignored, reported on stderr and counted once on
//! `source_invalid_headers_total` under the tenant the record's `Meta` gets; the message is
//! walked as if the header were absent. A `Fusion-Tenant` is not read when the subject
//! names a valid tenant, so a header the subject overrides is never counted.
//!
//! A payload that is not a record is nak'd like any other failure and reported on stderr; it
//! runs out `max_deliver` the same way a record without an id does, which is where the
//! dead-letter ticket picks it up.
//!
//! Naks carry a delay. When the engine gives none, [`nak_delay`] derives one from the
//! message's delivery count: 1s on the first failure, doubling to [`MAX_NAK_DELAY`], so a
//! sink that is down does not burn through `max_deliver` in milliseconds.
//!
//! The engine counts a redelivered record on `source_redeliveries_total` under its `Meta`
//! tenant. A payload that does not decode has no record, so the source counts it itself,
//! the redelivery and the nak it issues (`source_naks_total`), under the tenant its arrival
//! gives, which is the tenant `Meta` would have had.

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::{AckKind, message::Acker};
use fusion_core::io::{AckHandle, Envelope, Failure, Intake, Source, SourceError};
use fusion_core::meta::Meta;
use fusion_core::metrics::Metrics;
use fusion_core::record::Record;
use futures::StreamExt;
use tokio::runtime::Runtime;
use tokio::sync::watch;

use crate::headers::{self, InvalidHeader, Received};

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
    /// The first token of the subjects that name a tenant.
    tenant_prefix: String,
}

impl NatsSource {
    pub(crate) fn new(
        runtime: Arc<Runtime>,
        consumer: PullConsumer,
        shutdown: watch::Receiver<bool>,
        metrics: Metrics,
        tenant_prefix: String,
    ) -> Self {
        Self {
            runtime,
            consumer,
            shutdown,
            metrics,
            tenant_prefix,
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
            let (arrival, invalid_headers) = headers::arrival(
                &self.tenant_prefix,
                Received {
                    subject: &message.subject,
                    headers: message.headers.as_ref(),
                    published,
                    delivered,
                },
            );
            // The tenant the engine will give the record, so every series the source counts
            // agrees with the record's others; for a payload that does not decode, the only
            // tenant there is.
            let tenant = Meta::tenant_of(&arrival);
            self.report_invalid_headers(&message.subject, &invalid_headers, &tenant);
            let record = match serde_json::from_slice::<Record>(&message.payload) {
                Ok(record) => record,
                Err(err) => {
                    if delivered > 1 {
                        self.metrics.source_redelivery(&tenant);
                    }
                    eprintln!(
                        "nats source: nak of undecodable message on `{}` (delivery {delivered}): {err}",
                        message.subject
                    );
                    self.metrics.source_nak(&tenant);
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
            intake.send(Envelope {
                record,
                arrival,
                ack,
            })?;
        }
    }

    /// Log and count every pipeline header the source ignored, under `tenant`.
    fn report_invalid_headers(&self, subject: &str, invalid: &[InvalidHeader], tenant: &str) {
        for problem in invalid {
            eprintln!("nats source: ignored a pipeline header on `{subject}`: {problem}");
            self.metrics.source_invalid_header(tenant);
        }
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

    fn nak(self: Box<Self>, delay: Option<Duration>, _failure: Failure) {
        self.settle(AckKind::Nak(Some(
            delay.unwrap_or_else(|| nak_delay(self.delivered)),
        )));
    }
}
