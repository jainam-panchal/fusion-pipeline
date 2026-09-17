//! `source: {type: nats}`: a JetStream pull consumer feeding the engine.
//!
//! ```yaml
//! source:
//!   type: nats
//!   url: nats://127.0.0.1:4222   # overridden by NATS_URL
//!   stream: LOGS
//!   consumer: pipeline           # pull, explicit ack, ack_wait 30s, max_deliver 5, no backoff
//!   tenant_prefix: logs          # {tenant_prefix}.{tenant}.> names the tenant; the default
//!   dlq_prefix: dlq              # dead letters go to {dlq_prefix}.{tenant}; the default
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
//! runs out `max_deliver` the same way a record without an id does.
//!
//! Naks carry a delay. When the engine gives none, [`nak_delay`] derives one from the
//! message's delivery count: 1s on the first failure, doubling to [`MAX_NAK_DELAY`], so a
//! sink that is down does not burn through `max_deliver` in milliseconds.
//!
//! The dead-letter queue. A nak on the message's final delivery ([`is_final_delivery`]
//! under the consumer's `max_deliver`, read at startup) is not a nak: the source publishes
//! the message as it arrived to `{dlq_prefix}.{tenant}` with the headers
//! [`crate::headers::for_dead_letter`] writes, waits for the `PubAck`, and terminates the
//! message with the reason `dlq {node}: {kind}`, which JetStream puts on its
//! `MSG_TERMINATED` advisory. It counts `dlq_total{tenant, stage, reason}`. The publish is
//! tried [`DEAD_LETTER_RETRIES`] more times; when every try fails the message is nakked
//! with no delay, so JetStream gives up on it at once with a `MAX_DELIVERIES` advisory, and
//! `dlq_publish_errors_total` counts it. The message is then only in its stream, which the
//! stream sequence on the `dead_letter_failed` event finds. A stored dead letter is logged as
//! a `dead_letter` event, with the record id the engine's failure names and the record's
//! trace. Either way `dlq_publish_duration_seconds` times every
//! try together. A delivery whose message info the source could not read is never taken
//! for the final one. An undecodable payload takes the same path from the receive loop,
//! which it holds up while the tries last.
//!
//! The engine counts a redelivered record on `source_redeliveries_total` under its `Meta`
//! tenant. A payload that does not decode has no record, so the source counts it itself,
//! the redelivery and the nak it issues (`source_naks_total`), under the tenant its arrival
//! gives, which is the tenant `Meta` would have had, and its bytes on `bytes_in_total`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_nats::Subject;
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::{self, AckKind, message::Acker};
use fusion_core::events::{Event, EventKind};
use fusion_core::io::{AckHandle, Envelope, Failure, FailureKind, Intake, Source, SourceError};
use fusion_core::meta::{IngestionTime, Meta};
use fusion_core::record::Record;
use fusion_core::signals::Signals;
use fusion_core::trace::TraceKey;
use futures::StreamExt;
use tokio::runtime::Runtime;
use tokio::sync::watch;

use crate::headers::{self, DeadLetter, InvalidHeader, Received};
use crate::subject;

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

/// The delivery limit of a consumer whose `max_deliver` is `max_deliver`, `None` when it has
/// none. The server stores 0 as -1, unlimited, and async-nats reads a missing field as 0, so
/// only a positive value is a limit.
#[must_use]
pub fn delivery_limit(max_deliver: i64) -> Option<u64> {
    u64::try_from(max_deliver).ok().filter(|limit| *limit > 0)
}

/// Whether the `delivered`-th delivery of a message is its last under `limit`: JetStream
/// never delivers it again once the count has reached the limit.
#[must_use]
pub fn is_final_delivery(delivered: u64, limit: u64) -> bool {
    delivered >= limit
}

/// How many more times a failed dead-letter publish is tried, and the pause before each.
pub const DEAD_LETTER_RETRIES: [Duration; 3] = [
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_millis(1000),
];

/// Where a source sends the messages it gives up on.
#[derive(Debug)]
pub(crate) struct DeadLetters {
    context: jetstream::Context,
    /// The first token of the dead-letter subjects.
    prefix: String,
    /// The consumer's `max_deliver`.
    limit: u64,
    signals: Signals,
}

impl DeadLetters {
    pub(crate) fn new(
        context: jetstream::Context,
        prefix: String,
        limit: u64,
        signals: Signals,
    ) -> Self {
        Self {
            context,
            prefix,
            limit,
            signals,
        }
    }

    /// Log `kind` about the dead letter of `delivery` for `failure`.
    fn log(
        &self,
        kind: EventKind,
        delivery: &Delivery,
        position: &Position,
        failure: &Failure,
        message: String,
    ) {
        self.signals.emit(Event {
            stream_sequence: Some(position.stream_sequence),
            message,
            // The delivery failed, so the engine kept its trace whenever anything traces.
            trace: failure
                .record_id
                .filter(|_| self.signals.tracing())
                .map(|id| TraceKey::new(id, &delivery.tenant).delivery_context(position.delivered)),
            ..Event::of_failure(
                kind,
                delivery.tenant.as_str().into(),
                failure,
                position.delivered,
            )
        });
    }

    /// Settle a failed `delivery`: dead-letter and terminate it on its final delivery,
    /// otherwise nak it after `delay`, or after [`nak_delay`] when none is given.
    async fn settle_failure(
        &self,
        delivery: &Delivery,
        acker: &Acker,
        delay: Option<Duration>,
        failure: Failure,
    ) {
        let Some(position) = delivery
            .position
            .as_ref()
            .filter(|position| is_final_delivery(position.delivered, self.limit))
        else {
            let delivered = delivery.position.as_ref().map_or(1, |p| p.delivered);
            let nak = AckKind::Nak(Some(delay.unwrap_or_else(|| nak_delay(delivered))));
            settle(acker, nak).await;
            return;
        };
        let started = Instant::now();
        let published = self.publish(delivery, position, &failure).await;
        let metrics = self.signals.metrics();
        metrics.dlq_publish_duration(&delivery.tenant, started.elapsed());
        match published {
            Ok(()) => {
                // Counted on the `PubAck`, before the terminate: the dead letter is stored,
                // whatever becomes of the terminate.
                metrics.dead_lettered(&delivery.tenant, &failure.node, failure.kind);
                self.log(
                    EventKind::DeadLetter,
                    delivery,
                    position,
                    &failure,
                    failure.error.clone(),
                );
                self.terminate(delivery, acker, &failure).await;
            }
            Err(err) => {
                self.log(
                    EventKind::DeadLetterFailed,
                    delivery,
                    position,
                    &failure,
                    format!(
                        "could not dead-letter message {} of stream `{}` (`{}`), left in the \
                         stream: {err}; it failed with: {}",
                        position.stream_sequence,
                        position.stream,
                        delivery.message.subject,
                        failure.error
                    ),
                );
                metrics.dlq_publish_error(&delivery.tenant);
                // No delay: JetStream gives up on the message now rather than after one.
                settle(acker, AckKind::Nak(None)).await;
            }
        }
    }

    /// Publish `delivery`'s dead letter and wait for its `PubAck`, retrying on failure.
    async fn publish(
        &self,
        delivery: &Delivery,
        position: &Position,
        failure: &Failure,
    ) -> Result<(), jetstream::context::PublishError> {
        let subject = subject::dead_letter(&self.prefix, &delivery.tenant);
        let headers = headers::for_dead_letter(&DeadLetter {
            stream: &position.stream,
            stream_sequence: position.stream_sequence,
            subject: &delivery.message.subject,
            headers: delivery.message.headers.as_ref(),
            tenant: &delivery.tenant,
            ingestion_time: delivery.ingestion_time,
            failure,
        });
        let mut pauses = DEAD_LETTER_RETRIES.iter();
        loop {
            let attempt = async {
                self.context
                    .publish_with_headers(
                        subject.clone(),
                        headers.clone(),
                        delivery.message.payload.clone(),
                    )
                    .await?
                    .await
            };
            match (attempt.await, pauses.next()) {
                (Ok(_), _) => return Ok(()),
                (Err(err), None) => return Err(err),
                (Err(_), Some(pause)) => tokio::time::sleep(*pause).await,
            }
        }
    }

    /// Terminate `delivery` with the reason `dlq {node}: {kind}`. The client's own
    /// terminate carries no reason, so the reply is written by hand when there is one.
    async fn terminate(&self, delivery: &Delivery, acker: &Acker, failure: &Failure) {
        let Some(reply) = delivery.reply.clone() else {
            settle(acker, AckKind::Term).await;
            return;
        };
        let body = format!("+TERM dlq {}: {}", failure.node, failure.kind);
        if let Err(err) = self.context.client().publish(reply, body.into()).await {
            // The dead letter is stored. A lost terminate leaves the message unsettled on
            // its final delivery, so JetStream does not deliver it again: once `ack_wait`
            // runs out it gives up on the message with a `MAX_DELIVERIES` advisory.
            eprintln!("nats source: could not terminate a dead-lettered message: {err}");
        }
    }
}

/// Settle a message, reporting a failure: a lost ack or nak redelivers after `ack_wait`.
async fn settle(acker: &Acker, kind: AckKind) {
    if let Err(err) = acker.ack_with(kind).await {
        eprintln!("nats source: could not settle message: {err}");
    }
}

/// Where a message sits in its stream and consumer, from its JetStream message info.
#[derive(Debug)]
struct Position {
    stream: String,
    stream_sequence: u64,
    /// How many times JetStream has delivered the message, this one included.
    delivered: u64,
}

/// What a source keeps of one delivered message to settle it: all a dead letter needs. The
/// payload is reference-counted, so keeping it costs no copy, but keeps it alive until the
/// message is settled.
#[derive(Debug)]
struct Delivery {
    /// The message as it arrived: subject, headers and payload.
    message: async_nats::Message,
    /// The tenant the record's `Meta` gets.
    tenant: String,
    ingestion_time: Option<IngestionTime>,
    /// The subject a settlement is published to.
    reply: Option<Subject>,
    /// `None` when the message info could not be read; the delivery is then never final.
    position: Option<Position>,
}

/// A JetStream source. Build one through [`crate::Nats::source`].
#[derive(Debug)]
pub struct NatsSource {
    runtime: Arc<Runtime>,
    consumer: PullConsumer,
    shutdown: watch::Receiver<bool>,
    signals: Signals,
    /// The first token of the subjects that name a tenant.
    tenant_prefix: String,
    dead_letters: Arc<DeadLetters>,
}

impl NatsSource {
    pub(crate) fn new(
        runtime: Arc<Runtime>,
        consumer: PullConsumer,
        shutdown: watch::Receiver<bool>,
        signals: Signals,
        tenant_prefix: String,
        dead_letters: DeadLetters,
    ) -> Self {
        Self {
            runtime,
            consumer,
            shutdown,
            signals,
            tenant_prefix,
            dead_letters: Arc::new(dead_letters),
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
            let info = match message.info() {
                Ok(info) => Some(info),
                Err(err) => {
                    eprintln!(
                        "nats source: no message info on `{}`, so this delivery is not taken \
                         for the final one: {err}",
                        message.subject
                    );
                    None
                }
            };
            let position = info.as_ref().and_then(|info| {
                Some(Position {
                    stream: info.stream.to_owned(),
                    stream_sequence: info.stream_sequence,
                    delivered: u64::try_from(info.delivered).ok()?,
                })
            });
            let delivered = position.as_ref().map_or(1, |p| p.delivered);
            let published = info
                .as_ref()
                .and_then(|info| u64::try_from(info.published.unix_timestamp_nanos()).ok());
            let reply = message.message.reply.clone();
            let (message, acker) = message.split();
            let (arrival, invalid_headers) = headers::arrival(
                &self.tenant_prefix,
                Received {
                    subject: &message.subject,
                    headers: message.headers.as_ref(),
                    published,
                    delivered,
                    bytes: message.payload.len() as u64,
                },
            );
            // The tenant the engine will give the record, so every series the source counts
            // agrees with the record's others; for a payload that does not decode, the only
            // tenant there is.
            let tenant = Meta::tenant_of(&arrival);
            self.report_invalid_headers(&message.subject, &invalid_headers, &tenant);
            let decoded = serde_json::from_slice::<Record>(&message.payload);
            let delivery = Delivery {
                message,
                tenant: tenant.to_string(),
                ingestion_time: arrival.ingestion_time,
                reply,
                position,
            };
            let record = match decoded {
                Ok(record) => record,
                Err(err) => {
                    if let Some(bytes) = arrival.bytes {
                        self.signals.metrics().bytes_in(&tenant, bytes);
                    }
                    if delivered > 1 {
                        self.signals.metrics().source_redelivery(&tenant);
                    }
                    eprintln!(
                        "nats source: nak of undecodable message on `{}` (delivery {delivered}): {err}",
                        delivery.message.subject
                    );
                    self.signals.metrics().source_nak(&tenant);
                    let failure = Failure::at_source(FailureKind::Undecodable, err.to_string());
                    // Settled here, on the runtime: `NatsAck` blocks on the runtime and
                    // cannot be used from inside it.
                    self.dead_letters
                        .settle_failure(&delivery, &acker, None, failure)
                        .await;
                    continue;
                }
            };
            let ack = Box::new(NatsAck {
                runtime: Arc::clone(&self.runtime),
                acker,
                dead_letters: Arc::clone(&self.dead_letters),
                delivery,
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
            self.signals.metrics().source_invalid_header(tenant);
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
    dead_letters: Arc<DeadLetters>,
    delivery: Delivery,
}

impl AckHandle for NatsAck {
    fn ack(self: Box<Self>) {
        self.runtime.block_on(settle(&self.acker, AckKind::Ack));
    }

    fn nak(self: Box<Self>, delay: Option<Duration>, failure: Failure) {
        self.runtime.block_on(self.dead_letters.settle_failure(
            &self.delivery,
            &self.acker,
            delay,
            failure,
        ));
    }
}
