//! `sink.nats`: publish each record to a subject and wait for its `PubAck`.
//!
//! ```yaml
//! - id: out
//!   type: sink.nats
//!   url: nats://127.0.0.1:4222   # overridden by NATS_URL
//!   stream: PROCESSED            # must capture `subject`; checked at load
//!   subject: processed.logs
//! ```
//!
//! Each message carries the record as the last stage left it, and its `Meta` as the
//! [`crate::headers`]: nothing is written into the record.
//!
//! A batch is all-or-nothing from the engine's point of view: if any record's `PubAck` is
//! missing the whole write fails and the source message is nak'd, so records published
//! earlier in that batch are delivered again. That is the at-least-once contract.

use std::sync::Arc;

use async_nats::jetstream;
use fusion_core::io::{Outgoing, Sink, SinkError};
use tokio::runtime::Runtime;

use crate::codec::Encoding;
use crate::headers;

/// A JetStream sink. Build one through [`crate::Nats::sink`].
#[derive(Debug)]
pub struct NatsSink {
    runtime: Arc<Runtime>,
    context: jetstream::Context,
    stream: String,
    subject: String,
    /// How a record becomes a payload.
    encoding: Encoding,
}

/// Why a write did not get its `PubAck`.
#[derive(Debug, thiserror::Error)]
enum WriteError {
    #[error("record could not be serialized: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("publish to `{subject}` (stream `{stream}`) was not acknowledged: {source}")]
    Publish {
        subject: String,
        stream: String,
        #[source]
        source: jetstream::context::PublishError,
    },
}

impl NatsSink {
    pub(crate) fn new(
        runtime: Arc<Runtime>,
        context: jetstream::Context,
        stream: String,
        subject: String,
        encoding: Encoding,
    ) -> Self {
        Self {
            runtime,
            context,
            stream,
            subject,
            encoding,
        }
    }

    fn publish_error(&self, source: jetstream::context::PublishError) -> WriteError {
        WriteError::Publish {
            subject: self.subject.clone(),
            stream: self.stream.clone(),
            source,
        }
    }

    /// Publish every record of `batch` and wait for every `PubAck`; the payload bytes
    /// written.
    async fn publish_all(&self, batch: &[Outgoing<'_>]) -> Result<u64, WriteError> {
        // Send every publish first, then wait for the acks, so a batch costs one round trip.
        let mut acks = Vec::with_capacity(batch.len());
        let mut bytes = 0;
        for outgoing in batch {
            let payload = self.encoding.encode(outgoing.record)?;
            bytes += payload.len() as u64;
            let ack = self
                .context
                .publish_with_headers(
                    self.subject.clone(),
                    headers::for_meta(outgoing.meta),
                    payload.into(),
                )
                .await
                .map_err(|e| self.publish_error(e))?;
            acks.push(ack);
        }
        for ack in acks {
            ack.await.map_err(|e| self.publish_error(e))?;
        }
        Ok(bytes)
    }
}

impl Sink for NatsSink {
    fn write(&self, batch: &[Outgoing<'_>]) -> Result<u64, SinkError> {
        self.runtime
            .block_on(self.publish_all(batch))
            .map_err(SinkError::new)
    }
}
