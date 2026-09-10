//! `sink.nats`: publish each record to a subject and wait for its `PubAck`.
//!
//! ```yaml
//! - id: out
//!   type: sink.nats
//!   url: nats://127.0.0.1:4222   # overridden by NATS_URL
//!   stream: PROCESSED            # must capture `subject`; checked at load
//!   subject: processed.logs
//! ```

use std::sync::Arc;

use async_nats::jetstream;
use fusion_core::io::{Sink, SinkError};
use fusion_core::record::Record;
use tokio::runtime::Runtime;

/// A JetStream sink. Build one through [`crate::Nats::sink`].
#[derive(Debug)]
pub struct NatsSink {
    runtime: Arc<Runtime>,
    context: jetstream::Context,
    stream: String,
    subject: String,
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
    ) -> Self {
        Self {
            runtime,
            context,
            stream,
            subject,
        }
    }

    fn publish_error(&self, source: jetstream::context::PublishError) -> WriteError {
        WriteError::Publish {
            subject: self.subject.clone(),
            stream: self.stream.clone(),
            source,
        }
    }

    async fn publish_all(&self, records: &[Record]) -> Result<(), WriteError> {
        // Send every publish first, then wait for the acks, so a batch costs one round trip.
        let mut acks = Vec::with_capacity(records.len());
        for record in records {
            let payload = record.to_json()?;
            let ack = self
                .context
                .publish(self.subject.clone(), payload.into())
                .await
                .map_err(|e| self.publish_error(e))?;
            acks.push(ack);
        }
        for ack in acks {
            ack.await.map_err(|e| self.publish_error(e))?;
        }
        Ok(())
    }
}

impl Sink for NatsSink {
    fn write(&self, records: &[Record]) -> Result<(), SinkError> {
        self.runtime
            .block_on(self.publish_all(records))
            .map_err(SinkError::new)
    }
}
