//! NATS JetStream source and sink.
//!
//! The source is a pull consumer with explicit ack; the engine settles each message through
//! the envelope's ack handle. The sink publishes one record per message and returns only once
//! JetStream has answered with a `PubAck`, so an acked source message means the record is
//! durably stored downstream.

pub mod config;
pub mod subject;
