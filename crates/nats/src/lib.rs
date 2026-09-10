//! NATS JetStream source and sink.
//!
//! The source is a pull consumer with explicit ack; the engine settles each message through
//! the envelope's ack handle. The sink publishes one record per message and returns only once
//! JetStream has answered with a `PubAck`, so an acked source message means the record is
//! durably stored downstream.
//!
//! Both sit on one tokio runtime owned by [`Nats`]. The engine's threads are synchronous, so
//! the source drives its receive loop with `block_on` from the source thread and the sink
//! awaits its `PubAck`s with `block_on` from the worker that wrote.

pub mod config;
pub mod sink;
pub mod source;
pub mod subject;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_nats::jetstream::context::{ConsumerInfoErrorKind, GetStreamErrorKind};
use async_nats::jetstream::{self, ErrorCode, consumer::PullConsumer};
use fusion_core::config::{NodeConfig, SourceConfig};
use fusion_core::io::{Sink, Source};
use fusion_core::registry::Registry;
use tokio::runtime::Runtime;
use tokio::sync::watch;

pub use config::{SinkParams, SourceParams};
pub use sink::NatsSink;
pub use source::NatsSource;

/// Registry type name of the source (`source: {type: nats}`).
pub const SOURCE_TYPE: &str = "nats";
/// Registry type name of the sink node (`type: sink.nats`).
pub const SINK_TYPE: &str = "sink.nats";

/// How long the sink waits for a `PubAck` before treating the write as failed.
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a connection attempt may take before it is reported as a failure.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Errors from setting up or running against JetStream.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NatsError {
    /// The tokio runtime could not be created.
    #[error("could not start the NATS I/O runtime: {0}")]
    Runtime(#[source] std::io::Error),
    /// The server at `url` could not be reached.
    #[error("could not connect to NATS at {url}: {source}")]
    Connect {
        /// The URL that was tried.
        url: String,
        /// The connection error.
        #[source]
        source: async_nats::ConnectError,
    },
    /// The named stream does not exist on the server.
    #[error("stream `{stream}` does not exist at {url}; create it before starting the pipeline")]
    StreamMissing {
        /// The stream that was looked up.
        stream: String,
        /// The server it was looked up on.
        url: String,
    },
    /// The named consumer does not exist on the stream.
    #[error(
        "consumer `{consumer}` does not exist on stream `{stream}` at {url}; create it (pull, explicit ack) before starting the pipeline"
    )]
    ConsumerMissing {
        /// The stream the consumer was looked up on.
        stream: String,
        /// The consumer that was looked up.
        consumer: String,
        /// The server it was looked up on.
        url: String,
    },
    /// Any other JetStream API failure while setting up.
    #[error("JetStream request failed at {url}: {message}")]
    Request {
        /// The server the request went to.
        url: String,
        /// What the server or client reported.
        message: String,
    },
}

/// Shared runtime and connections for every NATS source and sink in a process.
///
/// Connections are cached per URL, so several sinks on one server share one connection.
/// Dropping `Nats` after the engine has joined shuts the runtime down; keep it alive for as
/// long as any source or sink built from it may run.
#[derive(Debug)]
pub struct Nats {
    runtime: Arc<Runtime>,
    connections: Mutex<BTreeMap<String, jetstream::Context>>,
    shutdown: watch::Sender<bool>,
}

impl Nats {
    /// Start the I/O runtime.
    ///
    /// # Errors
    ///
    /// [`NatsError::Runtime`] when the runtime threads cannot be spawned.
    pub fn new() -> Result<Self, NatsError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("nats-io")
            .enable_all()
            .build()
            .map_err(NatsError::Runtime)?;
        let (shutdown, _) = watch::channel(false);
        Ok(Self {
            runtime: Arc::new(runtime),
            connections: Mutex::new(BTreeMap::new()),
            shutdown,
        })
    }

    /// Register the `nats` source and the `sink.nats` node type. URLs in the YAML are
    /// overridden by `NATS_URL`.
    pub fn register(self: &Arc<Self>, registry: &mut Registry) {
        let for_source = Arc::clone(self);
        registry.register_source(SOURCE_TYPE, move |source: &SourceConfig| {
            let params: SourceParams = source.parse_params()?;
            for_source
                .source(&params)
                .map(|s| Box::new(s) as Box<dyn Source>)
                .map_err(|e| source.invalid_params(e.to_string()))
        });
        let for_sink = Arc::clone(self);
        registry.register_sink(SINK_TYPE, move |node: &NodeConfig| {
            let params: SinkParams = node.parse_params()?;
            for_sink
                .sink(&params)
                .map(|s| Box::new(s) as Box<dyn Sink>)
                .map_err(|e| node.invalid_params(e.to_string()))
        });
    }

    /// Ask every source built from this runtime to stop after the message it is on. The
    /// engine then drains and [`fusion_core::engine::Engine::join`] returns.
    pub fn shutdown(&self) {
        self.shutdown.send_replace(true);
    }

    /// Call [`Nats::shutdown`] when the process receives Ctrl-C (SIGINT).
    pub fn shutdown_on_ctrl_c(&self) {
        let shutdown = self.shutdown.clone();
        self.runtime.spawn(async move {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    eprintln!("nats source: stopping on Ctrl-C");
                    shutdown.send_replace(true);
                }
                Err(err) => eprintln!("nats source: cannot listen for Ctrl-C: {err}"),
            }
        });
    }

    /// Build the source for `params`, failing if the server, stream or consumer is missing.
    ///
    /// # Errors
    ///
    /// [`NatsError::Connect`], [`NatsError::StreamMissing`] or [`NatsError::ConsumerMissing`].
    pub fn source(&self, params: &SourceParams) -> Result<NatsSource, NatsError> {
        let url = config::url_from_env(params.url.as_deref());
        let context = self.connect(&url)?;
        let consumer: PullConsumer = self.runtime.block_on(async {
            let stream = context
                .get_stream(&params.stream)
                .await
                .map_err(|e| stream_error(e.kind(), &e.to_string(), &params.stream, &url))?;
            // `consumer_info` reports "not found" as a typed kind; `get_consumer` does not.
            stream.consumer_info(&params.consumer).await.map_err(|e| {
                if matches!(e.kind(), ConsumerInfoErrorKind::NotFound) {
                    NatsError::ConsumerMissing {
                        stream: params.stream.clone(),
                        consumer: params.consumer.clone(),
                        url: url.clone(),
                    }
                } else {
                    request_error(&url, &e)
                }
            })?;
            stream
                .get_consumer(&params.consumer)
                .await
                .map_err(|e| request_error(&url, &e))
        })?;
        Ok(NatsSource::new(
            Arc::clone(&self.runtime),
            consumer,
            self.shutdown.subscribe(),
        ))
    }

    /// Build the sink for `params`, failing if the server or stream is missing.
    ///
    /// # Errors
    ///
    /// [`NatsError::Connect`] or [`NatsError::StreamMissing`].
    pub fn sink(&self, params: &SinkParams) -> Result<NatsSink, NatsError> {
        let url = config::url_from_env(params.url.as_deref());
        let context = self.connect(&url)?;
        self.runtime.block_on(async {
            context
                .get_stream(&params.stream)
                .await
                .map_err(|e| stream_error(e.kind(), &e.to_string(), &params.stream, &url))
        })?;
        Ok(NatsSink::new(
            Arc::clone(&self.runtime),
            context,
            params.stream.clone(),
            params.subject.clone(),
        ))
    }

    fn connect(&self, url: &str) -> Result<jetstream::Context, NatsError> {
        let mut connections = self
            .connections
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(context) = connections.get(url) {
            return Ok(context.clone());
        }
        let client = self
            .runtime
            .block_on(
                async_nats::ConnectOptions::new()
                    .connection_timeout(CONNECT_TIMEOUT)
                    .connect(url),
            )
            .map_err(|source| NatsError::Connect {
                url: url.to_owned(),
                source,
            })?;
        // The context spawns its ack bookkeeping task on build, so build it on the runtime.
        let context = self.runtime.block_on(async {
            jetstream::ContextBuilder::new()
                .timeout(PUBLISH_TIMEOUT)
                .build(client)
        });
        connections.insert(url.to_owned(), context.clone());
        Ok(context)
    }
}

fn stream_error(kind: GetStreamErrorKind, message: &str, stream: &str, url: &str) -> NatsError {
    match kind {
        GetStreamErrorKind::JetStream(err) if err.error_code() == ErrorCode::STREAM_NOT_FOUND => {
            NatsError::StreamMissing {
                stream: stream.to_owned(),
                url: url.to_owned(),
            }
        }
        _ => NatsError::Request {
            url: url.to_owned(),
            message: message.to_owned(),
        },
    }
}

fn request_error(url: &str, err: &dyn std::fmt::Display) -> NatsError {
    NatsError::Request {
        url: url.to_owned(),
        message: err.to_string(),
    }
}
