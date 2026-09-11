//! Pipeline binary wiring: the default stage registry and the startup path.

use std::path::Path;
use std::sync::Arc;

use fusion_core::config::{Config, ConfigError};
use fusion_core::engine::{Engine, EngineError};
use fusion_core::metrics::Metrics;
use fusion_core::pipeline::Pipeline;
use fusion_core::registry::Registry;
use fusion_nats::{Nats, NatsError};
use fusion_otel::OtelError;

/// The registry the binary runs with: every built-in stage. Sinks and the source are
/// registered by the caller, since they depend on the deployment (NATS in production, memory
/// in tests).
#[must_use]
pub fn default_registry() -> Registry {
    let mut registry = Registry::new();
    fusion_stages::register_all(&mut registry);
    registry
}

/// Errors from starting the binary.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StartError {
    /// The config file could not be read.
    #[error("could not read config `{path}`: {source}")]
    ReadConfig {
        /// The path that was tried.
        path: String,
        /// The I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The config has no `source` block.
    #[error("config has no `source` block; the binary needs `source: {{type: nats, ...}}`")]
    NoSource,
    /// The config did not load, validate or compile.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// The NATS runtime could not start.
    #[error(transparent)]
    Nats(#[from] NatsError),
    /// The engine could not start.
    #[error(transparent)]
    Engine(#[from] EngineError),
    /// The OTLP exporter could not start or flush.
    #[error(transparent)]
    Telemetry(#[from] OtelError),
    /// The Ctrl-C listener could not be set up.
    #[error("could not set up the Ctrl-C handler: {0}")]
    Signals(#[source] std::io::Error),
}

/// First Ctrl-C stops the source so the workers drain; a second one exits at once, since the
/// drain can stall behind a sink that is not answering.
///
/// Draining settles only the envelopes already handed to workers. Messages the consumer had
/// pulled into its batch but not yet decoded are dropped unsettled and redeliver after
/// `ack_wait`, with their delivery count bumped, so the next run may start with
/// `num_redelivered > 0` and a longer first nak delay for those messages.
fn stop_on_ctrl_c(nats: Arc<Nats>) -> Result<(), StartError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .map_err(StartError::Signals)?;
    std::thread::Builder::new()
        .name("pipeline-signals".to_owned())
        .spawn(move || {
            runtime.block_on(async {
                if let Err(err) = tokio::signal::ctrl_c().await {
                    eprintln!("pipelined: cannot listen for Ctrl-C: {err}");
                    return;
                }
                eprintln!("pipelined: stopping on Ctrl-C; press again to exit without draining");
                nats.shutdown();
                if tokio::signal::ctrl_c().await.is_ok() {
                    std::process::exit(130);
                }
            });
        })
        .map_err(StartError::Signals)?;
    Ok(())
}

/// Load `path`, connect the NATS source and sinks, and run the engine until the source is
/// told to stop (Ctrl-C) and the workers have drained.
///
/// Metrics go over OTLP to the collector `OTEL_EXPORTER_OTLP_ENDPOINT` names; with no
/// endpoint in the environment nothing is exported.
///
/// # Errors
///
/// Any [`StartError`]: an unreadable or invalid config, a missing stream or consumer, an
/// unreachable server, an unusable OTLP configuration, or an engine failure.
pub fn run(path: &Path) -> Result<(), StartError> {
    let yaml = std::fs::read_to_string(path).map_err(|source| StartError::ReadConfig {
        path: path.display().to_string(),
        source,
    })?;
    let config = Config::from_yaml(&yaml)?;
    let source_config = config.source.as_ref().ok_or(StartError::NoSource)?;

    let telemetry = fusion_otel::init()?;
    let metrics = telemetry
        .as_ref()
        .map_or_else(Metrics::noop, fusion_otel::Telemetry::metrics);
    let nats = Arc::new(Nats::new(metrics.clone())?);
    let mut registry = default_registry();
    nats.register(&mut registry);

    let pipeline = Pipeline::compile(&config, &registry)?;
    let source = registry.build_source(source_config)?;
    let workers = pipeline.worker_count();

    stop_on_ctrl_c(Arc::clone(&nats))?;
    let engine = Engine::start(pipeline, source, workers, metrics)?;
    eprintln!(
        "pipelined: running with {workers} workers, metrics {}; Ctrl-C to stop",
        if telemetry.is_some() {
            "over OTLP"
        } else {
            "off"
        }
    );
    engine.join()?;
    if let Some(telemetry) = telemetry {
        telemetry.shutdown()?;
    }
    Ok(())
}
