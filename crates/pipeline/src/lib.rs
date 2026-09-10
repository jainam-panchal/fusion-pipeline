//! Pipeline binary wiring: the default stage registry and the startup path.

use std::path::Path;
use std::sync::Arc;

use fusion_core::config::{Config, ConfigError};
use fusion_core::engine::{Engine, EngineError};
use fusion_core::pipeline::Pipeline;
use fusion_core::registry::Registry;
use fusion_nats::{Nats, NatsError};

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
}

/// Load `path`, connect the NATS source and sinks, and run the engine until the source is
/// told to stop (Ctrl-C) and the workers have drained.
///
/// # Errors
///
/// Any [`StartError`]: an unreadable or invalid config, a missing stream or consumer, an
/// unreachable server, or an engine failure.
pub fn run(path: &Path) -> Result<(), StartError> {
    let yaml = std::fs::read_to_string(path).map_err(|source| StartError::ReadConfig {
        path: path.display().to_string(),
        source,
    })?;
    let config = Config::from_yaml(&yaml)?;
    let source_config = config.source.as_ref().ok_or(StartError::NoSource)?;

    let nats = Arc::new(Nats::new()?);
    let mut registry = default_registry();
    nats.register(&mut registry);

    let pipeline = Pipeline::compile(&config, &registry)?;
    let source = registry.build_source(source_config)?;
    let workers = pipeline.worker_count();

    nats.shutdown_on_ctrl_c();
    let engine = Engine::start(pipeline, source, workers)?;
    eprintln!("pipelined: running with {workers} workers; Ctrl-C to stop");
    engine.join()?;
    Ok(())
}
