//! Wiring: the stage registry every binary and test uses, plus a one-call
//! loader from YAML to a runnable pipeline.

use pipeline_core::config::{self, ConfigError};
use pipeline_core::engine::{BuildError, Pipeline, SinkBindings};
use pipeline_core::stage::StageRegistry;

/// Every built-in stage type, keyed by config `type`.
pub fn registry() -> StageRegistry {
    let mut registry = StageRegistry::new();
    pipeline_stages::register_all(&mut registry);
    registry
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Build(#[from] BuildError),
}

/// Load a YAML config and compile it against the built-in registry, binding
/// each sink node to the given sink implementation.
pub fn load(yaml: &str, sinks: SinkBindings) -> Result<Pipeline, LoadError> {
    let cfg = config::load_str(yaml)?;
    Ok(Pipeline::build(cfg, &registry(), sinks)?)
}
