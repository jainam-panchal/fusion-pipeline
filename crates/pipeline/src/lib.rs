//! Pipeline binary wiring: the default stage registry and the startup path.

use fusion_core::registry::Registry;

/// The registry the binary runs with: every built-in stage. Sinks are registered by the
/// caller, since they depend on the deployment (NATS in production, memory in tests).
#[must_use]
pub fn default_registry() -> Registry {
    let mut registry = Registry::new();
    fusion_stages::register_all(&mut registry);
    registry
}
