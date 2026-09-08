//! Built-in stages, registered by config `type`.

use pipeline_core::stage::StageRegistry;

pub mod filter;

/// Register every built-in stage type.
pub fn register_all(registry: &mut StageRegistry) {
    registry.register("filter", filter::Filter::build);
}
