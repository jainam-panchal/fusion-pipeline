//! Built-in pipeline stages.

pub mod filter;

pub use filter::Filter;

/// Register every built-in stage under its config `type` name.
pub fn register_all(registry: &mut fusion_core::registry::Registry) {
    registry.register_stage("filter", Filter::build);
}
