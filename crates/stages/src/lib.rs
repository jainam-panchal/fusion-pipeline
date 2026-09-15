//! Built-in pipeline stages.

mod condition;
pub mod dedupe;
pub mod extract;
pub mod filter;
mod key_hash;
pub mod redact;
mod regex_stage;
pub mod route;
pub mod sample;

pub use dedupe::Dedupe;
pub use extract::Extract;
pub use filter::Filter;
pub use redact::Redact;
pub use route::Route;
pub use sample::Sample;

/// Register every built-in stage under its config `type` name.
pub fn register_all(registry: &mut fusion_core::registry::Registry) {
    registry.register_stage("filter", Filter::build);
    registry.register_stage(fusion_core::route::ROUTE_KIND, Route::build);
    registry.register_stage("dedupe", Dedupe::build);
    registry.register_stage("extract", Extract::build);
    registry.register_stage("redact", Redact::build);
    registry.register_stage("sample", Sample::build);
}
