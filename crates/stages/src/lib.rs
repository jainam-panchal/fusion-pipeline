//! Built-in pipeline stages.

mod condition;
pub mod dedupe;
pub mod extract;
pub mod filter;
mod regex;
pub mod route;

pub use dedupe::Dedupe;
pub use extract::Extract;
pub use filter::Filter;
pub use route::Route;

/// Register every built-in stage under its config `type` name.
pub fn register_all(registry: &mut fusion_core::registry::Registry) {
    registry.register_stage("filter", Filter::build);
    registry.register_stage(fusion_core::route::ROUTE_KIND, Route::build);
    registry.register_stage("dedupe", Dedupe::build);
    registry.register_stage("pcre2_extract", Extract::build);
}
