//! Pipeline core: record model, config loader, DAG validation, engine, stage and I/O traits,
//! and the condition grammar.

pub mod condition;
pub mod config;
pub mod dag;
pub mod engine;
pub mod io;
pub mod memory;
pub mod meta;
pub mod metrics;
pub mod path;
pub mod pipeline;
pub mod record;
pub mod registry;
pub mod route;
pub mod stage;
pub mod state;
