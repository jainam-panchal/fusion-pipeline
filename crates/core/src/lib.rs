//! Pipeline core: record, config, DAG, engine, traits, condition grammar.

pub mod condition;
pub mod config;
pub mod engine;
pub mod memory;
pub mod record;
pub mod stage;
pub mod traits;

pub use record::{Kind, Record, RecordId};
