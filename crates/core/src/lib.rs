//! Pipeline core: record, config, DAG, engine, traits, condition grammar.

pub mod condition;
pub mod config;
pub mod record;

pub use record::{Kind, Record, RecordId};
