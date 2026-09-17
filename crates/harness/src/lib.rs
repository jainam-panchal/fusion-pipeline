//! The loghub harness: a producer that replays vendored loghub lines into NATS and a
//! verifier that checks what the pipeline delivered against what the producer expected.

pub mod cli;
pub mod expect;
pub mod loghub;
pub mod plan;
pub mod verdict;
