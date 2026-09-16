//! Throughput of the compose pipeline's stages through the engine, into memory sinks.
//! Ignored: it measures rather than asserts. Run it on a release build:
//!
//!     cargo test --release -p fusion-pipeline --test throughput -- --ignored --nocapture
//!
//! It prints records per second for `RECORDS` records (default 100 000) on 4 workers. The
//! bodies alternate between syslog lines with an address (parsed, masked) and plain text
//! (a non-match), each distinct so the dedupe node keeps them all.

use std::sync::Arc;
use std::time::{Duration, Instant};

use fusion_core::engine::Engine;
use fusion_core::memory::{AckOutcome, MemorySinks, MemorySource, MemoryStateStore};
use fusion_core::metrics::Metrics;
use fusion_core::pipeline::Pipeline;
use fusion_core::record::Record;
use fusion_core::registry::Registry;
use fusion_core::signals::Signals;
use fusion_core::trace::{RecordTrace, TraceSampling, TraceSink};
use fusion_pipeline::default_registry;

const WORKERS: usize = 4;

fn pipeline_yaml() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/pipeline.yaml");
    std::fs::read_to_string(path).expect("deploy/pipeline.yaml is readable")
}

fn registry(sinks: &MemorySinks) -> Registry {
    let mut registry = default_registry();
    registry.register_sink("sink.nats", sinks.clone());
    registry
}

fn record(id: u64) -> Record {
    let body = if id % 2 == 0 {
        format!(
            "Jun 14 15:16:01 combo sshd(pam_unix)[{id}]: authentication failure; rhost=10.0.0.{}",
            id % 250
        )
    } else {
        format!("disk full on volume {id}")
    };
    Record::from_json(
        &serde_json::json!({"id": id, "severity_text": "ERROR", "body": body}).to_string(),
    )
    .expect("record parses")
}

/// Records per second for `records` records through a fresh engine with `signals`.
fn measure(records: u64, signals: impl Into<Signals>) -> f64 {
    let sinks = MemorySinks::new();
    let pipeline =
        Pipeline::from_yaml(&pipeline_yaml(), &registry(&sinks)).expect("pipeline loads");
    let (source, input) = MemorySource::new();
    let engine = Engine::start(
        pipeline,
        Box::new(source),
        WORKERS,
        signals,
        Arc::new(MemoryStateStore::new()),
    )
    .expect("engine starts");
    let batch: Vec<Record> = (0..records).map(record).collect();
    let started = Instant::now();
    let probes: Vec<_> = batch.into_iter().map(|r| input.push(r)).collect();
    for probe in probes {
        assert_eq!(probe.wait(Duration::from_secs(60)), Some(AckOutcome::Ack));
    }
    let elapsed = started.elapsed();
    drop(input);
    engine.join().expect("clean shutdown");
    records as f64 / elapsed.as_secs_f64()
}

fn records() -> u64 {
    std::env::var("RECORDS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(100_000)
}

#[test]
#[ignore = "a measurement; run on a release build"]
fn throughput_untraced() {
    // No metrics, events or traces are recorded: the engine and the stages alone.
    let records = records();
    for run in 1..=3 {
        let rate = measure(records, Metrics::noop());
        println!("untraced, run {run}: {rate:.0} records/s");
    }
}

/// A trace sink that drops what it is given, so the measurement is the engine's cost of
/// tracing, not a store's.
struct Discard;

impl TraceSink for Discard {
    fn export(&self, _: RecordTrace) {}
}

#[test]
#[ignore = "a measurement; run on a release build"]
fn throughput_tracing_one_percent() {
    let records = records();
    for run in 1..=3 {
        let signals = Signals::new(Metrics::noop()).with_traces(Discard, TraceSampling::default());
        let rate = measure(records, signals);
        println!("tracing at 1%, run {run}: {rate:.0} records/s");
    }
}
