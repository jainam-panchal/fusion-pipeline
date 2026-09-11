//! The engine harness the trait-boundary tests share: a YAML config compiled with the
//! default registry plus an in-memory sink, an in-memory source to push envelopes through,
//! an in-memory state store, and the sinks to assert on.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use fusion_core::engine::Engine;
use fusion_core::memory::{MemoryInput, MemorySinks, MemorySource, MemoryStateStore};
use fusion_core::metrics::{InMemoryRecorder, Metric, Metrics};
use fusion_core::pipeline::Pipeline;
use fusion_core::registry::Registry;
use fusion_pipeline::default_registry;

/// How long a test waits for an ack handle to settle.
pub const WAIT: Duration = Duration::from_secs(5);

/// A running engine with its in-memory source, sinks, state store and metrics recorder.
pub struct Harness {
    pub engine: Engine,
    pub source: MemoryInput,
    pub sinks: MemorySinks,
    pub state: MemoryStateStore,
    pub recorder: InMemoryRecorder,
}

/// The default registry with `sink.memory` writing to `sinks`.
pub fn registry(sinks: &MemorySinks) -> Registry {
    let mut registry = default_registry();
    registry.register_sink("sink.memory", sinks.clone());
    registry
}

/// Compile `yaml` with the default registry plus `sink.memory` and start `workers` workers.
pub fn start(yaml: &str, workers: usize) -> Harness {
    let sinks = MemorySinks::new();
    start_with(yaml, workers, sinks.clone(), registry(&sinks))
}

/// As [`start`], with a registry the caller has extended.
pub fn start_with(yaml: &str, workers: usize, sinks: MemorySinks, registry: Registry) -> Harness {
    let pipeline = Pipeline::from_yaml(yaml, &registry).expect("pipeline loads");
    let (source, input) = MemorySource::new();
    let recorder = InMemoryRecorder::new();
    let state = MemoryStateStore::new();
    let engine = Engine::start(
        pipeline,
        Box::new(source),
        workers,
        Metrics::new(recorder.clone()),
        Arc::new(state.clone()),
    )
    .expect("engine starts");
    Harness {
        engine,
        source: input,
        sinks,
        state,
        recorder,
    }
}

impl Harness {
    /// Close the source and wait for the workers to drain.
    pub fn finish(self) {
        drop(self.source);
        self.engine.join().expect("clean shutdown");
    }

    /// The counter `metric` under exactly `labels`, zero if never counted.
    pub fn counter(&self, metric: Metric, labels: &[(&str, &str)]) -> u64 {
        self.recorder.counter(metric, labels)
    }

    /// Every sample of the histogram `metric` under exactly `labels`.
    pub fn samples(&self, metric: Metric, labels: &[(&str, &str)]) -> Vec<f64> {
        self.recorder.samples(metric, labels)
    }

    /// The ids of the records `sink` received, sorted.
    pub fn ids(&self, sink: &str) -> Vec<u64> {
        let mut ids: Vec<u64> = self
            .sinks
            .records(sink)
            .iter()
            .filter_map(|r| r.id.map(|id| id.0))
            .collect();
        ids.sort_unstable();
        ids
    }
}

/// Run `test` at one worker and at four.
pub fn for_each_worker_count(test: impl Fn(usize)) {
    for workers in [1, 4] {
        test(workers);
    }
}
