//! The engine harness the trait-boundary tests share: a YAML config compiled with the
//! default registry plus an in-memory sink, an in-memory source to push envelopes through,
//! an in-memory state store, and the sinks, metrics, events and record traces to assert on. It also owns where `deploy/` is,
//! so a test that drives a shipped config does not spell the path itself.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use fusion_core::engine::Engine;
use fusion_core::events::{Event, EventKind, InMemoryEventLog};
use fusion_core::memory::{AckProbe, MemoryInput, MemorySinks, MemorySource, MemoryStateStore};
use fusion_core::meta::{Arrival, IngestionTime};
use fusion_core::metrics::{CounterMetric, HistogramMetric, InMemoryRecorder, Metrics};
use fusion_core::pipeline::Pipeline;
use fusion_core::record::Record;
use fusion_core::registry::Registry;
use fusion_core::signals::Signals;
use fusion_core::state::StateStoreFactory;
use fusion_core::trace::{InMemoryTraceSink, RecordTrace, TraceSampling};
use fusion_pipeline::default_registry;

/// A config shipped under `deploy/`.
pub fn deploy_config(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|err| panic!("deploy/{name} is readable: {err}"))
}

/// How long a test waits for an ack handle to settle.
pub const WAIT: Duration = Duration::from_secs(5);

/// A running engine with its in-memory source, sinks, state store, metrics recorder, event
/// log and trace sink. Traces keep the default share of passing records.
pub struct Harness {
    pub engine: Engine,
    pub source: MemoryInput,
    pub sinks: MemorySinks,
    pub state: MemoryStateStore,
    pub recorder: InMemoryRecorder,
    pub event_log: InMemoryEventLog,
    pub trace_sink: InMemoryTraceSink,
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
    let state = MemoryStateStore::new();
    let factory: Arc<dyn StateStoreFactory> = Arc::new(state.clone());
    launch(yaml, workers, sinks, registry, state, factory)
}

/// As [`start_with`], with the state store the caller supplies (a real Dragonfly in the
/// ignored tests). `Harness::state` is then a memory store nothing writes to.
pub fn start_with_state(
    yaml: &str,
    workers: usize,
    sinks: MemorySinks,
    registry: Registry,
    factory: Arc<dyn StateStoreFactory>,
) -> Harness {
    launch(
        yaml,
        workers,
        sinks,
        registry,
        MemoryStateStore::new(),
        factory,
    )
}

fn launch(
    yaml: &str,
    workers: usize,
    sinks: MemorySinks,
    registry: Registry,
    state: MemoryStateStore,
    factory: Arc<dyn StateStoreFactory>,
) -> Harness {
    let pipeline = Pipeline::from_yaml(yaml, &registry).expect("pipeline loads");
    let (source, input) = MemorySource::new();
    let recorder = InMemoryRecorder::new();
    let event_log = InMemoryEventLog::new();
    let trace_sink = InMemoryTraceSink::new();
    let signals = Signals::new(Metrics::new(recorder.clone()))
        .with_events(event_log.clone())
        .with_traces(trace_sink.clone(), TraceSampling::default());
    let engine = Engine::start(pipeline, Box::new(source), workers, signals, factory)
        .expect("engine starts");
    Harness {
        engine,
        source: input,
        sinks,
        state,
        recorder,
        event_log,
        trace_sink,
    }
}

/// A record with `id` and `body`, the shape the dedupe tests push. It carries no tenant and
/// no time: those are the arrival's, given by [`Harness::push`] and its siblings.
pub fn body_record(id: u64, body: &str) -> Record {
    Record::from_json(&format!(r#"{{"id": {id}, "body": "{body}"}}"#)).expect("record parses")
}

/// [`body_record`] with `resource.host` set, or absent for `None`, the shape the
/// `consistent` sampling tests push.
pub fn host_record(id: u64, host: Option<&str>) -> Record {
    let mut record = body_record(id, "x");
    if let Some(host) = host {
        record.resource.insert(
            "host".to_owned(),
            serde_json::Value::String(host.to_owned()),
        );
    }
    record
}

/// The tenant [`Harness::push`] gives every record, as a NATS source reading `logs.acme.>`
/// would.
pub const TENANT: &str = "acme";

/// What a source subscribed to `tenant`'s subjects says about a first delivery: the tenant,
/// and no time, so the engine reads the worker clock.
pub fn arrival_as(tenant: &str) -> Arrival {
    Arrival {
        tenant: Some(tenant.to_owned()),
        ..Arrival::default()
    }
}

/// The labels of a `dedupe` drop by the node `dedupe_body` for tenant `acme`.
pub const DEDUPE_DROP: [(&str, &str); 3] = [
    ("tenant", "acme"),
    ("stage", "dedupe_body"),
    ("reason", "dedupe"),
];

impl Harness {
    /// Push `record` as a first delivery for tenant [`TENANT`], with no transport time.
    pub fn push(&self, record: Record) -> AckProbe {
        self.push_as(TENANT, record)
    }

    /// Push `record` as a first delivery for `tenant`, with no transport time.
    pub fn push_as(&self, tenant: &str, record: Record) -> AckProbe {
        self.source.push_arrival(record, arrival_as(tenant))
    }

    /// Push `record` as a first delivery for tenant [`TENANT`] that entered the transport at
    /// `ingestion_unix_nanos`, as the JetStream publish time says.
    pub fn push_at(&self, record: Record, ingestion_unix_nanos: u64) -> AckProbe {
        self.source.push_arrival(
            record,
            Arrival {
                ingestion_time: Some(IngestionTime::Reported(ingestion_unix_nanos)),
                ..arrival_as(TENANT)
            },
        )
    }

    /// Close the source and wait for the workers to drain.
    pub fn finish(self) {
        drop(self.source);
        self.engine.join().expect("clean shutdown");
    }

    /// The counter `metric` under exactly `labels`, zero if never counted.
    pub fn counter(&self, metric: CounterMetric, labels: &[(&str, &str)]) -> u64 {
        self.recorder.counter(metric, labels)
    }

    /// Every sample of the histogram `metric` under exactly `labels`.
    pub fn samples(&self, metric: HistogramMetric, labels: &[(&str, &str)]) -> Vec<f64> {
        self.recorder.samples(metric, labels)
    }

    /// Every event logged so far, in order.
    pub fn events(&self) -> Vec<Event> {
        self.event_log.events()
    }

    /// The events of `kind` logged so far, in order.
    pub fn events_of(&self, kind: EventKind) -> Vec<Event> {
        self.event_log.of_kind(kind)
    }

    /// Every record trace kept so far, in order.
    pub fn traces(&self) -> Vec<RecordTrace> {
        self.trace_sink.traces()
    }

    /// Push `record` for tenant [`TENANT`] as its `delivery_count`-th delivery.
    pub fn push_delivery(&self, record: Record, delivery_count: u64) -> AckProbe {
        self.source.push_arrival(
            record,
            Arrival {
                delivery_count,
                ..arrival_as(TENANT)
            },
        )
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
