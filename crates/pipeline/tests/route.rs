//! Routing, fan-out, fan-in and the branch-counted ack through the trait boundary: YAML
//! config in, envelopes pushed through the in-memory source, assertions on which sink saw
//! which record and how each ack handle was settled.

use std::time::Duration;

use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::engine::Engine;
use fusion_core::memory::{AckOutcome, MemoryInput, MemorySinks, MemorySource};
use fusion_core::pipeline::Pipeline;
use fusion_core::record::Record;
use fusion_core::registry::Registry;
use fusion_core::stage::{Context, Stage, StageOutput};
use fusion_pipeline::default_registry;

const WAIT: Duration = Duration::from_secs(5);

const BY_FORMAT: &str = r#"
nodes:
  - id: by_format
    type: route
    routes:
      linux: resource["log.format"] == "Linux"
      apache: resource["log.format"] == "Apache"
    default: other
  - id: linux_out
    type: sink.memory
    from: by_format.linux
  - id: apache_out
    type: sink.memory
    from: by_format.apache
  - id: other_out
    type: sink.memory
    from: by_format.other
"#;

/// A test-only stage that sets `attributes["touched"] = true`, so cross-branch isolation
/// is observable at a sink.
struct Touch;

impl Stage for Touch {
    fn process(&self, mut record: Record, _ctx: &Context<'_>) -> StageOutput {
        record
            .attributes
            .insert("touched".to_owned(), serde_json::Value::Bool(true));
        StageOutput::Pass(record)
    }
}

fn registry(sinks: &MemorySinks) -> Registry {
    let mut registry = default_registry();
    registry.register_sink("sink.memory", sinks.clone());
    registry.register_stage(
        "touch",
        |_: &NodeConfig| -> Result<Box<dyn Stage>, ConfigError> { Ok(Box::new(Touch)) },
    );
    registry
}

struct Harness {
    engine: Engine,
    source: MemoryInput,
    sinks: MemorySinks,
}

fn start(yaml: &str, workers: usize) -> Harness {
    let sinks = MemorySinks::new();
    let pipeline = Pipeline::from_yaml(yaml, &registry(&sinks)).expect("pipeline loads");
    let (source, input) = MemorySource::new();
    let engine = Engine::start(pipeline, Box::new(source), workers).expect("engine starts");
    Harness {
        engine,
        source: input,
        sinks,
    }
}

impl Harness {
    fn finish(self) {
        drop(self.source);
        self.engine.join().expect("clean shutdown");
    }

    fn ids(&self, sink: &str) -> Vec<u64> {
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

fn record(id: u64, format: &str) -> Record {
    Record::from_json(&format!(
        r#"{{"id": {id}, "body": "line", "resource": {{"log.format": "{format}", "tenant.id": "acme"}}}}"#
    ))
    .expect("record parses")
}

fn for_each_worker_count(test: impl Fn(usize)) {
    for workers in [1, 4] {
        test(workers);
    }
}

#[test]
fn each_record_lands_on_exactly_the_branch_its_condition_selects() {
    for_each_worker_count(|workers| {
        let h = start(BY_FORMAT, workers);

        let probes = [
            h.source.push(record(1, "Linux")),
            h.source.push(record(2, "Apache")),
            h.source.push(record(3, "Mac")),
        ];

        for probe in &probes {
            assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");
        }
        assert_eq!(h.ids("linux_out"), [1], "workers={workers}");
        assert_eq!(h.ids("apache_out"), [2], "workers={workers}");
        assert_eq!(h.ids("other_out"), [3], "workers={workers}");
        h.finish();
    });
}

#[test]
fn default_drop_drops_unmatched_records_and_still_acks() {
    let yaml = BY_FORMAT
        .replace("default: other", "default: drop")
        .replace("from: by_format.other", "from: by_format.linux");
    let h = start(&yaml, 1);

    let dropped = h.source.push(record(3, "Mac"));
    let kept = h.source.push(record(1, "Linux"));

    assert_eq!(dropped.wait(WAIT), Some(AckOutcome::Ack));
    assert_eq!(kept.wait(WAIT), Some(AckOutcome::Ack));
    assert_eq!(h.ids("linux_out"), [1]);
    assert_eq!(h.ids("other_out"), [1]);
    assert!(h.ids("apache_out").is_empty());
    h.finish();
}

const FAN_OUT: &str = r#"
nodes:
  - id: by_format
    type: route
    routes:
      linux: resource["log.format"] == "Linux"
    default: drop
  - id: archive
    type: sink.memory
    from: by_format.linux
  - id: search
    type: sink.memory
    from: by_format.linux
"#;

#[test]
fn fan_out_reaches_both_sinks_and_acks_once_after_both() {
    for_each_worker_count(|workers| {
        let h = start(FAN_OUT, workers);

        let probe = h.source.push(record(1, "Linux"));

        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");
        // The ack is observed only after the walk finished, so both sinks already hold it.
        assert_eq!(h.ids("archive"), [1], "workers={workers}");
        assert_eq!(h.ids("search"), [1], "workers={workers}");
        h.finish();
    });
}

#[test]
fn one_sink_failing_on_a_fan_out_record_naks_the_message_once() {
    for_each_worker_count(|workers| {
        let h = start(FAN_OUT, workers);
        h.sinks.fail_writes_to("search");

        let probe = h.source.push(record(1, "Linux"));

        assert!(
            matches!(probe.wait(WAIT), Some(AckOutcome::Nak(_))),
            "workers={workers}: {:?}",
            probe.outcome()
        );
        // The other branch still ran to completion; the nak waited for it.
        assert_eq!(h.ids("archive"), [1], "workers={workers}");
        assert!(h.ids("search").is_empty(), "workers={workers}");
        h.finish();
    });
}

#[test]
fn fan_in_node_receives_records_from_every_input() {
    let yaml = r#"
nodes:
  - id: by_format
    type: route
    routes:
      linux: resource["log.format"] == "Linux"
      apache: resource["log.format"] == "Apache"
    default: drop
  - id: linux_touch
    type: touch
    from: by_format.linux
  - id: apache_touch
    type: touch
    from: by_format.apache
  - id: out
    type: sink.memory
    from: [linux_touch, apache_touch]
"#;
    for_each_worker_count(|workers| {
        let h = start(yaml, workers);

        let probes = [
            h.source.push(record(1, "Linux")),
            h.source.push(record(2, "Apache")),
            h.source.push(record(3, "Mac")),
        ];

        for probe in &probes {
            assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");
        }
        assert_eq!(h.ids("out"), [1, 2], "workers={workers}");
        h.finish();
    });
}

#[test]
fn fan_in_node_receives_records_from_every_labelled_input() {
    // The README's shape: one node reading two labels of the same route.
    let yaml = r#"
nodes:
  - id: by_format
    type: route
    routes:
      linux: resource["log.format"] == "Linux"
      apache: resource["log.format"] == "Apache"
    default: other
  - id: linux_out
    type: sink.memory
    from: by_format.linux
  - id: rest
    type: sink.memory
    from: [by_format.apache, by_format.other]
"#;
    for_each_worker_count(|workers| {
        let h = start(yaml, workers);

        let probes = [
            h.source.push(record(1, "Linux")),
            h.source.push(record(2, "Apache")),
            h.source.push(record(3, "Mac")),
        ];

        for probe in &probes {
            assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");
        }
        assert_eq!(h.ids("linux_out"), [1], "workers={workers}");
        assert_eq!(h.ids("rest"), [2, 3], "workers={workers}");
        h.finish();
    });
}

#[test]
fn mutation_on_one_branch_is_not_visible_on_the_other() {
    // `touch` runs first in file order, while the other branch still shares the record.
    let yaml = r#"
nodes:
  - id: by_format
    type: route
    routes:
      linux: resource["log.format"] == "Linux"
    default: drop
  - id: touch
    type: touch
    from: by_format.linux
  - id: touched_out
    type: sink.memory
    from: touch
  - id: raw_out
    type: sink.memory
    from: by_format.linux
"#;
    let h = start(yaml, 1);

    let probe = h.source.push(record(1, "Linux"));

    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    let touched = h.sinks.records("touched_out");
    let raw = h.sinks.records("raw_out");
    assert_eq!(touched.len(), 1);
    assert_eq!(raw.len(), 1);
    assert_eq!(
        touched[0].attributes.get("touched"),
        Some(&serde_json::Value::Bool(true))
    );
    assert!(raw[0].attributes.get("touched").is_none(), "{raw:?}");
    h.finish();
}
