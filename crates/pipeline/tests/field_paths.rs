//! Flat dotted field paths through the trait boundary: a condition names a map key as one
//! dotted path, the loader and engine run it against records with OTel-style keys, and
//! bracket syntax is a config error naming the node.

use std::time::Duration;

use fusion_core::config::ConfigError;
use fusion_core::engine::Engine;
use fusion_core::memory::{AckOutcome, MemoryInput, MemorySinks, MemorySource};
use fusion_core::pipeline::Pipeline;
use fusion_core::record::Record;
use fusion_core::registry::Registry;
use fusion_pipeline::default_registry;

const WAIT: Duration = Duration::from_secs(5);

fn registry(sinks: &MemorySinks) -> Registry {
    let mut registry = default_registry();
    registry.register_sink("sink.memory", sinks.clone());
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
}

fn for_each_worker_count(test: impl Fn(usize)) {
    for workers in [1, 4] {
        test(workers);
    }
}

fn record(id: u64, attributes: &str, resource: &str) -> Record {
    Record::from_json(&format!(
        r#"{{"id": {id}, "body": "line", "attributes": {attributes}, "resource": {resource}}}"#
    ))
    .expect("record parses")
}

fn filter(condition: &str) -> String {
    format!(
        r#"
nodes:
  - id: keep_5xx
    type: filter
    condition: {condition}
    action: keep
  - id: out
    type: sink.memory
"#
    )
}

#[test]
fn filter_on_a_dotted_attribute_key_reads_the_flat_map() {
    for_each_worker_count(|workers| {
        let h = start(&filter("attributes.http.status >= 500"), workers);

        let kept = h.source.push(record(
            1,
            r#"{"http.status": 503}"#,
            r#"{"tenant.id": "acme"}"#,
        ));
        let dropped = h.source.push(record(
            2,
            r#"{"http.status": 200}"#,
            r#"{"tenant.id": "acme"}"#,
        ));
        let nested = h.source.push(record(
            3,
            r#"{"http": {"status": 503}}"#,
            r#"{"tenant.id": "acme"}"#,
        ));

        assert_eq!(kept.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");
        assert_eq!(
            dropped.wait(WAIT),
            Some(AckOutcome::Ack),
            "workers={workers}"
        );
        assert_eq!(
            nested.wait(WAIT),
            Some(AckOutcome::Ack),
            "workers={workers}"
        );
        let ids: Vec<u64> = h
            .sinks
            .records("out")
            .iter()
            .filter_map(|r| r.id.map(|id| id.0))
            .collect();
        assert_eq!(ids, [1], "only the flat key matches; workers={workers}");
        h.finish();
    });
}

#[test]
fn filter_on_resource_keys_including_hyphens_digits_and_quotes() {
    for_each_worker_count(|workers| {
        let yaml = filter(
            r#"resource.tenant.id == "acme" and resource.env == "prod" and resource.k8s.pod-name == "web-0" and attributes.5xx.count > 0 and attributes."Event ID" == 4625"#,
        );
        let h = start(&yaml, workers);

        let kept = h.source.push(record(
            1,
            r#"{"5xx.count": 2, "Event ID": 4625}"#,
            r#"{"tenant.id": "acme", "env": "prod", "k8s.pod-name": "web-0"}"#,
        ));
        let other_tenant = h.source.push(record(
            2,
            r#"{"5xx.count": 2, "Event ID": 4625}"#,
            r#"{"tenant.id": "beta", "env": "prod", "k8s.pod-name": "web-0"}"#,
        ));

        assert_eq!(kept.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");
        assert_eq!(
            other_tenant.wait(WAIT),
            Some(AckOutcome::Ack),
            "workers={workers}"
        );
        let delivered = h.sinks.records("out");
        assert_eq!(delivered.len(), 1, "workers={workers}");
        assert_eq!(delivered[0].id.map(|id| id.0), Some(1));
        h.finish();
    });
}

#[test]
fn bracket_syntax_in_a_condition_is_a_config_error_naming_the_node() {
    let sinks = MemorySinks::new();
    let registry = registry(&sinks);

    let err = Pipeline::from_yaml(&filter(r#"attributes["http.status"] >= 500"#), &registry)
        .expect_err("brackets are rejected at load");
    assert!(
        matches!(err, ConfigError::InvalidParams { ref node, ref message }
            if node == "keep_5xx" && message.contains("instead use `attributes.http.status`")),
        "{err}"
    );

    let route = r#"
nodes:
  - id: by_format
    type: route
    routes:
      linux: resource["log.format"] == "Linux"
    default: drop
  - id: out
    type: sink.memory
    from: by_format.linux
"#;
    let err = Pipeline::from_yaml(route, &registry).expect_err("brackets are rejected at load");
    assert!(
        matches!(err, ConfigError::InvalidParams { ref node, ref message }
            if node == "by_format" && message.contains("instead use `resource.log.format`")),
        "{err}"
    );

    let err =
        Pipeline::from_yaml(&filter("body.msg == 1"), &registry).expect_err("body has no fields");
    assert!(
        matches!(err, ConfigError::InvalidParams { ref node, ref message }
            if node == "keep_5xx" && message.contains("`body` is one value and has no fields")),
        "{err}"
    );
}
