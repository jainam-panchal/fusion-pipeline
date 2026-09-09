//! Walking skeleton through the trait boundary: YAML config in, envelopes pushed through an
//! in-memory source, assertions on what reached the in-memory sink and how each ack handle
//! was settled.

use std::time::Duration;

use fusion_core::engine::Engine;
use fusion_core::memory::{AckOutcome, MemorySinks, MemorySource};
use fusion_core::pipeline::Pipeline;
use fusion_core::record::Record;
use fusion_pipeline::default_registry;

const WAIT: Duration = Duration::from_secs(5);

const KEEP_ERRORS: &str = r#"
nodes:
  - id: keep_errors
    type: filter
    condition: severity_text == "ERROR"
    action: keep
  - id: out
    type: sink.memory
"#;

struct Harness {
    engine: Engine,
    source: fusion_core::memory::MemoryInput,
    sinks: MemorySinks,
}

fn start(yaml: &str, workers: usize) -> Harness {
    let sinks = MemorySinks::new();
    let mut registry = default_registry();
    registry.register_sink("sink.memory", sinks.clone());
    let pipeline = Pipeline::from_yaml(yaml, &registry).expect("pipeline loads");
    let (source, input) = MemorySource::new();
    let engine = Engine::start(pipeline, Box::new(source), workers).expect("engine starts");
    Harness {
        engine,
        source: input,
        sinks,
    }
}

impl Harness {
    /// Close the source and wait for the workers to drain.
    fn finish(self) {
        drop(self.source);
        self.engine.join().expect("clean shutdown");
    }
}

fn error_record(id: u64) -> Record {
    Record::from_json(&format!(
        r#"{{"id": {id}, "severity_text": "ERROR", "body": "disk full", "resource": {{"tenant.id": "acme"}}}}"#
    ))
    .expect("record parses")
}

fn info_record(id: u64) -> Record {
    Record::from_json(&format!(
        r#"{{"id": {id}, "severity_text": "INFO", "body": "started"}}"#
    ))
    .expect("record parses")
}

fn for_each_worker_count(test: impl Fn(usize)) {
    for workers in [1, 4] {
        test(workers);
    }
}

#[test]
fn record_passing_filter_reaches_sink_and_is_acked() {
    for_each_worker_count(|workers| {
        let h = start(KEEP_ERRORS, workers);

        let probe = h.source.push(error_record(1));

        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");
        let delivered = h.sinks.records("out");
        assert_eq!(delivered.len(), 1, "workers={workers}");
        assert_eq!(
            delivered[0].body.as_ref().and_then(|b| b.as_str()),
            Some("disk full")
        );
        h.finish();
    });
}

#[test]
fn record_dropped_by_filter_does_not_reach_sink_and_is_still_acked() {
    for_each_worker_count(|workers| {
        let h = start(KEEP_ERRORS, workers);

        let probe = h.source.push(info_record(2));

        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");
        assert!(h.sinks.records("out").is_empty(), "workers={workers}");
        h.finish();
    });
}

#[test]
fn drop_action_inverts_the_filter() {
    let yaml = KEEP_ERRORS.replace("action: keep", "action: drop");
    let h = start(&yaml, 1);

    let kept = h.source.push(info_record(3));
    let dropped = h.source.push(error_record(4));

    assert_eq!(kept.wait(WAIT), Some(AckOutcome::Ack));
    assert_eq!(dropped.wait(WAIT), Some(AckOutcome::Ack));
    let delivered = h.sinks.records("out");
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].id.map(|id| id.0), Some(3));
    h.finish();
}

#[test]
fn record_without_id_reaches_no_sink_and_is_nakked() {
    for_each_worker_count(|workers| {
        let h = start(KEEP_ERRORS, workers);
        let record = Record::from_json(r#"{"severity_text": "ERROR", "body": "no id"}"#)
            .expect("record parses");

        let probe = h.source.push(record);

        assert!(
            matches!(probe.wait(WAIT), Some(AckOutcome::Nak(_))),
            "workers={workers}"
        );
        assert!(h.sinks.records("out").is_empty(), "workers={workers}");
        h.finish();
    });
}

#[test]
fn many_records_are_all_settled_across_workers() {
    for_each_worker_count(|workers| {
        let h = start(KEEP_ERRORS, workers);

        let probes: Vec<_> = (0..200)
            .map(|i| {
                let record = if i % 2 == 0 {
                    error_record(i)
                } else {
                    info_record(i)
                };
                h.source.push(record)
            })
            .collect();

        for probe in probes {
            assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");
        }
        let mut ids: Vec<u64> = h
            .sinks
            .records("out")
            .iter()
            .filter_map(|r| r.id.map(|id| id.0))
            .collect();
        ids.sort_unstable();
        let expected: Vec<u64> = (0..200).step_by(2).collect();
        assert_eq!(ids, expected, "workers={workers}");
        h.finish();
    });
}

#[test]
fn filter_with_regex_operator_is_rejected_at_load_until_the_regex_ticket() {
    let yaml = KEEP_ERRORS.replace(r#"severity_text == "ERROR""#, r#"body =~ "disk""#);
    let mut registry = default_registry();
    registry.register_sink("sink.memory", MemorySinks::new());

    let err = Pipeline::from_yaml(&yaml, &registry).expect_err("regex ops not wired");

    let message = err.to_string();
    assert!(message.contains("keep_errors"), "{message}");
    assert!(message.contains("=~"), "{message}");
}

#[test]
fn unknown_node_type_is_rejected_naming_the_node() {
    let yaml = KEEP_ERRORS.replace("type: filter", "type: teleport");
    let mut registry = default_registry();
    registry.register_sink("sink.memory", MemorySinks::new());

    let err = Pipeline::from_yaml(&yaml, &registry).expect_err("unknown type");

    let message = err.to_string();
    assert!(
        message.contains("keep_errors") && message.contains("teleport"),
        "{message}"
    );
}

#[test]
fn worker_count_comes_from_config_or_defaults_to_cores() {
    let mut registry = default_registry();
    registry.register_sink("sink.memory", MemorySinks::new());

    let explicit = Pipeline::from_yaml(&format!("workers: 2\n{KEEP_ERRORS}"), &registry)
        .expect("pipeline loads");
    assert_eq!(explicit.workers(), Some(2));
    assert_eq!(explicit.worker_count(), 2);

    let defaulted = Pipeline::from_yaml(KEEP_ERRORS, &registry).expect("pipeline loads");
    assert_eq!(defaulted.workers(), None);
    assert!(defaulted.worker_count() >= 1);
}

#[test]
fn metric_and_span_records_are_rejected_and_acked_without_reaching_a_sink() {
    for_each_worker_count(|workers| {
        let h = start(KEEP_ERRORS, workers);
        let metric = Record::from_json(r#"{"id": 9, "kind": "metric", "severity_text": "ERROR"}"#)
            .expect("record parses");
        let span = Record::from_json(r#"{"id": 10, "kind": "span", "severity_text": "ERROR"}"#)
            .expect("record parses");

        let metric_probe = h.source.push(metric);
        let span_probe = h.source.push(span);

        assert_eq!(
            metric_probe.wait(WAIT),
            Some(AckOutcome::Ack),
            "workers={workers}"
        );
        assert_eq!(
            span_probe.wait(WAIT),
            Some(AckOutcome::Ack),
            "workers={workers}"
        );
        assert!(h.sinks.records("out").is_empty(), "workers={workers}");
        h.finish();
    });
}

#[test]
fn unknown_top_level_config_key_is_rejected() {
    let yaml = format!("worker: 2\n{KEEP_ERRORS}");
    let mut registry = default_registry();
    registry.register_sink("sink.memory", MemorySinks::new());

    let err = Pipeline::from_yaml(&yaml, &registry).expect_err("typo rejected");

    assert!(err.to_string().contains("worker"), "{err}");
}
