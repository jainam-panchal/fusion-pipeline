//! Walking skeleton: YAML in, records through in-memory source and sink,
//! ack handles observed. Every test runs with 1 and 4 worker threads.

use fusion_pipeline::load;
use pipeline_core::engine::SinkBindings;
use pipeline_core::memory::{envelope, AckProbe, MemorySink, MemorySource};
use pipeline_core::traits::AckOutcome;
use pipeline_core::Record;
use serde_json::json;
use std::sync::Arc;

const KEEP_ERRORS: &str = r#"
nodes:
  - id: keep_errors
    type: filter
    condition: 'severity_text == "ERROR"'
    action: keep
  - id: out
    type: sink.memory
"#;

fn error_record(id: u64) -> Record {
    let mut r = Record::log(id);
    r.severity_text = Some("ERROR".into());
    r.resource.insert("tenant.id".into(), json!("acme"));
    r
}

fn info_record(id: u64) -> Record {
    let mut r = error_record(id);
    r.severity_text = Some("INFO".into());
    r
}

/// Run `records` through `yaml` with one sink node called `out`, on
/// `workers` threads. Returns what the sink saw and each record's ack probe.
fn run(yaml: &str, records: Vec<Record>, workers: usize) -> (Vec<Record>, Vec<AckProbe>) {
    let sink = Arc::new(MemorySink::default());
    let mut bindings = SinkBindings::default();
    bindings.bind("out", sink.clone());
    let pipeline = load(yaml, bindings).expect("pipeline loads");

    let (envelopes, probes): (Vec<_>, Vec<_>) = records.into_iter().map(envelope).unzip();
    pipeline.run(Box::new(MemorySource::new(envelopes)), workers);
    (sink.records(), probes)
}

fn for_each_worker_count(f: impl Fn(usize)) {
    for workers in [1, 4] {
        f(workers);
    }
}

#[test]
fn record_passing_filter_reaches_sink_and_is_acked() {
    for_each_worker_count(|workers| {
        let (received, probes) = run(KEEP_ERRORS, vec![error_record(1)], workers);
        assert_eq!(received, vec![error_record(1)], "workers={workers}");
        assert_eq!(
            probes[0].outcome(),
            Some(AckOutcome::Ack),
            "workers={workers}"
        );
    });
}

#[test]
fn record_dropped_by_filter_skips_sink_but_is_still_acked() {
    for_each_worker_count(|workers| {
        let (received, probes) = run(KEEP_ERRORS, vec![info_record(2)], workers);
        assert!(received.is_empty(), "workers={workers}");
        assert_eq!(
            probes[0].outcome(),
            Some(AckOutcome::Ack),
            "workers={workers}"
        );
    });
}

#[test]
fn record_without_id_reaches_no_sink_and_is_nakked() {
    for_each_worker_count(|workers| {
        let mut r = error_record(3);
        r.id = None;
        let (received, probes) = run(KEEP_ERRORS, vec![r], workers);
        assert!(received.is_empty(), "workers={workers}");
        assert!(
            matches!(probes[0].outcome(), Some(AckOutcome::Nak(_))),
            "workers={workers}: {:?}",
            probes[0].outcome()
        );
    });
}

#[test]
fn filter_action_drop_inverts_the_condition() {
    let yaml = r#"
nodes:
  - id: drop_info
    type: filter
    condition: 'severity_text == "INFO"'
    action: drop
  - id: out
    type: sink.memory
"#;
    let (received, probes) = run(yaml, vec![info_record(4), error_record(5)], 1);
    assert_eq!(received, vec![error_record(5)]);
    assert_eq!(probes[0].outcome(), Some(AckOutcome::Ack));
    assert_eq!(probes[1].outcome(), Some(AckOutcome::Ack));
}

#[test]
fn many_records_on_four_workers_all_land_and_all_ack() {
    let records: Vec<Record> = (0..200)
        .map(|i| {
            if i % 2 == 0 {
                error_record(i)
            } else {
                info_record(i)
            }
        })
        .collect();
    let (received, probes) = run(KEEP_ERRORS, records, 4);
    let mut ids: Vec<u64> = received.iter().map(|r| r.id.unwrap().0).collect();
    ids.sort_unstable();
    assert_eq!(ids, (0..200).step_by(2).collect::<Vec<_>>());
    assert!(probes.iter().all(|p| p.outcome() == Some(AckOutcome::Ack)));
}

#[test]
fn sink_failure_naks_the_record() {
    let sink = Arc::new(MemorySink::rejecting());
    let mut bindings = SinkBindings::default();
    bindings.bind("out", sink.clone());
    let pipeline = load(KEEP_ERRORS, bindings).unwrap();
    let (env, probe) = envelope(error_record(6));
    pipeline.run(Box::new(MemorySource::new(vec![env])), 1);
    assert!(sink.records().is_empty());
    assert!(matches!(probe.outcome(), Some(AckOutcome::Nak(_))));
}

#[test]
fn unknown_stage_type_and_bad_condition_fail_at_load_with_node_id() {
    use fusion_pipeline::LoadError;
    use pipeline_core::engine::BuildError;

    let yaml = r#"
nodes:
  - id: mystery
    type: teleport
  - id: out
    type: sink.memory
"#;
    match load(yaml, SinkBindings::default()) {
        Err(LoadError::Build(BuildError::UnknownStageType { node, kind })) => {
            assert_eq!(node, "mystery");
            assert_eq!(kind, "teleport");
        }
        other => panic!("expected UnknownStageType, got {other:?}"),
    }

    let yaml = r#"
nodes:
  - id: broken
    type: filter
    condition: 'severity_text =='
    action: keep
  - id: out
    type: sink.memory
"#;
    match load(yaml, SinkBindings::default()) {
        Err(LoadError::Build(BuildError::Stage { node, .. })) => assert_eq!(node, "broken"),
        other => panic!("expected Stage error, got {other:?}"),
    }

    let mut bindings = SinkBindings::default();
    bindings.bind("out", Arc::new(MemorySink::default()));
    let yaml = r#"
nodes:
  - id: a
    type: filter
    condition: 'severity_text == "ERROR"'
    action: keep
  - id: out
    type: sink.memory
  - id: out2
    type: sink.memory
    from: a
"#;
    match load(yaml, bindings) {
        Err(LoadError::Build(BuildError::UnboundSink { node })) => assert_eq!(node, "out2"),
        other => panic!("expected UnboundSink, got {other:?}"),
    }
}
