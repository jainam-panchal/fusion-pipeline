//! Pipeline metrics through the trait boundary: YAML config in, envelopes pushed through the
//! in-memory source, assertions on what the in-memory recorder observed. Counting only; the
//! durations are in `metrics_durations.rs`.

mod common;

use fusion_core::memory::AckOutcome;
use fusion_core::meta::Arrival;
use fusion_core::metrics::CounterMetric;
use fusion_core::record::{Kind, Record};

use common::{WAIT, for_each_worker_count, start};

const KEEP_ERRORS: &str = r#"
nodes:
  - id: keep_errors
    type: filter
    condition: severity_text == "ERROR"
    action: keep
  - id: out
    type: sink.memory
"#;

const BY_FORMAT: &str = r#"
nodes:
  - id: by_format
    type: route
    routes:
      linux: resource.log.format == "Linux"
    default: drop
  - id: linux_out
    type: sink.memory
    from: by_format.linux
"#;

fn record(id: u64, severity: &str) -> Record {
    Record::from_json(&format!(
        r#"{{"id": {id}, "severity_text": "{severity}", "body": "disk full", "resource": {{"tenant.id": "acme"}}}}"#
    ))
    .expect("record parses")
}

fn tenantless_record(id: u64) -> Record {
    Record::from_json(&format!(
        r#"{{"id": {id}, "severity_text": "ERROR", "body": "disk full"}}"#
    ))
    .expect("record parses")
}

#[test]
fn a_record_that_reaches_a_sink_is_counted_in_and_out_of_every_node_it_touched() {
    for_each_worker_count(|workers| {
        let h = start(KEEP_ERRORS, workers);

        assert_eq!(h.push(record(1, "ERROR")).wait(WAIT), Some(AckOutcome::Ack));

        for stage in ["keep_errors", "out"] {
            let labels = [("tenant", "acme"), ("stage", stage)];
            assert_eq!(
                h.counter(CounterMetric::RecordsIn, &labels),
                1,
                "in {stage} workers={workers}"
            );
            assert_eq!(
                h.counter(CounterMetric::RecordsOut, &labels),
                1,
                "out {stage} workers={workers}"
            );
        }
        assert_eq!(
            h.counter(
                CounterMetric::RecordsDropped,
                &[
                    ("tenant", "acme"),
                    ("stage", "keep_errors"),
                    ("reason", "filter")
                ]
            ),
            0,
            "workers={workers}"
        );
        h.finish();
    });
}

#[test]
fn a_filtered_record_is_counted_in_but_dropped_with_reason_filter_and_never_leaves_the_node() {
    for_each_worker_count(|workers| {
        let h = start(KEEP_ERRORS, workers);

        assert_eq!(h.push(record(1, "INFO")).wait(WAIT), Some(AckOutcome::Ack));

        let stage = [("tenant", "acme"), ("stage", "keep_errors")];
        assert_eq!(
            h.counter(CounterMetric::RecordsIn, &stage),
            1,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(CounterMetric::RecordsOut, &stage),
            0,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(
                CounterMetric::RecordsDropped,
                &[
                    ("tenant", "acme"),
                    ("stage", "keep_errors"),
                    ("reason", "filter")
                ]
            ),
            1,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(
                CounterMetric::RecordsIn,
                &[("tenant", "acme"), ("stage", "out")]
            ),
            0
        );
        h.finish();
    });
}

#[test]
fn a_route_default_of_drop_is_counted_with_reason_route_default_drop() {
    for_each_worker_count(|workers| {
        let h = start(BY_FORMAT, workers);

        let mut mac = record(1, "ERROR");
        mac.resource.insert("log.format".to_owned(), "Mac".into());
        assert_eq!(h.push(mac).wait(WAIT), Some(AckOutcome::Ack));

        assert_eq!(
            h.counter(
                CounterMetric::RecordsDropped,
                &[
                    ("tenant", "acme"),
                    ("stage", "by_format"),
                    ("reason", "route_default_drop")
                ]
            ),
            1,
            "workers={workers}"
        );
        h.finish();
    });
}

#[test]
fn a_record_whose_source_names_no_tenant_is_counted_under_tenant_unknown() {
    for_each_worker_count(|workers| {
        let h = start(KEEP_ERRORS, workers);

        // An arrival that names no tenant; the test producer still sends the record's id.
        assert_eq!(
            h.push_as_producer(tenantless_record(1), Arrival::default())
                .wait(WAIT),
            Some(AckOutcome::Ack)
        );

        assert_eq!(
            h.counter(
                CounterMetric::RecordsIn,
                &[("tenant", "unknown"), ("stage", "keep_errors")]
            ),
            1,
            "workers={workers}"
        );
        h.finish();
    });
}

#[test]
fn a_record_without_an_id_is_dropped_at_the_source_with_reason_missing_id_and_naks() {
    for_each_worker_count(|workers| {
        let h = start(KEEP_ERRORS, workers);

        let mut no_id = record(1, "ERROR");
        no_id.id = None;
        assert_eq!(h.push(no_id).wait(WAIT), Some(AckOutcome::Nak(None)));

        assert_eq!(
            h.counter(
                CounterMetric::RecordsDropped,
                &[
                    ("tenant", "acme"),
                    ("stage", "source"),
                    ("reason", "missing_id")
                ]
            ),
            1,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(CounterMetric::SourceNaks, &[("tenant", "acme")]),
            1,
            "workers={workers}"
        );
        h.finish();
    });
}

#[test]
fn a_non_log_record_is_dropped_at_the_source_with_reason_invalid_record_and_acks() {
    for_each_worker_count(|workers| {
        let h = start(KEEP_ERRORS, workers);

        let metric = h.push_kind(record(1, "ERROR"), Kind::Metric);
        assert_eq!(metric.wait(WAIT), Some(AckOutcome::Ack));

        assert_eq!(
            h.counter(
                CounterMetric::RecordsDropped,
                &[
                    ("tenant", "acme"),
                    ("stage", "source"),
                    ("reason", "invalid_record")
                ]
            ),
            1,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(CounterMetric::SourceNaks, &[("tenant", "acme")]),
            0,
            "workers={workers}"
        );
        h.finish();
    });
}

#[test]
fn a_sink_that_cannot_confirm_durable_acceptance_counts_an_error_a_publish_error_and_a_nak() {
    for_each_worker_count(|workers| {
        let h = start(KEEP_ERRORS, workers);
        h.sinks.fail_writes_to("out");

        assert_eq!(
            h.push(record(1, "ERROR")).wait(WAIT),
            Some(AckOutcome::Nak(None))
        );

        let out = [("tenant", "acme"), ("stage", "out")];
        assert_eq!(
            h.counter(CounterMetric::RecordsIn, &out),
            1,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(CounterMetric::RecordsOut, &out),
            0,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(CounterMetric::RecordsErrored, &out),
            1,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(CounterMetric::SinkPublishErrors, &out),
            1,
            "workers={workers}"
        );
        assert_eq!(
            h.counter(CounterMetric::SourceNaks, &[("tenant", "acme")]),
            1,
            "workers={workers}"
        );
        h.finish();
    });
}

#[test]
fn a_fanned_out_record_is_counted_once_per_branch() {
    const FAN_OUT: &str = r#"
nodes:
  - id: keep_errors
    type: filter
    condition: severity_text == "ERROR"
    action: keep
  - id: out
    type: sink.memory
    from: keep_errors
  - id: archive
    type: sink.memory
    from: keep_errors
"#;
    for_each_worker_count(|workers| {
        let h = start(FAN_OUT, workers);

        assert_eq!(h.push(record(1, "ERROR")).wait(WAIT), Some(AckOutcome::Ack));

        assert_eq!(
            h.counter(
                CounterMetric::RecordsOut,
                &[("tenant", "acme"), ("stage", "keep_errors")]
            ),
            1
        );
        for stage in ["out", "archive"] {
            assert_eq!(
                h.counter(
                    CounterMetric::RecordsIn,
                    &[("tenant", "acme"), ("stage", stage)]
                ),
                1,
                "{stage} workers={workers}"
            );
        }
        h.finish();
    });
}

#[test]
fn source_counts_every_record_in_and_only_those_entering_the_graph_out() {
    for_each_worker_count(|workers| {
        let h = start(KEEP_ERRORS, workers);

        let mut no_id = record(1, "ERROR");
        no_id.id = None;
        let probes = [
            h.push(record(3, "ERROR")),
            h.push(no_id),
            h.push_kind(record(2, "ERROR"), Kind::Metric),
        ];
        for probe in &probes {
            assert!(probe.wait(WAIT).is_some(), "workers={workers}");
        }

        assert_eq!(
            h.counter(
                CounterMetric::RecordsIn,
                &[("tenant", "acme"), ("stage", "source")]
            ),
            3,
            "workers={workers}"
        );
        // Only the record with an id and of kind log entered the graph.
        assert_eq!(
            h.counter(
                CounterMetric::RecordsOut,
                &[("tenant", "acme"), ("stage", "source")]
            ),
            1,
            "workers={workers}"
        );
        h.finish();
    });
}

#[test]
fn a_stage_that_panics_counts_an_error_at_its_own_node_and_naks() {
    const PANICS: &str = r#"
nodes:
  - id: boom
    type: panics
  - id: out
    type: sink.memory
"#;
    let h = common::start_with_panics(PANICS);

    assert_eq!(
        h.push(record(1, "ERROR")).wait(WAIT),
        Some(AckOutcome::Nak(None))
    );

    assert_eq!(
        h.counter(
            CounterMetric::RecordsErrored,
            &[("tenant", "acme"), ("stage", "boom")]
        ),
        1
    );
    assert_eq!(
        h.counter(CounterMetric::SourceNaks, &[("tenant", "acme")]),
        1
    );
    // The worker survives the panic: the next record is processed normally.
    h.finish();
}
