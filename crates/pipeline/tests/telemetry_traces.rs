//! Record traces, through the trait boundary (issue #12, ADR 0006): a delivery's trace has
//! a span per node it visited, parented on the node the record came from. It is kept when
//! the walk failed or the record was redelivered, and otherwise for the sampled share of
//! record ids. Its ids are the record's, so a log line and the trace name each other.

mod common;

use std::collections::BTreeSet;

use common::{TENANT, WAIT, body_record, start};
use fusion_core::events::EventKind;
use fusion_core::io::FailureKind;
use fusion_core::memory::AckOutcome;
use fusion_core::metrics::CounterMetric;
use fusion_core::record::{Kind, Record, RecordId};
use fusion_core::stage::DropReason;
use fusion_core::trace::{NodeSpan, Settlement, SpanResult, TraceKey, TraceSampling};

/// A pass-through filter feeding two sinks, `good` then `bad`.
const FAN_OUT: &str = r#"
nodes:
  - id: keep_all
    type: filter
    condition: 'body == "x"'
    action: keep
  - id: good
    type: sink.memory
    from: keep_all
  - id: bad
    type: sink.memory
    from: keep_all
"#;

/// A route whose `err` label reaches a sink and whose default drops.
const ROUTED: &str = r#"
nodes:
  - id: by_body
    type: route
    routes:
      err: body == "x"
    default: drop
  - id: only_y
    type: filter
    condition: 'body == "y"'
    action: keep
    from: by_body.err
  - id: out
    type: sink.memory
    from: only_y
"#;

const TO_SINK: &str = r#"
nodes:
  - id: out
    type: sink.memory
"#;

fn key(id: u64) -> TraceKey {
    TraceKey::new(RecordId(id), TENANT)
}

/// The first id from `from` whose passing record the default share does not trace.
fn unsampled_id(from: u64) -> u64 {
    (from..)
        .find(|&id| !TraceSampling::default().keeps(key(id)))
        .expect("some id is not sampled")
}

fn span<'t>(spans: &'t [NodeSpan], node: &str) -> &'t NodeSpan {
    spans
        .iter()
        .find(|s| s.node == node)
        .unwrap_or_else(|| panic!("no span for `{node}` in {spans:?}"))
}

#[test]
fn a_failing_record_is_traced_with_a_span_per_node_on_both_branches() {
    let h = start(FAN_OUT, 1);
    h.sinks.fail_writes_to("bad");
    let id = unsampled_id(100);

    let probe = h.push(body_record(id, "x"));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));

    let traces = h.traces();
    assert_eq!(traces.len(), 1, "{traces:?}");
    let trace = &traces[0];
    assert_eq!(trace.trace_id, key(id).trace_id());
    assert_eq!(trace.span_id, key(id).delivery_span_id(1));
    assert_eq!(trace.record_id, RecordId(id));
    assert_eq!(&*trace.tenant, TENANT);
    assert_eq!(trace.delivery_count, 1);
    assert_eq!(trace.settlement, Settlement::Nak);

    let nodes: Vec<&str> = trace.spans.iter().map(|s| s.node.as_str()).collect();
    assert_eq!(nodes, ["keep_all", "good", "bad"]);
    let (filter, good, bad) = (
        span(&trace.spans, "keep_all"),
        span(&trace.spans, "good"),
        span(&trace.spans, "bad"),
    );
    assert_eq!(filter.parent_span_id, trace.span_id);
    assert_eq!(good.parent_span_id, filter.span_id);
    assert_eq!(bad.parent_span_id, filter.span_id);
    assert_eq!(filter.result, SpanResult::Pass);
    assert_eq!(good.result, SpanResult::Written);
    assert!(
        matches!(
            &bad.result,
            SpanResult::Error { failure: FailureKind::SinkError, error }
                if error.contains("set to fail")
        ),
        "{bad:?}"
    );

    let ids: BTreeSet<u64> = std::iter::once(trace.span_id.get())
        .chain(trace.spans.iter().map(|s| s.span_id.get()))
        .collect();
    assert_eq!(ids.len(), 4, "span ids are distinct");
    assert!(!ids.contains(&0), "span ids are never zero");
    for s in &trace.spans {
        assert!(
            trace.start <= s.start && s.start <= s.end && s.end <= trace.end,
            "{s:?}"
        );
    }
    h.finish();
}

#[test]
fn about_one_percent_of_passing_records_are_traced_and_always_the_same_ones() {
    let kept = || {
        let h = start(TO_SINK, 1);
        let probes: Vec<_> = (0..10_000).map(|id| h.push(body_record(id, "x"))).collect();
        for probe in probes {
            assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
        }
        let traces = h.traces();
        assert!(
            traces
                .iter()
                .all(|t| t.trace_id.get() != 0 && t.settlement == Settlement::Ack),
            "no zero trace id, only acks"
        );
        let ids: BTreeSet<u64> = traces.iter().map(|t| t.record_id.0).collect();
        assert_eq!(ids.len(), traces.len(), "one trace per record");
        h.finish();
        ids
    };
    // The ids the default share takes; `keeps` only picks them, the traces are the engine's.
    let sampled: BTreeSet<u64> = (0..10_000)
        .filter(|&id| TraceSampling::default().keeps(key(id)))
        .collect();

    let first = kept();
    assert!(
        (50..=150).contains(&first.len()),
        "kept {} of 10000",
        first.len()
    );
    assert_eq!(first, sampled, "exactly the sampled ids are traced");
    assert_eq!(kept(), first);
}

#[test]
fn a_passing_record_is_traced_only_when_its_id_is_sampled() {
    let h = start(TO_SINK, 1);
    let unsampled = unsampled_id(400);
    let sampled = (400..)
        .find(|&id| TraceSampling::default().keeps(key(id)))
        .expect("some id is sampled");

    for id in [unsampled, sampled] {
        assert_eq!(
            h.push(body_record(id, "x")).wait(WAIT),
            Some(AckOutcome::Ack)
        );
    }

    let traced: Vec<u64> = h.traces().iter().map(|t| t.record_id.0).collect();
    assert_eq!(traced, [sampled]);
    h.finish();
}

#[test]
fn a_passing_redelivery_of_record_zero_under_two_tenants_is_two_valid_traces() {
    let h = start(TO_SINK, 1);

    for tenant in ["acme", "beta"] {
        let probe = h.push_as_producer(
            body_record(0, "x"),
            fusion_core::meta::Arrival {
                delivery_count: 2,
                ..common::arrival_as(tenant)
            },
        );
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    }

    let traces = h.traces();
    assert_eq!(traces.len(), 2);
    assert!(traces.iter().all(|t| t.settlement == Settlement::Ack));
    assert!(
        traces
            .iter()
            .all(|t| t.trace_id.get() != 0 && t.span_id.get() != 0)
    );
    assert_ne!(traces[0].trace_id, traces[1].trace_id);
    h.finish();
}

#[test]
fn record_id_zero_has_a_valid_trace_id() {
    assert_ne!(key(0).trace_id().get(), 0);
    assert_ne!(key(0).delivery_span_id(1).get(), 0);
}

#[test]
fn the_same_id_under_two_tenants_is_two_traces() {
    let h = start(TO_SINK, 1);
    h.sinks.fail_writes_to("out");

    let acme = h.push_as("acme", body_record(5, "x"));
    let beta = h.push_as("beta", body_record(5, "x"));
    assert!(acme.wait(WAIT).is_some() && beta.wait(WAIT).is_some());

    let traces = h.traces();
    assert_eq!(traces.len(), 2);
    assert_ne!(traces[0].trace_id, traces[1].trace_id);
    assert_eq!(
        traces[0].trace_id,
        TraceKey::new(RecordId(5), "acme").trace_id()
    );
    assert_eq!(
        traces[1].trace_id,
        TraceKey::new(RecordId(5), "beta").trace_id()
    );
    h.finish();
}

#[test]
fn a_routed_span_names_its_label_and_a_drop_span_its_reason() {
    let h = start(ROUTED, 1);
    let id = unsampled_id(200);

    // A redelivery is always traced, so the unsampled id's trace is kept.
    let probe = h.push_delivery(body_record(id, "x"), 2);
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));

    let traces = h.traces();
    assert_eq!(traces.len(), 1, "{traces:?}");
    let trace = &traces[0];
    assert_eq!(trace.settlement, Settlement::Ack);
    let (route, filter) = (span(&trace.spans, "by_body"), span(&trace.spans, "only_y"));
    assert_eq!(route.result, SpanResult::Routed("err".to_owned()));
    assert_eq!(filter.parent_span_id, route.span_id);
    assert_eq!(filter.result, SpanResult::Drop(DropReason::Filter));
    assert_eq!(trace.spans.len(), 2, "the sink was never reached");
    h.finish();
}

#[test]
fn a_log_line_names_its_trace_and_a_redelivery_joins_the_same_trace() {
    let h = start(TO_SINK, 1);
    h.sinks.fail_writes_to("out");
    let id = unsampled_id(300);

    let first = h.push(body_record(id, "x"));
    assert_eq!(first.wait(WAIT), Some(AckOutcome::Nak(None)));
    h.sinks.recover_writes_to("out");
    let second = h.push_delivery(body_record(id, "x"), 2);
    assert_eq!(second.wait(WAIT), Some(AckOutcome::Ack));

    let traces = h.traces();
    assert_eq!(
        traces.len(),
        2,
        "the failure and the redelivery are both kept"
    );
    assert_eq!(traces[0].trace_id, traces[1].trace_id);
    assert_eq!(
        (traces[0].settlement, traces[1].settlement),
        (Settlement::Nak, Settlement::Ack)
    );
    assert_ne!(traces[0].span_id, traces[1].span_id);
    assert_ne!(traces[0].spans[0].span_id, traces[1].spans[0].span_id);

    let error = &h.events_of(EventKind::StageError)[0];
    let context = error.trace.expect("a failure names its trace");
    assert_eq!(context.trace_id, traces[0].trace_id);
    assert_eq!(context.span_id, traces[0].spans[0].span_id);
    let nak = &h.events_of(EventKind::Nak)[0];
    assert_eq!(nak.trace.map(|c| c.span_id), Some(traces[0].span_id));
    let redelivery = &h.events_of(EventKind::Redelivery)[0];
    assert_eq!(redelivery.trace.map(|c| c.span_id), Some(traces[1].span_id));
    h.finish();
}

#[test]
fn bytes_in_count_every_delivery_and_bytes_out_only_durable_writes() {
    let h = start(
        r#"
nodes:
  - id: good
    type: sink.memory
    from: source
  - id: bad
    type: sink.memory
    from: source
"#,
        1,
    );
    h.sinks.fail_writes_to("bad");
    let sized = |record: Record, kind: Kind, bytes: u64| {
        h.push_as_producer(
            record,
            fusion_core::meta::Arrival {
                kind: Some(kind),
                bytes: Some(bytes),
                ..common::arrival_as(TENANT)
            },
        )
    };

    let walked = sized(body_record(1, "x"), Kind::Log, 100);
    let metric = sized(body_record(2, "x"), Kind::Metric, 30);
    let no_size = h.push(body_record(3, "x"));
    for probe in [walked, metric, no_size] {
        assert!(probe.wait(WAIT).is_some());
    }

    assert_eq!(
        h.counter(CounterMetric::BytesIn, &[("tenant", TENANT)]),
        130,
        "the rejected metric counts, the record with no size does not"
    );
    // What `sink.memory` writes: a record's JSON.
    let written: u64 = h
        .sinks
        .records("good")
        .iter()
        .map(|r| r.to_json().expect("serializes").len() as u64)
        .sum();
    assert!(written > 0);
    assert_eq!(
        h.counter(
            CounterMetric::BytesOut,
            &[("tenant", TENANT), ("stage", "good")]
        ),
        written
    );
    assert_eq!(
        h.counter(
            CounterMetric::BytesOut,
            &[("tenant", TENANT), ("stage", "bad")]
        ),
        0
    );
    h.finish();
}

#[test]
fn a_panic_downstream_of_a_route_keeps_the_route_label() {
    let h = common::start_with_panics(
        r#"
nodes:
  - id: by_body
    type: route
    routes:
      err: body == "x"
    default: drop
  - id: boom
    type: panics
    from: by_body.err
  - id: out
    type: sink.memory
    from: boom
"#,
    );

    let probe = h.push(body_record(unsampled_id(500), "x"));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));

    let traces = h.traces();
    assert_eq!(traces.len(), 1);
    let route = span(&traces[0].spans, "by_body");
    assert_eq!(route.result, SpanResult::Routed("err".to_owned()));
    assert!(matches!(
        span(&traces[0].spans, "boom").result,
        SpanResult::Error {
            failure: FailureKind::Panic,
            ..
        }
    ));
    h.finish();
}
