//! The OTLP trace sink through the SDK boundary: a kept record trace comes out of a span
//! processor as a delivery span and one span per node, with the pipeline's ids, the engine's
//! times, the parent links and error status on a failed node. The in-memory exporter stands
//! in for the collector.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use fusion_core::io::FailureKind;
use fusion_core::record::RecordId;
use fusion_core::stage::DropReason;
use fusion_core::trace::{NodeSpan, RecordTrace, Settlement, SpanOutcome, TraceKey, TraceSink};
use fusion_otel::OtlpTraceSink;
use opentelemetry::trace::{SpanId, Status, TraceId};
use opentelemetry::{Key, Value};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SpanData};

fn export(trace: RecordTrace) -> Vec<SpanData> {
    let exporter = InMemorySpanExporter::default();
    let sink = OtlpTraceSink::with_exporter(exporter.clone(), fusion_otel::resource());
    sink.export(trace);
    sink.force_flush().expect("flushes");
    exporter.get_finished_spans().expect("exported")
}

fn attribute(span: &SpanData, name: &'static str) -> Option<Value> {
    span.attributes
        .iter()
        .find(|kv| kv.key == Key::from_static_str(name))
        .map(|kv| kv.value.clone())
}

fn text(value: &str) -> Option<Value> {
    Some(Value::from(value.to_owned()))
}

fn at(millis: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(1_000_000 + millis)
}

fn node(key: TraceKey, visit: u32, parent: u32, name: &str, outcome: SpanOutcome) -> NodeSpan {
    NodeSpan {
        span_id: key.span_id(2, visit),
        parent_span_id: key.span_id(2, parent),
        node: name.to_owned(),
        start: at(u64::from(visit) * 10),
        end: at(u64::from(visit) * 10 + 5),
        outcome,
        reason: None,
        failure: None,
        label: None,
        records: None,
        error: None,
    }
}

#[test]
fn a_kept_trace_exports_a_delivery_span_and_a_span_per_node() {
    let key = TraceKey::new(RecordId(9), "acme");
    let dropped = NodeSpan {
        reason: Some(DropReason::Filter),
        ..node(key, 1, 0, "keep_errors", SpanOutcome::Drop)
    };
    let failed = NodeSpan {
        failure: Some(FailureKind::SinkError),
        error: Some("downstream is gone".to_owned()),
        ..node(key, 2, 1, "out", SpanOutcome::Error)
    };
    let spans = export(RecordTrace {
        trace_id: key.trace_id(),
        span_id: key.delivery_span_id(2),
        record_id: RecordId(9),
        tenant: Arc::from("acme"),
        delivery_count: 2,
        settlement: Settlement::Nak,
        start: at(0),
        end: at(100),
        spans: vec![dropped, failed],
    });

    let names: Vec<&str> = spans.iter().map(|s| s.name.as_ref()).collect();
    assert_eq!(names.len(), 3, "{names:?}");
    let trace_id = TraceId::from(key.trace_id().0);
    assert!(spans.iter().all(|s| s.span_context.trace_id() == trace_id));
    assert!(spans.iter().all(|s| s.span_context.is_sampled()));

    let find = |name: &str| {
        spans
            .iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("no span `{name}` in {names:?}"))
    };
    let delivery = find("delivery");
    assert_eq!(
        delivery.span_context.span_id(),
        SpanId::from(key.delivery_span_id(2).0)
    );
    assert_eq!(delivery.parent_span_id, SpanId::INVALID);
    assert_eq!((delivery.start_time, delivery.end_time), (at(0), at(100)));
    assert_eq!(attribute(delivery, "record.id"), text("9"));
    assert_eq!(attribute(delivery, "tenant"), text("acme"));
    assert_eq!(attribute(delivery, "delivery_count"), Some(Value::I64(2)));
    assert_eq!(attribute(delivery, "settlement"), text("nak"));
    assert!(matches!(delivery.status, Status::Error { .. }));

    let filter = find("keep_errors");
    assert_eq!(
        filter.span_context.span_id(),
        SpanId::from(key.span_id(2, 1).0)
    );
    assert_eq!(filter.parent_span_id, delivery.span_context.span_id());
    assert_eq!((filter.start_time, filter.end_time), (at(10), at(15)));
    assert_eq!(attribute(filter, "node"), text("keep_errors"));
    assert_eq!(attribute(filter, "outcome"), text("drop"));
    assert_eq!(attribute(filter, "reason"), text("filter"));
    assert_eq!(attribute(filter, "record.id"), text("9"));
    assert_eq!(filter.status, Status::Unset);

    let sink = find("out");
    assert_eq!(sink.parent_span_id, filter.span_context.span_id());
    assert_eq!(attribute(sink, "outcome"), text("error"));
    assert_eq!(attribute(sink, "failure"), text("sink_error"));
    assert_eq!(
        sink.status,
        Status::Error {
            description: "downstream is gone".into()
        }
    );
}

#[test]
fn an_acked_delivery_is_not_an_error_and_names_its_label_and_split() {
    let key = TraceKey::new(RecordId(3), "acme");
    let routed = NodeSpan {
        label: Some("linux".to_owned()),
        ..node(key, 1, 0, "by_format", SpanOutcome::Routed)
    };
    let split = NodeSpan {
        records: Some(4),
        ..node(key, 2, 1, "lines", SpanOutcome::Split)
    };
    let spans = export(RecordTrace {
        trace_id: key.trace_id(),
        span_id: key.delivery_span_id(2),
        record_id: RecordId(3),
        tenant: Arc::from("acme"),
        delivery_count: 2,
        settlement: Settlement::Ack,
        start: at(0),
        end: at(100),
        spans: vec![routed, split],
    });

    let find = |name: &str| spans.iter().find(|s| s.name == name).expect("span");
    assert_eq!(find("delivery").status, Status::Unset);
    assert_eq!(attribute(find("by_format"), "label"), text("linux"));
    assert_eq!(attribute(find("lines"), "records"), Some(Value::I64(4)));
    assert_eq!(attribute(find("lines"), "reason"), None);
}

#[test]
fn the_sampler_argument_is_a_ratio_from_zero_to_one() {
    let key = TraceKey::new(RecordId(1), "acme");
    assert!(
        fusion_otel::sampling(Some("1"))
            .expect("a ratio")
            .keeps(key)
    );
    assert!(
        !fusion_otel::sampling(Some("0"))
            .expect("a ratio")
            .keeps(key)
    );
    assert_eq!(
        fusion_otel::sampling(None).expect("the default"),
        fusion_core::trace::TraceSampling::default()
    );
    assert_eq!(
        fusion_otel::sampling(Some("  ")).expect("the default"),
        fusion_core::trace::TraceSampling::default()
    );
    for bad in ["1.5", "-0.1", "one percent", "NaN"] {
        let err = fusion_otel::sampling(Some(bad)).expect_err(bad);
        assert!(err.to_string().contains("OTEL_TRACES_SAMPLER_ARG"), "{err}");
    }
}
