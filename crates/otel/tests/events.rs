//! The OTLP event log through the SDK boundary: an event comes out of a logger provider as
//! one log record named after its kind, at its severity, with the error text as its body,
//! the fixed fields as attributes and the record's trace as its trace context. The
//! in-memory exporter stands in for the collector.

use std::sync::Arc;

use fusion_core::events::{Event, EventKind, EventLog};
use fusion_core::io::FailureKind;
use fusion_core::record::RecordId;
use fusion_core::trace::TraceKey;
use fusion_otel::OtlpEventLog;
use opentelemetry::logs::{AnyValue, Severity};
use opentelemetry_sdk::logs::InMemoryLogExporter;
use opentelemetry_sdk::logs::in_memory_exporter::LogDataWithResource;

fn export(events: Vec<Event>) -> Vec<LogDataWithResource> {
    let exporter = InMemoryLogExporter::default();
    let log = OtlpEventLog::with_exporter(exporter.clone(), fusion_otel::resource());
    for event in events {
        log.emit(event);
    }
    log.force_flush().expect("flushes");
    exporter.get_emitted_logs().expect("exported")
}

fn attribute(log: &LogDataWithResource, name: &str) -> Option<AnyValue> {
    log.record
        .attributes_iter()
        .find(|(key, _)| key.as_str() == name)
        .map(|(_, value)| value.clone())
}

fn text(value: &str) -> Option<AnyValue> {
    Some(AnyValue::String(value.to_owned().into()))
}

#[test]
fn a_stage_error_exports_its_fields_severity_body_and_trace() {
    let context = TraceKey::new(RecordId(7), "acme").delivery_context(2);
    let logs = export(vec![Event {
        kind: EventKind::StageError,
        record_id: Some(RecordId(7)),
        tenant: Arc::from("acme"),
        node: "script".to_owned(),
        reason: Some(FailureKind::StageError),
        delivery_count: 2,
        stream_sequence: None,
        message: "no good".to_owned(),
        trace: Some(context),
    }]);

    assert_eq!(logs.len(), 1);
    let log = &logs[0];
    assert_eq!(log.record.event_name(), Some("stage_error"));
    assert_eq!(log.record.severity_number(), Some(Severity::Error));
    assert_eq!(log.record.severity_text(), Some("error"));
    assert_eq!(log.record.body(), text("no good").as_ref());
    assert_eq!(attribute(log, "event"), text("stage_error"));
    assert_eq!(attribute(log, "record.id"), text("7"));
    assert_eq!(attribute(log, "tenant"), text("acme"));
    assert_eq!(attribute(log, "node"), text("script"));
    assert_eq!(attribute(log, "reason"), text("stage_error"));
    assert_eq!(attribute(log, "delivery_count"), Some(AnyValue::Int(2)));
    assert_eq!(attribute(log, "stream_sequence"), None);
    let trace = log.record.trace_context().expect("a trace context");
    assert_eq!(trace.trace_id.to_string(), context.trace_id.to_string());
    assert_eq!(trace.span_id.to_string(), context.span_id.to_string());
    assert_eq!(
        log.resource
            .get(&opentelemetry::Key::from_static_str("service.name"))
            .map(|v| v.to_string()),
        Some("fusion-pipeline".to_owned())
    );
}

#[test]
fn an_event_without_a_record_or_reason_leaves_them_out() {
    let logs = export(vec![Event {
        kind: EventKind::Redelivery,
        record_id: None,
        tenant: Arc::from("unknown"),
        node: "source".to_owned(),
        reason: None,
        delivery_count: 3,
        stream_sequence: Some(42),
        message: String::new(),
        trace: None,
    }]);

    let log = &logs[0];
    assert_eq!(log.record.severity_number(), Some(Severity::Info));
    assert_eq!(log.record.body(), text("redelivery").as_ref());
    assert_eq!(attribute(log, "record.id"), None);
    assert_eq!(attribute(log, "reason"), None);
    assert_eq!(attribute(log, "stream_sequence"), Some(AnyValue::Int(42)));
    assert!(log.record.trace_context().is_none());
}

#[test]
fn every_kind_exports_at_its_severity() {
    let logs = export(
        EventKind::ALL
            .iter()
            .map(|&kind| Event {
                kind,
                record_id: None,
                tenant: Arc::from("acme"),
                node: "source".to_owned(),
                reason: None,
                delivery_count: 1,
                stream_sequence: None,
                message: String::new(),
                trace: None,
            })
            .collect(),
    );

    let severities: Vec<(Option<&str>, Option<Severity>)> = logs
        .iter()
        .map(|l| (l.record.event_name(), l.record.severity_number()))
        .collect();
    assert_eq!(
        severities,
        [
            (Some("stage_error"), Some(Severity::Error)),
            (Some("nak"), Some(Severity::Warn)),
            (Some("redelivery"), Some(Severity::Info)),
            (Some("dead_letter"), Some(Severity::Warn)),
            (Some("dead_letter_failed"), Some(Severity::Error)),
        ]
    );
}
