//! [`OtlpTraceSink`]: kept record traces as OTLP spans.
//!
//! The pipeline chooses its own trace and span ids and measures its own times (ADR 0006), and
//! the SDK's tracer does neither, so the sink builds each span's [`SpanData`] itself and
//! hands it to a batch span processor, as a tracer would on span end. Every span is marked
//! sampled: the keep decision was taken before the trace got here.
//!
//! The delivery span is named `delivery` and carries `record.id`, `tenant`,
//! `delivery_count` and `settlement`; a nakked delivery has error status. A node span is
//! named after the node and carries `node`, `outcome`, `record.id` and `tenant`, plus
//! `reason`, `failure`, `label` or `records` when they apply; a failed node has error
//! status with the error text.
//!
//! The batch processor's queue is bounded (`OTEL_BSP_*`) and drops rather than blocks.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::SystemTime;

use fusion_core::trace::{NodeSpan, RecordTrace, Settlement, SpanOutcome, TraceSink};
use opentelemetry::trace::{
    SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceId, TraceState,
};
use opentelemetry::{InstrumentationScope, KeyValue};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{BatchSpanProcessor, SpanData, SpanExporter, SpanProcessor};

use crate::{SERVICE_NAME, saturating_i64};

/// A [`TraceSink`] over a batch span processor.
#[derive(Debug, Clone)]
pub struct OtlpTraceSink {
    processor: Arc<BatchSpanProcessor>,
    scope: InstrumentationScope,
}

impl OtlpTraceSink {
    /// Export through a batch processor to `exporter`, under `resource`.
    #[must_use]
    pub fn with_exporter(exporter: impl SpanExporter + 'static, resource: Resource) -> Self {
        let mut processor = BatchSpanProcessor::builder(exporter).build();
        processor.set_resource(&resource);
        Self {
            processor: Arc::new(processor),
            scope: InstrumentationScope::builder(SERVICE_NAME).build(),
        }
    }

    /// Export what is queued.
    ///
    /// # Errors
    ///
    /// The SDK's error when the export fails.
    pub fn force_flush(&self) -> OTelSdkResult {
        self.processor.force_flush()
    }

    /// Export what is queued and stop the processor.
    ///
    /// # Errors
    ///
    /// The SDK's error when the final export fails.
    pub fn shutdown(&self) -> OTelSdkResult {
        self.processor.shutdown()
    }

    fn span(&self, trace_id: TraceId, span: Span) -> SpanData {
        SpanData {
            span_context: SpanContext::new(
                trace_id,
                SpanId::from(span.id),
                TraceFlags::SAMPLED,
                false,
                TraceState::default(),
            ),
            parent_span_id: span.parent,
            parent_span_is_remote: false,
            span_kind: SpanKind::Internal,
            name: span.name,
            start_time: span.start,
            end_time: span.end,
            attributes: span.attributes,
            dropped_attributes_count: 0,
            events: Default::default(),
            links: Default::default(),
            status: span.status,
            instrumentation_scope: self.scope.clone(),
        }
    }
}

/// What differs between the spans of one trace.
struct Span {
    id: u64,
    parent: SpanId,
    name: Cow<'static, str>,
    start: SystemTime,
    end: SystemTime,
    attributes: Vec<KeyValue>,
    status: Status,
}

fn node_attributes(trace: &RecordTrace, span: &NodeSpan) -> Vec<KeyValue> {
    let mut attributes = vec![
        KeyValue::new("node", span.node.clone()),
        KeyValue::new("outcome", span.outcome.as_str()),
        KeyValue::new("record.id", trace.record_id.to_string()),
        KeyValue::new("tenant", trace.tenant.to_string()),
    ];
    if let Some(reason) = span.reason {
        attributes.push(KeyValue::new("reason", reason.as_str()));
    }
    if let Some(failure) = span.failure {
        attributes.push(KeyValue::new("failure", failure.as_str()));
    }
    if let Some(label) = &span.label {
        attributes.push(KeyValue::new("label", label.clone()));
    }
    if let Some(records) = span.records {
        attributes.push(KeyValue::new("records", saturating_i64(records)));
    }
    attributes
}

impl TraceSink for OtlpTraceSink {
    fn export(&self, trace: RecordTrace) {
        let trace_id = TraceId::from(trace.trace_id.0);
        for span in &trace.spans {
            let status = if span.outcome == SpanOutcome::Error {
                Status::error(span.error.clone().unwrap_or_default())
            } else {
                Status::Unset
            };
            self.processor.on_end(self.span(
                trace_id,
                Span {
                    id: span.span_id.0,
                    parent: SpanId::from(span.parent_span_id.0),
                    name: Cow::Owned(span.node.clone()),
                    start: span.start,
                    end: span.end,
                    attributes: node_attributes(&trace, span),
                    status,
                },
            ));
        }
        let status = match trace.settlement {
            Settlement::Ack => Status::Unset,
            Settlement::Nak => Status::error("nak"),
        };
        self.processor.on_end(self.span(
            trace_id,
            Span {
                id: trace.span_id.0,
                parent: SpanId::INVALID,
                name: Cow::Borrowed("delivery"),
                start: trace.start,
                end: trace.end,
                attributes: vec![
                    KeyValue::new("record.id", trace.record_id.to_string()),
                    KeyValue::new("tenant", trace.tenant.to_string()),
                    KeyValue::new("delivery_count", saturating_i64(trace.delivery_count)),
                    KeyValue::new("settlement", trace.settlement.as_str()),
                ],
                status,
            },
        ));
    }
}
