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

use fusion_core::trace::{NodeSpan, RecordTrace, Settlement, SpanResult, TraceSink};
use opentelemetry::trace::{
    SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceId, TraceState,
};
use opentelemetry::{InstrumentationScope, KeyValue, Value};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{
    BatchConfig, BatchConfigBuilder, BatchSpanProcessor, SpanData, SpanExporter, SpanProcessor,
};

use crate::{SERVICE_NAME, saturating_i64};

/// A [`TraceSink`] over a batch span processor.
#[derive(Debug, Clone)]
pub struct OtlpTraceSink {
    processor: Arc<BatchSpanProcessor>,
    scope: InstrumentationScope,
}

impl OtlpTraceSink {
    /// Export through a batch processor to `exporter`, under `resource`, with the queue the
    /// `OTEL_BSP_*` environment sizes (2048 spans by default).
    #[must_use]
    pub fn with_exporter(exporter: impl SpanExporter + 'static, resource: Resource) -> Self {
        Self::with_config(exporter, resource, BatchConfigBuilder::default().build())
    }

    /// As [`OtlpTraceSink::with_exporter`], with a queue of `max_queue` spans, which is also
    /// the most spans one export carries. A full queue drops the span.
    ///
    /// For tests that need a queue of a known size; a deployment sizes it through the
    /// environment.
    #[doc(hidden)]
    #[must_use]
    pub fn with_queue(
        exporter: impl SpanExporter + 'static,
        resource: Resource,
        max_queue: usize,
    ) -> Self {
        let config = BatchConfigBuilder::default()
            .with_max_queue_size(max_queue)
            .with_max_export_batch_size(max_queue)
            .build();
        Self::with_config(exporter, resource, config)
    }

    fn with_config(
        exporter: impl SpanExporter + 'static,
        resource: Resource,
        config: BatchConfig,
    ) -> Self {
        let mut processor = BatchSpanProcessor::builder(exporter)
            .with_batch_config(config)
            .build();
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

/// The attributes of the node span `span`, after the ones every span of the trace shares.
/// Returns them with the span's status.
fn node_attributes(shared: &[KeyValue], span: NodeSpan) -> (Vec<KeyValue>, Status) {
    let mut attributes = Vec::with_capacity(shared.len() + 3);
    attributes.extend_from_slice(shared);
    attributes.push(KeyValue::new("node", span.node));
    attributes.push(KeyValue::new("outcome", span.result.outcome().as_str()));
    let status = match span.result {
        SpanResult::Pass | SpanResult::StateErrorPass | SpanResult::Written => Status::Unset,
        SpanResult::Routed(label) => {
            attributes.push(KeyValue::new("label", label));
            Status::Unset
        }
        SpanResult::Split(records) => {
            attributes.push(KeyValue::new("records", saturating_i64(records)));
            Status::Unset
        }
        SpanResult::Drop(reason) => {
            attributes.push(KeyValue::new("reason", reason.as_str()));
            Status::Unset
        }
        SpanResult::Error { failure, error } => {
            attributes.push(KeyValue::new("failure", failure.as_str()));
            Status::error(error)
        }
    };
    (attributes, status)
}

impl TraceSink for OtlpTraceSink {
    fn export(&self, trace: RecordTrace) {
        let RecordTrace {
            trace_id,
            span_id,
            record_id,
            tenant,
            delivery_count,
            settlement,
            start,
            end,
            spans,
        } = trace;
        let trace_id = TraceId::from(trace_id.get());
        // Every span carries these; the values are reference-counted, so each span clones a
        // pointer, not the text.
        let shared = [
            KeyValue::new(
                "record.id",
                Value::from(Arc::<str>::from(record_id.to_string())),
            ),
            KeyValue::new("tenant", Value::from(tenant)),
        ];
        for span in spans {
            let id = span.span_id.get();
            let parent = SpanId::from(span.parent_span_id.get());
            let name = Cow::Owned(span.node.clone());
            let (start, end) = (span.start, span.end);
            let (attributes, status) = node_attributes(&shared, span);
            self.processor.on_end(self.span(
                trace_id,
                Span {
                    id,
                    parent,
                    name,
                    start,
                    end,
                    attributes,
                    status,
                },
            ));
        }
        let status = match settlement {
            Settlement::Ack => Status::Unset,
            Settlement::Nak => Status::error("nak"),
        };
        let [record_id, tenant] = shared;
        self.processor.on_end(self.span(
            trace_id,
            Span {
                id: span_id.get(),
                parent: SpanId::INVALID,
                name: Cow::Borrowed("delivery"),
                start,
                end,
                attributes: vec![
                    record_id,
                    tenant,
                    KeyValue::new("delivery_count", saturating_i64(delivery_count)),
                    KeyValue::new("settlement", settlement.as_str()),
                ],
                status,
            },
        ));
    }
}
