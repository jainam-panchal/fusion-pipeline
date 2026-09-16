//! [`OtlpEventLog`]: the pipeline's events as OTLP log records.
//!
//! One event is one log record: its kind is the event name and the `event` attribute, its
//! severity the kind's, its error text the body (the kind when there is none), its fixed
//! fields attributes, and the record's trace its trace context, so Loki stores `trace_id`
//! and `span_id` beside the line. `record.id` is text: a snowflake does not fit an OTLP
//! integer, and Loki's structured metadata is text anyway. It surfaces there as `record_id`.
//!
//! Records go through the SDK's batch processor: a bounded queue (`OTEL_BLRP_*`) that drops
//! rather than blocks when the collector falls behind, so a flood of failures never holds up
//! a worker.

use fusion_core::events::{Event, EventLog, Severity as EventSeverity};
use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, LoggerProvider as _, Severity};
use opentelemetry::trace::{SpanId, TraceId};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{LogExporter, SdkLogger, SdkLoggerProvider};

use crate::SERVICE_NAME;

/// An [`EventLog`] over an OpenTelemetry logger. Clones share the logger.
#[derive(Debug, Clone)]
pub struct OtlpEventLog {
    provider: SdkLoggerProvider,
    logger: SdkLogger,
}

impl OtlpEventLog {
    /// Log through a batch processor exporting to `exporter`, under `resource`.
    #[must_use]
    pub fn with_exporter(exporter: impl LogExporter + 'static, resource: Resource) -> Self {
        let provider = SdkLoggerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build();
        let logger = provider.logger(SERVICE_NAME);
        Self { provider, logger }
    }

    /// Export what is queued.
    ///
    /// # Errors
    ///
    /// The SDK's error when the export fails.
    pub fn force_flush(&self) -> OTelSdkResult {
        self.provider.force_flush()
    }

    /// Export what is queued and stop the processor.
    ///
    /// # Errors
    ///
    /// The SDK's error when the final export fails.
    pub fn shutdown(&self) -> OTelSdkResult {
        self.provider.shutdown()
    }
}

const fn severity(severity: EventSeverity) -> Severity {
    match severity {
        EventSeverity::Info => Severity::Info,
        EventSeverity::Warn => Severity::Warn,
        EventSeverity::Error => Severity::Error,
    }
}

impl EventLog for OtlpEventLog {
    fn emit(&self, event: Event) {
        let mut record = self.logger.create_log_record();
        let kind = event.kind.as_str();
        record.set_event_name(kind);
        record.set_timestamp(std::time::SystemTime::now());
        record.set_severity_number(severity(event.kind.severity()));
        record.set_severity_text(event.kind.severity().as_str());
        record.set_body(AnyValue::from(if event.message.is_empty() {
            kind.to_owned()
        } else {
            event.message
        }));
        record.add_attribute("event", kind);
        if let Some(id) = event.record_id {
            record.add_attribute("record.id", id.to_string());
        }
        record.add_attribute("tenant", event.tenant.to_string());
        record.add_attribute("node", event.node);
        if let Some(reason) = event.reason {
            record.add_attribute("reason", reason.as_str());
        }
        record.add_attribute("delivery_count", saturating_i64(event.delivery_count));
        if let Some(sequence) = event.stream_sequence {
            record.add_attribute("stream_sequence", saturating_i64(sequence));
        }
        if let Some(trace) = event.trace {
            record.set_trace_context(
                TraceId::from(trace.trace_id.0),
                SpanId::from(trace.span_id.0),
                None,
            );
        }
        self.logger.emit(record);
    }
}

/// `value` as an OTLP integer, capped at `i64::MAX`.
pub(crate) fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
