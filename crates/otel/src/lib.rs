//! OTLP telemetry wiring: the [`Recorder`] that turns the engine's measurements into
//! OpenTelemetry instruments, the [`OtlpEventLog`] and [`OtlpTraceSink`] that turn its
//! events and kept record traces into log records and spans, and the exporters that ship
//! all three to the collector.
//!
//! Every metric becomes one instrument, created up front and named as the spec spells it: a
//! counter for each [`CounterMetric`], an `f64` histogram in seconds with sub-second buckets
//! for each [`HistogramMetric`]. The instruments sit in arrays indexed by the metric, built
//! from the closed sets, so a recorded metric always has its instrument. Labels become
//! attributes with the same names, so the collector's Prometheus exporter surfaces
//! `records_dropped_total{tenant, stage, reason}` verbatim.
//!
//! The process also reports its own CPU time, resident memory and thread count; see
//! [`process`].
//!
//! The resource carries `service.name` and a `service.instance.id` (the hostname, which in
//! compose is the container id), and the collector turns resource attributes into labels, so
//! several pipeline instances behind one consumer never write the same Prometheus series.
//!
//! Export is OTLP over HTTP/protobuf on a blocking client: the engine runs on plain threads,
//! and the SDK's periodic reader drives the exporter from its own thread, so no async
//! runtime is involved; the log and span batch processors run their own threads the same
//! way. Configuration is the standard OpenTelemetry environment:
//! `OTEL_EXPORTER_OTLP_ENDPOINT` selects the collector for every signal, and
//! `OTEL_EXPORTER_OTLP_{METRICS,LOGS,TRACES}_ENDPOINT` for one;
//! `OTEL_METRIC_EXPORT_INTERVAL` sets the metrics cadence, `OTEL_BLRP_*` and `OTEL_BSP_*` the
//! bounded log and span queues, and `OTEL_TRACES_SAMPLER_ARG` the share of passing records
//! traced (default 0.01). A signal with no endpoint is off: no metrics are recorded, events go
//! to stderr, and no trace is exported.

mod events;
pub mod process;
mod traces;

use fusion_core::events::StderrEventLog;
use fusion_core::metrics::{CounterMetric, HistogramMetric, Labels, Metrics, Recorder};
use fusion_core::signals::Signals;
use fusion_core::trace::{DEFAULT_SAMPLE_RATIO, TraceSampling};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider as _};
use opentelemetry_otlp::{LogExporter, MetricExporter, OTEL_EXPORTER_OTLP_ENDPOINT, SpanExporter};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;

pub use events::OtlpEventLog;
pub use traces::OtlpTraceSink;

/// The `service.name` resource attribute and the meter name.
pub const SERVICE_NAME: &str = "fusion-pipeline";

/// The signal-specific endpoint variables, each taking precedence over the general one.
const OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT";
const OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT";
const OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";

/// The share of passing records traced, as the standard sampler argument.
pub const OTEL_TRACES_SAMPLER_ARG: &str = "OTEL_TRACES_SAMPLER_ARG";

/// Histogram boundaries in seconds for stage runs and sink writes: 100µs to 10s.
const SECONDS_BOUNDARIES: [f64; 16] = [
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
    5.0, 10.0,
];

/// Histogram boundaries in seconds for end to end: 1ms to 2 minutes, since a record that
/// was nakked comes back after a backoff that sums to tens of seconds before its ack.
const END_TO_END_BOUNDARIES: [f64; 16] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
];

/// Errors from setting up or shutting down the exporter.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OtelError {
    /// An OTLP exporter could not be built from the environment.
    #[error("could not build the OTLP {signal} exporter: {source}")]
    Exporter {
        /// `metrics`, `logs` or `traces`.
        signal: &'static str,
        /// What the builder said.
        #[source]
        source: opentelemetry_otlp::ExporterBuildError,
    },
    /// `OTEL_TRACES_SAMPLER_ARG` is not a ratio.
    #[error("`{OTEL_TRACES_SAMPLER_ARG}` is `{0}`; it needs a number from 0 to 1")]
    SamplerArg(String),
    /// A provider or processor could not flush or shut down.
    #[error("could not shut down the {signal} exporter: {source}")]
    Shutdown {
        /// `metrics`, `logs` or `traces`.
        signal: &'static str,
        /// What the SDK said.
        #[source]
        source: opentelemetry_sdk::error::OTelSdkError,
    },
}

/// A [`Recorder`] over OpenTelemetry instruments, one per metric, indexed by it.
#[derive(Debug, Clone)]
pub struct OtlpRecorder {
    counters: [Counter<u64>; CounterMetric::ALL.len()],
    histograms: [Histogram<f64>; HistogramMetric::ALL.len()],
}

impl OtlpRecorder {
    /// Create every instrument on `meter`.
    #[must_use]
    pub fn new(meter: &Meter) -> Self {
        Self {
            counters: CounterMetric::ALL.map(|metric| meter.u64_counter(metric.as_str()).build()),
            histograms: HistogramMetric::ALL.map(|metric| {
                let boundaries = if metric == HistogramMetric::EndToEnd {
                    END_TO_END_BOUNDARIES.to_vec()
                } else {
                    SECONDS_BOUNDARIES.to_vec()
                };
                meter
                    .f64_histogram(metric.as_str())
                    .with_unit("s")
                    .with_boundaries(boundaries)
                    .build()
            }),
        }
    }
}

fn attributes(labels: &Labels<'_>) -> Vec<KeyValue> {
    labels
        .pairs()
        .map(|(name, value)| KeyValue::new(name, value.to_owned()))
        .collect()
}

impl Recorder for OtlpRecorder {
    fn count(&self, metric: CounterMetric, labels: &Labels<'_>, by: u64) {
        // `ALL` lists every value in declaration order, so the value is its own index.
        self.counters[metric as usize].add(by, &attributes(labels));
    }

    fn observe(&self, metric: HistogramMetric, labels: &Labels<'_>, value: f64) {
        self.histograms[metric as usize].record(value, &attributes(labels));
    }
}

/// The running exporters. Keep it alive for as long as the engine runs and call
/// [`Telemetry::shutdown`] afterwards so what is queued is flushed.
#[derive(Debug)]
pub struct Telemetry {
    meters: Option<SdkMeterProvider>,
    logs: Option<OtlpEventLog>,
    traces: Option<OtlpTraceSink>,
    signals: Signals,
}

impl Telemetry {
    /// The engine's and the source's handle on every signal.
    #[must_use]
    pub fn signals(&self) -> Signals {
        self.signals.clone()
    }

    /// Which signals are exported, for the startup line: `metrics, logs, traces`, or `off`.
    #[must_use]
    pub fn exported(&self) -> String {
        let on: Vec<&str> = [
            self.meters.as_ref().map(|_| "metrics"),
            self.logs.as_ref().map(|_| "logs"),
            self.traces.as_ref().map(|_| "traces"),
        ]
        .into_iter()
        .flatten()
        .collect();
        if on.is_empty() {
            "off".to_owned()
        } else {
            on.join(", ")
        }
    }

    /// Flush what has not been exported yet and stop every exporter thread.
    ///
    /// # Errors
    ///
    /// [`OtelError::Shutdown`] for the first exporter whose final export fails; the others
    /// are still shut down.
    pub fn shutdown(self) -> Result<(), OtelError> {
        let meters = self
            .meters
            .map(|p| p.shutdown().map_err(|e| ("metrics", e)));
        let logs = self.logs.map(|l| l.shutdown().map_err(|e| ("logs", e)));
        let traces = self.traces.map(|t| t.shutdown().map_err(|e| ("traces", e)));
        [meters, logs, traces]
            .into_iter()
            .flatten()
            .collect::<Result<Vec<()>, _>>()
            .map(drop)
            .map_err(|(signal, source)| OtelError::Shutdown { signal, source })
    }
}

/// The resource every metric is exported under: the service name and an instance id.
///
/// The instance id is the hostname, or the process id when the hostname is unreadable.
#[must_use]
pub fn resource() -> Resource {
    let instance = std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|h| h.trim().to_owned())
        .filter(|h| !h.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok().filter(|h| !h.is_empty()))
        .unwrap_or_else(|| std::process::id().to_string());
    Resource::builder()
        .with_service_name(SERVICE_NAME)
        .with_attribute(KeyValue::new("service.instance.id", instance))
        .build()
}

/// `value` as an OTLP integer, capped at `i64::MAX`.
fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Whether the environment names a collector for the signal whose own endpoint variable is
/// `specific`.
fn configured(specific: &str) -> bool {
    [specific, OTEL_EXPORTER_OTLP_ENDPOINT]
        .iter()
        .any(|var| std::env::var(var).is_ok_and(|v| !v.trim().is_empty()))
}

/// The share of passing records to trace, from the value of `OTEL_TRACES_SAMPLER_ARG`:
/// the default when unset or blank.
///
/// # Errors
///
/// [`OtelError::SamplerArg`] when the value is not a number from 0 to 1.
pub fn sampling(arg: Option<&str>) -> Result<TraceSampling, OtelError> {
    match arg.map(str::trim).filter(|arg| !arg.is_empty()) {
        None => Ok(TraceSampling::ratio(DEFAULT_SAMPLE_RATIO)),
        Some(arg) => arg
            .parse::<f64>()
            .ok()
            .filter(|ratio| (0.0..=1.0).contains(ratio))
            .map(TraceSampling::ratio)
            .ok_or_else(|| OtelError::SamplerArg(arg.to_owned())),
    }
}

/// Start an exporter for every signal the environment names a collector for. A signal with
/// none is off: metrics are not recorded, events are written to stderr, no trace is
/// exported.
///
/// # Errors
///
/// [`OtelError::Exporter`] when an endpoint or another `OTEL_EXPORTER_OTLP_*` variable is
/// unusable, [`OtelError::SamplerArg`] when traces are exported and the sampler argument is
/// unusable.
pub fn init() -> Result<Telemetry, OtelError> {
    let resource = resource();
    let meters = if configured(OTEL_EXPORTER_OTLP_METRICS_ENDPOINT) {
        let exporter = MetricExporter::builder()
            .with_http()
            .build()
            .map_err(|source| OtelError::Exporter {
                signal: "metrics",
                source,
            })?;
        Some(
            SdkMeterProvider::builder()
                .with_resource(resource.clone())
                .with_periodic_exporter(exporter)
                .build(),
        )
    } else {
        None
    };
    let logs = if configured(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT) {
        let exporter = LogExporter::builder()
            .with_http()
            .build()
            .map_err(|source| OtelError::Exporter {
                signal: "logs",
                source,
            })?;
        Some(OtlpEventLog::with_exporter(exporter, resource.clone()))
    } else {
        None
    };
    let traces = if configured(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) {
        let sampling = sampling(std::env::var(OTEL_TRACES_SAMPLER_ARG).ok().as_deref())?;
        let exporter = SpanExporter::builder()
            .with_http()
            .build()
            .map_err(|source| OtelError::Exporter {
                signal: "traces",
                source,
            })?;
        Some((OtlpTraceSink::with_exporter(exporter, resource), sampling))
    } else {
        None
    };

    let metrics = meters.as_ref().map_or_else(Metrics::noop, |provider| {
        let meter = provider.meter(SERVICE_NAME);
        process::observe(&meter);
        Metrics::new(OtlpRecorder::new(&meter))
    });
    let mut signals = Signals::new(metrics);
    signals = match &logs {
        Some(log) => signals.with_events(log.clone()),
        None => signals.with_events(StderrEventLog),
    };
    if let Some((sink, sampling)) = &traces {
        signals = signals.with_traces(sink.clone(), *sampling);
    }
    let traces = traces.map(|(sink, _)| sink);
    Ok(Telemetry {
        meters,
        logs,
        traces,
        signals,
    })
}
