//! OTLP telemetry wiring: the [`Recorder`] that turns the engine's measurements into
//! OpenTelemetry instruments, and the exporter that ships them to the collector.
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
//! runtime is involved. Configuration is the standard OpenTelemetry environment:
//! `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`) selects the
//! collector and `OTEL_METRIC_EXPORT_INTERVAL` the cadence. With neither endpoint set,
//! [`init`] reports that telemetry is off and the binary records nothing.

pub mod process;

use fusion_core::metrics::{CounterMetric, HistogramMetric, Labels, Metrics, Recorder};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider as _};
use opentelemetry_otlp::{MetricExporter, OTEL_EXPORTER_OTLP_ENDPOINT};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;

/// The `service.name` resource attribute and the meter name.
pub const SERVICE_NAME: &str = "fusion-pipeline";

/// The metrics-specific endpoint variable, which takes precedence over the general one.
const OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT";

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
    /// The OTLP exporter could not be built from the environment.
    #[error("could not build the OTLP metrics exporter: {0}")]
    Exporter(#[source] opentelemetry_otlp::ExporterBuildError),
    /// The meter provider could not flush or shut down.
    #[error("could not shut down the meter provider: {0}")]
    Shutdown(#[source] opentelemetry_sdk::error::OTelSdkError),
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

/// A running exporter. Keep it alive for as long as the engine runs and call
/// [`Telemetry::shutdown`] afterwards so the last interval is flushed.
#[derive(Debug)]
pub struct Telemetry {
    provider: SdkMeterProvider,
    metrics: Metrics,
}

impl Telemetry {
    /// The engine's handle on this exporter.
    #[must_use]
    pub fn metrics(&self) -> Metrics {
        self.metrics.clone()
    }

    /// Flush what has not been exported yet and stop the reader thread.
    ///
    /// # Errors
    ///
    /// [`OtelError::Shutdown`] when the final export fails.
    pub fn shutdown(self) -> Result<(), OtelError> {
        self.provider.shutdown().map_err(OtelError::Shutdown)
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

/// Whether the environment names a collector to export to.
#[must_use]
pub fn configured() -> bool {
    [
        OTEL_EXPORTER_OTLP_METRICS_ENDPOINT,
        OTEL_EXPORTER_OTLP_ENDPOINT,
    ]
    .iter()
    .any(|var| std::env::var(var).is_ok_and(|v| !v.trim().is_empty()))
}

/// Start exporting to the collector the environment names, or return `None` when it names
/// none.
///
/// # Errors
///
/// [`OtelError::Exporter`] when the endpoint or another `OTEL_EXPORTER_OTLP_*` variable is
/// unusable.
pub fn init() -> Result<Option<Telemetry>, OtelError> {
    if !configured() {
        return Ok(None);
    }
    let exporter = MetricExporter::builder()
        .with_http()
        .build()
        .map_err(OtelError::Exporter)?;
    let provider = SdkMeterProvider::builder()
        .with_resource(resource())
        .with_periodic_exporter(exporter)
        .build();
    let meter = provider.meter(SERVICE_NAME);
    process::observe(&meter);
    let metrics = Metrics::new(OtlpRecorder::new(&meter));
    Ok(Some(Telemetry { provider, metrics }))
}
