//! [`Signals`]: the one handle through which the engine and a source reach all three
//! telemetry signals, metrics, events and record traces (ADR 0006).

use std::sync::Arc;

use crate::events::{Event, EventLog, NoEvents};
use crate::metrics::Metrics;
use crate::trace::{NoTraces, RecordTrace, TraceSampling, TraceSink};

/// Metrics, the event log, the trace sink and the share of passing records to trace.
/// Cheap to clone. A `Metrics` converts into one that logs and traces nothing, so a caller
/// that only measures passes its `Metrics` where a `Signals` is taken.
#[derive(Clone)]
pub struct Signals {
    metrics: Metrics,
    events: Arc<dyn EventLog>,
    traces: Arc<dyn TraceSink>,
    sampling: TraceSampling,
}

impl std::fmt::Debug for Signals {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Signals")
            .field("sampling", &self.sampling)
            .finish_non_exhaustive()
    }
}

impl Signals {
    /// Measure through `metrics`; log and trace nothing.
    #[must_use]
    pub fn new(metrics: Metrics) -> Self {
        Self {
            metrics,
            events: Arc::new(NoEvents),
            traces: Arc::new(NoTraces),
            sampling: TraceSampling::default(),
        }
    }

    /// Record, log and trace nothing.
    #[must_use]
    pub fn noop() -> Self {
        Self::new(Metrics::noop())
    }

    /// Log to `events`.
    #[must_use]
    pub fn with_events(mut self, events: impl EventLog + 'static) -> Self {
        self.events = Arc::new(events);
        self
    }

    /// Export kept traces to `traces`, keeping `sampling` of the passing records.
    #[must_use]
    pub fn with_traces(
        mut self,
        traces: impl TraceSink + 'static,
        sampling: TraceSampling,
    ) -> Self {
        self.traces = Arc::new(traces);
        self.sampling = sampling;
        self
    }

    /// The metrics handle.
    #[must_use]
    pub const fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// The share of passing records traced.
    #[must_use]
    pub const fn sampling(&self) -> TraceSampling {
        self.sampling
    }

    /// Log `event`.
    pub fn emit(&self, event: Event) {
        self.events.emit(event);
    }

    /// Export a kept trace.
    pub fn export(&self, trace: RecordTrace) {
        self.traces.export(trace);
    }
}

impl From<Metrics> for Signals {
    fn from(metrics: Metrics) -> Self {
        Self::new(metrics)
    }
}
