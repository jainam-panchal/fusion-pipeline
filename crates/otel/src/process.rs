//! The process's own CPU time, resident memory and thread count, as OpenTelemetry's
//! `process.*` semantic conventions, read from `/proc/self` at every export.
//!
//! These are not pipeline metrics and sit outside the spec's closed set; they answer "what
//! does the pipeline cost" next to NATS's and Dragonfly's self-reported figures. Through the
//! collector's Prometheus exporter they surface as `process_cpu_time_seconds_total`,
//! `process_memory_usage_bytes` and `process_thread_count`. On a system without `/proc`
//! nothing is observed.

use opentelemetry::metrics::Meter;

/// Register the observable instruments on `meter`. The SDK owns the callbacks from here on.
pub fn observe(meter: &Meter) {
    meter
        .f64_observable_counter("process.cpu.time")
        .with_unit("s")
        .with_description("CPU time consumed by the pipeline process, user and system.")
        .with_callback(|observer| {
            if let Some(seconds) = cpu_seconds() {
                observer.observe(seconds, &[]);
            }
        })
        .build();
    meter
        .u64_observable_gauge("process.memory.usage")
        .with_unit("By")
        .with_description("Resident set size of the pipeline process.")
        .with_callback(|observer| {
            if let Some(bytes) = status_field("VmRSS:").map(|kib| kib * 1024) {
                observer.observe(bytes, &[]);
            }
        })
        .build();
    meter
        .u64_observable_gauge("process.thread.count")
        .with_unit("{thread}")
        .with_description("Threads in the pipeline process: workers, source, NATS I/O, exporter.")
        .with_callback(|observer| {
            if let Some(threads) = status_field("Threads:") {
                observer.observe(threads, &[]);
            }
        })
        .build();
}

/// Seconds this process has spent on a CPU: the first field of `/proc/self/schedstat`, in
/// nanoseconds, which needs no clock-tick conversion.
fn cpu_seconds() -> Option<f64> {
    let schedstat = std::fs::read_to_string("/proc/self/schedstat").ok()?;
    let nanos: u64 = schedstat.split_whitespace().next()?.parse().ok()?;
    Some(nanos as f64 / 1e9)
}

/// The number on the `/proc/self/status` line starting with `key` (`VmRSS:` is in kiB).
fn status_field(key: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix(key))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}
