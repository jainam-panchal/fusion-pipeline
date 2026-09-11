//! The OTLP recorder through the SDK boundary: measurements emitted through `Metrics` come
//! out of a meter provider as instruments named as the spec spells them, with the labels as
//! attributes and durations in seconds. The in-memory exporter stands in for the collector.

use fusion_core::metrics::{Labels, Metric, MetricKind, Metrics, Recorder};
use fusion_core::stage::DropReason;
use fusion_otel::OtlpRecorder;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

/// Run `emit` against a recorder on an in-memory provider and return what it exported.
fn export(emit: impl FnOnce(&OtlpRecorder)) -> Vec<ResourceMetrics> {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    let recorder = OtlpRecorder::new(&provider.meter("fusion-pipeline"));
    emit(&recorder);
    provider.force_flush().expect("flush");
    exporter.get_finished_metrics().expect("exported")
}

/// `(name, unit)` of every exported metric.
fn exported_names(exported: &[ResourceMetrics]) -> Vec<(String, String)> {
    let mut names: Vec<(String, String)> = exported
        .iter()
        .flat_map(|rm| rm.scope_metrics())
        .flat_map(|sm| sm.metrics())
        .map(|m| (m.name().to_owned(), m.unit().to_owned()))
        .collect();
    names.sort();
    names.dedup();
    names
}

#[test]
fn every_spec_metric_exports_under_the_spec_name_with_seconds_on_histograms() {
    let exported = export(|recorder| {
        for metric in Metric::ALL {
            let labels = Labels::new("acme", "keep_errors");
            match metric.kind() {
                MetricKind::Counter => recorder.count(metric, &labels, 1),
                MetricKind::Histogram => recorder.observe(metric, &labels, 0.5),
            }
        }
    });

    let mut expected: Vec<(String, String)> = Metric::ALL
        .iter()
        .map(|m| {
            let unit = match m.kind() {
                MetricKind::Counter => "",
                MetricKind::Histogram => "s",
            };
            (m.as_str().to_owned(), unit.to_owned())
        })
        .collect();
    expected.sort();
    assert_eq!(exported_names(&exported), expected);
}

#[test]
fn a_counter_exports_its_labels_as_attributes_and_its_running_total() {
    let exported = export(|recorder| {
        let metrics = Metrics::new(recorder.clone());
        metrics.dropped("acme", "keep_errors", DropReason::Filter);
        metrics.dropped("acme", "keep_errors", DropReason::Filter);
        metrics.dropped("acme", "keep_errors", DropReason::RouteDefaultDrop);
    });

    let dropped = exported
        .iter()
        .flat_map(|rm| rm.scope_metrics())
        .flat_map(|sm| sm.metrics())
        .find(|m| m.name() == "records_dropped_total")
        .expect("records_dropped_total exported");
    let AggregatedMetrics::U64(MetricData::Sum(sum)) = dropped.data() else {
        panic!(
            "records_dropped_total is a u64 sum, got {:?}",
            dropped.data()
        );
    };
    let mut points: Vec<(Vec<(String, String)>, u64)> = sum
        .data_points()
        .map(|dp| {
            let attrs: Vec<(String, String)> = {
                let mut a: Vec<(String, String)> = dp
                    .attributes()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect();
                a.sort();
                a
            };
            (attrs, dp.value())
        })
        .collect();
    points.sort();

    let filter = vec![
        ("reason".to_owned(), "filter".to_owned()),
        ("stage".to_owned(), "keep_errors".to_owned()),
        ("tenant".to_owned(), "acme".to_owned()),
    ];
    let route = vec![
        ("reason".to_owned(), "route_default_drop".to_owned()),
        ("stage".to_owned(), "keep_errors".to_owned()),
        ("tenant".to_owned(), "acme".to_owned()),
    ];
    assert_eq!(points, vec![(filter, 2), (route, 1)]);
}

#[test]
fn a_duration_histogram_has_sub_second_buckets_so_stage_latency_is_not_all_in_one_bin() {
    let exported = export(|recorder| {
        let metrics = Metrics::new(recorder.clone());
        metrics.stage_duration("acme", "keep_errors", std::time::Duration::from_micros(300));
    });

    let duration = exported
        .iter()
        .flat_map(|rm| rm.scope_metrics())
        .flat_map(|sm| sm.metrics())
        .find(|m| m.name() == "stage_duration_seconds")
        .expect("stage_duration_seconds exported");
    let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = duration.data() else {
        panic!(
            "stage_duration_seconds is an f64 histogram, got {:?}",
            duration.data()
        );
    };
    let point = histogram.data_points().next().expect("one data point");
    assert_eq!(point.count(), 1);
    assert!((point.sum() - 0.0003).abs() < 1e-9, "sum {}", point.sum());
    let bounds: Vec<f64> = point.bounds().collect();
    assert!(
        bounds.iter().filter(|b| **b < 0.01).count() >= 3,
        "buckets resolve sub-10ms latencies: {bounds:?}"
    );
    let first_nonempty = point
        .bucket_counts()
        .position(|c| c > 0)
        .expect("the sample landed in a bucket");
    assert!(
        first_nonempty > 0,
        "300µs is not in the lowest bucket: {bounds:?}"
    );
}

#[test]
fn the_resource_names_the_service_and_this_instance() {
    let resource = fusion_otel::resource();

    let name = resource
        .get(&opentelemetry::Key::from_static_str("service.name"))
        .expect("service.name set");
    assert_eq!(name.to_string(), "fusion-pipeline");
    let instance = resource
        .get(&opentelemetry::Key::from_static_str("service.instance.id"))
        .expect("service.instance.id set, so several instances never collide in Prometheus");
    assert!(!instance.to_string().is_empty());
}

/// Every metric with `name` in the export, with its unit and the sum of its data points.
fn exported_sum(exported: &[ResourceMetrics], name: &str) -> Option<(String, f64)> {
    let metric = exported
        .iter()
        .flat_map(|rm| rm.scope_metrics())
        .flat_map(|sm| sm.metrics())
        .find(|m| m.name() == name)?;
    let total = match metric.data() {
        AggregatedMetrics::F64(MetricData::Sum(sum)) => {
            sum.data_points().map(|dp| dp.value()).sum()
        }
        AggregatedMetrics::F64(MetricData::Gauge(gauge)) => {
            gauge.data_points().map(|dp| dp.value()).sum()
        }
        AggregatedMetrics::U64(MetricData::Sum(sum)) => {
            sum.data_points().map(|dp| dp.value() as f64).sum()
        }
        AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
            gauge.data_points().map(|dp| dp.value() as f64).sum()
        }
        AggregatedMetrics::I64(MetricData::Gauge(gauge)) => {
            gauge.data_points().map(|dp| dp.value() as f64).sum()
        }
        other => panic!("{name}: unexpected aggregation {other:?}"),
    };
    Some((metric.unit().to_owned(), total))
}

#[test]
fn the_process_reports_its_own_cpu_time_resident_memory_and_thread_count() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    fusion_otel::process::observe(&provider.meter("fusion-pipeline"));
    // Burn a little CPU so the counter is not zero on a fast machine.
    let mut x = 0u64;
    for i in 0..2_000_000u64 {
        x = x.wrapping_mul(31).wrapping_add(i);
    }
    assert_ne!(x, 1);
    provider.force_flush().expect("flush");
    let exported = exporter.get_finished_metrics().expect("exported");

    let (unit, cpu) =
        exported_sum(&exported, "process.cpu.time").expect("process.cpu.time exported");
    assert_eq!(unit, "s");
    assert!(cpu > 0.0, "cpu time {cpu}");
    let (unit, rss) =
        exported_sum(&exported, "process.memory.usage").expect("process.memory.usage exported");
    assert_eq!(unit, "By");
    assert!(rss > 1_000_000.0, "resident bytes {rss}");
    let (_, threads) =
        exported_sum(&exported, "process.thread.count").expect("process.thread.count exported");
    assert!(threads >= 1.0, "threads {threads}");
}
