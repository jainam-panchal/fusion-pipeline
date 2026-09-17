//! `loghub-verifier`: once the pipeline has settled every message the producer published,
//! read what reached the sink subjects and the dead-letter stream, judge it against the
//! expectations file, print the summary and export it to the collector.
//!
//! The pipeline has settled once consumer `LOGS/pipeline` shows nothing pending and nothing
//! awaiting an ack on three polls in a row, a second apart. Every sink subject
//! (`processed.>`) and every dead-letter subject (`dlq.>`) is then read from the start of its
//! stream through ephemeral ordered consumers, which belong to the harness: the verifier
//! creates no stream. The run script purges `PROCESSED` and `DLQ` before the producer starts,
//! so what is there is this run's.
//!
//! When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, the report goes to the collector as gauges
//! under `service.name=loghub-verifier`: `loghub_published`, `loghub_received`,
//! `loghub_missing`, `loghub_unexpected`, `loghub_extra_copies`, `loghub_dead_lettered`,
//! `loghub_edit_mismatch`, `loghub_duplicates_planned`, `loghub_duplicates_dropped`, and per
//! `format`, `loghub_extraction_checked`, `loghub_extraction_mismatch` and
//! `loghub_extraction_accuracy`. They are exported once, on exit; the collector keeps
//! serving a series for its `metric_expiration` (5m), and Prometheus keeps what it scraped.
//!
//! Exits 0 on a pass, 1 on a fail (anything missing, unexpected, dead-lettered or wrongly
//! edited, or `dedupe` dropping under half the planned duplicates), 2 when the run could not
//! be judged.

use std::io::BufRead;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use async_nats::jetstream::consumer::pull::OrderedConfig;
use async_nats::jetstream::{self, Context};
use fusion_harness::cli;
use fusion_harness::expect::Expectation;
use fusion_harness::verdict::{DeadLetter, Report, Written, judge};
use fusion_nats::headers::{RECORD_ID, TENANT};
use futures::StreamExt;
use opentelemetry::KeyValue;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::MetricExporter;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use serde_json::{Map, Value};

const USAGE: &str = "usage: loghub-verifier [--expectations target/loghub/expectations.jsonl] \
[--nats-url nats://127.0.0.1:4222] [--settle-timeout 120s]";

const FLAGS: [&str; 3] = ["--expectations", "--nats-url", "--settle-timeout"];

/// Where the pipeline reads from.
const SOURCE_STREAM: &str = "LOGS";
const SOURCE_CONSUMER: &str = "pipeline";

/// Where the POC config writes. Every subject there is read, so a message on a subject no set
/// routes to is unexpected rather than unseen.
const SINK_STREAM: &str = "PROCESSED";
const SINK_SUBJECTS: &str = "processed.>";
const DEAD_LETTER_STREAM: &str = "DLQ";
const DEAD_LETTER_SUBJECTS: &str = "dlq.>";

/// Clean consumer polls in a row that count as settled, and the pause between polls.
const SETTLED_POLLS: u32 = 3;
const POLL: Duration = Duration::from_secs(1);

/// How long a read waits for the next message before taking the subject as read.
const IDLE: Duration = Duration::from_secs(5);

struct Options {
    expectations: PathBuf,
    nats_url: String,
    settle_timeout: Duration,
}

fn options() -> Result<Options, String> {
    let flags = cli::parse(std::env::args().skip(1), &FLAGS)?;
    Ok(Options {
        expectations: flags.expectations(),
        nats_url: flags.nats_url(),
        settle_timeout: flags
            .get("--settle-timeout")
            .map_or(Ok(Duration::from_secs(120)), cli::duration)?,
    })
}

fn main() -> ExitCode {
    let options = match options() {
        Ok(options) => options,
        Err(message) => {
            eprintln!("loghub-verifier: {message}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let report = match read_expectations(&options).and_then(|expectations| {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|err| format!("could not start the runtime: {err}"))?;
        // The runtime is dropped before the export: the OTLP client blocks.
        runtime.block_on(observe(&options, &expectations))
    }) {
        Ok(report) => report,
        Err(message) => {
            eprintln!("loghub-verifier: {message}");
            return ExitCode::from(2);
        }
    };
    println!("{report}");
    if let Err(message) = export(&report) {
        eprintln!("loghub-verifier: the report was not exported: {message}");
    }
    if report.passed() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn read_expectations(options: &Options) -> Result<Vec<Expectation>, String> {
    let path = &options.expectations;
    let file = std::fs::File::open(path).map_err(|err| format!("{}: {err}", path.display()))?;
    std::io::BufReader::new(file)
        .lines()
        .enumerate()
        .map(|(n, line)| {
            let line = line.map_err(|err| format!("{}: {err}", path.display()))?;
            serde_json::from_str(&line)
                .map_err(|err| format!("{}:{}: {err}", path.display(), n + 1))
        })
        .collect()
}

async fn observe(options: &Options, expectations: &[Expectation]) -> Result<Report, String> {
    let client = async_nats::connect(&options.nats_url)
        .await
        .map_err(|err| format!("could not connect to {}: {err}", options.nats_url))?;
    let js = jetstream::new(client);
    settle(&js, options.settle_timeout).await?;

    let written = read(&js, SINK_STREAM, SINK_SUBJECTS)
        .await?
        .into_iter()
        .map(|message| Written {
            record_id: header(&message, RECORD_ID),
            tenant: header(&message, TENANT),
            attributes: serde_json::from_slice::<Map<String, Value>>(&message.payload)
                .ok()
                .and_then(|mut record| match record.remove("attributes") {
                    Some(Value::Object(attributes)) => Some(attributes),
                    _ => None,
                })
                .unwrap_or_default(),
            subject: message.subject.to_string(),
        })
        .collect::<Vec<_>>();
    let dead = read(&js, DEAD_LETTER_STREAM, DEAD_LETTER_SUBJECTS)
        .await?
        .into_iter()
        .map(|message| DeadLetter {
            record_id: header(&message, RECORD_ID),
        })
        .collect::<Vec<_>>();
    Ok(judge(expectations, &written, &dead))
}

fn header(message: &async_nats::Message, name: &str) -> Option<String> {
    message
        .headers
        .as_ref()?
        .get(name)
        .map(|value| value.as_str().to_owned())
}

/// Wait until the pipeline's consumer has nothing pending and nothing unacked on
/// [`SETTLED_POLLS`] polls in a row.
async fn settle(js: &Context, timeout: Duration) -> Result<(), String> {
    let stream = js
        .get_stream(SOURCE_STREAM)
        .await
        .map_err(|err| format!("stream {SOURCE_STREAM}: {err}"))?;
    let deadline = Instant::now() + timeout;
    let mut clean = 0;
    loop {
        let info = stream
            .consumer_info(SOURCE_CONSUMER)
            .await
            .map_err(|err| format!("consumer {SOURCE_STREAM}/{SOURCE_CONSUMER}: {err}"))?;
        if info.num_pending == 0 && info.num_ack_pending == 0 {
            clean += 1;
            if clean >= SETTLED_POLLS {
                return Ok(());
            }
        } else {
            clean = 0;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "the pipeline did not settle within {timeout:?}: {} pending, {} awaiting ack",
                info.num_pending, info.num_ack_pending
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Every message on `subjects` in `stream`, read from the start.
async fn read(
    js: &Context,
    stream: &str,
    subjects: &str,
) -> Result<Vec<async_nats::Message>, String> {
    let fail = |err: &dyn std::fmt::Display| format!("reading {subjects} from {stream}: {err}");
    let stream = js.get_stream(stream).await.map_err(|err| fail(&err))?;
    let consumer = stream
        .create_consumer(OrderedConfig {
            filter_subject: subjects.to_owned(),
            ..OrderedConfig::default()
        })
        .await
        .map_err(|err| fail(&err))?;
    let mut messages = consumer.messages().await.map_err(|err| fail(&err))?;
    let mut read = Vec::new();
    while let Ok(next) = tokio::time::timeout(IDLE, messages.next()).await {
        let Some(message) = next else { break };
        let message = message.map_err(|err| fail(&err))?;
        let pending = message.info().map_err(|err| fail(&err))?.pending;
        read.push(message.message);
        if pending == 0 {
            break;
        }
    }
    Ok(read)
}

/// Send `report` to the collector named by `OTEL_EXPORTER_OTLP_ENDPOINT`, if any.
fn export(report: &Report) -> Result<(), String> {
    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").map_or(true, |v| v.trim().is_empty()) {
        return Ok(());
    }
    let exporter = MetricExporter::builder()
        .with_http()
        .build()
        .map_err(|err| err.to_string())?;
    let provider = SdkMeterProvider::builder()
        .with_resource(
            Resource::builder()
                .with_service_name("loghub-verifier")
                .build(),
        )
        .with_periodic_exporter(exporter)
        .build();
    let meter = provider.meter("loghub-verifier");
    for (name, value) in [
        ("loghub_published", report.published),
        ("loghub_received", report.received),
        ("loghub_missing", report.missing),
        ("loghub_unexpected", report.unexpected),
        ("loghub_extra_copies", report.extra_copies),
        ("loghub_dead_lettered", report.dead_lettered),
        ("loghub_edit_mismatch", report.edit_mismatch),
        ("loghub_duplicates_planned", report.duplicates_planned),
        ("loghub_duplicates_dropped", report.duplicates_dropped),
    ] {
        meter.u64_gauge(name).build().record(value, &[]);
    }
    let checked = meter.u64_gauge("loghub_extraction_checked").build();
    let mismatch = meter.u64_gauge("loghub_extraction_mismatch").build();
    let accuracy = meter.f64_gauge("loghub_extraction_accuracy").build();
    // A set's name is its log format, the label the issue names.
    for (name, set) in &report.sets {
        let labels = [KeyValue::new("format", name.clone())];
        checked.record(set.checked, &labels);
        mismatch.record(set.mismatched, &labels);
        accuracy.record(set.accuracy(), &labels);
    }
    provider.force_flush().map_err(|err| err.to_string())?;
    provider.shutdown().map_err(|err| err.to_string())
}
