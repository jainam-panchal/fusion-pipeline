//! `loghub-verifier`: follow a run of the producer and the pipeline, judge what reached the
//! sink subjects and the dead-letter stream against the expectations file as it goes, and
//! once the run is over print the summary; the judgement goes to the collector throughout.
//!
//! It may start before the producer (the chaos run) or after it (`deploy/loghub-check.sh`).
//! It reads the expectations file as the producer writes it and every sink subject
//! (`processed.>`) and dead-letter subject (`dlq.>`) from the start of its stream through
//! ephemeral ordered consumers, which belong to the harness: the verifier creates no stream.
//! The run script removes the expectations file and its done marker and purges `LOGS`,
//! `PROCESSED` and `DLQ` before the producer starts, so what is there is this run's.
//!
//! Every [`TICK`] it judges what it has read so far; while records are in flight they count
//! as missing, and a message whose expectation the producer has not written yet waits for the
//! next judgement, so the numbers converge as the pipeline catches up. The run is over once the
//! producer has written its done marker, consumer `LOGS/pipeline` has shown nothing pending
//! and nothing awaiting an ack on three polls in a row, a second apart, and each stream has
//! been read to the end it had then (a sink's `PubAck` and a dead letter both come before the
//! source ack, so nothing arrives after). That judgement is final.
//!
//! When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, each judgement goes to the collector as gauges
//! under `service.name=loghub-verifier`: `loghub_published`, `loghub_received`,
//! `loghub_missing`, `loghub_unexpected`, `loghub_extra_copies`, `loghub_dead_lettered`,
//! `loghub_edit_mismatch`, `loghub_lua_mismatch`, `loghub_sampled_out`,
//! `loghub_repeated_on_every_subject`, `loghub_duplicates_planned`,
//! `loghub_duplicates_dropped`, `loghub_settled` (0 while the run goes on, 1 for the final
//! judgement), and per `format`, `loghub_extraction_checked`, `loghub_extraction_mismatch`
//! and `loghub_extraction_accuracy`. The collector keeps serving a series for its
//! `metric_expiration` (5m), and Prometheus keeps what it scraped.
//!
//! Exits 0 on a pass, 1 on a fail (anything missing, unexpected, dead-lettered or wrongly
//! written by `edit` or `lua`, or `dedupe` dropping under 80% of the planned duplicates), 2
//! when the run could not be judged: the producer failed or never finished, the pipeline did
//! not settle, or a stream could not be read.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use async_nats::jetstream::consumer::pull::OrderedConfig;
use async_nats::jetstream::{self, Context};
use fusion_harness::cli;
use fusion_harness::expect::Expectation;
use fusion_harness::follow::{self, LineBuffer, ProducerOutcome, StreamEnd};
use fusion_harness::verdict::{DeadLetter, Report, Written, judge, judge_so_far};
use fusion_nats::headers::{RECORD_ID, TENANT};
use futures::StreamExt;
use opentelemetry::KeyValue;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::MetricExporter;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use serde_json::{Map, Value};
use tokio::sync::mpsc as channel;

const USAGE: &str = "usage: loghub-verifier [--expectations target/loghub/expectations.jsonl] \
[--nats-url nats://127.0.0.1:4222] [--producer-timeout 600s] [--settle-timeout 120s]";

const FLAGS: [&str; 4] = [
    "--expectations",
    "--nats-url",
    "--producer-timeout",
    "--settle-timeout",
];

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

/// How often the run is judged and exported while it goes on.
const TICK: Duration = Duration::from_secs(5);

/// How long the reads may take to reach the ends the streams had once the pipeline settled.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

struct Options {
    expectations: PathBuf,
    nats_url: String,
    producer_timeout: Duration,
    settle_timeout: Duration,
}

fn options() -> Result<Options, String> {
    let flags = cli::parse(std::env::args().skip(1), &FLAGS)?;
    let timeout = |flag: &str, default: u64| {
        flags
            .get(flag)
            .map_or(Ok(Duration::from_secs(default)), cli::duration)
    };
    Ok(Options {
        expectations: flags.expectations(),
        nats_url: flags.nats_url(),
        producer_timeout: timeout("--producer-timeout", 600)?,
        settle_timeout: timeout("--settle-timeout", 120)?,
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
    let exporter = Exporter::start();
    let report = tokio::runtime::Runtime::new()
        .map_err(|err| format!("could not start the runtime: {err}"))
        .and_then(|runtime| runtime.block_on(observe(&options, exporter.as_ref())));
    let report = match report {
        Ok(report) => report,
        Err(message) => {
            eprintln!("loghub-verifier: {message}");
            if let Some(exporter) = exporter {
                exporter.finish();
            }
            return ExitCode::from(2);
        }
    };
    println!("{report}");
    if let Some(exporter) = exporter {
        exporter.send(&report, true);
        exporter.finish();
    }
    if report.passed() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// What a stream reader hands over.
enum Arrived {
    Written(u64, Written),
    Dead(u64, DeadLetter),
    Failed(String),
}

/// Which stage of its end the run is in.
enum Phase {
    /// Waiting for the producer's done marker.
    Producing,
    /// Waiting for the pipeline's consumer to settle.
    Settling { clean: u32, deadline: Instant },
    /// Reading the streams up to the ends they had once it settled.
    Reading {
        sinks: StreamEnd,
        dead: StreamEnd,
        deadline: Instant,
    },
}

async fn observe(options: &Options, exporter: Option<&Exporter>) -> Result<Report, String> {
    let client = async_nats::connect(&options.nats_url)
        .await
        .map_err(|err| format!("could not connect to {}: {err}", options.nats_url))?;
    let js = jetstream::new(client);
    let source = js
        .get_stream(SOURCE_STREAM)
        .await
        .map_err(|err| format!("stream {SOURCE_STREAM}: {err}"))?;

    let (tx, mut rx) = channel::unbounded_channel();
    let readers = [
        tokio::spawn(follow_stream(
            js.clone(),
            SINK_STREAM,
            SINK_SUBJECTS,
            tx.clone(),
        )),
        tokio::spawn(follow_stream(
            js.clone(),
            DEAD_LETTER_STREAM,
            DEAD_LETTER_SUBJECTS,
            tx,
        )),
    ];
    let result = async {
        let mut tail = Tail::new(&options.expectations);
        let marker = follow::done_marker(&options.expectations);
        let (mut written, mut dead) = (Vec::new(), Vec::new());
        let (mut seen_sinks, mut seen_dead) = (None, None);
        let start = Instant::now();
        let mut next_tick = start;
        let mut phase = Phase::Producing;
        loop {
            while let Ok(arrived) = rx.try_recv() {
                match arrived {
                    Arrived::Written(sequence, w) => {
                        seen_sinks = seen_sinks.max(Some(sequence));
                        written.push(w);
                    }
                    Arrived::Dead(sequence, d) => {
                        seen_dead = seen_dead.max(Some(sequence));
                        dead.push(d);
                    }
                    Arrived::Failed(message) => return Err(message),
                }
            }
            tail.read()?;
            let now = Instant::now();
            if now >= next_tick {
                let report = judge_so_far(&tail.expectations, &written, &dead);
                eprintln!(
                    "loghub-verifier: {:>4}s published {} received {} missing {} unexpected {}",
                    start.elapsed().as_secs(),
                    report.published,
                    report.received,
                    report.missing,
                    report.unexpected
                );
                if let Some(exporter) = exporter {
                    exporter.send(&report, false);
                }
                next_tick = now + TICK;
            }
            phase = match phase {
                Phase::Producing => match read_marker(&marker)? {
                    Some(ProducerOutcome::Published) => Phase::Settling {
                        clean: 0,
                        deadline: now + options.settle_timeout,
                    },
                    Some(ProducerOutcome::Failed) => {
                        return Err(
                            "the producer did not publish its plan; the run is not judged".into(),
                        );
                    }
                    None if now >= start + options.producer_timeout => {
                        return Err(format!(
                            "the producer did not finish within {:?}: no {}",
                            options.producer_timeout,
                            marker.display()
                        ));
                    }
                    None => Phase::Producing,
                },
                Phase::Settling { clean, deadline } => {
                    let info = source.consumer_info(SOURCE_CONSUMER).await.map_err(|err| {
                        format!("consumer {SOURCE_STREAM}/{SOURCE_CONSUMER}: {err}")
                    })?;
                    let idle = info.num_pending == 0 && info.num_ack_pending == 0;
                    let clean = if idle { clean + 1 } else { 0 };
                    if clean >= SETTLED_POLLS {
                        Phase::Reading {
                            sinks: stream_end(&js, SINK_STREAM).await?,
                            dead: stream_end(&js, DEAD_LETTER_STREAM).await?,
                            deadline: now + READ_TIMEOUT,
                        }
                    } else if now >= deadline {
                        return Err(format!(
                            "the pipeline did not settle within {:?}: {} pending, {} awaiting ack",
                            options.settle_timeout, info.num_pending, info.num_ack_pending
                        ));
                    } else {
                        Phase::Settling { clean, deadline }
                    }
                }
                Phase::Reading {
                    sinks,
                    dead: dead_end,
                    deadline,
                } => {
                    if sinks.reached(seen_sinks) && dead_end.reached(seen_dead) {
                        break;
                    }
                    if now >= deadline {
                        return Err(format!(
                            "the streams were not read to their end within {READ_TIMEOUT:?}: \
                             {SINK_STREAM} at {seen_sinks:?} of {}, {DEAD_LETTER_STREAM} at \
                             {seen_dead:?} of {}",
                            sinks.last_sequence, dead_end.last_sequence
                        ));
                    }
                    Phase::Reading {
                        sinks,
                        dead: dead_end,
                        deadline,
                    }
                }
            };
            tokio::time::sleep(POLL).await;
        }
        tail.read()?;
        if tail.buffer.has_partial() {
            return Err(format!(
                "{} ends without a newline after the producer finished",
                options.expectations.display()
            ));
        }
        Ok(judge(&tail.expectations, &written, &dead))
    }
    .await;
    for reader in readers {
        reader.abort();
    }
    result
}

/// The producer's outcome once its done marker is there.
fn read_marker(marker: &Path) -> Result<Option<ProducerOutcome>, String> {
    match std::fs::read_to_string(marker) {
        Ok(text) => follow::parse_done(&text)
            .map(Some)
            .ok_or_else(|| format!("{}: unreadable marker {text:?}", marker.display())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(format!("{}: {err}", marker.display())),
    }
}

/// The expectations file, read as it grows.
struct Tail {
    path: PathBuf,
    file: Option<File>,
    read: u64,
    lines: usize,
    buffer: LineBuffer,
    expectations: Vec<Expectation>,
}

impl Tail {
    fn new(path: &Path) -> Self {
        Self {
            path: path.to_owned(),
            file: None,
            read: 0,
            lines: 0,
            buffer: LineBuffer::default(),
            expectations: Vec::new(),
        }
    }

    /// Take in the lines written since the last call. A file not created yet has none.
    fn read(&mut self) -> Result<(), String> {
        let fail = |err: &dyn std::fmt::Display| format!("{}: {err}", self.path.display());
        if self.file.is_none() {
            match File::open(&self.path) {
                Ok(file) => self.file = Some(file),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(err) => return Err(fail(&err)),
            }
        }
        let Some(file) = self.file.as_mut() else {
            return Ok(());
        };
        let len = file.metadata().map_err(|err| fail(&err))?.len();
        if len < self.read {
            return Err(fail(&"rewritten while it was read; remove it before a run"));
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|err| fail(&err))?;
        self.read += bytes.len() as u64;
        for line in self.buffer.push(&bytes) {
            self.lines += 1;
            let expectation = serde_json::from_str(&line)
                .map_err(|err| format!("{}:{}: {err}", self.path.display(), self.lines))?;
            self.expectations.push(expectation);
        }
        Ok(())
    }
}

/// Where `stream` ends now.
async fn stream_end(js: &Context, stream: &str) -> Result<StreamEnd, String> {
    let info = js
        .get_stream(stream)
        .await
        .map_err(|err| format!("stream {stream}: {err}"))?;
    let state = &info.cached_info().state;
    Ok(StreamEnd {
        messages: state.messages,
        last_sequence: state.last_sequence,
    })
}

/// Hand every message on `subjects` in `stream`, from the start, to `tx` as it arrives, with
/// its stream sequence; a failure is handed over as the last thing.
async fn follow_stream(
    js: Context,
    stream: &'static str,
    subjects: &'static str,
    tx: channel::UnboundedSender<Arrived>,
) {
    let fail = |err: &dyn std::fmt::Display| format!("reading {subjects} from {stream}: {err}");
    let result = async {
        let consumer = js
            .get_stream(stream)
            .await
            .map_err(|err| fail(&err))?
            .create_consumer(OrderedConfig {
                filter_subject: subjects.to_owned(),
                ..OrderedConfig::default()
            })
            .await
            .map_err(|err| fail(&err))?;
        let mut messages = consumer.messages().await.map_err(|err| fail(&err))?;
        while let Some(message) = messages.next().await {
            let message = message.map_err(|err| fail(&err))?;
            let sequence = message.info().map_err(|err| fail(&err))?.stream_sequence;
            let message = message.message;
            let arrived = if stream == DEAD_LETTER_STREAM {
                Arrived::Dead(
                    sequence,
                    DeadLetter {
                        record_id: header(&message, RECORD_ID),
                    },
                )
            } else {
                Arrived::Written(sequence, written(&message))
            };
            if tx.send(arrived).is_err() {
                return Ok(());
            }
        }
        Err(fail(&"the consumer ended"))
    }
    .await;
    if let Err(message) = result {
        let _ = tx.send(Arrived::Failed(message));
    }
}

fn written(message: &async_nats::Message) -> Written {
    Written {
        record_id: header(message, RECORD_ID),
        tenant: header(message, TENANT),
        attributes: serde_json::from_slice::<Map<String, Value>>(&message.payload)
            .ok()
            .and_then(|mut record| match record.remove("attributes") {
                Some(Value::Object(attributes)) => Some(attributes),
                _ => None,
            })
            .unwrap_or_default(),
        subject: message.subject.to_string(),
    }
}

fn header(message: &async_nats::Message, name: &str) -> Option<String> {
    message
        .headers
        .as_ref()?
        .get(name)
        .map(|value| value.as_str().to_owned())
}

/// The collector export, on a thread of its own: the OTLP client blocks.
struct Exporter {
    tx: mpsc::Sender<(Report, bool)>,
    thread: std::thread::JoinHandle<()>,
}

impl Exporter {
    /// The exporter for the collector named by `OTEL_EXPORTER_OTLP_ENDPOINT`, if any.
    fn start() -> Option<Self> {
        if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").map_or(true, |v| v.trim().is_empty()) {
            return None;
        }
        let (tx, rx) = mpsc::channel::<(Report, bool)>();
        let thread = std::thread::spawn(move || {
            let provider = match MetricExporter::builder().with_http().build() {
                Ok(exporter) => SdkMeterProvider::builder()
                    .with_resource(
                        Resource::builder()
                            .with_service_name("loghub-verifier")
                            .build(),
                    )
                    .with_periodic_exporter(exporter)
                    .build(),
                Err(err) => {
                    eprintln!("loghub-verifier: the report is not exported: {err}");
                    return;
                }
            };
            let gauges = Gauges::new(&provider);
            let mut failing = false;
            let mut last = Ok(());
            for (report, settled) in rx {
                gauges.record(&report, settled);
                last = provider.force_flush().map_err(|err| err.to_string());
                if let Err(err) = &last
                    && !failing
                {
                    eprintln!("loghub-verifier: an export failed, still trying: {err}");
                }
                failing = last.is_err();
            }
            if let Err(err) = last.and(provider.shutdown().map_err(|err| err.to_string())) {
                eprintln!("loghub-verifier: the report was not exported: {err}");
            }
        });
        Some(Self { tx, thread })
    }

    /// Export `report`, final when `settled`.
    fn send(&self, report: &Report, settled: bool) {
        let _ = self.tx.send((report.clone(), settled));
    }

    /// Wait for what was sent to be exported.
    fn finish(self) {
        drop(self.tx);
        if self.thread.join().is_err() {
            eprintln!("loghub-verifier: the exporter thread panicked");
        }
    }
}

/// The report's gauges.
struct Gauges {
    counts: Vec<opentelemetry::metrics::Gauge<u64>>,
    settled: opentelemetry::metrics::Gauge<u64>,
    checked: opentelemetry::metrics::Gauge<u64>,
    mismatch: opentelemetry::metrics::Gauge<u64>,
    accuracy: opentelemetry::metrics::Gauge<f64>,
}

/// The report's counts, by gauge name.
fn counts(report: &Report) -> [(&'static str, u64); 12] {
    [
        ("loghub_published", report.published),
        ("loghub_received", report.received),
        ("loghub_missing", report.missing),
        ("loghub_unexpected", report.unexpected),
        ("loghub_extra_copies", report.extra_copies),
        ("loghub_dead_lettered", report.dead_lettered),
        ("loghub_edit_mismatch", report.edit_mismatch),
        ("loghub_lua_mismatch", report.lua_mismatch),
        ("loghub_sampled_out", report.sampled_out),
        (
            "loghub_repeated_on_every_subject",
            report.repeated_on_every_subject,
        ),
        ("loghub_duplicates_planned", report.duplicates_planned),
        ("loghub_duplicates_dropped", report.duplicates_dropped),
    ]
}

impl Gauges {
    fn new(provider: &SdkMeterProvider) -> Self {
        let meter = provider.meter("loghub-verifier");
        Self {
            counts: counts(&Report::default())
                .iter()
                .map(|(name, _)| meter.u64_gauge(*name).build())
                .collect(),
            settled: meter.u64_gauge("loghub_settled").build(),
            checked: meter.u64_gauge("loghub_extraction_checked").build(),
            mismatch: meter.u64_gauge("loghub_extraction_mismatch").build(),
            accuracy: meter.f64_gauge("loghub_extraction_accuracy").build(),
        }
    }

    fn record(&self, report: &Report, settled: bool) {
        for (gauge, (_, value)) in self.counts.iter().zip(counts(report)) {
            gauge.record(value, &[]);
        }
        self.settled.record(u64::from(settled), &[]);
        // A set's name is its log format, the label the issue names.
        for (name, set) in &report.sets {
            let labels = [KeyValue::new("format", name.clone())];
            self.checked.record(set.checked, &labels);
            self.mismatch.record(set.mismatched, &labels);
            self.accuracy.record(set.accuracy(), &labels);
        }
    }
}
