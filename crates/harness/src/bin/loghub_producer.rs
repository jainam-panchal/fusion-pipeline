//! `loghub-producer`: replay the vendored loghub lines into the `LOGS` stream and write what
//! the pipeline should do with each message to the expectations file.
//!
//! Each message goes to `logs.<tenant>.loghub`, one tenant per set, with the record id in `Fusion-Record-Id` (and
//! in `Nats-Msg-Id`, so a retried publish the server already stored is dropped as a
//! duplicate) and a payload of the raw line, its set in `resource.log.format` and its
//! `LineId` in `attributes["loghub.line_id"]`. An expectation is written only once the
//! message's `PubAck` is in, so the file lists exactly what the stream holds.
//!
//! Exits 1 when a message could not be published, or when sending fell behind far enough
//! that a body came back, or a duplicate trailed its original, by more than the plan allows.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_nats::HeaderMap;
use async_nats::jetstream::{self, Context};
use fusion_harness::cli;
use fusion_harness::expect::{Expectation, expectation};
use fusion_harness::loghub::{self, Line, Set};
use fusion_harness::plan::{DUP_LAG, PlanConfig, Planned, plan};
use fusion_nats::headers::{MSG_ID, RECORD_ID};
use futures::StreamExt;
use futures::stream::FuturesUnordered;

const USAGE: &str = "usage: loghub-producer [--rate 1667] [--count 100000] \
[--datasets Linux,OpenSSH,Apache,Mac] [--dup-percent 30] [--seed 1] [--dedupe-window 2s] \
[--expectations target/loghub/expectations.jsonl] [--nats-url nats://127.0.0.1:4222] \
[--testdata testdata/loghub]";

const FLAGS: [&str; 9] = [
    "--rate",
    "--count",
    "--datasets",
    "--dup-percent",
    "--seed",
    "--dedupe-window",
    "--expectations",
    "--nats-url",
    "--testdata",
];

/// Publishes awaiting their `PubAck` at once.
const IN_FLIGHT: usize = 512;

/// Tries per message, the first included.
const TRIES: u32 = 4;

struct Options {
    config: PlanConfig,
    sets: Vec<&'static Set>,
    expectations: PathBuf,
    nats_url: String,
    testdata: PathBuf,
}

fn options() -> Result<Options, String> {
    let flags = cli::parse(std::env::args().skip(1), &FLAGS)?;
    let defaults = PlanConfig::default();
    let config = PlanConfig {
        rate: flags.value("--rate", defaults.rate)?,
        count: flags.value("--count", defaults.count)?,
        dup_percent: flags.value("--dup-percent", defaults.dup_percent)?,
        seed: flags.value("--seed", defaults.seed)?,
        dedupe_window: flags
            .get("--dedupe-window")
            .map_or(Ok(defaults.dedupe_window), cli::duration)?,
    };
    let sets = flags
        .get("--datasets")
        .unwrap_or("Linux,OpenSSH,Apache,Mac")
        .split(',')
        .map(|name| loghub::set(name).ok_or_else(|| format!("unknown dataset `{name}`")))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Options {
        config,
        sets,
        expectations: flags
            .get("--expectations")
            .map_or_else(|| "target/loghub/expectations.jsonl".into(), PathBuf::from),
        nats_url: flags
            .get("--nats-url")
            .map(str::to_owned)
            .or_else(|| std::env::var("NATS_URL").ok())
            .unwrap_or_else(|| "nats://127.0.0.1:4222".to_owned()),
        testdata: flags
            .get("--testdata")
            .map_or_else(loghub::testdata, PathBuf::from),
    })
}

fn main() -> ExitCode {
    let options = match options() {
        Ok(options) => options,
        Err(message) => {
            eprintln!("loghub-producer: {message}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("loghub-producer: could not start the runtime: {err}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(&options)) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(message) => {
            eprintln!("loghub-producer: {message}");
            ExitCode::FAILURE
        }
    }
}

/// One set's lines with its name, so a planned message can be turned into a publish.
struct Loaded {
    set: &'static Set,
    lines: Vec<Line>,
}

/// What went out, for the summary and the exit code.
#[derive(Default)]
struct Tally {
    acked: u64,
    failed: u64,
    /// The longest a duplicate actually trailed its original.
    max_dup_lag: Duration,
    /// The shortest time before a body actually came back in its set's next cycle.
    min_repeat: Option<Duration>,
}

async fn run(options: &Options) -> Result<bool, String> {
    let loaded = options
        .sets
        .iter()
        .map(|set| {
            loghub::load(&options.testdata, set)
                .map(|lines| Loaded { set, lines })
                .map_err(|err| err.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let lens: Vec<usize> = loaded.iter().map(|l| l.lines.len()).collect();
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| err.to_string())?
        .as_millis();
    let base = u64::try_from(millis).map_err(|err| err.to_string())? << 22;
    let messages = plan(&options.config, &lens, base).map_err(|err| err.to_string())?;

    if let Some(dir) = options.expectations.parent() {
        std::fs::create_dir_all(dir).map_err(|err| format!("{}: {err}", dir.display()))?;
    }
    let file = File::create(&options.expectations)
        .map_err(|err| format!("{}: {err}", options.expectations.display()))?;
    let mut out = BufWriter::new(file);

    let client = async_nats::connect(&options.nats_url)
        .await
        .map_err(|err| format!("could not connect to {}: {err}", options.nats_url))?;
    let js = jetstream::new(client);

    let mut tally = Tally::default();
    let mut sent_at: HashMap<u64, Instant> = HashMap::new();
    let mut last_original: HashMap<(usize, usize), Instant> = HashMap::new();
    let mut pending = FuturesUnordered::new();
    let start = Instant::now();
    for message in &messages {
        tokio::time::sleep_until((start + message.at).into()).await;
        let now = Instant::now();
        match message.dup_of {
            Some(original) => {
                if let Some(at) = sent_at.get(&original) {
                    tally.max_dup_lag = tally.max_dup_lag.max(now - *at);
                }
            }
            None => {
                if let Some(previous) = last_original.insert((message.set, message.line), now) {
                    let gap = now - previous;
                    tally.min_repeat = Some(tally.min_repeat.map_or(gap, |m| m.min(gap)));
                }
            }
        }
        sent_at.insert(message.id, now);
        let publish = Publish::new(message, &loaded);
        // Spawned, so the publish goes out now rather than when the loop next polls it.
        pending.push(tokio::spawn(publish.send(js.clone())));
        while pending.len() >= IN_FLIGHT {
            if let Some(joined) = pending.next().await {
                settle(joined, &mut out, &mut tally)?;
            }
        }
    }
    while let Some(joined) = pending.next().await {
        settle(joined, &mut out, &mut tally)?;
    }
    out.flush()
        .map_err(|err| format!("{}: {err}", options.expectations.display()))?;
    let elapsed = start.elapsed();

    let needed_repeat = options.config.dedupe_window + DUP_LAG;
    let dup_ok = tally.max_dup_lag < options.config.dedupe_window;
    let repeat_ok = tally.min_repeat.is_none_or(|gap| gap >= needed_repeat);
    println!(
        "published {} of {} messages in {:.1}s ({:.0}/s), {} failed",
        tally.acked,
        messages.len(),
        elapsed.as_secs_f64(),
        tally.acked as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
        tally.failed
    );
    println!(
        "longest duplicate lag {:?} (must stay under {:?}), shortest body repeat {:?} (must be at least {:?})",
        tally.max_dup_lag, options.config.dedupe_window, tally.min_repeat, needed_repeat
    );
    println!("expectations: {}", options.expectations.display());
    if !dup_ok || !repeat_ok {
        eprintln!(
            "loghub-producer: sending fell behind the plan; the expectations no longer hold. \
             Lower --rate."
        );
    }
    Ok(tally.failed == 0 && dup_ok && repeat_ok)
}

/// One message ready to publish, with the expectation to write once it is acked.
struct Publish {
    subject: String,
    headers: HeaderMap,
    payload: bytes::Bytes,
    expectation: Expectation,
}

impl Publish {
    fn new(message: &Planned, loaded: &[Loaded]) -> Self {
        let Loaded { set, lines } = &loaded[message.set];
        let line = &lines[message.line];
        let mut headers = HeaderMap::new();
        headers.insert(RECORD_ID, message.id.to_string().as_str());
        headers.insert(MSG_ID, message.id.to_string().as_str());
        let payload = serde_json::json!({
            "body": line.body,
            "resource": {"log.format": set.name},
            "attributes": {"loghub.line_id": line.line_id},
        })
        .to_string();
        let expectation = expectation(
            message.id,
            set.name,
            line.line_id,
            message.cycle,
            message.dup_of,
            line.attributes.clone(),
        )
        .unwrap_or_else(|| unreachable!("the producer loads vendored sets only"));
        Self {
            subject: format!("logs.{}.loghub", set.tenant),
            headers,
            payload: payload.into(),
            expectation,
        }
    }

    /// Publish until a `PubAck` comes back or the tries run out.
    async fn send(self, js: Context) -> Sent {
        let mut last = String::new();
        for attempt in 0..TRIES {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(250 << attempt)).await;
            }
            let ack = match js
                .publish_with_headers(
                    self.subject.clone(),
                    self.headers.clone(),
                    self.payload.clone(),
                )
                .await
            {
                Ok(future) => future.await.map_err(|err| err.to_string()),
                Err(err) => Err(err.to_string()),
            };
            match ack {
                Ok(_) => return Ok(self.expectation),
                Err(err) => last = err,
            }
        }
        Err((self.expectation, last))
    }
}

type Sent = Result<Expectation, (Expectation, String)>;

fn settle(
    joined: Result<Sent, tokio::task::JoinError>,
    out: &mut impl Write,
    tally: &mut Tally,
) -> Result<(), String> {
    match joined.map_err(|err| format!("a publish task failed: {err}"))? {
        Ok(expectation) => {
            tally.acked += 1;
            let line = serde_json::to_string(&expectation).map_err(|err| err.to_string())?;
            writeln!(out, "{line}").map_err(|err| format!("expectations: {err}"))
        }
        Err((expectation, err)) => {
            tally.failed += 1;
            eprintln!(
                "loghub-producer: id {} ({} LineId {}) not published: {err}",
                expectation.id, expectation.set, expectation.line_id
            );
            Ok(())
        }
    }
}
