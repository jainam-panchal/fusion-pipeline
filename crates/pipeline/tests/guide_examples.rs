//! Every example under `docs/guide/examples/` runs here, so a page never shows a config,
//! input or output the pipeline does not produce.
//!
//! An example is a folder with three files:
//!
//! - `pipeline.yaml`: the config. Its `source` block, when there is one, only gives the
//!   tenant prefix; `sink.nats` nodes parse their real params and collect in memory.
//! - `input.yaml`: the messages, each a first delivery as NATS hands it over: `subject`,
//!   `headers` (a value, or a list for a header given more than once), `payload`, and
//!   optionally `published` (JetStream publish time, [`PUBLISHED`] when absent). The arrival
//!   comes from the NATS source's own header parsing, and a payload is decoded only when
//!   the arrival is a log, as the source does. A payload that is not a record is nakked
//!   without reaching the engine, as the source naks it.
//! - `expected.yaml`: `acks`, one `ack` or `nak` per message in order, and `sinks`, what each
//!   sink node wrote, in order. A written record's `headers`, when given, are compared with
//!   what the NATS sink writes. A sink node the file leaves out must write nothing.
//!
//! An example of a config the pipeline refuses has no `input.yaml`, and its `expected.yaml`
//! holds only `rejected`: the error text the pipeline gives at load.
//!
//! Messages are pushed through one worker, so each sink sees them in input order.

mod common;

use std::collections::BTreeMap;
use std::path::Path;

use common::{WAIT, files_under, guide_dir, nats_sink_registry, start_with};
use fusion_core::config::Config;
use fusion_core::memory::{AckOutcome, MemorySinks, OutgoingRecord};
use fusion_core::meta::Arrival;
use fusion_core::pipeline::Pipeline;
use fusion_core::record::Record;
use fusion_nats::SourceParams;
use fusion_nats::codec::{Codec, Encoding};
use fusion_nats::config::DEFAULT_TENANT_PREFIX;
use fusion_nats::headers::{Received, arrival, for_meta};
use fusion_pipeline::StartError;
use serde::Deserialize;
use serde_json::Value;

/// The JetStream publish time a message gets when its example names none.
const PUBLISHED: u64 = 1_758_000_000_000_000_000;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Message {
    subject: String,
    #[serde(default)]
    headers: BTreeMap<String, HeaderValues>,
    /// The payload as JSON, for a `codec: json` source. Exactly one of `payload` or `raw`.
    #[serde(default)]
    payload: Option<Value>,
    /// The payload as the bytes on the wire, for a `codec: text` source, or for showing a
    /// `codec: json` source a payload that is not JSON.
    #[serde(default)]
    raw: Option<String>,
    #[serde(default)]
    published: Option<u64>,
}

impl Message {
    /// The bytes this message carries.
    fn bytes(&self) -> Result<String, String> {
        match one_of(&self.payload, &self.raw, "a message")? {
            AsWritten::Json(payload) => {
                Ok(serde_json::to_string(payload).expect("payload serializes"))
            }
            AsWritten::Raw(raw) => Ok(raw.clone()),
        }
    }
}

/// A payload as an example writes it: `payload:` for JSON, `raw:` for the bytes on the wire.
enum AsWritten<'a> {
    Json(&'a Value),
    Raw(&'a String),
}

/// Exactly one of `payload` and `raw`, the rule an example's input and its expected output
/// both follow.
fn one_of<'a>(
    payload: &'a Option<Value>,
    raw: &'a Option<String>,
    what: &str,
) -> Result<AsWritten<'a>, String> {
    match (payload, raw) {
        (Some(payload), None) => Ok(AsWritten::Json(payload)),
        (None, Some(raw)) => Ok(AsWritten::Raw(raw)),
        _ => Err(format!("{what} needs exactly one of `payload` and `raw`")),
    }
}

/// A header's value, or its values when the message carries it more than once.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum HeaderValues {
    One(String),
    Many(Vec<String>),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    #[serde(default)]
    acks: Vec<Settled>,
    #[serde(default)]
    sinks: BTreeMap<String, Vec<Written>>,
    rejected: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Settled {
    Ack,
    Nak,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Written {
    #[serde(default)]
    headers: Option<BTreeMap<String, String>>,
    /// What the sink wrote, as JSON. Exactly one of `payload` or `raw`.
    #[serde(default)]
    payload: Option<Value>,
    /// What the sink wrote, as the bytes on the wire, for an `encoding: text` sink.
    #[serde(default)]
    raw: Option<String>,
}

/// An example folder, read and checked against itself.
enum Loaded {
    /// A config that loads, with messages to run through it.
    Runs(Example),
    /// A config the pipeline refuses at load, with the error it should give.
    Rejected { yaml: String, error: String },
}

/// A config that loads, with the messages to run through it and what should come out.
struct Example {
    yaml: String,
    tenant_prefix: String,
    /// How the source reads a payload, so the runner decodes as the config says.
    codec: Codec,
    /// Every sink node with the encoding it declared, so the runner compares what that sink
    /// would actually write.
    sinks: BTreeMap<String, Encoding>,
    input: Vec<Message>,
    expected: Expected,
}

/// What the pipeline did with an example's messages.
struct Outcome {
    settled: Vec<Option<Settled>>,
    written: BTreeMap<String, Vec<OutgoingRecord>>,
}

fn examples_dir() -> std::path::PathBuf {
    guide_dir().join("examples")
}

/// Every folder under the examples folder that holds a `pipeline.yaml`.
fn example_folders() -> Vec<std::path::PathBuf> {
    files_under(&examples_dir(), |file| file.ends_with("pipeline.yaml"))
        .into_iter()
        .filter_map(|file| file.parent().map(Path::to_path_buf))
        .collect()
}

fn read(dir: &Path, file: &str) -> Result<String, String> {
    std::fs::read_to_string(dir.join(file)).map_err(|err| format!("{file}: {err}"))
}

fn load(dir: &Path) -> Result<Loaded, String> {
    let yaml = read(dir, "pipeline.yaml")?;
    let mut expected: Expected = serde_yaml_ng::from_str(&read(dir, "expected.yaml")?)
        .map_err(|err| format!("expected.yaml: {err}"))?;
    if let Some(error) = expected.rejected.take() {
        if !expected.acks.is_empty() || !expected.sinks.is_empty() {
            return Err("expected.yaml with `rejected` lists no acks or sinks".to_owned());
        }
        if dir.join("input.yaml").exists() {
            return Err("a rejected config has no input.yaml".to_owned());
        }
        return Ok(Loaded::Rejected { yaml, error });
    }
    let config = Config::from_yaml(&yaml).map_err(|err| format!("config does not load: {err}"))?;
    let (tenant_prefix, codec) = match &config.source {
        Some(source) => {
            let params = source
                .parse_params::<SourceParams>()
                .map_err(|err| format!("source block: {err}"))?;
            (params.tenant_prefix, params.codec)
        }
        None => (DEFAULT_TENANT_PREFIX.to_owned(), Codec::default()),
    };
    let input: Vec<Message> = serde_yaml_ng::from_str(&read(dir, "input.yaml")?)
        .map_err(|err| format!("input.yaml: {err}"))?;
    if expected.acks.len() != input.len() {
        return Err(format!(
            "expected.yaml lists {} acks for {} input messages",
            expected.acks.len(),
            input.len()
        ));
    }
    // A sink node's params must parse, or a mistyped `encoding:` would silently compare as
    // `json`. Only `sink.nats` takes `SinkParams`; a `sink.memory` example has none.
    let sinks: BTreeMap<String, Encoding> = config
        .nodes
        .iter()
        .filter(|node| node.is_sink())
        .map(|node| {
            // Read `encoding` off the node itself rather than through one sink type's
            // params, so a sink kind added later is not silently compared as `json` — the
            // bug this hunk exists to prevent.
            let encoding = match node.params.get("encoding") {
                None => Encoding::default(),
                Some(value) => serde_yaml_ng::from_value(value.clone())
                    .map_err(|err| format!("sink `{}`: `encoding`: {err}", node.id))?,
            };
            Ok((node.id.clone(), encoding))
        })
        .collect::<Result<_, String>>()?;
    if let Some(unknown) = expected.sinks.keys().find(|id| !sinks.contains_key(*id)) {
        return Err(format!(
            "expected.yaml names `{unknown}`, which is not a sink node"
        ));
    }
    for message in &input {
        message.bytes()?;
    }
    Ok(Loaded::Runs(Example {
        yaml,
        tenant_prefix,
        codec,
        sinks,
        input,
        expected,
    }))
}

/// The error `pipelined` gives for `yaml`, checked in the order `fusion_pipeline::run`
/// checks it: the YAML, then the `source` block, then the graph and every node.
fn load_error(yaml: &str) -> Option<StartError> {
    let config = match Config::from_yaml(yaml) {
        Ok(config) => config,
        Err(err) => return Some(err.into()),
    };
    if config.source.is_none() {
        return Some(StartError::NoSource);
    }
    let sinks = MemorySinks::new();
    Pipeline::compile(&config, &nats_sink_registry(&sinks))
        .err()
        .map(StartError::from)
}

/// What is wrong when `yaml` does not fail to load with exactly `error`.
fn check_rejected(yaml: &str, error: &str) -> Vec<String> {
    match load_error(yaml) {
        None => vec![format!("expected the config to be rejected with: {error}")],
        Some(got) if got.to_string() == error => Vec::new(),
        Some(got) => vec![format!("rejected\n  expected {error}\n  got      {got}")],
    }
}

/// The arrival NATS would give `message`, and its payload as bytes on the wire.
fn arrival_of(message: &Message, tenant_prefix: &str) -> (Arrival, String) {
    let payload = message.bytes().expect("checked when the example loaded");
    let mut headers = async_nats::HeaderMap::new();
    for (name, values) in &message.headers {
        match values {
            HeaderValues::One(value) => headers.append(name.as_str(), value.as_str()),
            HeaderValues::Many(values) => {
                for value in values {
                    headers.append(name.as_str(), value.as_str());
                }
            }
        }
    }
    let (arrival, _ignored) = arrival(
        tenant_prefix,
        Received {
            subject: &message.subject,
            headers: Some(&headers),
            published: Some(message.published.unwrap_or(PUBLISHED)),
            delivered: 1,
            bytes: payload.len() as u64,
        },
    );
    (arrival, payload)
}

fn drive(example: &Example) -> Outcome {
    let sinks = MemorySinks::new();
    let h = start_with(&example.yaml, 1, sinks.clone(), nats_sink_registry(&sinks));
    let probes: Vec<_> = example
        .input
        .iter()
        .map(|message| {
            let (arrival, payload) = arrival_of(message, &example.tenant_prefix);
            // The source's own codec, so an example says what the config says.
            let decoded = if arrival.is_log() {
                example.codec.decode(payload.as_bytes())
            } else {
                Ok(Record::default())
            };
            decoded
                .ok()
                .map(|record| h.source.push_arrival(record, arrival))
        })
        .collect();
    let settled = probes
        .iter()
        .map(|probe| match probe {
            // The source naks a payload that is not a record before the engine sees it.
            None => Some(Settled::Nak),
            Some(probe) => probe.wait(WAIT).map(|outcome| match outcome {
                AckOutcome::Ack => Settled::Ack,
                AckOutcome::Nak(_) => Settled::Nak,
            }),
        })
        .collect();
    let written = example
        .sinks
        .keys()
        .map(|id| (id.clone(), sinks.outgoing(id)))
        .collect();
    h.finish();
    Outcome { settled, written }
}

/// The headers the NATS sink writes beside `outgoing`.
fn written_headers(outgoing: &OutgoingRecord) -> BTreeMap<String, String> {
    for_meta(&outgoing.meta)
        .iter()
        .map(|(name, values)| {
            let values: Vec<&str> = values.iter().map(|value| value.as_str()).collect();
            (name.to_string(), values.join(", "))
        })
        .collect()
}

/// Every difference between what `example` expects and `outcome`.
fn compare(example: &Example, outcome: &Outcome) -> Vec<String> {
    let mut problems = Vec::new();
    for (index, (want, got)) in example
        .expected
        .acks
        .iter()
        .zip(&outcome.settled)
        .enumerate()
    {
        if Some(*want) != *got {
            problems.push(format!(
                "message {}: expected {want:?}, got {got:?}",
                index + 1
            ));
        }
    }
    for (id, outgoing) in &outcome.written {
        let want = example
            .expected
            .sinks
            .get(id)
            .map_or(&[][..], Vec::as_slice);
        if want.len() != outgoing.len() {
            problems.push(format!(
                "sink `{id}`: expected {} records, got {}",
                want.len(),
                outgoing.len()
            ));
            continue;
        }
        for (index, (want, got)) in want.iter().zip(outgoing).enumerate() {
            match one_of(&want.payload, &want.raw, "an expected record") {
                // `payload:` is what an `encoding: json` sink writes.
                Ok(AsWritten::Json(expected)) => {
                    let payload: Value =
                        serde_json::from_str(&got.record.to_json().expect("serializes"))
                            .expect("record JSON parses");
                    if payload != *expected {
                        problems.push(format!(
                            "sink `{id}` record {}: payload\n  expected {expected}\n  got      {payload}",
                            index + 1,
                        ));
                    }
                }
                // `raw:` is the bytes this sink writes, under the encoding it declared.
                Ok(AsWritten::Raw(expected)) => {
                    let encoding = example.sinks.get(id).copied().unwrap_or_default();
                    let bytes = encoding.encode(&got.record).expect("record encodes");
                    let written = String::from_utf8_lossy(&bytes);
                    if written != *expected {
                        problems.push(format!(
                            "sink `{id}` record {}: raw\n  expected {expected}\n  got      {written}",
                            index + 1,
                        ));
                    }
                }
                Err(rule) => problems.push(format!("sink `{id}` record {}: {rule}", index + 1)),
            }
            if let Some(want_headers) = &want.headers {
                let got_headers = written_headers(got);
                if *want_headers != got_headers {
                    problems.push(format!(
                        "sink `{id}` record {}: headers\n  expected {want_headers:?}\n  got      {got_headers:?}",
                        index + 1
                    ));
                }
            }
        }
    }
    problems
}

#[test]
fn every_guide_example_produces_its_expected_output() {
    let root = examples_dir();
    let folders = example_folders();
    assert!(!folders.is_empty(), "no examples under {}", root.display());
    let failures: Vec<String> = folders
        .iter()
        .filter_map(|dir| {
            let problems = match load(dir) {
                Ok(Loaded::Runs(example)) => compare(&example, &drive(&example)),
                Ok(Loaded::Rejected { yaml, error }) => check_rejected(&yaml, &error),
                Err(problem) => vec![problem],
            };
            (!problems.is_empty()).then(|| {
                let name = dir.strip_prefix(&root).unwrap_or(dir).display();
                format!("{name}:\n{}", problems.join("\n"))
            })
        })
        .collect();
    assert!(failures.is_empty(), "\n{}", failures.join("\n\n"));
}

#[test]
fn every_example_folder_has_its_files_and_nothing_else() {
    for dir in example_folders() {
        let mut names: Vec<String> = std::fs::read_dir(&dir)
            .expect("example folder is readable")
            .map(|entry| {
                entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        names.sort();
        assert!(
            names == ["expected.yaml", "input.yaml", "pipeline.yaml"]
                || names == ["expected.yaml", "pipeline.yaml"],
            "{}: {names:?}",
            dir.display()
        );
    }
}
