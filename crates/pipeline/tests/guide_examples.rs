//! Every example under `docs/guide/examples/` runs here, so a page never shows a config,
//! input or output the pipeline does not produce.
//!
//! An example is a folder with three files:
//!
//! - `pipeline.yaml`: the config. Its `source` block, when there is one, only gives the
//!   tenant prefix; `sink.nats` nodes parse their real params and collect in memory.
//! - `input.yaml`: the messages, as NATS delivers them: `subject`, `headers`, `payload`, and
//!   optionally `published` (JetStream publish time, [`PUBLISHED`] when absent) and
//!   `delivered` (1 when absent). Headers become the arrival through the NATS source's own
//!   parsing.
//! - `expected.yaml`: `acks`, one `ack` or `nak` per message in order, and `sinks`, what each
//!   sink node wrote, in order. A written record's `headers`, when given, are compared with
//!   what the NATS sink would write. A sink node the file leaves out must write nothing.
//!
//! Messages are pushed through one worker, so each sink sees them in input order.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use common::{WAIT, start_with};
use fusion_core::config::{Config, NodeConfig};
use fusion_core::memory::{AckOutcome, MemorySinks};
use fusion_core::record::Record;
use fusion_core::registry::{Registry, SinkFactory};
use fusion_nats::headers::{Received, arrival, for_meta};
use fusion_nats::{SinkParams, SourceParams};
use fusion_pipeline::default_registry;
use serde::Deserialize;
use serde_json::Value;

/// The JetStream publish time a message gets when its example names none.
const PUBLISHED: u64 = 1_758_000_000_000_000_000;

/// The tenant prefix when the example's config has no `source` block.
const TENANT_PREFIX: &str = "logs";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Message {
    subject: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    payload: Value,
    #[serde(default)]
    published: Option<u64>,
    #[serde(default = "first_delivery")]
    delivered: u64,
}

const fn first_delivery() -> u64 {
    1
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    acks: Vec<Settled>,
    #[serde(default)]
    sinks: BTreeMap<String, Vec<Written>>,
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
    payload: Value,
}

fn examples_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/guide/examples")
}

/// Every folder under `root` that holds a `pipeline.yaml`, sorted.
fn example_folders(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|err| panic!("{} is readable: {err}", dir.display()));
        for entry in entries {
            let path = entry.expect("directory entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.file_name().is_some_and(|name| name == "pipeline.yaml") {
                found.push(dir.clone());
            }
        }
    }
    found.sort();
    found
}

fn read(dir: &Path, file: &str) -> String {
    std::fs::read_to_string(dir.join(file))
        .unwrap_or_else(|err| panic!("{}/{file} is readable: {err}", dir.display()))
}

/// The default registry with `sink.nats` parsing its real params and collecting in memory.
fn registry(sinks: &MemorySinks) -> Registry {
    let mut registry = default_registry();
    let collector = sinks.clone();
    registry.register_sink("sink.nats", move |node: &NodeConfig| {
        let _: SinkParams = NodeConfig::parse_params(node)?;
        collector.build(node)
    });
    registry
}

/// The headers `for_meta` writes, one value each.
fn written_headers(headers: &async_nats::HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .map(|(name, values)| {
            let values: Vec<&str> = values.iter().map(|value| value.as_str()).collect();
            assert_eq!(values.len(), 1, "header {name} written once");
            (name.to_string(), values[0].to_owned())
        })
        .collect()
}

/// Run the example in `dir` and return what went wrong, if anything.
fn run(dir: &Path) -> Result<(), String> {
    let yaml = read(dir, "pipeline.yaml");
    let config = Config::from_yaml(&yaml).map_err(|err| format!("config does not load: {err}"))?;
    let tenant_prefix = match &config.source {
        Some(source) => {
            source
                .parse_params::<SourceParams>()
                .map_err(|err| format!("source block: {err}"))?
                .tenant_prefix
        }
        None => TENANT_PREFIX.to_owned(),
    };
    let input: Vec<Message> = serde_yaml_ng::from_str(&read(dir, "input.yaml"))
        .map_err(|err| format!("input.yaml: {err}"))?;
    let expected: Expected = serde_yaml_ng::from_str(&read(dir, "expected.yaml"))
        .map_err(|err| format!("expected.yaml: {err}"))?;
    if expected.acks.len() != input.len() {
        return Err(format!(
            "expected.yaml lists {} acks for {} input messages",
            expected.acks.len(),
            input.len()
        ));
    }
    let sink_ids: Vec<&str> = config
        .nodes
        .iter()
        .filter(|node| node.is_sink())
        .map(|node| node.id.as_str())
        .collect();
    if let Some(unknown) = expected
        .sinks
        .keys()
        .find(|id| !sink_ids.contains(&id.as_str()))
    {
        return Err(format!(
            "expected.yaml names `{unknown}`, which is not a sink node"
        ));
    }

    let sinks = MemorySinks::new();
    let h = start_with(&yaml, 1, sinks.clone(), registry(&sinks));
    let probes: Vec<_> = input
        .iter()
        .map(|message| {
            let payload = serde_json::to_string(&message.payload).expect("payload serializes");
            let record = Record::from_json(&payload).unwrap_or_else(|err| {
                panic!("{}: a payload is not a record: {err}", dir.display())
            });
            let mut headers = async_nats::HeaderMap::new();
            for (name, value) in &message.headers {
                headers.insert(name.as_str(), value.as_str());
            }
            let (arrival, _ignored) = arrival(
                &tenant_prefix,
                Received {
                    subject: &message.subject,
                    headers: Some(&headers),
                    published: Some(message.published.unwrap_or(PUBLISHED)),
                    delivered: message.delivered,
                    bytes: payload.len() as u64,
                },
            );
            h.source.push_arrival(record, arrival)
        })
        .collect();
    let settled: Vec<Option<Settled>> = probes
        .iter()
        .map(|probe| {
            probe.wait(WAIT).map(|outcome| match outcome {
                AckOutcome::Ack => Settled::Ack,
                AckOutcome::Nak(_) => Settled::Nak,
            })
        })
        .collect();
    let written: BTreeMap<&str, _> = sink_ids
        .iter()
        .map(|id| (*id, sinks.outgoing(id)))
        .collect();
    h.finish();

    let mut problems = Vec::new();
    for (index, (want, got)) in expected.acks.iter().zip(&settled).enumerate() {
        if Some(*want) != *got {
            problems.push(format!(
                "message {}: expected {want:?}, got {got:?}",
                index + 1
            ));
        }
    }
    for (id, outgoing) in &written {
        let want = expected.sinks.get(*id).map_or(&[][..], Vec::as_slice);
        if want.len() != outgoing.len() {
            problems.push(format!(
                "sink `{id}`: expected {} records, got {}",
                want.len(),
                outgoing.len()
            ));
            continue;
        }
        for (index, (want, got)) in want.iter().zip(outgoing).enumerate() {
            let payload: Value = serde_json::from_str(&got.record.to_json().expect("serializes"))
                .expect("record JSON parses");
            if payload != want.payload {
                problems.push(format!(
                    "sink `{id}` record {}: payload\n  expected {}\n  got      {payload}",
                    index + 1,
                    want.payload
                ));
            }
            if let Some(want_headers) = &want.headers {
                let got_headers = written_headers(&for_meta(&got.meta));
                if *want_headers != got_headers {
                    problems.push(format!(
                        "sink `{id}` record {}: headers\n  expected {want_headers:?}\n  got      {got_headers:?}",
                        index + 1
                    ));
                }
            }
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems.join("\n"))
    }
}

#[test]
fn every_guide_example_produces_its_expected_output() {
    let root = examples_dir();
    let folders = example_folders(&root);
    assert!(!folders.is_empty(), "no examples under {}", root.display());
    let failures: Vec<String> = folders
        .iter()
        .filter_map(|dir| {
            run(dir).err().map(|problem| {
                let name = dir.strip_prefix(&root).unwrap_or(dir).display();
                format!("{name}:\n{problem}")
            })
        })
        .collect();
    assert!(failures.is_empty(), "\n{}", failures.join("\n\n"));
}

#[test]
fn every_example_folder_has_its_three_files_and_nothing_else() {
    let root = examples_dir();
    for dir in example_folders(&root) {
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
        assert_eq!(
            names,
            ["expected.yaml", "input.yaml", "pipeline.yaml"],
            "{}",
            dir.display()
        );
    }
}
