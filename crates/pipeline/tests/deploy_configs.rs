//! The configs shipped in `deploy/` are loaded, compiled and run through the in-memory
//! source and sinks, so a broken example fails here rather than at a customer's startup.
//!
//! `sink.nats` is registered against a factory that parses the real [`SinkParams`] and then
//! collects into memory: the sink's `stream` and `subject` are validated as the binary would
//! validate them, without a server.

mod common;

use std::path::PathBuf;
use std::sync::OnceLock;

use common::{WAIT, acme_host_record, for_each_worker_count, start_with};
use fusion_core::config::Config;
use fusion_core::memory::{AckOutcome, MemorySinks};
use fusion_core::record::Record;
use fusion_core::registry::Registry;
use fusion_nats::config::SinkParams;
use fusion_pipeline::default_registry;

/// A config shipped under `deploy/`.
fn deploy_config(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|err| panic!("{} is readable: {err}", name))
}

/// The default registry with `sink.nats` parsing real sink params and collecting in memory.
fn registry(sinks: &MemorySinks) -> Registry {
    let mut registry = default_registry();
    let collector = sinks.clone();
    registry.register_sink("sink.nats", move |node: &_| {
        let _: SinkParams = fusion_core::config::NodeConfig::parse_params(node)?;
        fusion_core::registry::SinkFactory::build(&collector, node)
    });
    registry
}

/// How many Linux hosts [`archived_host`] tries. At `percent: 50` about half are kept; any
/// `percent` that keeps one host in this many still finds one.
const FIXTURE_HOSTS: u64 = 256;

/// A Linux host the routing example's archive branch keeps, found by running the example
/// once over [`FIXTURE_HOSTS`] hosts, so the fan-out and nak tests below do not depend on
/// one host's hash: they hold for any archive `percent` that keeps at least one of those
/// hosts. The host test at the end proves the split itself.
fn archived_host() -> &'static str {
    static HOST: OnceLock<String> = OnceLock::new();
    HOST.get_or_init(|| {
        let sinks = MemorySinks::new();
        let h = start_with(
            &deploy_config("pipeline-routing.yaml"),
            1,
            sinks.clone(),
            registry(&sinks),
        );
        let probes: Vec<_> = (1..=FIXTURE_HOSTS)
            .map(|id| {
                h.source
                    .push(record_from_host(id, "INFO", "Linux", &format!("web-{id}")))
            })
            .collect();
        for probe in &probes {
            assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
        }
        // Routing first, so a routing regression fails here with its own message rather
        // than as "no host archived".
        assert_eq!(
            h.ids("linux_out").len() as u64,
            FIXTURE_HOSTS,
            "routing no longer sends every Linux record to linux_out"
        );
        let archived = h.ids("linux_archive");
        h.finish();
        let id = archived.first().unwrap_or_else(|| {
            panic!(
                "none of {FIXTURE_HOSTS} Linux hosts reached linux_archive: the `percent` of \
                 `half_the_hosts` in deploy/pipeline-routing.yaml keeps too few hosts for this \
                 fixture, or routing no longer reaches the archive branch"
            )
        });
        format!("web-{id}")
    })
}

fn record(id: u64, severity: &str, format: &str) -> Record {
    record_from_host(id, severity, format, archived_host())
}

/// [`acme_host_record`] with the severity and log format the routing example switches on.
fn record_from_host(id: u64, severity: &str, format: &str, host: &str) -> Record {
    let mut record = acme_host_record(id, Some(host));
    record.body = Some(serde_json::Value::String("line".to_owned()));
    record.severity_text = Some(severity.to_owned());
    record.resource.insert(
        "log.format".to_owned(),
        serde_json::Value::String(format.to_owned()),
    );
    record
}

#[test]
fn every_deploy_config_parses() {
    for name in ["pipeline.yaml", "pipeline-routing.yaml"] {
        Config::from_yaml(&deploy_config(name))
            .unwrap_or_else(|err| panic!("{name} parses: {err}"));
    }
}

#[test]
fn every_deploy_config_compiles_with_the_types_the_binary_registers() {
    for name in ["pipeline.yaml", "pipeline-routing.yaml"] {
        let sinks = MemorySinks::new();
        fusion_core::pipeline::Pipeline::from_yaml(&deploy_config(name), &registry(&sinks))
            .unwrap_or_else(|err| panic!("{name} compiles: {err}"));
    }
}

#[test]
fn the_routing_example_drops_debug_and_fans_each_format_to_its_sinks() {
    for_each_worker_count(|workers| {
        let sinks = MemorySinks::new();
        let h = start_with(
            &deploy_config("pipeline-routing.yaml"),
            workers,
            sinks.clone(),
            registry(&sinks),
        );

        let probes = [
            h.source.push(record(1, "ERROR", "Linux")),
            h.source.push(record(2, "WARN", "Apache")),
            h.source.push(record(3, "INFO", "Mac")),
            h.source.push(record(4, "DEBUG", "Linux")),
        ];

        for probe in &probes {
            assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");
        }
        assert_eq!(h.ids("linux_out"), [1], "workers={workers}");
        assert_eq!(h.ids("linux_archive"), [1], "workers={workers}");
        assert_eq!(h.ids("rest_out"), [2, 3], "workers={workers}");
        h.finish();
    });
}

#[test]
fn a_failing_branch_of_the_routing_example_naks_the_record_once() {
    let sinks = MemorySinks::new();
    sinks.fail_writes_to("linux_archive");
    let h = start_with(
        &deploy_config("pipeline-routing.yaml"),
        1,
        sinks.clone(),
        registry(&sinks),
    );

    let linux = h.source.push(record(1, "ERROR", "Linux"));
    let apache = h.source.push(record(2, "WARN", "Apache"));

    assert!(matches!(linux.wait(WAIT), Some(AckOutcome::Nak(_))));
    assert_eq!(apache.wait(WAIT), Some(AckOutcome::Ack));
    assert_eq!(h.ids("linux_out"), [1], "the other branch still wrote");
    h.finish();
}

/// The archive branch keeps half the Linux hosts, every record of a host together: the
/// main Linux sink sees every record, the archive sees all of some hosts and none of the
/// others.
#[test]
fn the_routing_example_archives_every_record_of_half_the_linux_hosts() {
    let sinks = MemorySinks::new();
    let h = start_with(
        &deploy_config("pipeline-routing.yaml"),
        4,
        sinks.clone(),
        registry(&sinks),
    );
    let hosts = 40;
    let probes: Vec<_> = (0..hosts)
        .flat_map(|host| (0..3).map(move |copy| (host, copy)))
        .map(|(host, copy)| {
            let host_name = format!("web-{host}");
            h.source.push(record_from_host(
                host * 10 + copy,
                "INFO",
                "Linux",
                &host_name,
            ))
        })
        .collect();
    for probe in &probes {
        assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    }

    assert_eq!(
        h.ids("linux_out").len() as u64,
        hosts * 3,
        "the main sink sees every record"
    );
    let archived = h.ids("linux_archive");
    let mut archived_hosts: Vec<u64> = archived.iter().map(|id| id / 10).collect();
    archived_hosts.dedup();
    assert_eq!(
        archived.len(),
        archived_hosts.len() * 3,
        "a host is archived whole or not at all"
    );
    assert!(
        !archived_hosts.is_empty() && (archived_hosts.len() as u64) < hosts,
        "{} of {hosts} hosts archived",
        archived_hosts.len()
    );
    h.finish();
}
