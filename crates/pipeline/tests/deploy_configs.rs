//! The configs shipped in `deploy/` are loaded, compiled and run through the in-memory
//! source and sinks, so a broken example fails here rather than at a customer's startup.
//!
//! `sink.nats` is registered against a factory that parses the real [`SinkParams`] and then
//! collects into memory: the sink's `stream` and `subject` are validated as the binary would
//! validate them, without a server.

mod common;

use std::sync::OnceLock;

use common::{WAIT, deploy_config, for_each_worker_count, host_record, start_with};
use fusion_core::config::Config;
use fusion_core::memory::{AckOutcome, MemorySinks};
use fusion_core::metrics::CounterMetric;
use fusion_core::record::Record;
use fusion_core::registry::Registry;
use fusion_nats::config::{SinkParams, SourceParams};
use fusion_nats::subject::covers_every_tenant;
use fusion_pipeline::default_registry;

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
            .map(|id| h.push(record_from_host(id, "INFO", "Linux", &format!("web-{id}"))))
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

/// [`host_record`] with the severity and log format the routing example switches on.
fn record_from_host(id: u64, severity: &str, format: &str, host: &str) -> Record {
    let mut record = host_record(id, Some(host));
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

/// The compose `nats-init` command that adds stream `name`, its continuation lines joined.
fn compose_stream_add(name: &str) -> String {
    let compose = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/compose.yaml"),
    )
    .expect("deploy/compose.yaml is readable");
    let joined = compose.replace("\\\n", " ");
    joined
        .lines()
        .find(|line| line.contains(&format!("nats stream add {name} ")))
        .unwrap_or_else(|| panic!("nats-init adds stream {name}"))
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Issue #10: the source refuses to start without a stream capturing every dead-letter
/// subject, so the stack creates one for the prefix each shipped pipeline uses, capped per
/// tenant so one tenant's dead letters cannot push another's out.
#[test]
fn compose_creates_the_dead_letter_stream_every_deploy_pipeline_needs() {
    let dlq = compose_stream_add("DLQ");
    let subjects = dlq
        .split("--subjects ")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .map(|subjects| subjects.trim_matches('\''))
        .unwrap_or_else(|| panic!("the DLQ stream names its subjects: {dlq}"));
    for flag in ["--discard old", "--max-msgs-per-subject ", "--dupe-window "] {
        assert!(dlq.contains(flag), "`{flag}` missing from: {dlq}");
    }
    for name in ["pipeline.yaml", "pipeline-routing.yaml"] {
        let config = Config::from_yaml(&deploy_config(name)).expect("config parses");
        let source = config.source.expect("a deploy pipeline names its source");
        let params: SourceParams = source.parse_params().expect("source params parse");
        assert!(
            covers_every_tenant(subjects, &params.dlq_prefix),
            "{name}: `{subjects}` does not cover `{}.<tenant>`",
            params.dlq_prefix
        );
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

/// A Linux syslog line the compose extract pattern parses: `Component` is
/// `sshd(pam_unix)`, `Content` the text after the colon.
const SYSLOG_LINE: &str = "Jun 14 15:16:01 combo sshd(pam_unix)[19939]: authentication failure";

#[test]
fn the_compose_pipeline_delivers_every_record_and_keeps_only_parsed_lines_on_the_parsed_branch() {
    for_each_worker_count(|workers| {
        let sinks = MemorySinks::new();
        let h = start_with(
            &deploy_config("pipeline.yaml"),
            workers,
            sinks.clone(),
            registry(&sinks),
        );
        let parsed = h.push(common::body_record(1, SYSLOG_LINE));
        let plain = h.push(common::body_record(2, "disk full"));
        assert_eq!(
            parsed.wait(WAIT),
            Some(AckOutcome::Ack),
            "workers={workers}"
        );
        assert_eq!(plain.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");

        assert_eq!(h.ids("out"), [1, 2], "the main sink sees every record");
        assert_eq!(
            h.ids("parsed_out"),
            [1],
            "the parsed branch keeps only the line the pattern parsed"
        );
        let on_parsed = &h.sinks.records("parsed_out")[0];
        assert_eq!(
            on_parsed.attributes.get("message"),
            Some(&serde_json::json!("authentication failure"))
        );
        assert_eq!(on_parsed.attributes.get("Content"), None);
        let on_main: Vec<_> = h.sinks.records("out");
        let main_parsed = on_main
            .iter()
            .find(|r| r.id.map(|id| id.0) == Some(1))
            .expect("record 1 on the main sink");
        assert_eq!(
            main_parsed.attributes.get("Content"),
            Some(&serde_json::json!("authentication failure")),
            "the main branch is untouched by the parsed branch's rename"
        );
        assert_eq!(
            main_parsed.attributes.get("service"),
            Some(&serde_json::json!("sshd(pam_unix)"))
        );
        assert_eq!(
            h.counter(
                CounterMetric::RecordsDropped,
                &[
                    ("tenant", "acme"),
                    ("stage", "only_parsed"),
                    ("reason", "edit_unapplied")
                ]
            ),
            1,
            "the plain body is the drop reason's producer"
        );
        assert_eq!(
            h.counter(
                CounterMetric::EditUnapplied,
                &[
                    ("tenant", "acme"),
                    ("stage", "tag_service"),
                    ("op", "copy"),
                    ("field", "attributes.Component"),
                    ("cause", "absent")
                ]
            ),
            1,
            "the plain body is the metric's producer, under skip"
        );
        h.finish();
    });
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
            h.push(record(1, "ERROR", "Linux")),
            h.push(record(2, "WARN", "Apache")),
            h.push(record(3, "INFO", "Mac")),
            h.push(record(4, "DEBUG", "Linux")),
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

    let linux = h.push(record(1, "ERROR", "Linux"));
    let apache = h.push(record(2, "WARN", "Apache"));

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
            h.push(record_from_host(
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
