//! The configs shipped in `deploy/` are loaded, compiled and run through the in-memory
//! source and sinks, so a broken example fails here rather than at a customer's startup.
//!
//! `sink.nats` is registered against a factory that parses the real [`SinkParams`] and then
//! collects into memory: the sink's `stream` and `subject` are validated as the binary would
//! validate them, without a server.

mod common;

use std::sync::OnceLock;

use common::{WAIT, arrival_as, deploy_config, for_each_worker_count, host_record, start_with};
use fusion_core::config::Config;
use fusion_core::memory::{AckOutcome, MemorySinks};
use fusion_core::meta::{Arrival, IngestionTime};
use fusion_core::metrics::CounterMetric;
use fusion_core::record::{Record, RecordId};
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
    for name in [
        "pipeline.yaml",
        "pipeline-routing.yaml",
        "pipeline-poc.yaml",
    ] {
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
    for name in [
        "pipeline.yaml",
        "pipeline-routing.yaml",
        "pipeline-poc.yaml",
    ] {
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
    for name in [
        "pipeline.yaml",
        "pipeline-routing.yaml",
        "pipeline-poc.yaml",
    ] {
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

/// Every `STRIDE`th distinct line of each loghub set goes through the POC config.
const LOGHUB_STRIDE: usize = 25;

/// How many cycles of the sampled lines go through: two, so `sample` keeps a line in one
/// cycle and leaves it out in another.
const LOGHUB_CYCLES: u64 = 2;

/// Ingestion time between one cycle and the next: past the POC config's 2s dedupe window, as
/// the producer's repeat bound keeps it, so a line's next cycle is not dropped as a repeat.
const CYCLE_GAP_NANOS: u64 = 10_000_000_000;

/// Every `stride`th distinct line of each loghub set, with its set.
fn loghub_lines(
    stride: usize,
) -> Vec<(
    &'static fusion_harness::loghub::Set,
    fusion_harness::loghub::Line,
)> {
    use fusion_harness::loghub::{self, SETS};
    let mut lines = Vec::new();
    for set in &SETS {
        let loaded = loghub::load(&loghub::testdata(), set).expect("set loads");
        lines.extend(loaded.into_iter().step_by(stride).map(|line| (set, line)));
    }
    lines
}

/// Send `lines` through `h` for `cycles` cycles as the producer sends them, and return the
/// expectations the producer would write. Within a cycle every original goes first, then each
/// duplicate after its original, stored later: only then is which copy `dedupe` keeps fixed
/// with four workers, since a duplicate handled before its original would hold the key, and
/// the original, older than the holder, would pass too. Each cycle is stored
/// [`CYCLE_GAP_NANOS`] after the one before.
fn send_loghub_cycles(
    h: &common::Harness,
    lines: &[(
        &'static fusion_harness::loghub::Set,
        fusion_harness::loghub::Line,
    )],
    cycles: u64,
) -> Vec<fusion_harness::expect::Expectation> {
    use fusion_harness::expect::expectation;
    use fusion_harness::loghub;

    let per_cycle = 2 * lines.len() as u64;
    let mut expectations = Vec::new();
    for cycle in 0..cycles {
        for dup in [false, true] {
            let mut probes = Vec::new();
            for (index, (set, line)) in lines.iter().enumerate() {
                let original = cycle * per_cycle + index as u64 + 1;
                let (id, dup_of) = if dup {
                    (original + lines.len() as u64, Some(original))
                } else {
                    (original, None)
                };
                // The id in `Fusion-Record-Id` only, not in the payload.
                let record = Record::from_json(&loghub::payload(set, line, cycle, 0).to_string())
                    .expect("record parses");
                let arrival = Arrival {
                    record_id: Some(RecordId(id)),
                    ingestion_time: Some(IngestionTime::Reported(
                        1_000_000_000 + cycle * CYCLE_GAP_NANOS + id,
                    )),
                    ..arrival_as(set.tenant)
                };
                probes.push(h.source.push_arrival(record, arrival));
                expectations.push(expectation(id, set, line, cycle, dup_of));
            }
            for probe in &probes {
                assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
            }
        }
    }
    expectations
}

/// What the POC config's NATS sinks wrote, as the verifier reads it back.
fn loghub_written(sinks: &MemorySinks) -> Vec<fusion_harness::verdict::Written> {
    let config = Config::from_yaml(&deploy_config("pipeline-poc.yaml")).expect("config parses");
    config
        .nodes
        .iter()
        .filter(|node| node.kind == "sink.nats")
        .flat_map(|node| {
            let params: SinkParams = node.parse_params().expect("sink params parse");
            sinks
                .outgoing(&node.id)
                .into_iter()
                .map(move |out| fusion_harness::verdict::Written {
                    subject: params.subject.clone(),
                    record_id: Some(out.meta.record_id.0.to_string()),
                    tenant: Some(out.meta.tenant.to_string()),
                    attributes: out.record.attributes,
                })
        })
        .collect()
}

/// Issues #13 and #14: the loghub harness judges the POC pipeline against what it writes down
/// by hand (the subjects each set reaches, which lines `sample` keeps, `dedupe` dropping a
/// planned duplicate, what `edit` and `lua` write) and the structured CSV, not against a run of
/// the pipeline. This checks that what it writes down is the config's: every sampled line goes
/// through twice per cycle, as the producer sends a line and its duplicate, over two cycles,
/// and the verifier's own verdict over what the sinks wrote passes (nothing missing or
/// unexpected, `edit` and `lua` as expected, enough duplicates dropped), with each set
/// reaching exactly its subjects, some lines left off the main subject by `sample`, and every
/// set's extraction compared. How well each pattern extracts is `extract_loghub.rs`'s
/// question, and the live run's report; it is not gated here.
#[test]
fn the_poc_pipeline_treats_each_loghub_set_as_the_harness_expects() {
    use fusion_harness::expect::sample_keeps;
    use fusion_harness::loghub::SETS;
    use fusion_harness::verdict::judge;

    let sinks = MemorySinks::new();
    let yaml = deploy_config("pipeline-poc.yaml");
    let h = start_with(&yaml, 4, sinks.clone(), registry(&sinks));
    let lines = loghub_lines(LOGHUB_STRIDE);
    let expectations = send_loghub_cycles(&h, &lines, LOGHUB_CYCLES);

    let report = judge(&expectations, &loghub_written(&sinks), &[]);
    assert!(report.passed(), "{report}");
    assert_eq!(
        (report.edit_mismatch, report.lua_mismatch),
        (0, 0),
        "{report}"
    );
    let groups = expectations.len() as u64 / 2;
    let kept = lines
        .iter()
        .flat_map(|(_, line)| (0..LOGHUB_CYCLES).map(move |cycle| (line.line_id, cycle)))
        .filter(|&(line_id, cycle)| sample_keeps(line_id, cycle))
        .count() as u64;
    assert_eq!(report.sampled_out, groups - kept, "{report}");
    assert!(
        report.sampled_out > 0 && kept > 0,
        "both verdicts occur: {report}"
    );
    let linux = expectations.iter().filter(|e| e.set == "Linux").count() as u64 / 2;
    let unjudged = expectations
        .iter()
        .filter(|e| e.dup_of.is_some() && e.subjects.is_empty())
        .count() as u64;
    assert_eq!(report.duplicates_planned, groups - unjudged, "{report}");
    assert_eq!(
        report.duplicates_dropped, report.duplicates_planned,
        "one copy in memory, so dedupe drops every duplicate: {report}"
    );
    assert_eq!(report.extra_copies, 0, "{report}");
    assert_eq!(
        report.received,
        kept + linux,
        "one copy of each kept line on main, and of every Linux line on audit: {report}"
    );
    assert_eq!(report.sets.len(), SETS.len(), "{report}");
    for (set, extraction) in &report.sets {
        assert!(extraction.checked > 0, "{set}: {report}");
    }
    h.finish();
}

/// Issue #14: while Dragonfly is paused, `dedupe_body` cannot claim a key, and under
/// `on_state_error: pass` it forwards the record: a duplicate that gets through then is a
/// state error, never a `dedupe` drop, and never a nak that would spend a delivery.
#[test]
fn the_poc_pipeline_forwards_every_copy_while_the_state_store_is_down() {
    use fusion_harness::loghub::SETS;
    use fusion_harness::verdict::judge;

    let sinks = MemorySinks::new();
    let yaml = deploy_config("pipeline-poc.yaml");
    let h = start_with(&yaml, 4, sinks.clone(), registry(&sinks));
    h.state.fail_all(true);
    let lines = loghub_lines(4 * LOGHUB_STRIDE);
    let expectations = send_loghub_cycles(&h, &lines, 1);

    let report = judge(&expectations, &loghub_written(&sinks), &[]);
    assert_eq!(
        (report.missing, report.unexpected),
        (0, 0),
        "every copy reaches every subject it expects: {report}"
    );
    assert_eq!(
        report.duplicates_dropped, 0,
        "no duplicate is dropped: {report}"
    );
    assert!(report.duplicates_planned > 0, "{report}");
    let (mut dedupe_drops, mut state_errors, mut naks) = (0, 0, 0);
    for set in &SETS {
        let stage = [("tenant", set.tenant), ("stage", "dedupe_body")];
        dedupe_drops += h.counter(
            CounterMetric::RecordsDropped,
            &[
                ("tenant", set.tenant),
                ("stage", "dedupe_body"),
                ("reason", "dedupe"),
            ],
        );
        state_errors += h.counter(CounterMetric::StateErrors, &stage);
        naks += h.counter(CounterMetric::SourceNaks, &[("tenant", set.tenant)]);
    }
    assert_eq!(dedupe_drops, 0);
    assert_eq!(state_errors, expectations.len() as u64);
    assert_eq!(naks, 0, "every message was acked");
    h.finish();
}
