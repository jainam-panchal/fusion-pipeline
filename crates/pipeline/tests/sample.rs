//! The `sample` node through the trait boundary: YAML config in, envelopes pushed through
//! the in-memory source, assertions on which records reached the in-memory sink, how each
//! ack handle settled, what the state store holds and what the recorder counted.

mod common;

use std::time::Duration;


use common::{WAIT, acme_record as record, start};
use fusion_core::memory::{AckOutcome, AckProbe};
use fusion_core::metrics::Metric;
use fusion_core::record::Record;
use fusion_core::state::StateStore as _;

const RANDOM_TENTH: &str = r#"
name: ingest
nodes:
  - id: keep_some
    type: sample
    mode: random
    percent: 10
  - id: out
    type: sink.memory
"#;

/// The labels of a `sample` drop by the node `keep_some` for tenant `acme`.
const SAMPLE_DROP: [(&str, &str); 3] = [
    ("tenant", "acme"),
    ("stage", "keep_some"),
    ("reason", "sample"),
];
const STAGE: [(&str, &str); 2] = [("tenant", "acme"), ("stage", "keep_some")];

/// Every probe settled as `expected`.
fn assert_all(probes: &[AckProbe], expected: AckOutcome) {
    for (i, probe) in probes.iter().enumerate() {
        assert_eq!(probe.wait(WAIT), Some(expected), "record {i}");
    }
}

#[test]
fn random_at_ten_percent_keeps_between_nine_and_eleven_percent_of_100k_records() {
    let h = start(RANDOM_TENTH, 4);
    let probes: Vec<AckProbe> = (1..=100_000)
        .map(|id| h.source.push(record(id, "disk full")))
        .collect();
    assert_all(&probes, AckOutcome::Ack);

    let kept = h.ids("out").len() as u64;
    assert!((9_000..=11_000).contains(&kept), "kept {kept}");
    assert_eq!(h.counter(Metric::RecordsDropped, &SAMPLE_DROP), 100_000 - kept);
    assert_eq!(h.counter(Metric::StateOps, &STAGE), 0, "random needs no state");
    h.finish();
}

#[test]
fn random_gives_a_redelivered_record_the_same_verdict() {
    let h = start(RANDOM_TENTH, 1);
    let ids: Vec<u64> = (1..=200).collect();
    for &id in &ids {
        assert_eq!(h.source.push(record(id, "x")).wait(WAIT), Some(AckOutcome::Ack));
    }
    let first_pass = h.ids("out");
    assert!(!first_pass.is_empty() && first_pass.len() < ids.len(), "{first_pass:?}");

    for &id in &ids {
        assert_eq!(h.source.push(record(id, "x")).wait(WAIT), Some(AckOutcome::Ack));
    }

    let mut twice = first_pass.clone();
    twice.extend(first_pass.iter().copied());
    twice.sort_unstable();
    assert_eq!(h.ids("out"), twice, "every kept id kept again, every dropped id dropped again");
    h.finish();
}

const EVERY_TENTH: &str = r#"
name: ingest
nodes:
  - id: keep_some
    type: sample
    mode: every_nth
    n: 10
  - id: out
    type: sink.memory
"#;

#[test]
fn every_nth_at_ten_keeps_exactly_one_thousand_of_ten_thousand_records_across_four_workers() {
    let h = start(EVERY_TENTH, 4);
    let probes: Vec<AckProbe> = (1..=10_000)
        .map(|id| h.source.push(record(id, "disk full")))
        .collect();
    assert_all(&probes, AckOutcome::Ack);

    assert_eq!(h.ids("out").len(), 1_000);
    assert_eq!(h.counter(Metric::RecordsDropped, &SAMPLE_DROP), 9_000);
    h.finish();
}

#[test]
fn every_nth_keeps_the_first_record_of_each_n_so_a_small_tenant_still_gets_one_through() {
    let h = start(EVERY_TENTH, 1);
    for id in 1..=12 {
        assert_eq!(h.source.push(record(id, "x")).wait(WAIT), Some(AckOutcome::Ack));
    }

    assert_eq!(h.ids("out"), vec![1, 11]);
    h.finish();
}

/// Per delivery, per the spec amendment: a redelivered message is a new delivery and takes
/// a new count. Here record 1 took count 1 (kept) and comes back as count 13 (dropped).
#[test]
fn every_nth_counts_a_redelivered_record_again_because_sampling_is_per_delivery() {
    let h = start(EVERY_TENTH, 1);
    for id in 1..=12 {
        assert_eq!(h.source.push(record(id, "x")).wait(WAIT), Some(AckOutcome::Ack));
    }
    assert_eq!(h.source.push(record(1, "x")).wait(WAIT), Some(AckOutcome::Ack));

    assert_eq!(h.ids("out"), vec![1, 11]);
    assert_eq!(
        h.state.get("ingest:acme:keep_some:sample:count").expect("store answers"),
        Some(b"13".to_vec()),
        "13 deliveries, count 13"
    );
    assert_eq!(h.state.keys(), vec!["ingest:acme:keep_some:sample:count"], "no key per record");
    h.finish();
}

/// The count lives 24 h from its last `incr`: a tenant with a record every 23 h never sees
/// it restart, a tenant silent for 25 h does, and its first record back is kept.
#[test]
fn every_nth_count_lives_a_day_from_its_last_record_then_restarts_at_one() {
    let h = start(EVERY_TENTH, 1);
    let day = Duration::from_secs(24 * 60 * 60);

    assert_eq!(h.source.push(record(1, "x")).wait(WAIT), Some(AckOutcome::Ack));
    h.state.advance(day - Duration::from_secs(3600));
    assert_eq!(h.source.push(record(2, "x")).wait(WAIT), Some(AckOutcome::Ack));
    h.state.advance(day - Duration::from_secs(3600));
    assert_eq!(h.source.push(record(3, "x")).wait(WAIT), Some(AckOutcome::Ack), "refreshed");
    h.state.advance(day + Duration::from_secs(3600));
    assert_eq!(h.source.push(record(4, "x")).wait(WAIT), Some(AckOutcome::Ack), "expired");

    assert_eq!(h.ids("out"), vec![1, 4], "counts 1, 2, 3, then 1 again");
    h.finish();
}

fn tenant_record(id: u64, tenant: &str) -> Record {
    Record::from_json(&format!(
        r#"{{"id": {id}, "body": "x", "resource": {{"tenant.id": "{tenant}"}}}}"#
    ))
    .expect("record parses")
}

#[test]
fn every_nth_keeps_one_count_per_tenant_so_a_small_tenant_is_not_drowned_by_a_big_one() {
    let h = start(EVERY_TENTH, 1);
    for id in 1..=30 {
        assert_eq!(h.source.push(tenant_record(id, "acme")).wait(WAIT), Some(AckOutcome::Ack));
    }
    for id in 101..=103 {
        assert_eq!(h.source.push(tenant_record(id, "beta")).wait(WAIT), Some(AckOutcome::Ack));
    }

    assert_eq!(h.ids("out"), vec![1, 11, 21, 101], "acme's 1, 11, 21; beta's first");
    h.finish();
}

#[test]
fn every_nth_with_the_store_down_passes_by_default_and_counts_the_error() {
    let h = start(EVERY_TENTH, 1);
    h.state.fail_all(true);
    for id in 1..=5 {
        assert_eq!(h.source.push(record(id, "x")).wait(WAIT), Some(AckOutcome::Ack));
    }

    assert_eq!(h.ids("out"), vec![1, 2, 3, 4, 5], "uncounted, forwarded");
    assert_eq!(h.counter(Metric::StateErrors, &STAGE), 5);
    assert_eq!(h.counter(Metric::RecordsDropped, &SAMPLE_DROP), 0);
    h.finish();
}

#[test]
fn every_nth_with_the_store_down_and_nak_policy_naks() {
    let yaml = EVERY_TENTH.replace("    n: 10\n", "    n: 10\n    on_state_error: nak\n");
    let h = start(&yaml, 1);
    h.state.fail_all(true);

    assert!(matches!(
        h.source.push(record(1, "x")).wait(WAIT),
        Some(AckOutcome::Nak(_))
    ));
    assert!(h.ids("out").is_empty());
    assert_eq!(h.counter(Metric::StateErrors, &STAGE), 1);
    h.finish();
}

const CONSISTENT_HALF: &str = r#"
name: ingest
nodes:
  - id: keep_some
    type: sample
    mode: consistent
    percent: 50
    key: [resource.host]
  - id: out
    type: sink.memory
"#;

fn host_record(id: u64, host: Option<&str>) -> Record {
    let host = host.map_or(String::new(), |h| format!(r#", "host": "{h}""#));
    Record::from_json(&format!(
        r#"{{"id": {id}, "body": "x", "resource": {{"tenant.id": "acme"{host}}}}}"#
    ))
    .expect("record parses")
}

/// Ids are `host_index * 10 + copy`, so the host of a kept id is recoverable.
fn push_hosts(h: &common::Harness, hosts: u64, copies: u64) -> Vec<AckProbe> {
    (0..hosts)
        .flat_map(|host| {
            (0..copies).map(move |copy| (host, copy))
        })
        .map(|(host, copy)| {
            h.source.push(host_record(host * 10 + copy, Some(&format!("web-{host}"))))
        })
        .collect()
}

#[test]
fn consistent_at_fifty_percent_keeps_or_drops_every_record_of_a_host_together() {
    let h = start(CONSISTENT_HALF, 4);
    let probes = push_hosts(&h, 1_000, 3);
    assert_all(&probes, AckOutcome::Ack);

    let kept = h.ids("out");
    let mut kept_hosts: Vec<u64> = kept.iter().map(|id| id / 10).collect();
    kept_hosts.dedup();
    assert_eq!(kept.len(), kept_hosts.len() * 3, "every kept host has all 3 copies");
    assert!(
        (450..=550).contains(&kept_hosts.len()),
        "{} of 1000 hosts kept",
        kept_hosts.len()
    );
    assert_eq!(h.counter(Metric::RecordsDropped, &SAMPLE_DROP), 3_000 - kept.len() as u64);
    assert_eq!(h.counter(Metric::StateOps, &STAGE), 0, "consistent needs no state");
    h.finish();
}

#[test]
fn consistent_decides_all_records_missing_the_key_field_together() {
    let h = start(CONSISTENT_HALF, 1);
    for id in 1..=20 {
        assert_eq!(h.source.push(host_record(id, None)).wait(WAIT), Some(AckOutcome::Ack));
    }

    let kept = h.ids("out").len();
    assert!(kept == 0 || kept == 20, "all or none, got {kept}");
    h.finish();
}

/// Kept at 20% implies kept at 50%: the same hosts, plus more.
#[test]
fn consistent_at_a_lower_percent_keeps_a_subset_of_the_hosts_kept_at_a_higher_percent() {
    let low = start(&CONSISTENT_HALF.replace("percent: 50", "percent: 20"), 1);
    let high = start(CONSISTENT_HALF, 1);
    assert_all(&push_hosts(&low, 300, 1), AckOutcome::Ack);
    assert_all(&push_hosts(&high, 300, 1), AckOutcome::Ack);

    let low_kept = low.ids("out");
    let high_kept = high.ids("out");
    assert!(!low_kept.is_empty() && low_kept.len() < high_kept.len());
    assert!(low_kept.iter().all(|id| high_kept.contains(id)), "subset");
    low.finish();
    high.finish();
}

#[test]
fn random_at_one_hundred_percent_keeps_everything() {
    let h = start(&RANDOM_TENTH.replace("percent: 10", "percent: 100"), 1);
    let probes: Vec<AckProbe> = (1..=1_000).map(|id| h.source.push(record(id, "x"))).collect();
    assert_all(&probes, AckOutcome::Ack);

    assert_eq!(h.ids("out").len(), 1_000);
    assert_eq!(h.counter(Metric::RecordsDropped, &SAMPLE_DROP), 0);
    h.finish();
}

/// Snowflake ids: a millisecond timestamp in the high bits, a running number in the low
/// bits, a few hundred per millisecond. Near-identical high bits must not skew the coin.
#[test]
fn random_at_ten_percent_holds_on_snowflake_shaped_ids() {
    let h = start(RANDOM_TENTH, 4);
    let base_ms: u64 = 1_800_000_000_000;
    let probes: Vec<AckProbe> = (0..100_000u64)
        .map(|i| (base_ms + i / 400) << 22 | (i % 400))
        .map(|id| h.source.push(record(id, "x")))
        .collect();
    assert_all(&probes, AckOutcome::Ack);

    let kept = h.ids("out").len();
    assert!((9_000..=11_000).contains(&kept), "kept {kept}");
    h.finish();
}

/// Two `random` nodes in series at 10% each keep about 1%, not 10%: each node's coin is
/// its own.
#[test]
fn two_random_nodes_in_series_keep_the_product_of_their_shares() {
    let yaml = r#"
name: ingest
nodes:
  - id: first
    type: sample
    mode: random
    percent: 10
  - id: second
    type: sample
    mode: random
    percent: 10
  - id: out
    type: sink.memory
"#;
    let h = start(yaml, 4);
    let probes: Vec<AckProbe> = (1..=100_000).map(|id| h.source.push(record(id, "x"))).collect();
    assert_all(&probes, AckOutcome::Ack);

    let kept = h.ids("out").len();
    assert!((700..=1_300).contains(&kept), "kept {kept}, expected about 1,000");
    h.finish();
}
