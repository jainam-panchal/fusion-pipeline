//! Loading the vendored loghub sets: one line per distinct body, joined to its row of the
//! structured CSV under the normalisation rules in `testdata/loghub/README.md`.

use std::collections::BTreeSet;

use fusion_harness::loghub::{self, SETS};

#[test]
fn every_set_loads_one_line_per_distinct_body() {
    let distinct = [
        ("Linux", 2000),
        ("OpenSSH", 2000),
        ("Apache", 1461),
        ("Mac", 1991),
    ];
    for (name, expected) in distinct {
        let set = loghub::set(name).expect("a vendored set");
        let lines = loghub::load(&loghub::testdata(), set).expect("set loads");
        assert_eq!(lines.len(), expected, "{name}");
        let bodies: BTreeSet<_> = lines.iter().map(|l| l.body.as_str()).collect();
        assert_eq!(bodies.len(), lines.len(), "{name}: bodies are distinct");
    }
}

#[test]
fn a_repeated_body_keeps_its_first_line_id() {
    let set = loghub::set("Apache").expect("a vendored set");
    let lines = loghub::load(&loghub::testdata(), set).expect("set loads");
    let raw =
        std::fs::read_to_string(loghub::testdata().join("Apache/Apache_2k.log")).expect("raw log");
    let raw: Vec<&str> = raw.lines().collect();
    for line in &lines {
        let first = raw
            .iter()
            .position(|r| *r == line.body)
            .expect("body is a raw line");
        assert_eq!(line.line_id, first + 1, "{}", line.body);
    }
}

#[test]
fn the_last_mac_line_is_loaded_without_a_trailing_newline() {
    let set = loghub::set("Mac").expect("a vendored set");
    let lines = loghub::load(&loghub::testdata(), set).expect("set loads");
    let last = lines.iter().max_by_key(|l| l.line_id).expect("lines");
    assert_eq!(last.line_id, 2000);
    assert!(
        last.body.ends_with("wakeEventHandlerThread"),
        "{}",
        last.body
    );
}

#[test]
fn attributes_follow_the_readme_rules() {
    let set = loghub::set("Linux").expect("a vendored set");
    let lines = loghub::load(&loghub::testdata(), set).expect("set loads");
    let first = lines.iter().find(|l| l.line_id == 1).expect("line 1");
    // Rule 1: `19939.0` is `19939`.
    assert_eq!(first.attributes["PID"], "19939");
    assert_eq!(first.attributes["Component"], "sshd(pam_unix)");
    // Rule 2: an empty cell is an absent attribute.
    let without_pid = lines
        .iter()
        .find(|l| !l.attributes.contains_key("PID"))
        .expect("a Linux line without a PID");
    let header = without_pid.body.split(": ").next().unwrap_or_default();
    assert!(!header.contains('['), "{}", without_pid.body);
    // Only the set's columns: no EventId or template.
    assert!(!first.attributes.contains_key("EventId"));
}

#[test]
fn normalise_applies_each_rule_to_its_column_only() {
    assert_eq!(loghub::normalise("PID", "19939.0"), "19939");
    assert_eq!(loghub::normalise("PID", ".0"), ".0");
    assert_eq!(loghub::normalise("Content", "text  "), "text");
    assert_eq!(loghub::normalise("Time", "10:00 "), "10:00 ");
    assert_eq!(loghub::normalise("Content", "1.0"), "1");
}

#[test]
fn the_payload_carries_the_line_and_its_cycle_for_the_sample_key() {
    let set = loghub::set("Linux").expect("a vendored set");
    let line = loghub::Line {
        line_id: 7,
        body: "text".to_owned(),
        attributes: std::collections::BTreeMap::new(),
    };
    let payload = loghub::payload(set, &line, 3, 42);
    assert_eq!(payload["attributes"]["loghub.line_id"], 7);
    assert_eq!(payload["attributes"]["loghub.cycle"], 3);
    assert_eq!(payload["body"], "text");
    assert_eq!(payload["resource"]["log.format"], "Linux");
    assert_eq!(payload["observed_time_unix_nano"], 42);
}

#[test]
fn each_set_has_its_own_tenant() {
    let tenants: BTreeSet<_> = SETS.iter().map(|s| s.tenant).collect();
    assert_eq!(tenants.len(), SETS.len());
    assert!(loghub::set("Windows").is_none());
}
