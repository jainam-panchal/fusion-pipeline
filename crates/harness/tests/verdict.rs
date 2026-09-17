//! The verifier's verdict: expectations against what the sinks and the dead-letter stream
//! hold. Delivery is judged per duplicate group and sink; extraction per group on the main
//! sink.

use std::collections::BTreeMap;

use fusion_harness::expect::{AUDIT, Expectation, MAIN, expectation};
use fusion_harness::verdict::{DeadLetter, Delivery, judge};
use serde_json::{Map, Value, json};

fn attrs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

fn apache_attrs() -> BTreeMap<String, String> {
    attrs(&[
        ("Time", "Sun Dec 04 04:47:44 2005"),
        ("Level", "notice"),
        ("Content", "ok"),
    ])
}

/// An original Apache line `line_id` in cycle 0, and optionally its duplicate `dup`.
fn apache(id: u64, line_id: usize, dup_of: Option<u64>) -> Expectation {
    expectation(id, "Apache", line_id, 0, dup_of, apache_attrs()).expect("known set")
}

fn linux(id: u64) -> Expectation {
    expectation(
        id,
        "Linux",
        1,
        0,
        None,
        attrs(&[("Level", "combo"), ("Content", "x")]),
    )
    .expect("known set")
}

/// What a sink wrote for `e`: its id and tenant headers and its expected attributes, plus
/// the attributes the pipeline adds.
fn delivered(e: &Expectation, subject: &str) -> Delivery {
    let mut attributes: Map<String, Value> = e
        .attributes
        .iter()
        .map(|(k, v)| (k.clone(), json!(v)))
        .collect();
    attributes.insert("service".into(), json!("x"));
    attributes.insert("loghub.line_id".into(), json!(e.line_id));
    Delivery {
        subject: subject.to_owned(),
        record_id: Some(e.id.to_string()),
        tenant: Some(e.tenant.clone()),
        attributes,
    }
}

#[test]
fn every_group_delivered_to_every_sink_passes() {
    let expectations = [apache(1, 1, None), linux(2)];
    let deliveries = [
        delivered(&expectations[0], MAIN),
        delivered(&expectations[1], MAIN),
        delivered(&expectations[1], AUDIT),
    ];
    let report = judge(&expectations, &deliveries, &[]);
    assert!(report.passed(), "{report}");
    assert_eq!(report.published, 2);
    assert_eq!(report.received, 3);
    assert_eq!(
        (report.missing, report.unexpected, report.extra_copies),
        (0, 0, 0)
    );
    assert_eq!(report.formats["Apache"].checked, 1);
    assert_eq!(report.formats["Apache"].mismatched, 0);
}

#[test]
fn a_group_that_reached_no_sink_is_missing() {
    let expectations = [apache(1, 1, None), apache(2, 2, None)];
    let deliveries = [delivered(&expectations[0], MAIN)];
    let report = judge(&expectations, &deliveries, &[]);
    assert!(!report.passed());
    assert_eq!(report.missing, 1);
    assert!(report.to_string().contains("LineId 2"), "{report}");
}

#[test]
fn a_dropped_duplicate_is_not_missing_when_its_original_arrived() {
    let expectations = [apache(1, 1, None), apache(2, 1, Some(1))];
    let report = judge(&expectations, &[delivered(&expectations[0], MAIN)], &[]);
    assert!(report.passed(), "{report}");
}

#[test]
fn a_delivered_duplicate_satisfies_its_group() {
    let expectations = [apache(1, 1, None), apache(2, 1, Some(1))];
    let report = judge(&expectations, &[delivered(&expectations[1], MAIN)], &[]);
    assert!(report.passed(), "{report}");
}

#[test]
fn both_copies_delivered_is_an_extra_copy_not_a_failure() {
    let expectations = [apache(1, 1, None), apache(2, 1, Some(1))];
    let deliveries = [
        delivered(&expectations[0], MAIN),
        delivered(&expectations[1], MAIN),
    ];
    let report = judge(&expectations, &deliveries, &[]);
    assert!(report.passed(), "{report}");
    assert_eq!(report.extra_copies, 1);
}

#[test]
fn a_redelivered_message_is_an_extra_copy() {
    let expectations = [apache(1, 1, None)];
    let deliveries = [
        delivered(&expectations[0], MAIN),
        delivered(&expectations[0], MAIN),
    ];
    let report = judge(&expectations, &deliveries, &[]);
    assert!(report.passed(), "{report}");
    assert_eq!(report.received, 1);
    assert_eq!(report.extra_copies, 1);
}

#[test]
fn a_linux_group_missing_from_the_audit_sink_is_missing() {
    let expectations = [linux(1)];
    let report = judge(&expectations, &[delivered(&expectations[0], MAIN)], &[]);
    assert_eq!(report.missing, 1);
    assert!(!report.passed());
}

#[test]
fn deliveries_nobody_expected_are_unexpected() {
    let e = apache(1, 1, None);
    let unknown_id = Delivery {
        record_id: Some("99".into()),
        ..delivered(&e, MAIN)
    };
    let wrong_sink = delivered(&e, AUDIT);
    let wrong_tenant = Delivery {
        tenant: Some("linux".into()),
        ..delivered(&e, MAIN)
    };
    let no_id = Delivery {
        record_id: None,
        ..delivered(&e, MAIN)
    };
    let bad_id = Delivery {
        record_id: Some("1x".into()),
        ..delivered(&e, MAIN)
    };
    for bad in [unknown_id, wrong_sink, wrong_tenant, no_id, bad_id] {
        let deliveries = [delivered(&e, MAIN), bad.clone()];
        let report = judge(std::slice::from_ref(&e), &deliveries, &[]);
        assert_eq!(report.unexpected, 1, "{bad:?}");
        assert!(!report.passed());
    }
}

#[test]
fn an_attribute_that_differs_is_a_mismatch_of_its_format() {
    let expectations = [apache(1, 7, None), apache(2, 8, None)];
    let mut wrong = delivered(&expectations[1], MAIN);
    wrong.attributes.insert("Level".into(), json!("error"));
    let deliveries = [delivered(&expectations[0], MAIN), wrong];
    let report = judge(&expectations, &deliveries, &[]);
    assert!(
        report.passed(),
        "extraction is reported, not gated: {report}"
    );
    let apache = &report.formats["Apache"];
    assert_eq!((apache.checked, apache.mismatched), (2, 1));
    assert_eq!(apache.mismatched_lines, [8].into());
    assert!((apache.accuracy() - 0.5).abs() < f64::EPSILON);
}

#[test]
fn extraction_compares_the_set_columns_only_under_the_readme_rules() {
    let e = apache(1, 1, None);
    let mut got = delivered(&e, MAIN);
    // Content keeps the line's trailing space; the CSV does not.
    got.attributes.insert("Content".into(), json!("ok  "));
    let report = judge(std::slice::from_ref(&e), &[got], &[]);
    assert_eq!(report.formats["Apache"].mismatched, 0, "{report}");

    // An absent attribute equals an empty cell, and a present one where the cell is empty
    // is a mismatch.
    let sparse = expectation(1, "Apache", 1, 0, None, attrs(&[("Time", "t")])).expect("set");
    let mut got = delivered(&sparse, MAIN);
    let report = judge(
        std::slice::from_ref(&sparse),
        std::slice::from_ref(&got),
        &[],
    );
    assert_eq!(report.formats["Apache"].mismatched, 0, "{report}");
    got.attributes.insert("Level".into(), json!("notice"));
    let report = judge(std::slice::from_ref(&sparse), &[got], &[]);
    assert_eq!(report.formats["Apache"].mismatched, 1, "{report}");
}

#[test]
fn the_audit_copy_is_not_checked_for_extraction() {
    let e = linux(1);
    let mut raw = delivered(&e, AUDIT);
    raw.attributes.clear();
    let report = judge(std::slice::from_ref(&e), &[delivered(&e, MAIN), raw], &[]);
    assert!(report.passed(), "{report}");
    assert_eq!(report.formats["Linux"].checked, 1);
    assert_eq!(report.formats["Linux"].mismatched, 0);
}

#[test]
fn a_dead_letter_fails_the_run() {
    let expectations = [apache(1, 1, None), apache(2, 1, Some(1))];
    let deliveries = [delivered(&expectations[1], MAIN)];
    let dead = [DeadLetter {
        record_id: Some("1".into()),
    }];
    let report = judge(&expectations, &deliveries, &dead);
    assert_eq!((report.missing, report.dead_lettered), (0, 1));
    assert!(!report.passed(), "{report}");
}

#[test]
fn a_dead_letter_nobody_published_is_unexpected() {
    let e = apache(1, 1, None);
    let dead = [DeadLetter { record_id: None }];
    let report = judge(std::slice::from_ref(&e), &[delivered(&e, MAIN)], &dead);
    assert_eq!((report.unexpected, report.dead_lettered), (1, 0));
}

#[test]
fn only_linux_fans_out_to_the_audit_sink() {
    assert_eq!(linux(1).sinks, [MAIN, AUDIT]);
    for set in ["OpenSSH", "Apache", "Mac"] {
        let e = expectation(1, set, 1, 0, None, BTreeMap::new()).expect("known set");
        assert_eq!(e.sinks, [MAIN], "{set}");
    }
    assert!(expectation(1, "Windows", 1, 0, None, BTreeMap::new()).is_none());
    let dup = apache(2, 1, Some(1));
    assert_eq!(dup.drop.as_deref(), Some("dedupe"));
    assert_eq!(apache(1, 1, None).drop, None);
}

#[test]
fn a_line_that_mismatches_in_every_cycle_is_listed_once() {
    let mut expectations = Vec::new();
    let mut deliveries = Vec::new();
    for (id, line_id, cycle) in [(1, 9, 0), (2, 3, 0), (3, 9, 1), (4, 3, 1)] {
        let e = expectation(id, "Apache", line_id, cycle, None, apache_attrs()).expect("set");
        let mut wrong = delivered(&e, MAIN);
        wrong.attributes.insert("Level".into(), json!("error"));
        deliveries.push(wrong);
        expectations.push(e);
    }
    let report = judge(&expectations, &deliveries, &[]);
    let apache = &report.formats["Apache"];
    assert_eq!((apache.checked, apache.mismatched), (4, 4));
    assert_eq!(apache.mismatched_lines, [3, 9].into());
}
