//! The verifier's verdict: expectations against what the sinks wrote and the dead-letter
//! stream holds. What reached each subject is judged per duplicate group, `dedupe` by the share
//! of planned duplicates it dropped, and extraction and `edit` per group on the main subject.

use std::collections::BTreeMap;

use fusion_core::stage::DropReason;
use fusion_harness::expect::{Expectation, expectation, sample_keeps};
use fusion_harness::loghub::{self, AUDIT, Line, MAIN};
use fusion_harness::verdict::{DeadLetter, Written, judge};
use serde_json::{Map, Value, json};

fn attrs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

fn line(line_id: usize, attributes: BTreeMap<String, String>) -> Line {
    Line {
        line_id,
        body: format!("line {line_id}"),
        attributes,
    }
}

fn apache_attrs() -> BTreeMap<String, String> {
    attrs(&[
        ("Time", "Sun Dec 04 04:47:44 2005"),
        ("Level", "notice"),
        ("Content", "ok"),
    ])
}

fn in_set(
    set: &str,
    id: u64,
    line_id: usize,
    cycle: u64,
    dup_of: Option<u64>,
    attributes: BTreeMap<String, String>,
) -> Expectation {
    let set = loghub::set(set).expect("a vendored set");
    expectation(id, set, &line(line_id, attributes), cycle, dup_of)
}

/// Apache line `line_id` in cycle 0, a duplicate of `dup_of` when given.
fn apache(id: u64, line_id: usize, dup_of: Option<u64>) -> Expectation {
    in_set("Apache", id, line_id, 0, dup_of, apache_attrs())
}

fn linux(id: u64) -> Expectation {
    let attributes = attrs(&[("Level", "combo"), ("Component", "su"), ("Content", "x")]);
    in_set("Linux", id, 1, 0, None, attributes)
}

/// What the POC config writes for `e` on `subject`: its id and tenant headers, the
/// attributes extraction lifts, the ones `edit` and `lua` write, and the line id and cycle
/// the producer sent.
fn written(e: &Expectation, subject: &str) -> Written {
    let mut attributes: Map<String, Value> = e
        .attributes
        .iter()
        .map(|(k, v)| (k.clone(), json!(v)))
        .collect();
    for (target, source) in &e.edits.copied {
        if let Some(value) = e.attributes.get(source) {
            attributes.insert(target.clone(), json!(value));
        }
    }
    for (target, value) in &e.edits.set {
        attributes.insert(target.clone(), json!(value));
    }
    for (target, source) in &e.lua.lengths {
        if let Some(value) = e.attributes.get(source) {
            attributes.insert(target.clone(), json!(value.len()));
        }
    }
    attributes.insert("loghub.line_id".into(), json!(e.line_id));
    attributes.insert("loghub.cycle".into(), json!(e.cycle));
    Written {
        subject: subject.to_owned(),
        record_id: Some(e.id.to_string()),
        tenant: Some(e.tenant.clone()),
        attributes,
    }
}

#[test]
fn every_group_written_to_every_subject_passes() {
    let expectations = [apache(1, 1, None), linux(2)];
    let written = [
        written(&expectations[0], MAIN),
        written(&expectations[1], MAIN),
        written(&expectations[1], AUDIT),
    ];
    let report = judge(&expectations, &written, &[]);
    assert!(report.passed(), "{report}");
    assert_eq!(report.published, 2);
    assert_eq!(report.received, 3);
    assert_eq!(
        (report.missing, report.unexpected, report.extra_copies),
        (0, 0, 0)
    );
    assert_eq!(report.edit_mismatch, 0);
    assert_eq!(report.sets["Apache"].checked, 1);
    assert_eq!(report.sets["Apache"].mismatched, 0);
}

#[test]
fn a_group_that_reached_no_subject_is_missing() {
    let expectations = [apache(1, 1, None), apache(2, 2, None)];
    let written = [written(&expectations[0], MAIN)];
    let report = judge(&expectations, &written, &[]);
    assert!(!report.passed());
    assert_eq!(report.missing, 1);
    assert!(report.to_string().contains("LineId 2"), "{report}");
}

#[test]
fn a_dropped_duplicate_is_not_missing_when_its_original_arrived() {
    let expectations = [apache(1, 1, None), apache(2, 1, Some(1))];
    let report = judge(&expectations, &[written(&expectations[0], MAIN)], &[]);
    assert!(report.passed(), "{report}");
    assert_eq!(
        (report.duplicates_planned, report.duplicates_dropped),
        (1, 1)
    );
}

#[test]
fn a_written_duplicate_satisfies_its_group_and_its_original_counts_as_the_drop() {
    let expectations = [apache(1, 1, None), apache(2, 1, Some(1))];
    let report = judge(&expectations, &[written(&expectations[1], MAIN)], &[]);
    assert!(report.passed(), "{report}");
    assert_eq!(
        (report.duplicates_planned, report.duplicates_dropped),
        (1, 1)
    );
}

/// `groups` Apache lines, each sent as an original and one planned duplicate.
fn duplicated_groups(groups: u64) -> Vec<Expectation> {
    (0..groups)
        .flat_map(|group| {
            let original = 10 * group + 1;
            let line_id = usize::try_from(group).expect("small") + 1;
            [
                apache(original, line_id, None),
                apache(original + 1, line_id, Some(original)),
            ]
        })
        .collect()
}

/// Every original written to the main subject, and the duplicates of the first `undropped`
/// groups too.
fn written_with_undropped(expectations: &[Expectation], undropped: usize) -> Vec<Written> {
    expectations
        .iter()
        .filter(|e| {
            e.dup_of
                .is_none_or(|original| original <= 10 * undropped as u64)
        })
        .map(|e| written(e, MAIN))
        .collect()
}

#[test]
fn both_copies_written_is_an_extra_copy_and_an_undropped_duplicate() {
    let expectations = duplicated_groups(5);
    let report = judge(
        &expectations,
        &written_with_undropped(&expectations, 1),
        &[],
    );
    assert!(report.passed(), "80% of the duplicates dropped: {report}");
    assert_eq!(report.extra_copies, 1);
    assert_eq!(
        (report.duplicates_planned, report.duplicates_dropped),
        (5, 4)
    );
    assert!(
        report.to_string().contains("at least 80%: held"),
        "{report}"
    );
}

#[test]
fn a_dedupe_that_drops_under_80_percent_of_the_duplicates_fails_the_run() {
    let expectations = duplicated_groups(5);
    // Two duplicates of five arrive: 3 of 5 dropped, 60%.
    let report = judge(
        &expectations,
        &written_with_undropped(&expectations, 2),
        &[],
    );
    assert_eq!((report.missing, report.unexpected), (0, 0));
    assert_eq!(
        (report.duplicates_planned, report.duplicates_dropped),
        (5, 3)
    );
    assert!(!report.passed(), "{report}");
    assert!(
        report.to_string().contains("at least 80%: FAILED"),
        "{report}"
    );
}

#[test]
fn a_run_without_planned_duplicates_does_not_judge_dedupe() {
    let expectations = [apache(1, 1, None)];
    let report = judge(&expectations, &[written(&expectations[0], MAIN)], &[]);
    assert_eq!(report.duplicates_planned, 0);
    assert!(report.passed(), "{report}");
}

#[test]
fn a_rewritten_message_is_an_extra_copy() {
    let expectations = [apache(1, 1, None)];
    let written = [
        written(&expectations[0], MAIN),
        written(&expectations[0], MAIN),
    ];
    let report = judge(&expectations, &written, &[]);
    assert!(report.passed(), "{report}");
    assert_eq!(report.received, 1);
    assert_eq!(report.extra_copies, 1);
}

#[test]
fn a_linux_group_missing_from_the_audit_subject_is_missing() {
    let expectations = [linux(1)];
    let report = judge(&expectations, &[written(&expectations[0], MAIN)], &[]);
    assert_eq!(report.missing, 1);
    assert!(!report.passed());
}

#[test]
fn messages_nobody_expected_are_unexpected() {
    let e = apache(1, 1, None);
    let unknown_id = Written {
        record_id: Some("99".into()),
        ..written(&e, MAIN)
    };
    let wrong_subject = written(&e, AUDIT);
    let other_subject = written(&e, "processed.logs");
    let wrong_tenant = Written {
        tenant: Some("linux".into()),
        ..written(&e, MAIN)
    };
    let no_id = Written {
        record_id: None,
        ..written(&e, MAIN)
    };
    let bad_id = Written {
        record_id: Some("1x".into()),
        ..written(&e, MAIN)
    };
    for bad in [
        unknown_id,
        wrong_subject,
        other_subject,
        wrong_tenant,
        no_id,
        bad_id,
    ] {
        let written = [written(&e, MAIN), bad.clone()];
        let report = judge(std::slice::from_ref(&e), &written, &[]);
        assert_eq!(report.unexpected, 1, "{bad:?}");
        assert!(!report.passed());
    }
}

#[test]
fn an_attribute_that_differs_is_a_mismatch_of_its_set() {
    let expectations = [apache(1, 7, None), apache(2, 8, None)];
    let mut wrong = written(&expectations[1], MAIN);
    wrong.attributes.insert("Level".into(), json!("error"));
    let written = [written(&expectations[0], MAIN), wrong];
    let report = judge(&expectations, &written, &[]);
    assert!(
        report.passed(),
        "extraction is reported, not gated: {report}"
    );
    let apache = &report.sets["Apache"];
    assert_eq!((apache.checked, apache.mismatched), (2, 1));
    assert_eq!(apache.mismatched_lines, [8].into());
    assert!((apache.accuracy() - 0.5).abs() < f64::EPSILON);
}

#[test]
fn extraction_compares_the_set_columns_only_under_the_readme_rules() {
    let e = apache(1, 1, None);
    let mut got = written(&e, MAIN);
    // Content keeps the line's trailing space; the CSV does not.
    got.attributes.insert("Content".into(), json!("ok  "));
    let report = judge(std::slice::from_ref(&e), &[got], &[]);
    assert_eq!(report.sets["Apache"].mismatched, 0, "{report}");

    // An absent attribute equals an empty cell, and a present one where the cell is empty
    // is a mismatch.
    let sparse = in_set("Apache", 1, 1, 0, None, attrs(&[("Time", "t")]));
    let mut got = written(&sparse, MAIN);
    let report = judge(
        std::slice::from_ref(&sparse),
        std::slice::from_ref(&got),
        &[],
    );
    assert_eq!(report.sets["Apache"].mismatched, 0, "{report}");
    got.attributes.insert("Level".into(), json!("notice"));
    let report = judge(std::slice::from_ref(&sparse), &[got], &[]);
    assert_eq!(report.sets["Apache"].mismatched, 1, "{report}");
}

#[test]
fn what_edit_wrote_is_checked_against_the_extracted_attributes() {
    let e = linux(1);
    let audit = written(&e, AUDIT);

    let mut no_name = written(&e, MAIN);
    no_name.attributes.remove("pipeline");
    let mut wrong_copy = written(&e, MAIN);
    wrong_copy
        .attributes
        .insert("service".into(), json!("cron"));
    let mut no_copy = written(&e, MAIN);
    no_copy.attributes.remove("service");
    for bad in [no_name, wrong_copy, no_copy] {
        let report = judge(std::slice::from_ref(&e), &[bad.clone(), audit.clone()], &[]);
        assert_eq!(report.edit_mismatch, 1, "{bad:?}");
        assert!(!report.passed(), "{report}");
    }

    // The copy follows what extraction lifted, right or wrong: a mismatched component is
    // extraction's finding, not edit's.
    let mut misparsed = written(&e, MAIN);
    misparsed
        .attributes
        .insert("Component".into(), json!("cron"));
    misparsed.attributes.insert("service".into(), json!("cron"));
    let report = judge(std::slice::from_ref(&e), &[misparsed, audit.clone()], &[]);
    assert_eq!(report.edit_mismatch, 0, "{report}");
    assert_eq!(report.sets["Linux"].mismatched, 1, "{report}");

    // No component, nothing to copy: a `service` there is a mismatch.
    let bare = in_set("Apache", 2, 1, 0, None, apache_attrs());
    let mut invented = written(&bare, MAIN);
    assert!(!invented.attributes.contains_key("service"));
    invented.attributes.insert("service".into(), json!("x"));
    let report = judge(std::slice::from_ref(&bare), &[invented], &[]);
    assert_eq!(report.edit_mismatch, 1, "{report}");
}

#[test]
fn the_audit_copy_is_checked_for_neither_extraction_nor_edit() {
    let e = linux(1);
    let mut raw = written(&e, AUDIT);
    raw.attributes.clear();
    let report = judge(std::slice::from_ref(&e), &[written(&e, MAIN), raw], &[]);
    assert!(report.passed(), "{report}");
    assert_eq!(report.sets["Linux"].checked, 1);
    assert_eq!(report.sets["Linux"].mismatched, 0);
    assert_eq!(report.edit_mismatch, 0);
}

#[test]
fn a_dead_letter_fails_the_run() {
    let expectations = [apache(1, 1, None), apache(2, 1, Some(1))];
    let written = [written(&expectations[1], MAIN)];
    let dead = [DeadLetter {
        record_id: Some("1".into()),
    }];
    let report = judge(&expectations, &written, &dead);
    assert_eq!((report.missing, report.dead_lettered), (0, 1));
    assert!(!report.passed(), "{report}");
}

#[test]
fn a_dead_letter_nobody_published_is_unexpected() {
    let e = apache(1, 1, None);
    let dead = [DeadLetter { record_id: None }];
    let report = judge(std::slice::from_ref(&e), &[written(&e, MAIN)], &dead);
    assert_eq!((report.unexpected, report.dead_lettered), (1, 0));
}

#[test]
fn a_line_that_mismatches_in_every_cycle_is_listed_once() {
    let lines: Vec<usize> = (1..)
        .filter(|&line_id| sample_keeps(line_id, 0) && sample_keeps(line_id, 1))
        .take(2)
        .collect();
    let mut expectations = Vec::new();
    let mut written_all = Vec::new();
    let sends = [
        (1, lines[1], 0),
        (2, lines[0], 0),
        (3, lines[1], 1),
        (4, lines[0], 1),
    ];
    for (id, line_id, cycle) in sends {
        let e = in_set("Apache", id, line_id, cycle, None, apache_attrs());
        let mut wrong = written(&e, MAIN);
        wrong.attributes.insert("Level".into(), json!("error"));
        written_all.push(wrong);
        expectations.push(e);
    }
    let report = judge(&expectations, &written_all, &[]);
    let apache = &report.sets["Apache"];
    assert_eq!((apache.checked, apache.mismatched), (4, 4));
    assert_eq!(apache.mismatched_lines, lines.iter().copied().collect());
}

#[test]
fn an_expectation_names_its_subjects_drop_and_edits() {
    assert_eq!(linux(1).subjects, [MAIN, AUDIT]);
    for set in ["OpenSSH", "Apache", "Mac"] {
        let e = in_set(set, 1, 1, 0, None, BTreeMap::new());
        assert_eq!(e.subjects, [MAIN], "{set}");
    }
    let dup = apache(2, 1, Some(1));
    assert_eq!(dup.drop, Some(DropReason::Dedupe));
    assert_eq!(apache(1, 1, None).drop, None);
    assert_eq!(dup.edits.set, attrs(&[("pipeline", "poc")]));
    assert_eq!(dup.edits.copied, attrs(&[("service", "Component")]));

    // The expectations file spells the drop reason by its name, and reads it back.
    let line = serde_json::to_string(&dup).expect("serialises");
    assert!(line.contains(r#""drop":"dedupe""#), "{line}");
    let back: Expectation = serde_json::from_str(&line).expect("parses");
    assert_eq!(back, dup);
    let unknown = line.replace(r#""drop":"dedupe""#, r#""drop":"typo""#);
    assert!(serde_json::from_str::<Expectation>(&unknown).is_err());
}

/// The first line `sample` leaves out in cycle 0.
fn sampled_out_line() -> usize {
    (1..)
        .find(|&line_id| !sample_keeps(line_id, 0))
        .expect("a line")
}

#[test]
fn a_sampled_out_group_is_neither_missing_nor_a_dedupe_drop() {
    let line_id = sampled_out_line();
    let expectations = [
        apache(1, 1, None),
        apache(2, line_id, None),
        apache(3, line_id, Some(2)),
    ];
    let report = judge(&expectations, &[written(&expectations[0], MAIN)], &[]);
    assert!(report.passed(), "{report}");
    assert_eq!(report.missing, 0);
    assert_eq!(report.sampled_out, 1, "{report}");
    assert_eq!(
        (report.duplicates_planned, report.duplicates_dropped),
        (0, 0),
        "a sample drop is not a dedupe drop"
    );
    assert!(report.to_string().contains("sampled_out    1"), "{report}");
}

#[test]
fn a_main_copy_of_a_sampled_out_group_is_unexpected() {
    let e = apache(1, sampled_out_line(), None);
    let report = judge(std::slice::from_ref(&e), &[written(&e, MAIN)], &[]);
    assert!(!report.passed());
    assert_eq!(report.unexpected, 1);
    assert_eq!(report.sets["Apache"].checked, 0);
}

#[test]
fn a_sampled_out_linux_group_is_judged_for_dedupe_on_the_audit_subject() {
    let line_id = sampled_out_line();
    let attributes = attrs(&[("Component", "su"), ("Content", "x")]);
    let original = in_set("Linux", 1, line_id, 0, None, attributes.clone());
    let dup = in_set("Linux", 2, line_id, 0, Some(1), attributes);
    assert_eq!(original.subjects, [AUDIT]);
    let expectations = [original, dup];

    let report = judge(&expectations, &[written(&expectations[0], AUDIT)], &[]);
    assert!(report.passed(), "{report}");
    assert_eq!(
        (report.duplicates_planned, report.duplicates_dropped),
        (1, 1)
    );

    let both = [
        written(&expectations[0], AUDIT),
        written(&expectations[1], AUDIT),
    ];
    let report = judge(&expectations, &both, &[]);
    assert_eq!(
        (report.duplicates_planned, report.duplicates_dropped),
        (1, 0)
    );
    assert_eq!(report.extra_copies, 1);
}

#[test]
fn what_lua_wrote_is_checked_against_the_raw_content() {
    // A trailing space is in the extracted `Content` and in its length, though the CSV
    // comparison trims it.
    let attributes = attrs(&[("Time", "t"), ("Level", "notice"), ("Content", "ok ")]);
    let e = in_set("Apache", 1, 1, 0, None, attributes);
    let mut ok = written(&e, MAIN);
    ok.attributes.insert("Content".into(), json!("ok "));
    ok.attributes.insert("content_bytes".into(), json!(3));
    let report = judge(std::slice::from_ref(&e), &[ok.clone()], &[]);
    assert!(report.passed(), "{report}");
    assert_eq!(report.lua_mismatch, 0);

    let mut wrong = ok.clone();
    wrong.attributes.insert("content_bytes".into(), json!(2));
    let report = judge(std::slice::from_ref(&e), &[wrong], &[]);
    assert!(!report.passed());
    assert_eq!(report.lua_mismatch, 1);
    assert!(report.to_string().contains("lua_mismatch   1"), "{report}");
    assert!(report.to_string().contains("LineId 1"), "{report}");

    let mut missing = ok.clone();
    missing.attributes.remove("content_bytes");
    assert_eq!(
        judge(std::slice::from_ref(&e), &[missing], &[]).lua_mismatch,
        1
    );

    let mut as_text = ok;
    as_text
        .attributes
        .insert("content_bytes".into(), json!("3"));
    assert_eq!(judge(&[e], &[as_text], &[]).lua_mismatch, 1);
}

#[test]
fn a_record_without_content_carries_no_length() {
    let e = in_set("Apache", 1, 1, 0, None, attrs(&[("Time", "t")]));
    let mut w = written(&e, MAIN);
    assert!(!w.attributes.contains_key("content_bytes"));
    assert_eq!(
        judge(std::slice::from_ref(&e), &[w.clone()], &[]).lua_mismatch,
        0
    );
    w.attributes.insert("content_bytes".into(), json!(0));
    assert_eq!(judge(&[e], &[w], &[]).lua_mismatch, 1);
}

#[test]
fn a_group_repeated_on_every_one_of_its_subjects_is_reported() {
    // A kill between the two sinks of a Linux record: after the redelivery both subjects
    // hold two copies of it. Reported as evidence of a mid-fan-out kill, never gated.
    let e = linux(1);
    let twice = [
        written(&e, MAIN),
        written(&e, AUDIT),
        written(&e, MAIN),
        written(&e, AUDIT),
    ];
    let report = judge(std::slice::from_ref(&e), &twice, &[]);
    assert!(report.passed(), "{report}");
    assert_eq!(report.repeated_on_every_subject, 1);
    assert!(
        report.to_string().contains("repeated_on_every_subject 1"),
        "{report}"
    );

    let once_more_on_main = [written(&e, MAIN), written(&e, AUDIT), written(&e, MAIN)];
    let report = judge(std::slice::from_ref(&e), &once_more_on_main, &[]);
    assert_eq!(report.repeated_on_every_subject, 0);

    let apache_twice = apache(2, 1, None);
    let w = written(&apache_twice, MAIN);
    let report = judge(&[apache_twice], &[w.clone(), w], &[]);
    assert_eq!(
        report.repeated_on_every_subject, 0,
        "a group with one subject has no fan-out to repeat"
    );
}
