//! What the producer expects of a message: which of its set's subjects the POC config's
//! `sample` node leaves it, and what the config's `edit` and `lua` nodes write.

use std::collections::BTreeMap;

use fusion_harness::expect::{SAMPLE_PERCENT, expectation, sample_keeps};
use fusion_harness::loghub::{self, AUDIT, Line, MAIN};

fn line(line_id: usize) -> Line {
    Line {
        line_id,
        body: format!("line {line_id}"),
        attributes: BTreeMap::new(),
    }
}

/// The first (line, cycle) at or after line 1, cycle 0 on which `sample` says `keep`.
fn first_with(keep: bool) -> (usize, u64) {
    (1..=2000)
        .flat_map(|line_id| (0..4).map(move |cycle| (line_id, cycle)))
        .find(|&(line_id, cycle)| sample_keeps(line_id, cycle) == keep)
        .expect("both verdicts occur")
}

#[test]
fn sample_keeps_about_its_percent_of_the_lines_of_every_cycle() {
    let (mut kept, mut total) = (0_u32, 0_u32);
    for line_id in 1..=2000 {
        for cycle in 0..12 {
            total += 1;
            kept += u32::from(sample_keeps(line_id, cycle));
        }
    }
    let share = f64::from(kept) * 100.0 / f64::from(total);
    assert!(
        (SAMPLE_PERCENT - 2.0..=SAMPLE_PERCENT + 2.0).contains(&share),
        "{share}% kept"
    );
}

#[test]
fn a_line_sampled_out_in_one_cycle_is_kept_in_another() {
    // The cycle is part of the key, so no line is left out of the main subject for a whole
    // run, and extraction is compared for it in some cycle.
    let never_kept = (1..=2000)
        .filter(|&line_id| (0..12).all(|cycle| !sample_keeps(line_id, cycle)))
        .count();
    assert_eq!(never_kept, 0);
    let (line_id, cycle) = first_with(false);
    assert!((0..12).any(|other| other != cycle && sample_keeps(line_id, other)));
}

#[test]
fn a_sampled_out_line_keeps_only_the_subjects_before_the_sample_node() {
    let (line_id, cycle) = first_with(false);
    let linux = loghub::set("Linux").expect("a vendored set");
    let e = expectation(1, linux, &line(line_id), cycle, None);
    assert_eq!(e.subjects, [AUDIT]);
    for name in ["OpenSSH", "Apache", "Mac"] {
        let set = loghub::set(name).expect("a vendored set");
        let e = expectation(1, set, &line(line_id), cycle, None);
        assert!(e.subjects.is_empty(), "{name}: {:?}", e.subjects);
    }

    let (line_id, cycle) = first_with(true);
    let e = expectation(1, linux, &line(line_id), cycle, None);
    assert_eq!(e.subjects, [MAIN, AUDIT]);
}

#[test]
fn the_lua_node_writes_the_byte_length_of_the_content() {
    let apache = loghub::set("Apache").expect("a vendored set");
    let e = expectation(1, apache, &line(1), 0, None);
    assert_eq!(
        e.lua.lengths,
        BTreeMap::from([("content_bytes".to_owned(), "Content".to_owned())])
    );
}
