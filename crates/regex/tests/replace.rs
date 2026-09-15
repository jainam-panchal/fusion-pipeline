//! Every match, not only the leftmost: `captures_iter` and `replace_all` agree on both
//! engines, including the empty-match rule, and the replacement is literal text.
#![allow(clippy::unwrap_used)]

mod common;

use std::num::NonZeroU32;

use common::{BOTH_ENGINES, compile, compile_on};
use fusion_regex::{EngineChoice, Limits, MatchError, Regex};

fn spans(re: &Regex, hay: &str) -> Vec<(usize, usize)> {
    re.captures_iter(hay)
        .map(|caps| {
            let span = caps.unwrap().span(0).unwrap();
            (span.start, span.end)
        })
        .collect()
}

#[test]
fn every_non_overlapping_match_is_replaced_on_both_engines() {
    for engine in BOTH_ENGINES {
        let re = compile_on(engine, r"\d+");
        assert_eq!(
            re.replace_all("id=42 user=7 host=x", "N").unwrap(),
            Some("id=N user=N host=x".to_owned()),
            "{engine:?}"
        );
        assert_eq!(
            spans(&re, "id=42 user=7"),
            vec![(3, 5), (11, 12)],
            "{engine:?}"
        );
    }
}

#[test]
fn no_match_is_none_on_both_engines() {
    for engine in BOTH_ENGINES {
        let re = compile_on(engine, r"\d+");
        assert_eq!(
            re.replace_all("no digits", "N").unwrap(),
            None,
            "{engine:?}"
        );
        assert!(spans(&re, "no digits").is_empty(), "{engine:?}");
    }
}

#[test]
fn replacement_is_literal_text_with_no_group_expansion() {
    for engine in BOTH_ENGINES {
        let re = compile_on(engine, r"(?<n>\d+)");
        assert_eq!(
            re.replace_all("a1b", "[$n$1$0\\1]").unwrap(),
            Some("a[$n$1$0\\1]b".to_owned()),
            "{engine:?}"
        );
    }
}

#[test]
fn empty_matches_follow_the_same_rule_on_both_engines() {
    // The `regex` crate's rule, which the facade pins for both engines: an empty match
    // that ends where the previous match ended is skipped, so `x*` on `axxb` has no empty
    // match at 3, and the search advances one character after it.
    type Case = (&'static str, &'static str, &'static [(usize, usize)]);
    let cases: [Case; 3] = [
        ("x*", "axxb", &[(0, 0), (1, 3), (4, 4)]),
        (r"\b", "ab cd", &[(0, 0), (2, 2), (3, 3), (5, 5)]),
        ("(?:)", "ab", &[(0, 0), (1, 1), (2, 2)]),
    ];
    for (pattern, hay, expected) in cases {
        for engine in BOTH_ENGINES {
            let re = compile_on(engine, pattern);
            assert_eq!(
                spans(&re, hay),
                expected,
                "{engine:?} `{pattern}` on `{hay}`"
            );
        }
    }
    for engine in BOTH_ENGINES {
        let re = compile_on(engine, "x*");
        assert_eq!(
            re.replace_all("axxb", "-").unwrap(),
            Some("-a-b-".to_owned()),
            "{engine:?}"
        );
    }
}

#[test]
fn empty_matches_advance_by_one_character_not_one_byte() {
    for engine in BOTH_ENGINES {
        let re = compile_on(engine, "(?:)");
        assert_eq!(
            re.replace_all("aé", "-").unwrap(),
            Some("-a-é-".to_owned()),
            "{engine:?}"
        );
    }
}

#[test]
fn a_backtracking_only_pattern_replaces_every_match() {
    // Lookbehind keeps it on PCRE2 under `Auto`.
    let re = Regex::new(r"(?<=:)\d+").unwrap();
    assert_eq!(re.engine(), fusion_regex::Engine::Backtracking);
    assert_eq!(
        re.replace_all("a:1 b:22 c3", "#").unwrap(),
        Some("a:# b:# c3".to_owned())
    );
}

#[test]
fn input_too_large_is_checked_once_for_the_whole_call() {
    for engine in BOTH_ENGINES {
        let re = compile(
            r"\d",
            engine,
            Limits {
                input_bytes: 4,
                ..Limits::default()
            },
        );
        let err = re.replace_all("12345", "x").unwrap_err();
        assert!(
            matches!(err, MatchError::InputTooLarge { len: 5, limit: 4 }),
            "{engine:?}: {err}"
        );
        assert_eq!(
            re.replace_all("1234", "x").unwrap(),
            Some("xxxx".to_owned())
        );
    }
}

#[test]
fn the_work_budget_is_shared_across_the_matches_of_one_call() {
    // Each match costs a handful of pattern items; a budget that comfortably covers one
    // match must still trip once the loop has spent it across many.
    let re = compile(
        r"(?<=:)\d",
        EngineChoice::Backtracking,
        Limits {
            work_limit: NonZeroU32::new(40),
            ..Limits::default()
        },
    );
    assert_eq!(re.replace_all(":1", "x").unwrap(), Some(":x".to_owned()));
    let many = ":1".repeat(200);
    let err = re.replace_all(&many, "x").unwrap_err();
    assert!(matches!(err, MatchError::WorkLimit), "{err}");
}
