//! Each runtime limit trips on a known pattern and returns its own error variant.
#![allow(clippy::unwrap_used)]

mod common;

use common::{NESTED_BACKTRACKING as NESTED, compile};
use fusion_regex::{Engine, EngineChoice, Limits, MatchError, Options, Regex};

fn backtracking(pattern: &str, limits: Limits) -> Regex {
    let re = compile(pattern, EngineChoice::Auto, limits);
    assert_eq!(re.engine(), Engine::Backtracking);
    re
}

fn adversarial() -> String {
    let mut s = "a".repeat(30);
    s.push('!');
    s
}

#[test]
fn match_limit_trips_with_its_own_variant() {
    let re = backtracking(
        NESTED,
        Limits {
            match_limit: 1000,
            ..Limits::default()
        },
    );
    assert_eq!(
        re.captures(&adversarial()).unwrap_err(),
        MatchError::MatchLimit
    );
    assert_eq!(
        re.is_match(&adversarial()).unwrap_err(),
        MatchError::MatchLimit
    );
}

#[test]
fn depth_limit_trips_with_its_own_variant() {
    let re = backtracking(
        NESTED,
        Limits {
            depth_limit: 5,
            ..Limits::default()
        },
    );
    assert_eq!(
        re.captures(&adversarial()).unwrap_err(),
        MatchError::DepthLimit
    );
}

#[test]
fn heap_limit_trips_with_its_own_variant() {
    // Each iteration of a repeated group pushes a backtracking frame; ten thousand of them
    // need more than 64 KiB of frame heap.
    let re = backtracking(
        r"^(?=a)(?:a|b)*$",
        Limits {
            heap_limit_kib: 64,
            ..Limits::default()
        },
    );
    let mut hay = "ab".repeat(5000);
    hay.push('!');
    assert_eq!(re.captures(&hay).unwrap_err(), MatchError::HeapLimit);
}

#[test]
fn work_limit_trips_with_its_own_variant_on_a_start_loop_quadratic() {
    // `match_limit` resets at every start position, so only the work limit sees the
    // O(n²) of a group loop that restarts everywhere on a record that never matches.
    let re = backtracking(
        r"(?:a|b)*(?=c)",
        Limits {
            work_limit: Some(100_000),
            ..Limits::default()
        },
    );
    assert_eq!(
        re.is_match(&"ab".repeat(2048)).unwrap_err(),
        MatchError::WorkLimit
    );
    assert!(!re.is_match(&"ab".repeat(32)).unwrap());
    assert!(re.is_match("abc").unwrap());
}

#[test]
fn work_limit_off_lets_the_same_match_run_to_completion() {
    let re = backtracking(
        r"(?:a|b)*(?=c)",
        Limits {
            work_limit: None,
            ..Limits::default()
        },
    );
    assert!(!re.is_match(&"ab".repeat(256)).unwrap());
}

#[test]
fn limits_are_generous_enough_for_an_ordinary_line() {
    let re = backtracking(r"^(?=a)(?:a|b)*$", Limits::default());
    assert!(re.is_match(&"ab".repeat(5000)).unwrap());
}

#[test]
fn input_size_limit_applies_on_both_engines() {
    for engine in [EngineChoice::Linear, EngineChoice::Backtracking] {
        let re = Regex::with_options(
            r"\w+",
            &Options {
                limits: Limits {
                    input_bytes: 8,
                    ..Limits::default()
                },
                engine,
                ..Options::unchecked()
            },
        )
        .unwrap();
        assert!(re.is_match("12345678").unwrap(), "{engine:?}");
        assert_eq!(
            re.is_match("123456789").unwrap_err(),
            MatchError::InputTooLarge { len: 9, limit: 8 },
            "{engine:?}"
        );
    }
}
