//! Each runtime limit trips on a known pattern and returns its own error variant.
#![allow(clippy::unwrap_used)]

use fusion_regex::{Engine, EngineChoice, Limits, MatchError, Options, Regex};

/// Exponential on PCRE2; the lookahead keeps it off the linear engine.
const NESTED: &str = r"^(?=a)(a+)+$";

fn backtracking(pattern: &str, limits: Limits) -> Regex {
    let re = Regex::with_options(
        pattern,
        &Options {
            limits,
            engine: EngineChoice::Auto,
            ..Options::unchecked()
        },
    )
    .unwrap();
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
