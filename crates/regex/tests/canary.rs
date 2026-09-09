//! Load-time canary through the public API.
#![allow(clippy::unwrap_used)]

use fusion_regex::canary::{CanaryConfig, InputShape, run};
use fusion_regex::{CompileError, Engine, Limits, Options, RedosPolicy, Regex};

const LINUX_SYSLOG: &str = r"^(?<Month>[A-Z][a-z]{2}) +(?<Date>\d{1,2}) (?<Time>\d{2}:\d{2}:\d{2}) (?<Level>\S+) (?<Component>[^\[:]+)(?:\[(?<PID>\d+)\])?: (?<Content>.*)$";

fn tight() -> CanaryConfig {
    CanaryConfig {
        match_limit: 10_000,
        ..CanaryConfig::default()
    }
}

#[test]
fn canary_rejects_nested_quantifier_under_a_tight_match_limit() {
    let trip = run(r"(a+)+$", &tight(), &Limits::default())
        .unwrap()
        .expect("should trip");
    assert_eq!(trip.match_limit, 10_000);
    assert!(trip.input_len >= 1024, "{trip:?}");
}

#[test]
fn canary_accepts_bounded_digits() {
    assert_eq!(
        run(r"^\d{1,3}$", &tight(), &Limits::default()).unwrap(),
        None
    );
}

#[test]
fn canary_accepts_the_loghub_linux_pattern_and_common_shapes() {
    for pattern in [
        LINUX_SYSLOG,
        r"\w+@\w+\.\w+",
        r"[a-z]+[0-9]+",
        r".*error.*",
        r"^(\d+)\.(\d+)$",
    ] {
        assert_eq!(
            run(pattern, &CanaryConfig::default(), &Limits::default()).unwrap(),
            None,
            "{pattern}"
        );
    }
}

#[test]
fn canary_covers_backtracking_only_syntax_and_a_literal_prefix() {
    // Lookahead keeps this off the linear engine; the `x` prefix means the adversarial
    // input must start with the pattern's literal alphabet before the repeated run.
    let trip = run(r"x(?=a)(a+)+$", &tight(), &Limits::default()).unwrap();
    assert!(trip.is_some());
}

#[test]
fn canary_uses_the_literal_sequence_as_a_prefix() {
    // The required literal has a repeated character, so a de-duplicated alphabet would
    // never satisfy it and the nested loop would never be reached.
    let trip = run(r"ERROR: (a+)+$", &tight(), &Limits::default()).unwrap();
    assert!(trip.is_some());
}

#[test]
fn canary_default_limit_passes_group_loops_at_64_kib() {
    for pattern in [
        r"^(?:ab)*$",
        r"^(?:a|b)*$",
        r"^(?:\w\s)*$",
        r"^([a-z]{2,5}-)*$",
    ] {
        assert_eq!(
            run(pattern, &CanaryConfig::default(), &Limits::default()).unwrap(),
            None,
            "{pattern}"
        );
    }
}

#[test]
fn canary_reports_syntax_errors_as_compile_errors() {
    let err = run(r"a(", &tight(), &Limits::default()).unwrap_err();
    assert!(matches!(err, CompileError::Syntax { .. }));
}

#[test]
fn checked_options_reject_and_warn_policy_keeps_the_trip() {
    // The lookahead keeps the pattern on the backtracking engine, where the canary runs.
    let reject = Options {
        canary: Some(tight()),
        lint: false,
        ..Options::default()
    };
    let err = Regex::with_options(r"(?=a)(a+)+$", &reject).unwrap_err();
    assert!(matches!(err, CompileError::CanaryTripped(_)), "{err:?}");

    let warn = Options {
        on_redos_risk: RedosPolicy::Warn,
        ..reject
    };
    let re = Regex::with_options(r"(?=a)(a+)+$", &warn).unwrap();
    assert!(re.canary_warning().is_some());
    assert!(
        Regex::with_options(r"^(?=\d)\d{1,3}$", &warn)
            .unwrap()
            .canary_warning()
            .is_none()
    );
}

#[test]
fn canary_is_skipped_for_patterns_on_the_linear_engine() {
    // `(a+)+$` trips the canary on PCRE2, but it compiles on the linear engine, which cannot
    // backtrack, so the verdict would describe an engine the pattern never runs on.
    let options = Options {
        canary: Some(tight()),
        lint: false,
        ..Options::default()
    };
    let re = Regex::with_options(r"(a+)+$", &options).unwrap();
    assert_eq!(re.engine(), Engine::Linear);
    assert!(re.canary_warning().is_none());
}

#[test]
fn canary_rejects_nested_quantifier_under_the_default_limit_too() {
    let trip = run(r"(a+)+$", &CanaryConfig::default(), &Limits::default())
        .unwrap()
        .expect("should trip");
    assert_eq!(trip.input_len, 1024);
    assert!(
        matches!(trip.input_shape, InputShape::RepeatedThenPoison('a')),
        "{trip:?}"
    );
}

#[test]
fn canary_skips_sizes_above_the_input_limit() {
    let limits = Limits {
        input_bytes: 512,
        ..Limits::default()
    };
    assert_eq!(run(r"(a+)+$", &tight(), &limits).unwrap(), None);
}

#[test]
fn default_options_run_every_layer_and_reject() {
    let err = Regex::with_options(r"(a|aa)*b", &Options::default()).unwrap_err();
    assert!(matches!(err, CompileError::RedosRisk(_)), "{err:?}");
    let err = Regex::with_options(
        r"(?=a)(a+)+$",
        &Options {
            lint: false,
            ..Options::default()
        },
    )
    .unwrap_err();
    assert!(matches!(err, CompileError::CanaryTripped(_)), "{err:?}");
}

#[test]
fn checked_options_run_all_three_layers() {
    let err = Regex::with_options(r"(a|aa)*b", &Options::checked()).unwrap_err();
    assert!(matches!(err, CompileError::RedosRisk(_)), "{err:?}");
    assert!(Regex::with_options(LINUX_SYSLOG, &Options::checked()).is_ok());
}
