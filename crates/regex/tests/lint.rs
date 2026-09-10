//! Structural ReDoS lint verdicts through the public API.
#![allow(clippy::unwrap_used)]

mod common;

use common::{LINUX_SYSLOG, lint_only};
use fusion_regex::lint::{RedosRisk, lint};
use fusion_regex::{CompileError, Options, RedosPolicy, Regex};

fn kinds(pattern: &str) -> Vec<&'static str> {
    lint(pattern)
        .unwrap_or_else(|| panic!("{pattern} should parse"))
        .iter()
        .map(|r| match r {
            RedosRisk::NestedQuantifiers { .. } => "nested",
            RedosRisk::OverlappingAlternation { .. } => "alternation",
            RedosRisk::OverlappingSuffix { .. } => "suffix",
            RedosRisk::NotParsed => "not-parsed",
            other => panic!("unexpected risk {other:?}"),
        })
        .collect()
}

#[test]
fn nested_unbounded_quantifiers_are_flagged() {
    assert_eq!(kinds(r"(a+)+$"), vec!["nested"]);
    assert_eq!(kinds(r"^(\w+\s?)*$"), vec!["nested"]);
    assert_eq!(kinds(r"(?:[a-z]{2,5})+"), vec!["nested"]);
}

#[test]
fn overlapping_alternation_under_repetition_is_flagged() {
    assert_eq!(kinds(r"(a|aa)*b"), vec!["alternation"]);
    assert_eq!(kinds(r"(\d|\w)+"), vec!["alternation"]);
}

#[test]
fn disjoint_alternation_under_repetition_passes() {
    assert!(kinds(r"(foo|bar)*").is_empty());
    assert!(kinds(r"(?:GET|POST|PUT)+").is_empty());
}

#[test]
fn unbounded_quantifier_followed_by_overlapping_quantifier_is_flagged() {
    assert_eq!(kinds(r"\w*\d+"), vec!["suffix"]);
    assert_eq!(kinds(r":\s*(?<Content>.*)$"), vec!["suffix"]);
}

#[test]
fn benign_patterns_pass() {
    assert!(kinds(r"^\d{1,3}$").is_empty());
    assert!(kinds(LINUX_SYSLOG).is_empty());
    assert!(kinds(r"(?<ip>\d{1,3}(?:\.\d{1,3}){3})").is_empty());
    assert!(
        kinds(r"(a{2})+").is_empty(),
        "fixed-width inner repetition is unambiguous"
    );
    assert!(kinds(r"\w+@\w+\.\w+").is_empty());
}

#[test]
fn findings_carry_the_offset_of_the_outer_repetition() {
    assert_eq!(
        lint(r"xy(a+)+$"),
        Some(vec![RedosRisk::NestedQuantifiers { offset: 2 }])
    );
}

#[test]
fn pcre2_only_syntax_is_desugared_before_linting() {
    // Lookahead, atomic group and a backreference are stripped; the nested loop remains.
    let risks = lint(r"(?=a)(?>x)(b)\1(a+)+$").unwrap();
    assert_eq!(risks.len(), 1);
    assert!(matches!(risks[0], RedosRisk::NestedQuantifiers { .. }));
    // A possessive quantifier is not a nested repetition.
    assert_eq!(lint(r"(a++)$"), Some(Vec::new()));
}

#[test]
fn unparseable_pattern_reports_not_parsed_rather_than_clean() {
    assert_eq!(lint(r"(?R)(?(1)a|b)"), None);
}

#[test]
fn reject_policy_fails_compilation_and_warn_policy_keeps_findings() {
    let options = lint_only();
    let err = Regex::with_options(r"(a+)+$", &options).unwrap_err();
    assert!(
        matches!(err, CompileError::RedosRisk(ref risks) if risks.len() == 1),
        "{err:?}"
    );

    let warn = Options {
        on_redos_risk: RedosPolicy::Warn,
        ..options
    };
    let re = Regex::with_options(r"(a+)+$", &warn).unwrap();
    assert_eq!(re.redos_warnings().len(), 1);
    assert!(
        Regex::with_options(LINUX_SYSLOG, &warn)
            .unwrap()
            .redos_warnings()
            .is_empty()
    );
}

#[test]
fn unparseable_pattern_is_a_finding_when_no_canary_can_check_it() {
    // A PCRE2 conditional never parses on regex-syntax (`(?R)` would: it is the CRLF flag
    // there). With the canary off nothing else looks at it, so silence must not read as
    // clean; with the canary on, the canary is the check.
    let no_canary = lint_only();
    let err = Regex::with_options(r"(a)?(?(1)b|c)", &no_canary).unwrap_err();
    assert!(
        matches!(err, CompileError::RedosRisk(ref r) if r == &[RedosRisk::NotParsed]),
        "{err:?}"
    );

    let warn = Options {
        on_redos_risk: RedosPolicy::Warn,
        ..no_canary
    };
    let re = Regex::with_options(r"(a)?(?(1)b|c)", &warn).unwrap();
    assert_eq!(re.redos_warnings(), &[RedosRisk::NotParsed]);

    assert!(Regex::with_options(r"(a)?(?(1)b|c)", &Options::checked()).is_ok());
}
