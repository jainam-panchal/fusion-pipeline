//! Structural ReDoS lint verdicts through the public API.
#![allow(clippy::unwrap_used)]

use fusion_regex::lint::{RedosRisk, lint};
use fusion_regex::{CompileError, Options, RedosPolicy, Regex};

const LINUX_SYSLOG: &str = r"^(?<Month>[A-Z][a-z]{2}) +(?<Date>\d{1,2}) (?<Time>\d{2}:\d{2}:\d{2}) (?<Level>\S+) (?<Component>[^\[:]+)(?:\[(?<PID>\d+)\])?: (?<Content>.*)$";

fn kinds(pattern: &str) -> Vec<&'static str> {
    let report = lint(pattern);
    assert!(report.parsed, "{pattern} should parse");
    report
        .risks
        .iter()
        .map(|r| match r {
            RedosRisk::NestedQuantifiers { .. } => "nested",
            RedosRisk::OverlappingAlternation { .. } => "alternation",
            RedosRisk::OverlappingSuffix { .. } => "suffix",
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
    assert!(kinds(r"(a{2})+").is_empty(), "fixed-width inner repetition is unambiguous");
    assert!(kinds(r"\w+@\w+\.\w+").is_empty());
}

#[test]
fn findings_carry_the_offset_of_the_outer_repetition() {
    let report = lint(r"xy(a+)+$");
    assert_eq!(report.risks, vec![RedosRisk::NestedQuantifiers { offset: 2 }]);
}

#[test]
fn pcre2_only_syntax_is_desugared_before_linting() {
    // Lookahead, atomic group and a backreference are stripped; the nested loop remains.
    let report = lint(r"(?=a)(?>x)(b)\1(a+)+$");
    assert!(report.parsed);
    assert_eq!(report.risks.len(), 1);
    assert!(matches!(report.risks[0], RedosRisk::NestedQuantifiers { .. }));
    // A possessive quantifier is not a nested repetition.
    let report = lint(r"(a++)$");
    assert!(report.parsed);
    assert!(report.risks.is_empty());
}

#[test]
fn unparseable_pattern_reports_not_parsed_rather_than_clean() {
    let report = lint(r"(?R)(?(1)a|b)");
    assert!(!report.parsed);
    assert!(report.risks.is_empty());
}

#[test]
fn reject_policy_fails_compilation_and_warn_policy_keeps_findings() {
    let options = Options { lint: true, canary: None, ..Options::default() };
    let err = Regex::with_options(r"(a+)+$", &options).unwrap_err();
    assert!(matches!(err, CompileError::RedosRisk(ref risks) if risks.len() == 1), "{err:?}");

    let warn = Options { on_redos_risk: RedosPolicy::Warn, ..options };
    let re = Regex::with_options(r"(a+)+$", &warn).unwrap();
    assert_eq!(re.redos_warnings().len(), 1);
    assert!(Regex::with_options(LINUX_SYSLOG, &warn).unwrap().redos_warnings().is_empty());
}
