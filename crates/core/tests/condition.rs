//! Condition grammar: `field op literal` with `and`, `or`, `not` and parentheses, evaluated
//! against records in the OTLP-semantic wire shape.

use fusion_core::condition::{Condition, ConditionError};
use fusion_core::record::Record;

fn record() -> Record {
    Record::from_json(
        r#"{
            "id": 7,
            "kind": "log",
            "time_unix_nano": 1700000000000000000,
            "severity_text": "ERROR",
            "severity_number": 17,
            "body": "disk full on /var",
            "attributes": {"http.path": "/api/v1", "http.status": 503, "retry": true, "ratio": 0.25},
            "resource": {"tenant.id": "acme", "service": {"name": "api"}}
        }"#,
    )
    .expect("record parses")
}

fn eval(expr: &str) -> bool {
    Condition::parse(expr)
        .unwrap_or_else(|e| panic!("{expr}: {e}"))
        .matches(&record())
}

#[test]
fn eq_on_string_number_and_bool() {
    assert!(eval(r#"severity_text == "ERROR""#));
    assert!(!eval(r#"severity_text == "WARN""#));
    assert!(eval("severity_number == 17"));
    assert!(eval(r#"attributes["retry"] == true"#));
    assert!(eval(r#"attributes["ratio"] == 0.25"#));
}

#[test]
fn ne() {
    assert!(eval(r#"severity_text != "WARN""#));
    assert!(!eval(r#"severity_text != "ERROR""#));
    assert!(eval("severity_number != 3"));
}

#[test]
fn lt() {
    assert!(eval("severity_number < 18"));
    assert!(!eval("severity_number < 17"));
}

#[test]
fn gt() {
    assert!(eval("severity_number > 16"));
    assert!(!eval("severity_number > 17"));
}

#[test]
fn le() {
    assert!(eval("severity_number <= 17"));
    assert!(!eval("severity_number <= 16"));
}

#[test]
fn ge() {
    assert!(eval("severity_number >= 17"));
    assert!(!eval("severity_number >= 18"));
}

#[test]
fn ordering_works_on_strings_lexicographically() {
    assert!(eval(r#"severity_text < "WARN""#));
    assert!(eval(r#"severity_text >= "ERROR""#));
}

#[test]
fn ordering_between_mismatched_types_is_false() {
    assert!(!eval(r#"severity_number < "18""#));
    assert!(!eval(r#"severity_text > 1"#));
}

#[test]
fn eq_between_mismatched_types_is_false_and_ne_is_true() {
    assert!(!eval(r#"severity_number == "17""#));
    assert!(eval(r#"severity_number != "17""#));
}

#[test]
fn missing_field_equals_null_and_nothing_else() {
    assert!(eval("trace_id == null"));
    assert!(eval(r#"attributes["nope"] == null"#));
    assert!(!eval(r#"attributes["nope"] == "x""#));
    assert!(eval(r#"attributes["nope"] != "x""#));
    assert!(!eval(r#"attributes["nope"] < 1"#));
}

#[test]
fn and_or_not_with_precedence() {
    assert!(eval(
        r#"severity_number >= 17 and severity_text == "ERROR""#
    ));
    assert!(!eval(
        r#"severity_number >= 17 and severity_text == "WARN""#
    ));
    assert!(eval(r#"severity_number < 0 or severity_text == "ERROR""#));
    assert!(eval(r#"not severity_number < 0"#));
    // `and` binds tighter than `or`.
    assert!(eval(
        r#"severity_number < 0 and severity_number > 0 or severity_text == "ERROR""#
    ));
    // `not` binds tighter than `and`.
    assert!(eval(
        r#"not severity_number < 0 and severity_text == "ERROR""#
    ));
}

#[test]
fn parentheses_group() {
    assert!(!eval(
        r#"severity_number < 0 and (severity_number > 0 or severity_text == "ERROR")"#
    ));
    assert!(eval(
        r#"(severity_number < 0 or severity_number > 0) and severity_text == "ERROR""#
    ));
    assert!(eval(
        r#"not (severity_number < 0 or severity_text == "WARN")"#
    ));
}

#[test]
fn dotted_and_bracketed_paths_reach_into_maps() {
    assert!(eval(r#"attributes["http.path"] == "/api/v1""#));
    assert!(eval(r#"attributes["http.status"] >= 500"#));
    assert!(eval(r#"resource["tenant.id"] == "acme""#));
    assert!(eval(r#"resource.service.name == "api""#));
    assert!(eval(r#"resource["service"]["name"] == "api""#));
    assert!(eval(r#"body == "disk full on /var""#));
    assert!(eval(r#"kind == "log""#));
    assert!(eval("id == 7"));
    assert!(eval("time_unix_nano > 1600000000000000000"));
}

#[test]
fn single_quoted_strings_and_escapes() {
    assert!(eval(r#"severity_text == 'ERROR'"#));
    assert!(eval(r#"body == "disk full on \/var""#));
    assert!(eval(r#"attributes["http.path"] == "\/api\/v1""#));
}

#[test]
fn regex_operators_parse_but_are_not_wired_yet() {
    let c = Condition::parse(r#"body =~ "disk""#).expect("parses");
    assert!(c.has_regex_ops());
    let c = Condition::parse(r#"body !~ "disk""#).expect("parses");
    assert!(c.has_regex_ops());
    assert!(
        !Condition::parse("severity_number > 1")
            .expect("parses")
            .has_regex_ops()
    );
}

#[test]
fn parse_errors_name_the_problem() {
    let err = Condition::parse("severity_number >").expect_err("incomplete");
    assert!(matches!(err, ConditionError::UnexpectedEnd), "{err}");

    let err = Condition::parse(r#"severity_number = 1"#).expect_err("bad op");
    assert!(
        matches!(err, ConditionError::UnexpectedChar { .. }),
        "{err}"
    );

    let err = Condition::parse(r#"nonsense == 1"#).expect_err("unknown root");
    assert!(
        matches!(err, ConditionError::UnknownField { ref name } if name == "nonsense"),
        "{err}"
    );

    let err = Condition::parse(r#"severity_number.x == 1"#).expect_err("scalar has no children");
    assert!(matches!(err, ConditionError::NotAMap { .. }), "{err}");

    let err = Condition::parse(r#"(severity_number == 1"#).expect_err("unbalanced");
    assert!(matches!(err, ConditionError::UnexpectedEnd), "{err}");

    let err = Condition::parse(r#"severity_number == 1 2"#).expect_err("trailing");
    assert!(
        matches!(err, ConditionError::UnexpectedToken { .. }),
        "{err}"
    );
}
