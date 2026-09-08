use pipeline_core::condition::{Condition, EvalError, ParseError};
use pipeline_core::Record;
use serde_json::json;

fn record() -> Record {
    let mut r = Record::log(7);
    r.severity_text = Some("ERROR".into());
    r.severity_number = Some(17);
    r.body = json!("disk full");
    r.attributes.insert("http.path".into(), json!("/health"));
    r.attributes.insert("http.status".into(), json!(503));
    r.attributes.insert("latency_ms".into(), json!(12.5));
    r.attributes.insert("retry".into(), json!(true));
    r.resource.insert("tenant.id".into(), json!("acme"));
    r
}

fn eval(expr: &str) -> bool {
    Condition::parse(expr)
        .unwrap_or_else(|e| panic!("parse {expr:?}: {e}"))
        .eval(&record())
        .unwrap_or_else(|e| panic!("eval {expr:?}: {e}"))
}

#[test]
fn equals_on_string_number_and_bool() {
    assert!(eval(r#"severity_text == "ERROR""#));
    assert!(!eval(r#"severity_text == "WARN""#));
    assert!(eval("severity_number == 17"));
    assert!(eval(r#"attributes["http.status"] == 503"#));
    assert!(eval("attributes.retry == true"));
    assert!(eval("attributes.latency_ms == 12.5"));
}

#[test]
fn not_equals() {
    assert!(eval(r#"severity_text != "WARN""#));
    assert!(!eval(r#"severity_text != "ERROR""#));
    assert!(eval("severity_number != 3"));
}

#[test]
fn less_than_and_less_or_equal() {
    assert!(eval("severity_number < 18"));
    assert!(!eval("severity_number < 17"));
    assert!(eval("severity_number <= 17"));
    assert!(!eval("severity_number <= 16"));
    assert!(eval("attributes.latency_ms < 13"));
}

#[test]
fn greater_than_and_greater_or_equal() {
    assert!(eval("severity_number > 16"));
    assert!(!eval("severity_number > 17"));
    assert!(eval("severity_number >= 17"));
    assert!(!eval("severity_number >= 18"));
    assert!(eval(r#"attributes["http.status"] >= 500"#));
}

#[test]
fn string_ordering_is_lexicographic() {
    assert!(eval(r#"severity_text < "FATAL""#));
    assert!(eval(r#"severity_text > "DEBUG""#));
}

#[test]
fn and_or_not_and_parentheses() {
    assert!(eval(r#"severity_text == "ERROR" and severity_number > 10"#));
    assert!(!eval(
        r#"severity_text == "ERROR" and severity_number > 100"#
    ));
    assert!(eval(r#"severity_text == "WARN" or severity_number > 10"#));
    assert!(!eval(r#"severity_text == "WARN" or severity_number > 100"#));
    assert!(eval(r#"not severity_text == "WARN""#));
    assert!(!eval(r#"not severity_text == "ERROR""#));
    // `and` binds tighter than `or`.
    assert!(eval(
        r#"severity_text == "WARN" or severity_number > 10 and attributes.retry == true"#
    ));
    // Parentheses override that.
    assert!(!eval(
        r#"(severity_text == "WARN" or severity_number > 10) and attributes.retry == false"#
    ));
    assert!(eval(
        r#"not (severity_text == "WARN" or severity_number > 100)"#
    ));
}

#[test]
fn paths_reach_nested_and_bracketed_fields() {
    assert!(eval(r#"resource["tenant.id"] == "acme""#));
    assert!(eval(r#"resource['tenant.id'] == "acme""#));
    assert!(eval(r#"body == "disk full""#));
    assert!(eval("id == 7"));
}

#[test]
fn missing_field_is_never_equal_and_always_not_equal() {
    assert!(!eval(r#"attributes.absent == "x""#));
    assert!(eval(r#"attributes.absent != "x""#));
    assert!(!eval("attributes.absent < 1"));
    assert!(!eval("attributes.absent > 1"));
}

#[test]
fn mismatched_types_do_not_compare() {
    assert!(!eval(r#"severity_number == "17""#));
    assert!(eval(r#"severity_number != "17""#));
    assert!(!eval(r#"severity_text < 5"#));
}

#[test]
fn regex_operators_parse_but_are_not_wired_yet() {
    let cond = Condition::parse(r#"body =~ "disk.*""#).expect("=~ parses");
    assert!(matches!(
        cond.eval(&record()),
        Err(EvalError::RegexNotWired)
    ));
    let cond = Condition::parse(r#"body !~ "disk.*""#).expect("!~ parses");
    assert!(matches!(
        cond.eval(&record()),
        Err(EvalError::RegexNotWired)
    ));
}

#[test]
fn parse_errors_name_the_position() {
    assert!(matches!(
        Condition::parse("severity_text =="),
        Err(ParseError { .. })
    ));
    assert!(matches!(
        Condition::parse(r#"severity_text ~ "x""#),
        Err(ParseError { .. })
    ));
    assert!(matches!(
        Condition::parse(r#"(severity_text == "x""#),
        Err(ParseError { .. })
    ));
    assert!(matches!(
        Condition::parse(r#"severity_text == "x" extra"#),
        Err(ParseError { .. })
    ));
    let err = Condition::parse(r#"severity_text == "x" extra"#).unwrap_err();
    assert_eq!(err.offset, 21);
}

#[test]
fn number_literal_does_not_swallow_a_following_operator_or_path() {
    // `5-3` is not one number: the lexer stops at the sign and the parser
    // then rejects the trailing `-3`.
    let err = Condition::parse("severity_number == 5-3").unwrap_err();
    assert_eq!(err.offset, 20);
    assert!(Condition::parse("attributes.latency_ms == 1.25e1")
        .unwrap()
        .eval(&record())
        .unwrap());
    assert!(Condition::parse("attributes.latency_ms > -1")
        .unwrap()
        .eval(&record())
        .unwrap());
}
