//! Condition grammar: `field op literal` with `and`, `or`, `not` and parentheses, evaluated
//! against records in the OTLP-semantic wire shape.

use fusion_core::condition::{Condition, ConditionError};
use fusion_core::path::PathError;
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
            "attributes": {"http.path": "/api/v1", "http.status": 503, "retry": true, "ratio": 0.25,
                           "x-request-id": "abc", "5xx.count": 2, "something something": 1,
                           "Event ID.code": 4625},
            "resource": {"tenant.id": "acme", "service.name": "api", "k8s.pod-name": "web-0"}
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
    assert!(eval(r#"attributes.retry == true"#));
    assert!(eval(r#"attributes.ratio == 0.25"#));
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
    assert!(eval(r#"attributes.nope == null"#));
    assert!(!eval(r#"attributes.nope == "x""#));
    assert!(eval(r#"attributes.nope != "x""#));
    assert!(!eval(r#"attributes.nope < 1"#));
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
fn dotted_paths_name_flat_map_keys() {
    assert!(eval(r#"attributes.http.path == "/api/v1""#));
    assert!(eval("attributes.http.status >= 500"));
    assert!(eval(r#"resource.tenant.id == "acme""#));
    assert!(eval(r#"resource.service.name == "api""#));
    assert!(eval(r#"body == "disk full on /var""#));
    assert!(eval(r#"kind == "log""#));
    assert!(eval("id == 7"));
    assert!(eval("time_unix_nano > 1600000000000000000"));
}

#[test]
fn segments_with_digits_hyphens_and_quotes() {
    assert!(eval(r#"attributes.x-request-id == "abc""#));
    assert!(eval("attributes.5xx.count > 1"));
    assert!(eval(r#"resource.k8s.pod-name == "web-0""#));
    assert!(eval(r#"attributes."something something" == 1"#));
    assert!(eval(r#"attributes."Event ID".code == 4625"#));
    assert!(eval(
        r#"attributes.5xx.count > 1 and resource.k8s.pod-name != "web-1""#
    ));
}

#[test]
fn bracket_syntax_is_a_parse_error() {
    let err = Condition::parse(r#"attributes["http.path"] == "x""#).expect_err("brackets are gone");
    assert!(
        matches!(err, ConditionError::Field { offset: 0, ref source } if matches!(source, PathError::BracketSyntax { .. })),
        "{err}"
    );
    assert!(
        err.to_string()
            .contains("instead use `attributes.http.path`"),
        "{err}"
    );
}

#[test]
fn body_and_scalars_take_no_segments_and_maps_need_a_key() {
    let err = Condition::parse("body.x == 1").expect_err("body is whole");
    assert!(
        matches!(err, ConditionError::Field { ref source, .. } if matches!(source, PathError::NotAMap { .. })),
        "{err}"
    );
    let err = Condition::parse("severity_number.x == 1").expect_err("scalar");
    assert!(
        matches!(err, ConditionError::Field { ref source, .. } if matches!(source, PathError::NotAMap { .. })),
        "{err}"
    );
    let err = Condition::parse("attributes == null").expect_err("map needs key");
    assert!(
        matches!(err, ConditionError::Field { ref source, .. } if matches!(source, PathError::MapNeedsKey { .. })),
        "{err}"
    );
    let err =
        Condition::parse("severity_number > 1 and attributes.a:b == 1").expect_err("bad char");
    assert!(
        matches!(err, ConditionError::Field { offset: 24, ref source } if matches!(source, PathError::InvalidSegment { .. })),
        "{err}"
    );
}

#[test]
fn single_quoted_strings_and_escapes() {
    assert!(eval(r#"severity_text == 'ERROR'"#));
    assert!(eval(r#"body == "disk full on \/var""#));
    assert!(eval(r#"attributes.http.path == "\/api\/v1""#));
}

#[test]
fn regex_operators_parse_and_list_their_patterns() {
    let c = Condition::parse(r#"body =~ "disk" and (body !~ "ok" or severity_number > 1)"#)
        .expect("parses");
    assert_eq!(c.regex_patterns(), vec!["disk", "ok"]);
    let c = Condition::parse("severity_number > 1").expect("parses");
    assert!(c.regex_patterns().is_empty());
}

#[test]
fn regex_operators_need_a_string_literal() {
    let err = Condition::parse("body =~ 42").expect_err("rejected");
    assert!(
        matches!(err, ConditionError::RegexNeedsString { offset: 8 }),
        "{err:?}"
    );
    let err = Condition::parse("body !~ null").expect_err("rejected");
    assert!(
        matches!(err, ConditionError::RegexNeedsString { .. }),
        "{err:?}"
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
        matches!(err, ConditionError::Field { offset: 0, ref source } if matches!(source, PathError::UnknownField { name } if name == "nonsense")),
        "{err}"
    );

    let err = Condition::parse(r#"(severity_number == 1"#).expect_err("unbalanced");
    assert!(matches!(err, ConditionError::UnexpectedEnd), "{err}");

    let err = Condition::parse(r#"severity_number == 1 2"#).expect_err("trailing");
    assert!(
        matches!(err, ConditionError::UnexpectedToken { .. }),
        "{err}"
    );
}

#[test]
fn unclosed_quote_in_a_path_is_the_path_error_with_its_offset() {
    let err =
        Condition::parse(r#"severity_number > 1 and attributes."open == 1"#).expect_err("unclosed");
    assert!(
        matches!(err, ConditionError::Field { offset: 24, ref source } if matches!(source, PathError::UnterminatedQuote { .. })),
        "{err}"
    );
    assert!(err.to_string().contains("offset 24"), "{err}");
    assert!(err.to_string().contains("instead close it"), "{err}");
}

#[test]
fn quoted_segment_escapes_match_the_path_rule() {
    assert!(eval(r#"attributes."Event ID".code == 4625"#));
    let err = Condition::parse(r#"attributes."a\nb" == 1"#).expect_err("unknown escape");
    assert!(
        matches!(err, ConditionError::Field { ref source, .. } if matches!(source, PathError::InvalidSegment { ch: 'n', .. })),
        "{err}"
    );
    let c =
        Condition::parse(r#"attributes."a[0]" == 1"#).expect("brackets inside quotes are key text");
    assert!(!c.matches(&record()));
}

#[test]
fn text_glued_to_a_closing_quote_is_the_path_error_with_a_hint() {
    let err = Condition::parse(r#"attributes."a"b == 1"#).expect_err("glued");
    assert!(
        matches!(err, ConditionError::Field { offset: 0, ref source } if matches!(source, PathError::InvalidSegment { instead, .. } if instead == "attributes.ab")),
        "{err}"
    );
}

#[test]
fn unterminated_quote_inside_a_bracket_is_the_bracket_error_for_both_quote_kinds() {
    for (expr, hint) in [
        (r#"attributes['a == 1"#, r#"attributes."a == 1""#),
        (r#"attributes["a == 1"#, r#"attributes."a == 1""#),
    ] {
        let err = Condition::parse(expr).expect_err(expr);
        assert!(
            matches!(err, ConditionError::Field { offset: 0, ref source } if matches!(source, PathError::BracketSyntax { instead } if instead == hint)),
            "{expr}: {err}"
        );
    }
}
