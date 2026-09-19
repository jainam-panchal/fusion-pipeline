//! Field paths: a path is a list of segments walked over a record, which is any JSON value.
//! `meta` is the one reserved root, a leading dot names the record only, and `.` is the
//! whole record.

use fusion_core::meta::{IngestionTime, Meta};
use fusion_core::path::{FieldPath, FieldValue, Num, PathError};
use fusion_core::record::{Record, RecordId};
use serde_json::{Value, json};

/// The `Meta` the reads below see: values of its own, so a test can tell a meta path from a
/// payload key spelled the same way.
fn meta() -> Meta {
    Meta {
        record_id: RecordId(99),
        tenant: "acme".into(),
        ingestion_time: IngestionTime::Reported(1_700_000_000_000_000_000),
        delivery_count: 1,
    }
}

fn record(value: Value) -> Record {
    Record::new(value)
}

fn read(path: &str, record: &Record) -> FieldValue<'static> {
    // The borrow is the record's, not the path's; the tests below only compare.
    let path = FieldPath::parse(path).expect("parses");
    let meta = meta();
    match path.read(record, &meta) {
        FieldValue::Null => FieldValue::Null,
        FieldValue::Bool(b) => FieldValue::Bool(b),
        FieldValue::Num(n) => FieldValue::Num(n),
        FieldValue::Str(_) | FieldValue::Json(_) => unreachable!("use read_json"),
        _ => unreachable!(),
    }
}

fn write(path: &str, record: &mut Record, value: Value) {
    FieldPath::parse(path)
        .expect("parses")
        .writable()
        .expect("writable")
        .write(record, value);
}

fn remove(path: &str, record: &mut Record) -> Option<Value> {
    FieldPath::parse(path)
        .expect("parses")
        .writable()
        .expect("writable")
        .remove(record)
}

#[test]
fn a_top_level_key_is_one_segment() {
    let r = record(json!({"level": "error", "msg": "auth failure"}));
    let meta = meta();
    let path = FieldPath::parse("level").expect("parses");
    assert_eq!(path.read(&r, &meta), FieldValue::Str("error"));
}

#[test]
fn a_key_inside_an_object_is_the_next_segment() {
    let r = record(json!({"test": 12, "test2": {"key1": "ans1", "key2": 123}}));
    let meta = meta();
    let path = FieldPath::parse("test2.key2").expect("parses");
    assert_eq!(path.read(&r, &meta), FieldValue::Num(Num::Int(123)));
}

#[test]
fn a_key_whose_name_holds_a_dot_is_quoted() {
    let r = record(json!({"resource": {"log.format": "Linux"}}));
    let meta = meta();
    let quoted = FieldPath::parse(r#"resource."log.format""#).expect("parses");
    assert_eq!(quoted.read(&r, &meta), FieldValue::Str("Linux"));

    // The unquoted spelling now means two levels, and this record has none.
    let nested = FieldPath::parse("resource.log.format").expect("parses");
    assert_eq!(nested.read(&r, &meta), FieldValue::Null);
}

#[test]
fn a_list_position_is_a_segment() {
    let r = record(json!({
        "attributes": [
            {"key": "db.port", "value": {"intValue": 5432}},
            {"key": "db.name", "value": {"stringValue": "orders"}},
        ]
    }));
    let meta = meta();
    let path = FieldPath::parse("attributes.0.value.intValue").expect("parses");
    assert_eq!(path.read(&r, &meta), FieldValue::Num(Num::Int(5432)));

    let path = FieldPath::parse("attributes.1.key").expect("parses");
    assert_eq!(path.read(&r, &meta), FieldValue::Str("db.name"));

    // Past the end, and a position on something that is not a list, are both absent.
    let path = FieldPath::parse("attributes.2.key").expect("parses");
    assert_eq!(path.read(&r, &meta), FieldValue::Null);
    let path = FieldPath::parse("attributes.0.key.0").expect("parses");
    assert_eq!(path.read(&r, &meta), FieldValue::Null);
}

#[test]
fn a_dot_is_the_whole_record() {
    let meta = meta();
    let whole = FieldPath::parse(".").expect("parses");

    let text = record(json!("Jun 14 15:16:01 combo sshd[19939]: failure"));
    assert_eq!(
        whole.read(&text, &meta),
        FieldValue::Str("Jun 14 15:16:01 combo sshd[19939]: failure")
    );

    let object = record(json!({"level": "error"}));
    assert_eq!(
        whole.read(&object, &meta),
        FieldValue::Json(&json!({"level": "error"}))
    );
}

#[test]
fn a_leading_dot_names_the_record_only() {
    let r = record(json!({"meta": {"id": "the producer's own"}, "0": "zero"}));
    let meta = meta();

    // Without the dot, `meta` is the pipeline's.
    let pipeline = FieldPath::parse("meta.id").expect("parses");
    assert_eq!(pipeline.read(&r, &meta), FieldValue::Num(Num::Int(99)));

    // With it, the payload's.
    let payload = FieldPath::parse(".meta.id").expect("parses");
    assert_eq!(
        payload.read(&r, &meta),
        FieldValue::Str("the producer's own")
    );

    // And it is how a root that is not a bare word is written.
    let zero = FieldPath::parse(".0").expect("parses");
    assert_eq!(zero.read(&r, &meta), FieldValue::Str("zero"));
}

#[test]
fn a_path_that_matches_nothing_reads_as_null() {
    let r = record(json!({"level": "error"}));
    let meta = meta();
    for path in ["severty_text", "a.b.c.d", "level.deeper", "attributes.0"] {
        let path = FieldPath::parse(path).expect("every path parses");
        assert_eq!(path.read(&r, &meta), FieldValue::Null, "{path}");
    }
}

#[test]
fn any_json_is_a_record_and_no_field_has_a_type() {
    let mut r = record(json!({"severity_number": "high", "id": "a-uuid"}));
    let meta = meta();
    // Both would have failed to decode before; both are ordinary values now.
    let severity = FieldPath::parse("severity_number").expect("parses");
    assert_eq!(severity.read(&r, &meta), FieldValue::Str("high"));

    // And both can be written to anything.
    write("severity_number", &mut r, json!([1, 2]));
    assert_eq!(r.value()["severity_number"], json!([1, 2]));
    write("kind", &mut r, json!(7));
    assert_eq!(r.value()["kind"], json!(7));
}

#[test]
fn a_write_makes_the_path_exist() {
    let mut r = Record::default();
    write("test2.key1", &mut r, json!("ans1"));
    assert_eq!(r.value(), &json!({"test2": {"key1": "ans1"}}));

    // A missing key is created all the way down.
    write("a.b.c", &mut r, json!(1));
    assert_eq!(r.value()["a"]["b"]["c"], json!(1));
}

#[test]
fn a_write_replaces_what_is_in_the_way() {
    // A scalar in the way becomes an object; this is the one way a write loses data, and it
    // is what keeps `extract` and `redact` from naking on a record's shape.
    let mut r = record(json!({"body": "a line"}));
    write("body.parsed", &mut r, json!(true));
    assert_eq!(r.value(), &json!({"body": {"parsed": true}}));

    // So does a list with no such position.
    let mut r = record(json!({"attributes": []}));
    write("attributes.0.x", &mut r, json!(1));
    assert_eq!(r.value(), &json!({"attributes": {"0": {"x": 1}}}));
}

#[test]
fn a_write_into_a_list_position_that_exists_keeps_the_list() {
    let mut r = record(json!({"attributes": [{"key": "a"}, {"key": "b"}]}));
    write("attributes.1.key", &mut r, json!("changed"));
    assert_eq!(
        r.value(),
        &json!({"attributes": [{"key": "a"}, {"key": "changed"}]})
    );
}

#[test]
fn a_write_through_the_whole_record_replaces_it() {
    let mut r = record(json!({"level": "error"}));
    write(".", &mut r, json!("a raw line"));
    assert_eq!(r.value(), &json!("a raw line"));
}

#[test]
fn remove_gives_back_what_was_there() {
    let mut r = record(json!({"a": {"b": 1, "c": 2}}));
    assert_eq!(remove("a.b", &mut r), Some(json!(1)));
    assert_eq!(r.value(), &json!({"a": {"c": 2}}));

    // An absent path is nothing to do.
    assert_eq!(remove("a.b", &mut r), None);
    assert_eq!(remove("nowhere.at.all", &mut r), None);
}

#[test]
fn removing_a_list_position_closes_the_gap() {
    let mut r = record(json!({"items": [1, 2, 3]}));
    assert_eq!(remove("items.1", &mut r), Some(json!(2)));
    assert_eq!(r.value(), &json!({"items": [1, 3]}));
    assert_eq!(remove("items.9", &mut r), None);
}

#[test]
fn removing_the_whole_record_leaves_null() {
    let mut r = record(json!({"level": "error"}));
    assert_eq!(remove(".", &mut r), Some(json!({"level": "error"})));
    assert_eq!(r.value(), &Value::Null);
}

#[test]
fn a_record_keeps_every_key_it_arrived_with() {
    let json = r#"{"level":"error","msg":"auth failure","rhost":"10.0.0.1","host":"web-1"}"#;
    let r = Record::from_json(json).expect("any JSON is a record");
    // Every key survives, values and all. Key order does not: `serde_json` is built here
    // without `preserve_order`, so an object is a `BTreeMap` and comes out sorted. That is
    // the property `dedupe` and `sample` hash a nested key by, so it is kept on purpose.
    assert_eq!(
        r.to_json().expect("serializes"),
        r#"{"host":"web-1","level":"error","msg":"auth failure","rhost":"10.0.0.1"}"#
    );
    assert_eq!(
        r.value(),
        &json!({"level": "error", "msg": "auth failure", "rhost": "10.0.0.1", "host": "web-1"})
    );
}

#[test]
fn two_records_differing_only_in_key_order_are_one_value() {
    // What the last test's sorting buys: a `dedupe` or `sample` key naming an object cannot
    // give one answer on the first delivery and another on a redelivery.
    let a = Record::from_json(r#"{"a":{"x":1,"y":2}}"#).expect("parses");
    let b = Record::from_json(r#"{"a":{"y":2,"x":1}}"#).expect("parses");
    assert_eq!(a, b);
    assert_eq!(a.to_json().ok(), b.to_json().ok());
}

#[test]
fn meta_paths_read_the_pipeline_and_refuse_every_write() {
    let r = record(json!({}));
    let meta = meta();
    let tenant = FieldPath::parse("meta.tenant").expect("parses");
    assert_eq!(tenant.read(&r, &meta), FieldValue::Str("acme"));

    for path in [
        "meta.id",
        "meta.tenant",
        "meta.ingestion_time",
        "meta.delivery_count",
    ] {
        let path = FieldPath::parse(path).expect("parses");
        let err = path.writable().expect_err("the pipeline's");
        assert!(matches!(err, PathError::ReadOnly { .. }), "{err}");
    }
}

#[test]
fn meta_needs_one_known_field() {
    for path in ["meta", "meta.nonsense", "meta.id.more"] {
        let err = FieldPath::parse(path).expect_err("not a meta field");
        assert!(
            matches!(err, PathError::UnknownMetaField { .. }),
            "{path}: {err}"
        );
    }
}

#[test]
fn parse_errors_name_the_problem_and_the_fix() {
    let err = FieldPath::parse(r#"attributes["http.status"]"#).expect_err("brackets are gone");
    let PathError::BracketSyntax { instead } = &err else {
        panic!("{err}");
    };
    // The hint keeps the key whole, since that is what the bracket named.
    assert_eq!(instead, r#"attributes."http.status""#);

    let err = FieldPath::parse("a..b").expect_err("empty segment");
    assert!(matches!(err, PathError::EmptySegment { .. }), "{err}");

    let err = FieldPath::parse("a.").expect_err("trailing dot");
    assert!(matches!(err, PathError::EmptySegment { .. }), "{err}");

    let err = FieldPath::parse("").expect_err("empty path");
    assert!(matches!(err, PathError::EmptySegment { .. }), "{err}");

    let err = FieldPath::parse(r#"attributes."unclosed"#).expect_err("unclosed quote");
    assert!(matches!(err, PathError::UnterminatedQuote { .. }), "{err}");

    let err = FieldPath::parse("attributes.Event ID").expect_err("a space needs quotes");
    let PathError::InvalidSegment { ch, instead, .. } = &err else {
        panic!("{err}");
    };
    assert_eq!(*ch, ' ');
    assert_eq!(instead, r#"attributes."Event ID""#);
}

#[test]
fn a_path_round_trips_through_its_text() {
    for text in [
        ".",
        "level",
        "test2.key2",
        r#"resource."log.format""#,
        "attributes.0.value.intValue",
        r#"attributes."Event ID".code"#,
        "meta.tenant",
        ".meta.id",
    ] {
        let path = FieldPath::parse(text).expect("parses");
        assert_eq!(path.to_string(), text, "{text}");
        assert_eq!(
            FieldPath::parse(&path.to_string()).expect("re-parses"),
            path,
            "{text}"
        );
    }
}

#[test]
fn a_bare_root_and_a_dotted_root_are_the_same_path() {
    // The leading dot is a way to write a path, not a different path, so it is not part of
    // the canonical text unless the root is `meta`.
    for (dotted, bare) in [(".level", "level"), (".0", "0")] {
        let path = FieldPath::parse(dotted).expect("parses");
        assert_eq!(path, FieldPath::parse(bare).expect("parses"), "{dotted}");
        assert_eq!(path.to_string(), bare, "{dotted}");
    }
}

#[test]
fn a_write_path_reads_what_it_writes() {
    let mut r = record(json!({}));
    let path = FieldPath::parse("a.b").expect("parses");
    let write = path.writable().expect("the record's");
    assert_eq!(write.read(&r), FieldValue::Null);
    write.write(&mut r, json!("here"));
    assert_eq!(write.read(&r), FieldValue::Str("here"));
    assert_eq!(write.child("c").read(&r), FieldValue::Null);
}

#[test]
fn a_write_paths_child_is_one_segment_below_it() {
    let mut r = record(json!({}));
    let into = FieldPath::parse("attributes")
        .expect("parses")
        .writable()
        .expect("the record's");
    into.child("Month").write(&mut r, json!("Jun"));
    assert_eq!(r.value(), &json!({"attributes": {"Month": "Jun"}}));
    assert_eq!(into.child("Month").to_string(), "attributes.Month");
}

#[test]
fn field_values_view_json_as_it_is() {
    assert_eq!(FieldValue::from_json(&Value::Null), FieldValue::Null);
    assert_eq!(FieldValue::from_json(&json!(true)), FieldValue::Bool(true));
    assert_eq!(FieldValue::from_json(&json!("s")), FieldValue::Str("s"));
    assert_eq!(
        FieldValue::from_json(&json!(3)),
        FieldValue::Num(Num::Int(3))
    );
    let list = json!([1]);
    assert_eq!(FieldValue::from_json(&list), FieldValue::Json(&list));
}

#[test]
fn reading_a_scalar_path_needs_no_record_shape() {
    // The helper exists to keep the borrow simple; this pins its two live arms.
    let r = record(json!({"n": 1, "b": false}));
    assert_eq!(read("n", &r), FieldValue::Num(Num::Int(1)));
    assert_eq!(read("b", &r), FieldValue::Bool(false));
    assert_eq!(read("missing", &r), FieldValue::Null);
}

#[test]
fn a_position_is_digits_with_no_leading_zero() {
    let list = record(json!({"attributes": [0, 1, 2, 3, 4, 5, 6, 7]}));
    let object = record(json!({"attributes": {"007": "x", "7": "y"}}));
    let meta = meta();

    let plain = FieldPath::parse("attributes.7").expect("parses");
    assert_eq!(plain.read(&list, &meta), FieldValue::Num(Num::Int(7)));
    assert_eq!(plain.read(&object, &meta), FieldValue::Str("y"));

    // `007` is a key, never a position, so one path does not mean two things by shape.
    let padded = FieldPath::parse("attributes.007").expect("parses");
    assert_eq!(padded.read(&object, &meta), FieldValue::Str("x"));
    assert_eq!(padded.read(&list, &meta), FieldValue::Null);
}

#[test]
fn an_error_quotes_the_path_the_author_wrote() {
    // The leading dot is stripped before the segments are read, so the error used to quote
    // the remainder and name a path nobody wrote.
    let err = FieldPath::parse("..").expect_err("empty segment");
    assert!(
        matches!(&err, PathError::EmptySegment { path } if path == ".."),
        "{err}"
    );
    let err = FieldPath::parse(r#"."unclosed"#).expect_err("unclosed quote");
    assert!(
        matches!(&err, PathError::UnterminatedQuote { path } if path == r#"."unclosed"#),
        "{err}"
    );
}
