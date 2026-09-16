//! Field paths: one dotted path names one record field. Under `attributes`, `resource` and
//! `scope` the rest of the path, joined with dots, is the flat map key.

use fusion_core::meta::{IngestionTime, Meta};
use fusion_core::path::{FieldPath, FieldValue, Num, PathError};
use fusion_core::record::{Kind, Record, RecordId};
use serde_json::{Value, json};

#[test]
fn map_paths_join_the_rest_into_one_key() {
    let path = FieldPath::parse("attributes.http.status").expect("parses");
    assert_eq!(path.map_key(), Some("http.status"));

    let path = FieldPath::parse("resource.tenant.id").expect("parses");
    assert_eq!(path.map_key(), Some("tenant.id"));

    let path = FieldPath::parse("resource.env").expect("parses");
    assert_eq!(path.map_key(), Some("env"));

    let path = FieldPath::parse("scope.name").expect("parses");
    assert_eq!(path.map_key(), Some("name"));
}

#[test]
fn top_level_paths_have_no_key() {
    let path = FieldPath::parse("severity_text").expect("parses");
    assert_eq!(path.map_key(), None);
    let path = FieldPath::parse("body").expect("parses");
    assert_eq!(path.map_key(), None);
}

#[test]
fn parse_errors_name_the_problem() {
    let err = FieldPath::parse(r#"attributes["http.path"]"#).expect_err("brackets are gone");
    assert!(matches!(err, PathError::BracketSyntax { .. }), "{err}");

    let err = FieldPath::parse("body.x").expect_err("body is addressed as a whole");
    assert!(
        matches!(err, PathError::NotAMap { ref field, .. } if field == "body"),
        "{err}"
    );

    let err = FieldPath::parse("severity_number.x").expect_err("scalar has no children");
    assert!(
        matches!(err, PathError::NotAMap { ref field, .. } if field == "severity_number"),
        "{err}"
    );

    let err = FieldPath::parse("attributes").expect_err("map needs a key");
    assert!(
        matches!(err, PathError::MapNeedsKey { ref field } if field == "attributes"),
        "{err}"
    );

    let err = FieldPath::parse("nonsense.x").expect_err("unknown root");
    assert!(
        matches!(err, PathError::UnknownField { ref name } if name == "nonsense"),
        "{err}"
    );

    for bad in ["", "attributes.", ".attributes", "attributes..x"] {
        let err = FieldPath::parse(bad).expect_err("empty segment");
        assert!(
            matches!(err, PathError::EmptySegment { .. }),
            "{bad:?}: {err}"
        );
    }

    let err = FieldPath::parse("attributes.http:path").expect_err("not a segment");
    assert!(
        matches!(err, PathError::InvalidSegment { ref segment, ch: ':', .. } if segment == "http:path"),
        "{err}"
    );
}

fn record() -> Record {
    Record::from_json(
        r#"{
            "id": 7,
            "kind": "log",
            "severity_text": "ERROR",
            "severity_number": 17,
            "body": "disk full on /var",
            "attributes": {"http.path": "/api/v1", "http.status": 503},
            "resource": {"tenant.id": "acme", "env": "prod"}
        }"#,
    )
    .expect("record parses")
}

/// The `Meta` the reads below see: a different id and tenant from the record's, so a test
/// can tell which one a path read.
fn meta() -> Meta {
    Meta {
        record_id: RecordId(99),
        tenant: "from-meta".into(),
        ingestion_time: IngestionTime::Reported(9_000_000_000),
        delivery_count: 2,
    }
}

/// Read `path` from `record` (and [`meta`]) as an owned JSON value, `None` when absent, so
/// assertions can use `json!` literals.
fn read_from(record: &Record, path: &str) -> Option<Value> {
    match FieldPath::parse(path)
        .expect("parses")
        .read(record, &meta())
    {
        FieldValue::Null => None,
        FieldValue::Bool(b) => Some(json!(b)),
        FieldValue::Num(Num::Int(i)) => Some(json!(i)),
        FieldValue::Num(Num::Float(f)) => Some(json!(f)),
        FieldValue::Str(s) => Some(json!(s)),
        FieldValue::Json(v) => Some(v.clone()),
        other => panic!("unexpected view {other:?}"),
    }
}

#[test]
fn read_borrows_strings_and_views_a_structured_body() {
    let mut r = record();
    let meta = meta();
    let FieldValue::Str(text) = FieldPath::parse("severity_text")
        .expect("parses")
        .read(&r, &meta)
    else {
        panic!("severity_text is a string");
    };
    assert!(std::ptr::eq(text, r.severity_text.as_deref().expect("set")));

    r.body = Some(json!({"raw": "x"}));
    let FieldValue::Json(body) = FieldPath::parse("body").expect("parses").read(&r, &meta) else {
        panic!("structured body is a view");
    };
    assert_eq!(body, &json!({"raw": "x"}));
}

fn read(path: &str) -> Option<Value> {
    read_from(&record(), path)
}

#[test]
fn read_resolves_map_keys_and_top_level_fields() {
    assert_eq!(read("attributes.http.status"), Some(json!(503)));
    assert_eq!(read("attributes.http.path"), Some(json!("/api/v1")));
    assert_eq!(read("resource.tenant.id"), Some(json!("acme")));
    assert_eq!(read("resource.env"), Some(json!("prod")));
    assert_eq!(read("attributes.nope"), None);
    assert_eq!(read("scope.nope"), None);
    assert_eq!(read("id"), Some(json!(7)));
    assert_eq!(read("kind"), Some(json!("log")));
    assert_eq!(read("severity_text"), Some(json!("ERROR")));
    assert_eq!(read("severity_number"), Some(json!(17)));
    assert_eq!(read("body"), Some(json!("disk full on /var")));
    assert_eq!(read("trace_id"), None);
    assert_eq!(read("time_unix_nano"), None);
}

#[test]
fn segments_allow_digits_and_hyphens() {
    let path = FieldPath::parse("attributes.5xx.count").expect("parses");
    assert_eq!(path.map_key(), Some("5xx.count"));

    let path = FieldPath::parse("resource.k8s.pod-name").expect("parses");
    assert_eq!(path.map_key(), Some("k8s.pod-name"));

    let path = FieldPath::parse("attributes.x-request-id").expect("parses");
    assert_eq!(path.map_key(), Some("x-request-id"));

    for bad in ["attributes.http path", "attributes.a:b", "attributes.a/b"] {
        let err = FieldPath::parse(bad).expect_err("not a segment");
        assert!(
            matches!(err, PathError::InvalidSegment { .. }),
            "{bad:?}: {err}"
        );
    }
}

#[test]
fn quoted_segments_reach_keys_with_other_characters() {
    let path = FieldPath::parse(r#"attributes."something something""#).expect("parses");
    assert_eq!(path.map_key(), Some("something something"));

    let path = FieldPath::parse(r#"attributes."Event ID".code"#).expect("parses");
    assert_eq!(path.map_key(), Some("Event ID.code"));

    let path = FieldPath::parse(r#"resource.k8s."pod/name""#).expect("parses");
    assert_eq!(path.map_key(), Some("k8s.pod/name"));

    let path = FieldPath::parse(r#"attributes."say \"hi\"""#).expect("parses");
    assert_eq!(path.map_key(), Some(r#"say "hi""#));

    let path = FieldPath::parse(r#"attributes."plain""#).expect("parses");
    assert_eq!(path.map_key(), Some("plain"));
    assert_eq!(path.to_string(), "attributes.plain");
    assert_eq!(
        FieldPath::parse(r#"attributes."Event ID".code"#)
            .expect("parses")
            .to_string(),
        r#"attributes."Event ID".code"#
    );

    let err = FieldPath::parse(r#"attributes."open"#).expect_err("unterminated");
    assert!(matches!(err, PathError::UnterminatedQuote { .. }), "{err}");

    let err = FieldPath::parse(r#""attributes".x"#).expect_err("root is never quoted");
    assert!(matches!(err, PathError::UnknownField { .. }), "{err}");

    let err = FieldPath::parse("attributes.something something").expect_err("space needs quotes");
    assert!(
        matches!(err, PathError::InvalidSegment { ref instead, .. } if instead == r#"attributes."something something""#),
        "{err}"
    );
}

fn write(record: &mut Record, path: &str, value: Value) -> Result<(), PathError> {
    FieldPath::parse(path).expect("parses").write(record, value)
}

#[test]
fn write_creates_or_replaces_a_map_key() {
    let mut record = record();

    write(&mut record, "attributes.http.route", json!("/api/{id}")).expect("writes");
    assert_eq!(
        record.attributes.get("http.route"),
        Some(&json!("/api/{id}"))
    );

    write(&mut record, "attributes.http.route", json!("/api/{id}/v2")).expect("replaces");
    assert_eq!(
        record.attributes.get("http.route"),
        Some(&json!("/api/{id}/v2"))
    );

    write(&mut record, "resource.env", json!("staging")).expect("writes");
    assert_eq!(read_from(&record, "resource.env"), Some(json!("staging")));

    write(&mut record, "scope.name", json!("otel-sdk")).expect("writes");
    assert_eq!(record.scope.get("name"), Some(&json!("otel-sdk")));
    assert!(
        record.attributes.get("http").is_none(),
        "no nesting is created"
    );
}

#[test]
fn id_and_kind_are_payload_and_take_their_types() {
    let mut record = record();

    write(&mut record, "id", json!(8)).expect("an id is a non-negative integer");
    assert_eq!(record.id, Some(RecordId(8)));
    write(&mut record, "kind", json!("metric")).expect("a kind is one of the three");
    assert_eq!(record.kind, Kind::Metric);

    let before = record.clone();
    for (path, value, expected) in [
        ("id", json!(-1), "a non-negative integer"),
        ("id", json!("8"), "a non-negative integer"),
        ("kind", json!("trace"), "`log`, `metric` or `span`"),
        ("kind", json!(1), "`log`, `metric` or `span`"),
        ("kind", json!(null), "`log`, `metric` or `span`"),
    ] {
        let err = write(&mut record, path, value).expect_err("wrong type");
        assert!(
            matches!(err, PathError::WrongType { ref field, expected: e, .. } if field == path && e == expected),
            "{path}: {err}"
        );
    }
    assert_eq!(record, before);

    write(&mut record, "id", json!(null)).expect("null clears the id");
    assert_eq!(record.id, None);
}

#[test]
fn write_of_wrong_type_is_refused_and_leaves_the_record_unchanged() {
    let mut record = record();
    let before = record.clone();
    let cases = [
        ("severity_number", json!("17")),
        ("severity_number", json!(99_999_999_999_i64)),
        ("severity_number", json!(1.5)),
        ("severity_text", json!(17)),
        ("trace_id", json!(1)),
        ("span_id", json!(true)),
        ("time_unix_nano", json!(-1)),
        ("observed_time_unix_nano", json!("now")),
    ];
    for (path, value) in cases {
        let err = write(&mut record, path, value.clone()).expect_err("refused");
        assert!(
            matches!(err, PathError::WrongType { ref field, .. } if field == path),
            "{path} = {value}: {err}"
        );
    }
    assert_eq!(record, before);
}

#[test]
fn write_of_array_or_object_under_a_map_key_is_refused() {
    let mut record = record();
    let before = record.clone();
    for value in [json!({"a": 1}), json!([1, 2])] {
        let err = write(&mut record, "attributes.x", value.clone()).expect_err("maps are flat");
        assert!(
            matches!(err, PathError::WrongType { ref field, .. } if field == "attributes.x"),
            "{value}: {err}"
        );
    }
    assert_eq!(record, before);
}

#[test]
fn write_of_typed_fields_with_the_right_type_reads_back() {
    let mut record = record();
    write(&mut record, "severity_number", json!(4)).expect("writes");
    write(&mut record, "severity_text", json!("WARN")).expect("writes");
    write(
        &mut record,
        "trace_id",
        json!("4bf92f3577b34da6a3ce929d0e0e4736"),
    )
    .expect("writes");
    write(&mut record, "span_id", json!("00f067aa0ba902b7")).expect("writes");
    write(
        &mut record,
        "time_unix_nano",
        json!(1_700_000_000_000_000_000_u64),
    )
    .expect("writes");
    write(&mut record, "body", json!({"raw": "kept whole"})).expect("body takes any value");
    write(&mut record, "attributes.\"Event ID\"", json!(4625)).expect("writes");

    assert_eq!(read_from(&record, "severity_number"), Some(json!(4)));
    assert_eq!(read_from(&record, "severity_text"), Some(json!("WARN")));
    assert_eq!(
        read_from(&record, "trace_id"),
        Some(json!("4bf92f3577b34da6a3ce929d0e0e4736"))
    );
    assert_eq!(
        read_from(&record, "span_id"),
        Some(json!("00f067aa0ba902b7"))
    );
    assert_eq!(
        read_from(&record, "time_unix_nano"),
        Some(json!(1_700_000_000_000_000_000_u64))
    );
    assert_eq!(
        read_from(&record, "body"),
        Some(json!({"raw": "kept whole"}))
    );
    assert_eq!(
        read_from(&record, "attributes.\"Event ID\""),
        Some(json!(4625))
    );

    write(&mut record, "severity_text", Value::Null).expect("null clears");
    assert_eq!(read_from(&record, "severity_text"), None);
    write(&mut record, "attributes.nullable", Value::Null).expect("null is stored under a key");
    assert_eq!(record.attributes.get("nullable"), Some(&Value::Null));
}

#[test]
fn remove_deletes_the_field_and_returns_the_old_value() {
    let mut record = record();
    let remove = |record: &mut Record, path: &str| {
        FieldPath::parse(path)
            .expect("parses")
            .remove(record)
            .expect("a record field can always be removed")
    };

    assert_eq!(
        remove(&mut record, "attributes.http.path"),
        Some(json!("/api/v1"))
    );
    assert!(record.attributes.get("http.path").is_none());
    assert_eq!(remove(&mut record, "attributes.http.path"), None);

    assert_eq!(remove(&mut record, "severity_text"), Some(json!("ERROR")));
    assert_eq!(record.severity_text, None);
    assert_eq!(
        remove(&mut record, "body"),
        Some(json!("disk full on /var"))
    );
    assert_eq!(record.body, None);
    assert_eq!(remove(&mut record, "trace_id"), None);

    record.kind = Kind::Span;
    assert_eq!(remove(&mut record, "id"), Some(json!(7)));
    assert_eq!(record.id, None);
    assert_eq!(remove(&mut record, "kind"), Some(json!("span")));
    assert_eq!(record.kind, Kind::Log, "a removed kind is the wire default");
}

#[test]
fn quoted_segments_may_hold_brackets_and_dots_and_display_round_trips() {
    for (text, key) in [
        (r#"attributes."a[0]""#, "a[0]"),
        (r#"attributes."a.b""#, "a.b"),
        (r#"attributes."back\\slash""#, r"back\slash"),
    ] {
        let path = FieldPath::parse(text).expect(text);
        assert_eq!(path.map_key(), Some(key), "{text}");
        let shown = path.to_string();
        let again = FieldPath::parse(&shown).expect(&shown);
        assert_eq!(again.map_key(), Some(key), "{text} -> {shown}");
    }
}

#[test]
fn hints_are_themselves_valid_paths() {
    let cases = [
        (r#"attributes["a b"]"#, r#"attributes."a b""#),
        (r#"attributes["http.status"]"#, "attributes.http.status"),
        (r#"attributes."x y".x y"#, r#"attributes."x y"."x y""#),
        ("attributes.a:b.c", r#"attributes."a:b".c"#),
        (r#"attributes."a"b"#, "attributes.ab"),
    ];
    for (bad, hint) in cases {
        let err = FieldPath::parse(bad).expect_err(bad);
        let instead = match &err {
            PathError::BracketSyntax { instead, .. }
            | PathError::InvalidSegment { instead, .. } => instead.clone(),
            other => panic!("{bad}: unexpected {other}"),
        };
        assert_eq!(instead, hint, "{bad}: {err}");
        FieldPath::parse(&instead).unwrap_or_else(|e| panic!("hint `{instead}` for `{bad}`: {e}"));
    }
}

#[test]
fn empty_quoted_segment_and_unknown_escapes_are_rejected() {
    let err = FieldPath::parse(r#"attributes."""#).expect_err("empty");
    assert!(matches!(err, PathError::EmptySegment { .. }), "{err}");

    let err = FieldPath::parse(r#"attributes."a\nb""#).expect_err("only \\\" and \\\\ escape");
    assert!(
        matches!(err, PathError::InvalidSegment { ch: 'n', ref instead, .. } if instead == r#"attributes."a\\nb""#),
        "{err}"
    );
}

#[test]
fn bracket_hints_unquote_each_part_before_rebuilding() {
    let cases = [
        (r#"attributes."a"[0]"#, "attributes.a.0"),
        (r#"attributes["a\"b"]"#, r#"attributes."a\"b""#),
        (r#"attributes['x y']"#, r#"attributes."x y""#),
        (r#"resource["service"]["name"]"#, "resource.service.name"),
        (r#"attributes["a[0]"]"#, r#"attributes."a[0]""#),
        (r#"attributes["a]b"]"#, r#"attributes."a]b""#),
        (r#"attributes['a]b'].c"#, r#"attributes."a]b".c"#),
        (r#"attributes["open"#, "attributes.open"),
        (r#"attributes['a == 1"#, r#"attributes."a == 1""#),
    ];
    for (bad, hint) in cases {
        let err = FieldPath::parse(bad).expect_err(bad);
        let PathError::BracketSyntax { instead } = &err else {
            panic!("{bad}: unexpected {err}");
        };
        assert_eq!(instead, hint, "{bad}: {err}");
        FieldPath::parse(instead).unwrap_or_else(|e| panic!("hint `{instead}` for `{bad}`: {e}"));
    }
}

#[test]
fn display_round_trips_keys_with_empty_dot_parts() {
    for (text, key) in [
        (r#"attributes."a..b""#, "a..b"),
        (r#"attributes.".a""#, ".a"),
        (r#"attributes."a.""#, "a."),
        (r#"attributes.a."b.c".d"#, "a.b.c.d"),
    ] {
        let path = FieldPath::parse(text).expect(text);
        assert_eq!(path.map_key(), Some(key), "{text}");
        let shown = path.to_string();
        let again = FieldPath::parse(&shown).unwrap_or_else(|e| panic!("{text} -> {shown}: {e}"));
        assert_eq!(again.map_key(), Some(key), "{text} -> {shown}");
    }
}

#[test]
fn empty_brackets_hint_the_shape_of_a_key() {
    for bad in [
        "attributes[]",
        r#"attributes[""]"#,
        "attributes['']",
        "attributes[][]",
    ] {
        let err = FieldPath::parse(bad).expect_err(bad);
        let PathError::BracketSyntax { instead } = &err else {
            panic!("{bad}: unexpected {err}");
        };
        assert_eq!(instead, "attributes.<key>", "{bad}: {err}");
    }
    let err = FieldPath::parse("body[]").expect_err("body[]");
    assert!(
        matches!(err, PathError::BracketSyntax { ref instead } if instead == "body"),
        "{err}"
    );
}

#[test]
fn every_kind_round_trips_through_its_wire_name() {
    let names: Vec<&str> = Kind::ALL.into_iter().map(Kind::as_str).collect();
    // The spec's record model names these three; a kind added to the enum fails here, and
    // whoever extends this list amends the spec with it.
    assert_eq!(names, ["log", "metric", "span"], "the spec's record model");
    for kind in Kind::ALL {
        let name = kind.as_str();
        assert_eq!(Kind::parse(name), Some(kind), "{name}");
        assert_eq!(
            serde_json::to_value(kind).expect("serializes"),
            json!(name),
            "serde writes as_str"
        );
        assert_eq!(
            serde_json::from_value::<Kind>(json!(name)).expect("deserializes"),
            kind,
            "serde reads as_str"
        );
        let mut record = record();
        write(&mut record, "kind", json!(name)).expect("a wire name is a kind");
        assert_eq!(
            FieldPath::parse("kind")
                .expect("parses")
                .read(&record, &meta()),
            FieldValue::Str(name)
        );
    }
    for name in ["LOG", "logs", ""] {
        assert_eq!(Kind::parse(name), None, "{name:?} is not a kind");
        assert!(
            serde_json::from_value::<Kind>(json!(name)).is_err(),
            "{name:?} is not a kind on the wire either"
        );
    }
}

#[test]
fn an_unknown_field_is_refused_listing_every_root_in_reading_order() {
    let err = FieldPath::parse("nonsense").expect_err("not a root");
    assert_eq!(
        err.to_string(),
        "`nonsense` is not a record field; instead use one of id, kind, body, severity_text, \
         severity_number, time_unix_nano, observed_time_unix_nano, trace_id, span_id, \
         attributes.<key>, resource.<key>, scope.<key>, meta.<field>"
    );
}

#[test]
fn meta_paths_read_the_records_meta_not_the_record() {
    assert_eq!(read("meta.id"), Some(json!(99)));
    assert_eq!(read("meta.tenant"), Some(json!("from-meta")));
    assert_eq!(read("meta.ingestion_time"), Some(json!(9_000_000_000_u64)));
    assert_eq!(read("meta.delivery_count"), Some(json!(2)));
    assert_eq!(
        read("id"),
        Some(json!(7)),
        "the record's own id is still there"
    );
    assert_eq!(read("resource.tenant.id"), Some(json!("acme")));
}

#[test]
fn a_meta_path_is_one_field_and_displays_as_written() {
    for text in [
        "meta.id",
        "meta.tenant",
        "meta.ingestion_time",
        "meta.delivery_count",
    ] {
        let path = FieldPath::parse(text).expect("parses");
        assert!(path.is_meta(), "{text}");
        assert_eq!(path.map_key(), None, "{text}");
        assert_eq!(path.to_string(), text);
    }
    assert!(
        !FieldPath::parse("resource.tenant.id")
            .expect("parses")
            .is_meta()
    );
}

#[test]
fn meta_alone_or_with_an_unknown_field_lists_the_meta_fields() {
    for text in ["meta", "meta.nope", "meta.tenant.id", "meta.Tenant"] {
        let err = FieldPath::parse(text).expect_err(text);
        match &err {
            PathError::UnknownMetaField { path } => assert_eq!(path, text),
            other => panic!("{text}: {other:?}"),
        }
        assert!(
            err.to_string()
                .contains("`meta.id`, `meta.tenant`, `meta.ingestion_time`, `meta.delivery_count`"),
            "{err}"
        );
    }
}

#[test]
fn a_meta_path_refuses_every_write_and_removal() {
    let path = FieldPath::parse("meta.tenant").expect("parses");
    let mut record = record();
    let before = record.clone();
    let err = path
        .write(&mut record, json!("beta"))
        .expect_err("read-only");
    assert_eq!(
        err,
        PathError::ReadOnly {
            path: "meta.tenant".to_owned()
        }
    );
    assert!(
        err.to_string().contains(
            "instead copy it into a record field: `copy {from: meta.tenant, to: <field>}`"
        ),
        "{err}"
    );
    assert_eq!(
        path.remove(&mut record),
        Err(PathError::ReadOnly {
            path: "meta.tenant".to_owned()
        })
    );
    assert_eq!(record, before, "the record is unchanged");
}
