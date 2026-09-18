//! `codec` and `encoding`: which bytes become which record, and back. Pure functions over
//! bytes, so they are asserted here rather than through `Source`/`Sink` (issue #79).

use fusion_core::record::Record;
use fusion_nats::codec::{Codec, Encoding};
use serde_json::json;

#[test]
fn json_decodes_any_json_value_not_only_an_object() {
    for text in [
        r#"{"level":"error","rhost":"10.0.0.1"}"#,
        r#"["a","b"]"#,
        r#""a string""#,
        "42",
        "null",
    ] {
        let record = Codec::Json.decode(text.as_bytes()).expect("any JSON");
        assert_eq!(
            record.value(),
            &serde_json::from_str::<serde_json::Value>(text).expect("parses"),
            "{text}"
        );
    }
}

#[test]
fn json_refuses_a_payload_that_is_not_json() {
    let err = Codec::Json
        .decode(b"Jun 14 15:16:01 combo sshd: failure")
        .expect_err("not JSON");
    assert!(!err.is_empty(), "the message is what the failure carries");
}

#[test]
fn text_makes_the_line_itself_the_record() {
    let line = "Jun 14 15:16:01 combo sshd[19939]: authentication failure";
    let record = Codec::Text.decode(line.as_bytes()).expect("valid UTF-8");
    assert_eq!(record.value(), &json!(line), "no wrapper, no keys");
}

#[test]
fn text_refuses_bytes_that_are_not_utf8() {
    let err = Codec::Text.decode(&[0xff, 0xfe]).expect_err("not UTF-8");
    assert!(!err.is_empty());
}

#[test]
fn json_is_the_default_both_ways() {
    assert_eq!(Codec::default(), Codec::Json);
    assert_eq!(Encoding::default(), Encoding::Json);
}

#[test]
fn json_writes_the_record_as_the_last_stage_left_it() {
    let record = Record::new(json!({"level": "error", "rhost": "10.0.0.1"}));
    let bytes = Encoding::Json.encode(&record).expect("encodes");
    assert_eq!(bytes, br#"{"level":"error","rhost":"10.0.0.1"}"#.to_vec());
}

#[test]
fn text_writes_a_string_record_as_its_bytes() {
    let line = "auth failure from rhost=[ip]";
    let record = Record::new(json!(line));
    assert_eq!(
        Encoding::Text.encode(&record).expect("encodes"),
        line.as_bytes().to_vec(),
        "byte for byte, with no quotes around it"
    );
}

#[test]
fn text_writes_any_other_record_as_its_json_rather_than_failing() {
    // A stage turned the line into an object. Naking would dead-letter a good record, so the
    // sink writes what it has.
    let record = Record::new(json!({"body": "a line"}));
    assert_eq!(
        Encoding::Text.encode(&record).expect("encodes"),
        br#"{"body":"a line"}"#.to_vec()
    );
}

#[test]
fn a_text_record_round_trips_through_both_ends() {
    let line = "Jun 14 15:16:01 combo sshd[19939]: authentication failure";
    let record = Codec::Text.decode(line.as_bytes()).expect("decodes");
    let bytes = Encoding::Text.encode(&record).expect("encodes");
    assert_eq!(bytes, line.as_bytes().to_vec());
}
