//! Following a run: the expectations file read while the producer writes it, the producer's
//! done marker, and when a stream has been read to its end.

use std::path::Path;

use fusion_harness::follow::{
    LineBuffer, ProducerOutcome, StreamEnd, done_marker, parse_done, write_done,
};

#[test]
fn a_line_is_handed_over_only_once_its_newline_is_in() {
    let mut buffer = LineBuffer::default();
    assert!(buffer.push(b"{\"a\":").is_empty());
    assert_eq!(buffer.push(b"1}\n{\"b\""), ["{\"a\":1}"]);
    assert!(buffer.has_partial());
    assert_eq!(
        buffer.push(b":2}\n\n{\"c\":3}\n"),
        ["{\"b\":2}", "{\"c\":3}"]
    );
    assert!(!buffer.has_partial());
}

#[test]
fn a_line_split_inside_a_multibyte_character_is_joined_whole() {
    let text = "{\"body\":\"caf\u{e9}\"}\n".as_bytes();
    let (head, tail) = text.split_at(13);
    let mut buffer = LineBuffer::default();
    assert!(buffer.push(head).is_empty());
    assert_eq!(buffer.push(tail), ["{\"body\":\"caf\u{e9}\"}"]);
}

#[test]
fn the_done_marker_sits_beside_the_expectations_file() {
    assert_eq!(
        done_marker(Path::new("target/loghub/expectations.jsonl")),
        Path::new("target/loghub/expectations.jsonl.done")
    );
}

#[test]
fn the_done_marker_says_whether_the_producer_published_its_plan() {
    let dir = std::env::temp_dir().join(format!("fusion-follow-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let marker = dir.join("expectations.jsonl.done");
    for outcome in [ProducerOutcome::Published, ProducerOutcome::Failed] {
        write_done(&marker, outcome).expect("written");
        let text = std::fs::read_to_string(&marker).expect("read");
        assert_eq!(parse_done(&text), Some(outcome), "{text:?}");
    }
    std::fs::remove_dir_all(&dir).expect("cleaned");
    assert_eq!(
        parse_done(""),
        None,
        "a marker being written is not read yet"
    );
    assert_eq!(parse_done("maybe\n"), None);
}

#[test]
fn an_empty_stream_is_read_to_its_end_whatever_its_sequence() {
    // A purged stream keeps its sequence: no message will ever carry `last_sequence`.
    let end = StreamEnd {
        messages: 0,
        last_sequence: 5_000,
    };
    assert!(end.reached(None));
}

#[test]
fn a_stream_is_read_to_its_end_once_its_last_sequence_was_seen() {
    let end = StreamEnd {
        messages: 3,
        last_sequence: 12,
    };
    assert!(!end.reached(None));
    assert!(!end.reached(Some(11)));
    assert!(end.reached(Some(12)));
    assert!(end.reached(Some(13)));
}
