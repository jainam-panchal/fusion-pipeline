//! Following a run: the expectations file read while the producer writes it, the producer's
//! done marker, and when a stream has been read to its end.

use std::path::Path;

use fusion_harness::follow::{
    LineBuffer, ProducerOutcome, StreamEnd, Tail, done_marker, parse_done, write_done,
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

/// A fresh directory for one test.
fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("fusion-tail-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

fn line(id: u64) -> String {
    let set = fusion_harness::loghub::set("Apache").expect("a vendored set");
    let line = fusion_harness::loghub::Line {
        line_id: 1,
        body: "x".into(),
        attributes: std::collections::BTreeMap::new(),
    };
    let e = fusion_harness::expect::expectation(id, set, &line, 0, None);
    serde_json::to_string(&e).expect("serialises")
}

#[test]
fn the_tail_reads_expectations_as_they_are_written() {
    use std::io::Write as _;
    let dir = scratch("grows");
    let path = dir.join("expectations.jsonl");
    let mut tail = Tail::new(&path);
    tail.read().expect("a file not created yet has nothing");
    assert!(tail.expectations().is_empty());

    let mut file = std::fs::File::create(&path).expect("created");
    let (first, second) = (line(1), line(2));
    write!(file, "{first}\n{}", &second[..10]).expect("written");
    tail.read().expect("read");
    assert_eq!(tail.expectations().len(), 1);
    assert!(!tail.ends_whole(), "half a line is held back");

    writeln!(file, "{}", &second[10..]).expect("written");
    tail.read().expect("read");
    let ids: Vec<u64> = tail.expectations().iter().map(|e| e.id).collect();
    assert_eq!(ids, [1, 2]);
    assert!(tail.ends_whole());
    std::fs::remove_dir_all(&dir).expect("cleaned");
}

#[test]
fn the_tail_names_the_line_that_does_not_parse() {
    let dir = scratch("bad");
    let path = dir.join("expectations.jsonl");
    std::fs::write(&path, format!("{}\nnot json\n", line(1))).expect("written");
    let error = Tail::new(&path).read().expect_err("refused");
    assert!(error.contains("expectations.jsonl:2:"), "{error}");
    std::fs::remove_dir_all(&dir).expect("cleaned");
}

#[test]
fn the_tail_refuses_a_file_rewritten_under_it() {
    let dir = scratch("rewritten");
    let path = dir.join("expectations.jsonl");
    std::fs::write(&path, format!("{}\n{}\n", line(1), line(2))).expect("written");
    let mut tail = Tail::new(&path);
    tail.read().expect("read");
    std::fs::write(&path, format!("{}\n", line(3))).expect("rewritten shorter");
    let error = tail.read().expect_err("refused");
    assert!(error.contains("rewritten"), "{error}");
    std::fs::remove_dir_all(&dir).expect("cleaned");
}
