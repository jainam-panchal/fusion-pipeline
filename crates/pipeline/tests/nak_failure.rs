//! Why a record was nakked, through the trait boundary: every nak carries the first failure
//! of the walk (the node, a closed-set kind and the error text), so a transport can say why
//! when it gives up on the message. Core has no dead-letter queue; the NATS source builds
//! one on this.

mod common;

use common::{WAIT, body_record, start};
use fusion_core::io::FailureKind;
use fusion_core::memory::AckOutcome;
use fusion_core::record::Record;

/// A `lua` node that raises on every record, under `on_error: nak`, into one sink.
const RAISES: &str = r#"
nodes:
  - id: script
    type: lua
    on_error: nak
    source: |
      function process(record)
        error("no good")
      end
  - id: out
    type: sink.memory
"#;

const TO_SINK: &str = r#"
nodes:
  - id: out
    type: sink.memory
"#;

#[test]
fn a_failing_sink_naks_with_its_node_and_error() {
    let h = start(TO_SINK, 1);
    h.sinks.fail_writes_to("out");

    let probe = h.push(body_record(1, "x"));

    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));
    let failure = probe.failure().expect("a nak carries its failure");
    assert_eq!(failure.node, "out");
    assert_eq!(failure.kind, FailureKind::SinkError);
    assert!(
        failure.error.contains("memory sink `out` is set to fail"),
        "{}",
        failure.error
    );
    h.finish();
}

#[test]
fn a_stage_error_names_its_node_and_kind() {
    let h = start(RAISES, 1);

    let probe = h.push(body_record(1, "x"));

    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));
    let failure = probe.failure().expect("a nak carries its failure");
    assert_eq!(failure.node, "script");
    assert_eq!(failure.kind, FailureKind::StageError);
    assert!(failure.error.contains("no good"), "{}", failure.error);
    h.finish();
}

#[test]
fn a_state_error_under_nak_is_kind_state_error() {
    const DEDUPE: &str = r#"
nodes:
  - id: dedupe_body
    type: dedupe
    key: [body]
    window: 10s
    on_state_error: nak
  - id: out
    type: sink.memory
"#;
    let h = start(DEDUPE, 1);
    h.state.fail_all(true);

    let probe = h.push(body_record(1, "x"));

    assert!(matches!(probe.wait(WAIT), Some(AckOutcome::Nak(_))));
    let failure = probe.failure().expect("a nak carries its failure");
    assert_eq!(failure.node, "dedupe_body");
    assert_eq!(failure.kind, FailureKind::StateError);
    h.finish();
}

#[test]
fn a_record_without_an_id_fails_at_source_with_missing_id() {
    let h = start(TO_SINK, 1);

    let probe = h.push(Record::from_json(r#"{"body": "no id"}"#).expect("record parses"));

    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));
    let failure = probe.failure().expect("a nak carries its failure");
    assert_eq!(failure.node, "source");
    assert_eq!(failure.kind, FailureKind::MissingId);
    h.finish();
}

#[test]
fn a_panicking_stage_fails_with_kind_panic() {
    let h = common::start_with_panics(
        r#"
nodes:
  - id: boom
    type: panics
  - id: out
    type: sink.memory
"#,
    );

    let probe = h.push(body_record(1, "x"));

    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));
    let failure = probe.failure().expect("a nak carries its failure");
    assert_eq!(failure.node, "boom");
    assert_eq!(failure.kind, FailureKind::Panic);
    h.finish();
}

#[test]
fn a_failure_then_a_panic_reports_the_first_failure() {
    // Branches run in consumer order: the failing sink `first` before the panicking `boom`.
    let h = common::start_with_panics(
        r#"
nodes:
  - id: first
    type: sink.memory
    from: source
  - id: boom
    type: panics
    from: source
  - id: out
    type: sink.memory
"#,
    );
    h.sinks.fail_writes_to("first");

    let probe = h.push(body_record(1, "x"));

    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));
    let failure = probe.failure().expect("a nak carries its failure");
    assert_eq!(failure.node, "first");
    assert_eq!(failure.kind, FailureKind::SinkError);
    h.finish();
}

#[test]
fn with_two_failing_branches_the_first_in_walk_order_is_reported() {
    let h = start(
        r#"
nodes:
  - id: a
    type: sink.memory
    from: source
  - id: b
    type: sink.memory
    from: source
"#,
        1,
    );
    h.sinks.fail_writes_to("a");
    h.sinks.fail_writes_to("b");

    let probe = h.push(body_record(1, "x"));

    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));
    assert_eq!(probe.failure().expect("failure").node, "a");
    h.finish();
}

#[test]
fn an_acked_record_has_no_failure() {
    let h = start(TO_SINK, 1);

    let probe = h.push(body_record(1, "x"));

    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    assert_eq!(probe.failure(), None);
    h.finish();
}
