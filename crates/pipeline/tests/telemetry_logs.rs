//! Structured pipeline logs, through the trait boundary (issue #12): every stage error and
//! every nak is one event naming the record, the tenant, the node and the reason, and a
//! redelivery is one event too. Passing and dropped records log nothing.

mod common;

use common::{TENANT, WAIT, body_record, start, start_with};
use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::events::EventKind;
use fusion_core::io::FailureKind;
use fusion_core::memory::{AckOutcome, MemorySinks};
use fusion_core::record::{Record, RecordId};
use fusion_core::stage::{Context, Stage, StageOutput};
use fusion_core::trace::TraceKey;

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

/// Two sinks reading the source, `good` first.
const FAN_OUT: &str = r#"
nodes:
  - id: good
    type: sink.memory
    from: source
  - id: bad
    type: sink.memory
    from: source
"#;

const DEDUPE: &str = r#"
nodes:
  - id: dedupe_body
    type: dedupe
    key: [body]
    window: 10s
    on_state_error: POLICY
  - id: out
    type: sink.memory
"#;

const TO_SINK: &str = r#"
nodes:
  - id: keep_x
    type: filter
    condition: 'body == "x"'
    action: keep
  - id: out
    type: sink.memory
"#;

fn trace_key(id: u64) -> TraceKey {
    TraceKey::new(RecordId(id), TENANT)
}

#[test]
fn a_stage_error_is_one_event_naming_record_tenant_node_and_reason() {
    let h = start(RAISES, 1);

    let probe = h.push(body_record(7, "x"));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));

    let errors = h.events_of(EventKind::StageError);
    assert_eq!(errors.len(), 1, "{errors:?}");
    let error = &errors[0];
    assert_eq!(error.record_id, Some(RecordId(7)));
    assert_eq!(&*error.tenant, TENANT);
    assert_eq!(error.node, "script");
    assert_eq!(error.reason, Some(FailureKind::StageError));
    assert_eq!(error.delivery_count, 1);
    assert!(error.message.contains("no good"), "{}", error.message);
    let trace = error.trace.expect("a record with an id has a trace");
    assert_eq!(trace.trace_id, trace_key(7).trace_id());
    h.finish();
}

#[test]
fn a_failing_branch_logs_its_error_then_one_nak_with_the_first_failure() {
    let h = start(FAN_OUT, 1);
    h.sinks.fail_writes_to("bad");

    let probe = h.push(body_record(8, "x"));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));

    let events = h.events();
    let kinds: Vec<EventKind> = events.iter().map(|e| e.kind).collect();
    assert_eq!(kinds, [EventKind::StageError, EventKind::Nak], "{events:?}");
    let nak = &events[1];
    assert_eq!(nak.record_id, Some(RecordId(8)));
    assert_eq!(&*nak.tenant, TENANT);
    assert_eq!(nak.node, "bad");
    assert_eq!(nak.reason, Some(FailureKind::SinkError));
    assert!(nak.message.contains("set to fail"), "{}", nak.message);
    assert_eq!(nak.trace, Some(trace_key(8).delivery_context(1)));
    assert_eq!(events[0].node, "bad");
    assert_eq!(events[0].reason, Some(FailureKind::SinkError));
    h.finish();
}

#[test]
fn a_nak_names_the_first_failure_when_two_branches_fail() {
    let h = start(FAN_OUT, 1);
    h.sinks.fail_writes_to("good");
    h.sinks.fail_writes_to("bad");

    let probe = h.push(body_record(9, "x"));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));

    let errors: Vec<String> = h
        .events_of(EventKind::StageError)
        .into_iter()
        .map(|e| e.node)
        .collect();
    assert_eq!(errors, ["good", "bad"]);
    let naks = h.events_of(EventKind::Nak);
    assert_eq!(naks.len(), 1);
    assert_eq!(naks[0].node, "good");
    h.finish();
}

#[test]
fn a_record_without_an_id_logs_a_nak_at_source_with_no_record_and_no_trace() {
    let h = start(TO_SINK, 1);

    let probe = h.push(Record::from_json(r#"{"body": "x"}"#).expect("record parses"));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));

    let events = h.events();
    assert_eq!(events.len(), 1, "{events:?}");
    let nak = &events[0];
    assert_eq!(nak.kind, EventKind::Nak);
    assert_eq!(nak.record_id, None);
    assert_eq!(&*nak.tenant, TENANT);
    assert_eq!(nak.node, "source");
    assert_eq!(nak.reason, Some(FailureKind::MissingId));
    assert_eq!(nak.trace, None);
    h.finish();
}

#[test]
fn a_state_error_under_nak_logs_state_error_and_under_pass_logs_nothing() {
    let h = start(&DEDUPE.replace("POLICY", "nak"), 1);
    h.state.fail_all(true);
    let probe = h.push(body_record(10, "x"));
    assert!(matches!(probe.wait(WAIT), Some(AckOutcome::Nak(_))));
    let reasons: Vec<(EventKind, Option<FailureKind>)> =
        h.events().iter().map(|e| (e.kind, e.reason)).collect();
    assert_eq!(
        reasons,
        [
            (EventKind::StageError, Some(FailureKind::StateError)),
            (EventKind::Nak, Some(FailureKind::StateError)),
        ]
    );
    h.finish();

    let h = start(&DEDUPE.replace("POLICY", "pass"), 1);
    h.state.fail_all(true);
    let probe = h.push(body_record(11, "x"));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));
    assert_eq!(h.events(), []);
    h.finish();
}

#[test]
fn a_redelivery_is_one_event_with_its_delivery_count() {
    let h = start(TO_SINK, 1);

    let probe = h.push_delivery(body_record(12, "x"), 3);
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Ack));

    let events = h.events();
    assert_eq!(events.len(), 1, "{events:?}");
    let redelivery = &events[0];
    assert_eq!(redelivery.kind, EventKind::Redelivery);
    assert_eq!(redelivery.record_id, Some(RecordId(12)));
    assert_eq!(&*redelivery.tenant, TENANT);
    assert_eq!(redelivery.node, "source");
    assert_eq!(redelivery.reason, None);
    assert_eq!(redelivery.delivery_count, 3);
    assert_eq!(redelivery.trace, Some(trace_key(12).delivery_context(3)));
    h.finish();
}

#[test]
fn passing_and_dropped_records_log_nothing() {
    let h = start(TO_SINK, 1);

    let passed = h.push(body_record(13, "x"));
    let dropped = h.push(body_record(14, "y"));
    assert_eq!(passed.wait(WAIT), Some(AckOutcome::Ack));
    assert_eq!(dropped.wait(WAIT), Some(AckOutcome::Ack));

    assert_eq!(h.events(), []);
    assert_eq!(h.ids("out"), [13]);
    h.finish();
}

/// A test-only stage that panics, so the engine's containment path is observable.
struct Panics;

impl Stage for Panics {
    fn process(&self, _record: Record, _ctx: &Context<'_>) -> StageOutput {
        panic!("stage blew up");
    }
}

#[test]
fn a_panicking_stage_logs_a_stage_error_of_kind_panic() {
    let sinks = MemorySinks::new();
    let mut registry = common::registry(&sinks);
    registry.register_stage(
        "panics",
        |_: &NodeConfig| -> Result<Box<dyn Stage>, ConfigError> { Ok(Box::new(Panics)) },
    );
    let h = start_with(
        r#"
nodes:
  - id: boom
    type: panics
  - id: out
    type: sink.memory
"#,
        1,
        sinks,
        registry,
    );

    let probe = h.push(body_record(15, "x"));
    assert_eq!(probe.wait(WAIT), Some(AckOutcome::Nak(None)));

    let events = h.events();
    let summary: Vec<(EventKind, &str, Option<FailureKind>)> = events
        .iter()
        .map(|e| (e.kind, e.node.as_str(), e.reason))
        .collect();
    assert_eq!(
        summary,
        [
            (EventKind::StageError, "boom", Some(FailureKind::Panic)),
            (EventKind::Nak, "boom", Some(FailureKind::Panic)),
        ]
    );
    h.finish();
}
