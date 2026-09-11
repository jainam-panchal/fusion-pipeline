//! The `dedupe` node through the trait boundary: YAML config in, envelopes pushed through
//! the in-memory source, assertions on which records reached the in-memory sink, how each
//! ack handle settled, what the state store holds and what the recorder counted.

mod common;

use fusion_core::memory::AckOutcome;
use fusion_core::metrics::Metric;
use fusion_core::record::Record;

use common::{WAIT, for_each_worker_count, start};

const DEDUPE_BODY: &str = r#"
name: ingest
nodes:
  - id: dedupe_body
    type: dedupe
    key: [body]
    window: 10s
  - id: out
    type: sink.memory
"#;

fn record(id: u64, body: &str) -> Record {
    Record::from_json(&format!(
        r#"{{"id": {id}, "body": "{body}", "resource": {{"tenant.id": "acme"}}}}"#
    ))
    .expect("record parses")
}

#[test]
fn two_different_records_with_the_same_key_inside_the_window_pass_once_and_drop_once() {
    for_each_worker_count(|workers| {
        let h = start(DEDUPE_BODY, workers);

        let first = h.source.push(record(101, "disk full"));
        assert_eq!(first.wait(WAIT), Some(AckOutcome::Ack), "workers={workers}");
        let second = h.source.push(record(102, "disk full"));
        assert_eq!(
            second.wait(WAIT),
            Some(AckOutcome::Ack),
            "workers={workers}"
        );

        assert_eq!(h.ids("out"), vec![101], "workers={workers}");
        assert_eq!(
            h.counter(
                Metric::RecordsDropped,
                &[
                    ("tenant", "acme"),
                    ("stage", "dedupe_body"),
                    ("reason", "dedupe")
                ]
            ),
            1,
            "workers={workers}"
        );
        h.finish();
    });
}
