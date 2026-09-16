//! Source and sink against a live JetStream server, through the `Source` and `Sink` traits
//! and the engine. Ignored by default; run with a server at `NATS_URL` (default
//! `nats://127.0.0.1:4222`, which `deploy/compose.yaml` provides):
//!
//!     cargo test -p fusion-nats --test jetstream -- --ignored

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_nats::jetstream::consumer::{AckPolicy, pull};
use async_nats::jetstream::{self, stream};
use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::engine::Engine;
use fusion_core::memory::{MemorySinks, MemoryStateStore};
use fusion_core::metrics::{InMemoryRecorder, Metric, Metrics};
use fusion_core::pipeline::Pipeline;
use fusion_core::record::Record;
use fusion_core::registry::Registry;
use fusion_core::stage::{Context, Stage, StageError, StageOutput};
use fusion_nats::config::{SinkParams, SourceParams, url_from_env};
use fusion_nats::{Nats, NatsError};
use futures::StreamExt;

const SETTLE_TIMEOUT: Duration = Duration::from_secs(10);

fn url() -> String {
    url_from_env(None)
}

/// A unique name per test so parallel tests never share a stream.
fn unique(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("{prefix}_{pid}_{n}_{nanos}")
}

/// Test-side JetStream client, independent of the crate under test.
struct JetStreamClient {
    rt: tokio::runtime::Runtime,
    js: jetstream::Context,
}

impl JetStreamClient {
    fn connect() -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let js = rt.block_on(async {
            let client = async_nats::connect(url())
                .await
                .expect("test can reach the NATS server at NATS_URL");
            jetstream::new(client)
        });
        Self { rt, js }
    }

    fn create_stream(&self, name: &str, subjects: &[&str]) -> stream::Stream {
        self.rt
            .block_on(self.js.create_stream(stream::Config {
                name: name.to_owned(),
                subjects: subjects.iter().map(|s| (*s).to_owned()).collect(),
                ..Default::default()
            }))
            .expect("stream created")
    }

    fn create_pull_consumer(&self, stream: &stream::Stream, name: &str, ack_policy: AckPolicy) {
        self.rt
            .block_on(stream.create_consumer(pull::Config {
                durable_name: Some(name.to_owned()),
                ack_policy,
                ack_wait: Duration::from_secs(30),
                max_deliver: 5,
                ..Default::default()
            }))
            .expect("consumer created");
    }

    fn delete_stream(&self, name: &str) {
        self.rt
            .block_on(self.js.delete_stream(name))
            .expect("stream deleted");
    }

    /// Best-effort cleanup: the stream may already be gone.
    fn try_delete_stream(&self, name: &str) {
        let _ = self.rt.block_on(self.js.delete_stream(name));
    }

    fn publish(&self, subject: &str, payload: &str) {
        self.rt
            .block_on(async {
                self.js
                    .publish(subject.to_owned(), payload.to_owned().into())
                    .await?
                    .await
            })
            .expect("published");
    }

    /// The first message on `stream`'s `subject`, as a string, or `None` within `SETTLE_TIMEOUT`.
    fn first_payload(&self, stream: &str, subject: &str) -> Option<String> {
        self.rt.block_on(async {
            let stream = self.js.get_stream(stream).await.expect("stream exists");
            let consumer = stream
                .create_consumer(pull::Config {
                    filter_subject: subject.to_owned(),
                    ..Default::default()
                })
                .await
                .expect("probe consumer");
            let mut messages = consumer.messages().await.expect("messages");
            let message = tokio::time::timeout(SETTLE_TIMEOUT, messages.next())
                .await
                .ok()??;
            let message = message.ok()?;
            String::from_utf8(message.payload.to_vec()).ok()
        })
    }

    fn consumer_info(&self, stream: &str, consumer: &str) -> jetstream::consumer::Info {
        self.rt.block_on(async {
            let stream = self.js.get_stream(stream).await.expect("stream exists");
            let mut consumer: jetstream::consumer::PullConsumer = stream
                .get_consumer(consumer)
                .await
                .expect("consumer exists");
            consumer.info().await.expect("consumer info").clone()
        })
    }
}

/// These pipelines have no stateful node; the engine never opens the store.
fn no_state() -> std::sync::Arc<MemoryStateStore> {
    std::sync::Arc::new(MemoryStateStore::new())
}

fn wait_until(timeout: Duration, mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    check()
}

struct Fixture {
    client: JetStreamClient,
    in_stream: String,
    out_stream: String,
    consumer: String,
    tenant_prefix: String,
    out_subject: String,
}

impl Fixture {
    fn new(tag: &str) -> Self {
        let client = JetStreamClient::connect();
        let in_stream = unique(&format!("LOGS_{tag}"));
        let out_stream = unique(&format!("PROCESSED_{tag}"));
        let consumer = "pipeline".to_owned();
        let tenant_prefix = unique("logs").to_ascii_lowercase();
        let out_subject = format!("{}.out", unique("processed").to_ascii_lowercase());
        let input = client.create_stream(&in_stream, &[&format!("{tenant_prefix}.>")]);
        client.create_pull_consumer(&input, &consumer, AckPolicy::Explicit);
        client.create_stream(&out_stream, &[&out_subject]);
        Self {
            client,
            in_stream,
            out_stream,
            consumer,
            tenant_prefix,
            out_subject,
        }
    }

    fn source_params(&self) -> SourceParams {
        SourceParams {
            url: Some(url()),
            stream: self.in_stream.clone(),
            consumer: self.consumer.clone(),
        }
    }

    fn sink_params(&self) -> SinkParams {
        SinkParams {
            url: Some(url()),
            stream: self.out_stream.clone(),
            subject: self.out_subject.clone(),
        }
    }

    fn in_subject(&self, tenant: &str) -> String {
        format!("{}.{tenant}.syslog", self.tenant_prefix)
    }

    fn consumer_info(&self) -> jetstream::consumer::Info {
        self.client.consumer_info(&self.in_stream, &self.consumer)
    }

    /// Whether every message the consumer has seen is acknowledged and none is waiting.
    fn consumer_settled(&self) -> bool {
        let info = self.consumer_info();
        info.num_ack_pending == 0 && info.num_pending == 0
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.client.try_delete_stream(&self.in_stream);
        self.client.try_delete_stream(&self.out_stream);
    }
}

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn sink_write_returns_once_the_record_is_in_the_stream() {
    let fixture = Fixture::new("sink");
    let nats = Nats::new(Metrics::noop()).expect("nats runtime");
    let sink = nats.sink(&fixture.sink_params()).expect("sink builds");
    let record = Record::from_json(r#"{"id": 7, "body": "hello"}"#).expect("record parses");

    fusion_core::io::Sink::write(&sink, std::slice::from_ref(&record)).expect("write acked");

    let payload = fixture
        .client
        .first_payload(&fixture.out_stream, &fixture.out_subject)
        .expect("record is in the sink stream");
    assert_eq!(
        Record::from_json(&payload).expect("sink emits a record"),
        record
    );
}

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn sink_fails_fast_when_its_stream_is_missing() {
    let nats = Nats::new(Metrics::noop()).expect("nats runtime");
    let params = SinkParams {
        url: Some(url()),
        stream: unique("MISSING"),
        subject: "processed.nowhere".to_owned(),
    };

    let err = nats.sink(&params).expect_err("missing stream rejected");

    assert!(matches!(err, NatsError::StreamMissing { .. }), "{err}");
    assert!(err.to_string().contains(&params.stream), "{err}");
}

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn sink_fails_fast_when_its_stream_does_not_capture_the_subject() {
    let fixture = Fixture::new("capture");
    let nats = Nats::new(Metrics::noop()).expect("nats runtime");
    let params = SinkParams {
        subject: "processed.elsewhere".to_owned(),
        ..fixture.sink_params()
    };

    let err = nats.sink(&params).expect_err("uncaptured subject rejected");

    assert!(matches!(err, NatsError::SubjectNotCaptured { .. }), "{err}");
    assert!(err.to_string().contains("processed.elsewhere"), "{err}");
    assert!(err.to_string().contains(&fixture.out_stream), "{err}");
}

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn source_fails_fast_when_stream_or_consumer_is_missing() {
    let fixture = Fixture::new("src_missing");
    let nats = Nats::new(Metrics::noop()).expect("nats runtime");

    let no_stream = SourceParams {
        stream: unique("MISSING"),
        ..fixture.source_params()
    };
    let err = nats
        .source(&no_stream)
        .expect_err("missing stream rejected");
    assert!(matches!(err, NatsError::StreamMissing { .. }), "{err}");
    assert!(err.to_string().contains(&no_stream.stream), "{err}");

    let no_consumer = SourceParams {
        consumer: "nobody".to_owned(),
        ..fixture.source_params()
    };
    let err = nats
        .source(&no_consumer)
        .expect_err("missing consumer rejected");
    assert!(matches!(err, NatsError::ConsumerMissing { .. }), "{err}");
    assert!(err.to_string().contains("nobody"), "{err}");
}

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn source_fails_fast_when_the_consumer_does_not_ack_explicitly() {
    let fixture = Fixture::new("ackpolicy");
    let nats = Nats::new(Metrics::noop()).expect("nats runtime");
    let input = fixture
        .client
        .rt
        .block_on(fixture.client.js.get_stream(&fixture.in_stream))
        .expect("stream exists");
    fixture
        .client
        .create_pull_consumer(&input, "fire_and_forget", AckPolicy::None);
    let params = SourceParams {
        consumer: "fire_and_forget".to_owned(),
        ..fixture.source_params()
    };

    let err = nats.source(&params).expect_err("ack policy none rejected");

    assert!(
        matches!(err, NatsError::ConsumerNotExplicitAck { .. }),
        "{err}"
    );
    assert!(err.to_string().contains("fire_and_forget"), "{err}");
    assert!(err.to_string().contains("none"), "{err}");
}

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn connect_fails_fast_when_the_server_is_unreachable() {
    let nats = Nats::new(Metrics::noop()).expect("nats runtime");
    let params = SinkParams {
        url: Some("nats://127.0.0.1:1".to_owned()),
        stream: "PROCESSED".to_owned(),
        subject: "processed.logs".to_owned(),
    };

    let err = nats.sink(&params).expect_err("unreachable server rejected");

    assert!(matches!(err, NatsError::Connect { .. }), "{err}");
    assert!(err.to_string().contains("127.0.0.1:1"), "{err}");
}

/// Source into the engine into an in-memory sink: the record arrives with the tenant stamped
/// from the subject and the JetStream publish time as its ingestion time, a record that
/// carries its own timestamp keeps it, and the consumer shows both acknowledged.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn source_stamps_tenant_from_subject_and_acks_after_the_sink() {
    let fixture = Fixture::new("src");
    let nats = Nats::new(Metrics::noop()).expect("nats runtime");
    let sinks = MemorySinks::new();
    let mut registry = Registry::new();
    registry.register_sink("sink.memory", sinks.clone());
    let pipeline = Pipeline::from_yaml("nodes:\n  - id: out\n    type: sink.memory\n", &registry)
        .expect("pipeline loads");
    let source = nats
        .source(&fixture.source_params())
        .expect("source builds");
    let engine = Engine::start(pipeline, Box::new(source), 2, Metrics::noop(), no_state())
        .expect("engine starts");

    fixture.client.publish(
        &fixture.in_subject("acme"),
        r#"{"id": 42, "body": "no tenant here"}"#,
    );
    fixture.client.publish(
        &fixture.in_subject("acme"),
        r#"{"id": 43, "body": "dated", "observed_time_unix_nano": 5}"#,
    );
    fixture.client.publish(
        &fixture.in_subject("acme"),
        r#"{"id": 44, "body": "event timed", "time_unix_nano": 7}"#,
    );

    assert!(
        wait_until(SETTLE_TIMEOUT, || sinks.records("out").len() == 3),
        "records reach the memory sink"
    );
    let mut records = sinks.records("out");
    records.sort_by_key(|r| r.id);
    assert_eq!(records[0].tenant(), Some("acme"));
    assert_eq!(records[0].id.map(|id| id.0), Some(42));
    let observed = records[0]
        .observed_time_unix_nano
        .expect("observed_time_unix_nano stamped from the JetStream publish time");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    assert!(
        u128::from(observed) <= now && now - u128::from(observed) < 60_000_000_000,
        "publish time {observed} is recent"
    );
    assert_eq!(
        records[1].observed_time_unix_nano,
        Some(5),
        "a record's own observed time stands"
    );
    assert_eq!(
        records[2].observed_time_unix_nano, None,
        "a record with an event time is not given an observed time"
    );

    assert!(
        wait_until(SETTLE_TIMEOUT, || fixture.consumer_settled()),
        "consumer shows the message acknowledged"
    );
    assert_eq!(fixture.consumer_info().num_redelivered, 0);

    nats.shutdown();
    engine.join().expect("clean shutdown");
}

/// Fails a record's first delivery, and on a later one writes the context's `Meta` into
/// `attributes.meta.*` so the sink shows what the source filled in.
struct RevealOnRedelivery;

impl Stage for RevealOnRedelivery {
    fn process(&self, mut record: Record, ctx: &Context<'_>) -> StageOutput {
        let meta = ctx.meta;
        if meta.delivery_count == 1 {
            return StageOutput::Error(StageError::new("first delivery"));
        }
        for (key, value) in [
            ("meta.tenant", serde_json::json!(&*meta.tenant)),
            (
                "meta.ingestion_time",
                serde_json::json!(meta.ingestion_time),
            ),
            (
                "meta.delivery_count",
                serde_json::json!(meta.delivery_count),
            ),
        ] {
            record.attributes.insert(key.to_owned(), value);
        }
        StageOutput::Pass(record)
    }
}

/// Source into the engine: the record's `Meta` carries the subject's tenant, the JetStream
/// publish time and the delivery count, and the publish time is the same on redelivery.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn source_fills_meta_with_the_subject_tenant_the_publish_time_and_the_delivery_count() {
    let fixture = Fixture::new("meta");
    let nats = Nats::new(Metrics::noop()).expect("nats runtime");
    let sinks = MemorySinks::new();
    let mut registry = Registry::new();
    registry.register_sink("sink.memory", sinks.clone());
    registry.register_stage(
        "reveal",
        |_: &NodeConfig| -> Result<Box<dyn Stage>, ConfigError> {
            Ok(Box::new(RevealOnRedelivery))
        },
    );
    let pipeline = Pipeline::from_yaml(
        "nodes:\n  - id: reveal\n    type: reveal\n  - id: out\n    type: sink.memory\n",
        &registry,
    )
    .expect("pipeline loads");
    let source = nats
        .source(&fixture.source_params())
        .expect("source builds");
    let engine = Engine::start(pipeline, Box::new(source), 1, Metrics::noop(), no_state())
        .expect("engine starts");

    fixture.client.publish(
        &fixture.in_subject("acme"),
        r#"{"id": 42, "body": "no tenant, no time"}"#,
    );

    // The first delivery is nakked with a 1 s delay; the second passes.
    assert!(
        wait_until(SETTLE_TIMEOUT, || sinks.records("out").len() == 1),
        "the redelivered record reaches the memory sink"
    );
    let record = &sinks.records("out")[0];
    let attr = |key: &str| record.attributes[&format!("meta.{key}")].clone();
    assert_eq!(attr("tenant"), serde_json::json!("acme"));
    assert_eq!(attr("delivery_count"), serde_json::json!(2));
    let stamped = record
        .observed_time_unix_nano
        .expect("the publish time is stamped into the payload too");
    assert_eq!(
        attr("ingestion_time"),
        serde_json::json!(stamped),
        "Meta's ingestion time is the publish time, unchanged by redelivery"
    );

    assert!(
        wait_until(SETTLE_TIMEOUT, || fixture.consumer_settled()),
        "consumer shows the message acknowledged"
    );
    nats.shutdown();
    engine.join().expect("clean shutdown");
}

/// Source into the NATS sink whose stream has been deleted: the write fails, the record is
/// nak'd, JetStream redelivers it, and the redelivery is counted for the record's tenant.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn sink_failure_naks_the_source_message_and_jetstream_redelivers() {
    let fixture = Fixture::new("nak");
    let recorder = InMemoryRecorder::new();
    let metrics = Metrics::new(recorder.clone());
    let nats = std::sync::Arc::new(Nats::new(metrics.clone()).expect("nats runtime"));
    let mut registry = Registry::new();
    nats.register(&mut registry);
    let yaml = format!(
        "nodes:\n  - id: out\n    type: sink.nats\n    url: {}\n    stream: {}\n    subject: {}\n",
        url(),
        fixture.out_stream,
        fixture.out_subject
    );
    let pipeline = Pipeline::from_yaml(&yaml, &registry).expect("pipeline loads");
    let source = nats
        .source(&fixture.source_params())
        .expect("source builds");
    let engine =
        Engine::start(pipeline, Box::new(source), 1, metrics, no_state()).expect("engine starts");

    fixture.client.delete_stream(&fixture.out_stream);
    fixture.client.publish(
        &fixture.in_subject("acme"),
        r#"{"id": 43, "body": "sink is gone"}"#,
    );

    assert!(
        wait_until(SETTLE_TIMEOUT * 3, || {
            fixture
                .client
                .consumer_info(&fixture.in_stream, &fixture.consumer)
                .num_redelivered
                > 0
        }),
        "consumer shows the message redelivered"
    );
    assert!(
        wait_until(SETTLE_TIMEOUT, || {
            recorder.counter(Metric::SourceRedeliveries, &[("tenant", "acme")]) > 0
        }),
        "the source counted the redelivery for tenant acme"
    );
    assert!(
        recorder.counter(Metric::SourceNaks, &[("tenant", "acme")]) > 0,
        "the engine counted the nak"
    );

    nats.shutdown();
    engine.join().expect("clean shutdown");
}

/// A payload that is not a record is nak'd and redelivered, and the source keeps serving
/// the next one meanwhile.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn undecodable_payload_is_nakd_and_the_source_keeps_going() {
    let fixture = Fixture::new("garbage");
    let nats = Nats::new(Metrics::noop()).expect("nats runtime");
    let sinks = MemorySinks::new();
    let mut registry = Registry::new();
    registry.register_sink("sink.memory", sinks.clone());
    let pipeline = Pipeline::from_yaml("nodes:\n  - id: out\n    type: sink.memory\n", &registry)
        .expect("pipeline loads");
    let source = nats
        .source(&fixture.source_params())
        .expect("source builds");
    let engine = Engine::start(pipeline, Box::new(source), 1, Metrics::noop(), no_state())
        .expect("engine starts");

    fixture
        .client
        .publish(&fixture.in_subject("acme"), "this is not json");
    fixture.client.publish(
        &fixture.in_subject("acme"),
        r#"{"id": 44, "body": "after garbage"}"#,
    );

    assert!(
        wait_until(SETTLE_TIMEOUT, || !sinks.records("out").is_empty()),
        "the record after the garbage reaches the sink"
    );
    assert_eq!(sinks.records("out")[0].id.map(|id| id.0), Some(44));
    assert!(
        wait_until(SETTLE_TIMEOUT, || fixture.consumer_info().num_redelivered
            > 0),
        "the garbage is redelivered"
    );

    nats.shutdown();
    engine.join().expect("clean shutdown");
}
