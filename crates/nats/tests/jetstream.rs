//! Source and sink against a live JetStream server, through the `Source` and `Sink` traits
//! and the engine. Ignored by default; run with a server at `NATS_URL` (default
//! `nats://127.0.0.1:4222`, which `deploy/compose.yaml` provides):
//!
//!     cargo test -p fusion-nats --test jetstream -- --ignored

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_nats::jetstream::consumer::{AckPolicy, pull};
use async_nats::jetstream::{self, stream};
use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::engine::Engine;
use fusion_core::events::{EventKind, InMemoryEventLog};
use fusion_core::io::{FailureKind, Outgoing};
use fusion_core::memory::{MemorySinks, MemoryStateStore};
use fusion_core::meta::{IngestionTime, Meta, unix_nanos_now};
use fusion_core::metrics::{CounterMetric, HistogramMetric, InMemoryRecorder, Metrics};
use fusion_core::pipeline::Pipeline;
use fusion_core::record::{Record, RecordId};
use fusion_core::registry::Registry;
use fusion_core::signals::Signals;
use fusion_core::stage::{Context, Stage, StageError, StageOutput};
use fusion_core::trace::{InMemoryTraceSink, TraceKey, TraceSampling};
use fusion_nats::config::{SinkParams, SourceParams, url_from_env};
use fusion_nats::headers::{INGESTION_TIME, INGESTION_TIME_KIND, RECORD_ID, RECORD_KIND, TENANT};
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
        self.create_consumer(
            stream,
            pull::Config {
                durable_name: Some(name.to_owned()),
                ack_policy,
                ..consumer_config(5)
            },
        );
    }

    fn create_consumer(&self, stream: &stream::Stream, config: pull::Config) {
        self.rt
            .block_on(stream.create_consumer(config))
            .expect("consumer created");
    }

    fn create_stream_with(&self, config: stream::Config) -> stream::Stream {
        self.rt
            .block_on(self.js.create_stream(config))
            .expect("stream created")
    }

    /// Subscribe to `subject` on the core connection. Messages wait in the subscription
    /// until [`JetStreamClient::next_within`] reads them.
    fn subscribe(&self, subject: String) -> Subscription {
        let subscriber = self
            .rt
            .block_on(self.js.client().subscribe(subject))
            .expect("subscribed");
        Subscription {
            subscriber: Some(subscriber),
            runtime: self.rt.handle().clone(),
        }
    }

    /// The next message on `subscription` within `timeout`, if any.
    fn next_within(
        &self,
        subscription: &mut Subscription,
        timeout: Duration,
    ) -> Option<async_nats::Message> {
        let subscriber = subscription.subscriber.as_mut()?;
        self.rt
            .block_on(async { tokio::time::timeout(timeout, subscriber.next()).await })
            .ok()
            .flatten()
    }

    /// Every message on `stream`, in order, as payload bytes, subject and headers.
    fn messages(&self, stream: &str) -> Vec<(Vec<u8>, String, async_nats::HeaderMap)> {
        self.rt.block_on(async {
            let mut stream = self.js.get_stream(stream).await.expect("stream exists");
            let last = stream
                .info()
                .await
                .expect("stream info")
                .state
                .last_sequence;
            let mut found = Vec::new();
            for sequence in 1..=last {
                if let Ok(message) = stream.get_raw_message(sequence).await {
                    found.push((
                        message.payload.to_vec(),
                        message.subject.to_string(),
                        message.headers,
                    ));
                }
            }
            found
        })
    }

    fn publish_bytes(&self, subject: &str, headers: async_nats::HeaderMap, payload: &[u8]) {
        self.rt
            .block_on(async {
                self.js
                    .publish_with_headers(subject.to_owned(), headers, payload.to_vec().into())
                    .await?
                    .await
            })
            .expect("published");
    }

    fn get_stream(&self, name: &str) -> stream::Stream {
        self.rt
            .block_on(self.js.get_stream(name))
            .expect("stream exists")
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

    /// Publish `payload` on `subject` as record `id`, the way a producer does.
    fn publish(&self, subject: &str, id: u64, payload: &str) {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(RECORD_ID, id.to_string().as_str());
        self.publish_with_headers(subject, headers, payload);
    }

    /// Publish `payload` on `subject` with `headers`.
    fn publish_with_headers(&self, subject: &str, headers: async_nats::HeaderMap, payload: &str) {
        self.rt
            .block_on(async {
                self.js
                    .publish_with_headers(subject.to_owned(), headers, payload.to_owned().into())
                    .await?
                    .await
            })
            .expect("published");
    }

    /// The first message on `stream`'s `subject`, its payload as a string and its headers, or
    /// `None` within `SETTLE_TIMEOUT`.
    fn first_message(
        &self,
        stream: &str,
        subject: &str,
    ) -> Option<(String, async_nats::HeaderMap)> {
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
            let payload = String::from_utf8(message.payload.to_vec()).ok()?;
            Some((payload, message.headers.clone().unwrap_or_default()))
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

/// A core subscription the test thread owns. Dropping a subscriber spawns its unsubscribe,
/// so it is dropped inside the runtime.
struct Subscription {
    subscriber: Option<async_nats::Subscriber>,
    runtime: tokio::runtime::Handle,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let _entered = self.runtime.enter();
        drop(self.subscriber.take());
    }
}

/// The consumer settings the compose stack uses, with `max_deliver` deliveries.
fn consumer_config(max_deliver: i64) -> pull::Config {
    pull::Config {
        durable_name: Some("pipeline".to_owned()),
        ack_policy: AckPolicy::Explicit,
        ack_wait: Duration::from_secs(30),
        max_deliver,
        ..Default::default()
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
    dlq_stream: String,
    consumer: String,
    tenant_prefix: String,
    dlq_prefix: String,
    out_subject: String,
}

impl Fixture {
    fn new(tag: &str) -> Self {
        Self::build(tag, 5, |_| {})
    }

    /// A fixture whose consumer delivers a message `max_deliver` times.
    fn with_max_deliver(tag: &str, max_deliver: i64) -> Self {
        Self::build(tag, max_deliver, |_| {})
    }

    /// A fixture whose consumer delivers `max_deliver` times and whose dead-letter stream is
    /// `dlq` adjusted by `adjust_dlq`.
    fn build(tag: &str, max_deliver: i64, adjust_dlq: impl FnOnce(&mut stream::Config)) -> Self {
        let client = JetStreamClient::connect();
        let in_stream = unique(&format!("LOGS_{tag}"));
        let out_stream = unique(&format!("PROCESSED_{tag}"));
        let dlq_stream = unique(&format!("DLQ_{tag}"));
        let consumer = "pipeline".to_owned();
        let tenant_prefix = unique("logs").to_ascii_lowercase();
        let dlq_prefix = unique("dlq").to_ascii_lowercase();
        let out_subject = format!("{}.out", unique("processed").to_ascii_lowercase());
        let input = client.create_stream(&in_stream, &[&format!("{tenant_prefix}.>")]);
        client.create_consumer(&input, consumer_config(max_deliver));
        client.create_stream(&out_stream, &[&out_subject]);
        let mut dlq = stream::Config {
            name: dlq_stream.clone(),
            subjects: vec![format!("{dlq_prefix}.>")],
            duplicate_window: Duration::from_secs(120),
            ..Default::default()
        };
        adjust_dlq(&mut dlq);
        client.create_stream_with(dlq);
        Self {
            client,
            in_stream,
            out_stream,
            dlq_stream,
            consumer,
            tenant_prefix,
            dlq_prefix,
            out_subject,
        }
    }

    fn source_params(&self) -> SourceParams {
        SourceParams {
            url: Some(url()),
            stream: self.in_stream.clone(),
            consumer: self.consumer.clone(),
            tenant_prefix: self.tenant_prefix.clone(),
            dlq_prefix: self.dlq_prefix.clone(),
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

    fn dlq_subject(&self, tenant: &str) -> String {
        format!("{}.{tenant}", self.dlq_prefix)
    }

    /// Subscribe to the consumer's JetStream advisory `kind` (`MSG_TERMINATED`,
    /// `MAX_DELIVERIES`).
    fn advisories(&self, kind: &str) -> Subscription {
        self.client.subscribe(format!(
            "$JS.EVENT.ADVISORY.CONSUMER.{kind}.{}.{}",
            self.in_stream, self.consumer
        ))
    }

    /// Every dead letter so far: payload, subject and headers.
    fn dead_letters(&self) -> Vec<(Vec<u8>, String, async_nats::HeaderMap)> {
        self.client.messages(&self.dlq_stream)
    }

    /// Start `yaml` (compiled with `sink.memory` on `sinks` and the NATS types) on this
    /// fixture's source, one worker.
    fn start(
        &self,
        nats: &std::sync::Arc<Nats>,
        yaml: &str,
        sinks: &MemorySinks,
        signals: impl Into<Signals>,
    ) -> Engine {
        let mut registry = Registry::new();
        nats.register(&mut registry);
        registry.register_sink("sink.memory", sinks.clone());
        registry.register_stage("fails_first", fails_first);
        let pipeline = Pipeline::from_yaml(yaml, &registry).expect("pipeline loads");
        let source = nats.source(&self.source_params()).expect("source builds");
        Engine::start(pipeline, Box::new(source), 1, signals, no_state()).expect("engine starts")
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
        self.client.try_delete_stream(&self.dlq_stream);
    }
}

/// A stage that fails a record's first delivery and passes every later one.
struct FailsFirst;

impl Stage for FailsFirst {
    fn process(&self, record: Record, ctx: &Context<'_>) -> StageOutput {
        if ctx.meta.delivery_count == 1 {
            StageOutput::Error(StageError::new("first delivery"))
        } else {
            StageOutput::Pass(record)
        }
    }
}

fn fails_first(_: &NodeConfig) -> Result<Box<dyn Stage>, ConfigError> {
    Ok(Box::new(FailsFirst))
}

const TO_MEMORY: &str = "nodes:\n  - id: out\n    type: sink.memory\n";

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn sink_write_returns_once_the_record_is_in_the_stream() {
    let fixture = Fixture::new("sink");
    let nats = Nats::new(Metrics::noop()).expect("nats runtime");
    let sink = nats.sink(&fixture.sink_params()).expect("sink builds");
    let record = Record::from_json(r#"{"id": 7, "body": "hello"}"#).expect("record parses");

    let meta = Meta {
        record_id: RecordId(7),
        tenant: "acme".into(),
        ingestion_time: IngestionTime::Reported(9_000_000_000),
        delivery_count: 1,
    };
    fusion_core::io::Sink::write(
        &sink,
        &[Outgoing {
            meta: &meta,
            record: &record,
        }],
    )
    .expect("write acked");

    let (payload, headers) = fixture
        .client
        .first_message(&fixture.out_stream, &fixture.out_subject)
        .expect("record is in the sink stream");
    assert_eq!(
        Record::from_json(&payload).expect("sink emits a record"),
        record,
        "the record is published exactly as written"
    );
    let header = |name: &str| headers.get(name).map(|v| v.as_str().to_owned());
    assert_eq!(header(RECORD_ID).as_deref(), Some("7"));
    assert_eq!(header(TENANT).as_deref(), Some("acme"));
    assert_eq!(header(INGESTION_TIME).as_deref(), Some("9000000000"));
    assert_eq!(header(INGESTION_TIME_KIND).as_deref(), Some("reported"));
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

/// Source into the engine into an in-memory sink: the record arrives exactly as published,
/// its `Meta` carries the subject's tenant and the publish time, and the message is acked once
/// the sink has it.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn source_writes_nothing_into_the_record_and_acks_after_the_sink() {
    let fixture = Fixture::new("ack");
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

    let published = [
        r#"{"id": 42, "body": "no tenant here"}"#,
        r#"{"id": 43, "body": "dated", "observed_time_unix_nano": 5}"#,
        r#"{"id": 44, "body": "event timed", "time_unix_nano": 7, "resource": {"tenant.id": "beta"}}"#,
    ];
    for (id, payload) in (42..).zip(published) {
        fixture
            .client
            .publish(&fixture.in_subject("acme"), id, payload);
    }

    assert!(
        wait_until(SETTLE_TIMEOUT, || sinks.outgoing("out").len() == 3),
        "records reach the memory sink"
    );
    let mut written = sinks.outgoing("out");
    written.sort_by_key(|w| w.meta.record_id);
    let now = unix_nanos_now();
    for ((w, payload), id) in written.iter().zip(published).zip(42..) {
        assert_eq!(
            w.meta.record_id,
            RecordId(id),
            "the record id is the header's"
        );
        assert_eq!(
            w.record,
            Record::from_json(payload).expect("record parses"),
            "the record is exactly as published"
        );
        assert_eq!(&*w.meta.tenant, "acme", "the subject's tenant wins");
        let IngestionTime::Reported(time) = w.meta.ingestion_time else {
            panic!("the publish time is reported: {:?}", w.meta.ingestion_time);
        };
        assert!(
            time <= now && now - time < 60_000_000_000,
            "the publish time {time} is recent, not the record's own"
        );
    }

    assert!(
        wait_until(SETTLE_TIMEOUT, || fixture.consumer_settled()),
        "consumer shows the message acknowledged"
    );
    assert_eq!(fixture.consumer_info().num_redelivered, 0);

    nats.shutdown();
    engine.join().expect("clean shutdown");
}

/// Fails a record's first delivery, noting its `Meta` ingestion time in `first`, and on a
/// later one writes the context's `Meta` into `attributes.meta.*` so the sink shows what
/// the source filled in.
struct RevealOnRedelivery {
    first: Arc<AtomicU64>,
}

impl Stage for RevealOnRedelivery {
    fn process(&self, mut record: Record, ctx: &Context<'_>) -> StageOutput {
        let meta = ctx.meta;
        if meta.delivery_count == 1 {
            self.first
                .store(meta.ingestion_time.unix_nanos(), Ordering::SeqCst);
            return StageOutput::Error(StageError::new("first delivery"));
        }
        for (key, value) in [
            ("meta.tenant", serde_json::json!(&*meta.tenant)),
            (
                "meta.ingestion_time",
                serde_json::json!(meta.ingestion_time.unix_nanos()),
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
    let first = Arc::new(AtomicU64::new(0));
    let first_seen = Arc::clone(&first);
    registry.register_stage(
        "reveal",
        move |_: &NodeConfig| -> Result<Box<dyn Stage>, ConfigError> {
            Ok(Box::new(RevealOnRedelivery {
                first: Arc::clone(&first_seen),
            }))
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
        42,
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
    assert_eq!(record.observed_time_unix_nano, None, "nothing is stamped");
    let first = first.load(Ordering::SeqCst);
    let now = unix_nanos_now();
    assert!(
        first > 0 && first <= now && now - first < 60_000_000_000,
        "the first delivery saw the publish time: {first}"
    );
    assert_eq!(
        attr("ingestion_time"),
        serde_json::json!(first),
        "the redelivery sees the same publish time as the first delivery"
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
        43,
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
            recorder.counter(CounterMetric::SourceRedeliveries, &[("tenant", "acme")]) > 0
        }),
        "the source counted the redelivery for tenant acme"
    );
    assert!(
        recorder.counter(CounterMetric::SourceNaks, &[("tenant", "acme")]) > 0,
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

    fixture.client.publish_with_headers(
        &fixture.in_subject("acme"),
        async_nats::HeaderMap::new(),
        "this is not json",
    );
    fixture.client.publish(
        &fixture.in_subject("acme"),
        44,
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

/// A message whose `Fusion-Record-Kind` is not `log` is acked without being walked, id or
/// not, and the log behind it reaches the sink.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn a_message_the_transport_says_is_not_a_log_is_acked_and_never_walked() {
    let fixture = Fixture::new("kind");
    let nats = Nats::new(Metrics::noop()).expect("nats runtime");
    let sinks = MemorySinks::new();
    let mut registry = Registry::new();
    registry.register_sink("sink.memory", sinks.clone());
    let pipeline = Pipeline::from_yaml(TO_MEMORY, &registry).expect("pipeline loads");
    let source = nats
        .source(&fixture.source_params())
        .expect("source builds");
    let engine = Engine::start(pipeline, Box::new(source), 1, Metrics::noop(), no_state())
        .expect("engine starts");

    let mut metric = async_nats::HeaderMap::new();
    metric.insert(RECORD_ID, "45");
    metric.insert(RECORD_KIND, "metric");
    fixture.client.publish_with_headers(
        &fixture.in_subject("acme"),
        metric,
        r#"{"id": 45, "kind": "log", "body": "a metric, whatever the payload says"}"#,
    );
    let mut span = async_nats::HeaderMap::new();
    span.insert(RECORD_KIND, "span");
    fixture.client.publish_with_headers(
        &fixture.in_subject("acme"),
        span,
        r#"{"body": "a span with no id"}"#,
    );
    fixture.client.publish(
        &fixture.in_subject("acme"),
        46,
        r#"{"id": 46, "kind": "metric", "body": "a log, whatever the payload says"}"#,
    );

    assert!(
        wait_until(SETTLE_TIMEOUT, || fixture.consumer_settled()),
        "every message is acknowledged"
    );
    let written = sinks.outgoing("out");
    assert_eq!(written.len(), 1, "only the log is walked");
    assert_eq!(written[0].meta.record_id, RecordId(46));
    assert_eq!(
        fixture.consumer_info().num_redelivered,
        0,
        "nothing is nak'd"
    );

    nats.shutdown();
    engine.join().expect("clean shutdown");
}

/// Two pipelines in a row over NATS: the first publishes to a subject that names no tenant,
/// and the second takes the tenant and the first pipeline's ingestion time from the pipeline
/// headers, while the record itself is never written to by either.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn a_downstream_pipeline_takes_the_tenant_and_the_first_ingestion_time_from_the_headers() {
    let fixture = Fixture::new("chain");
    let upstream_nats = std::sync::Arc::new(Nats::new(Metrics::noop()).expect("nats runtime"));
    let mut upstream_registry = Registry::new();
    upstream_nats.register(&mut upstream_registry);
    let upstream_yaml = format!(
        "nodes:\n  - id: out\n    type: sink.nats\n    url: {}\n    stream: {}\n    subject: {}\n",
        url(),
        fixture.out_stream,
        fixture.out_subject
    );
    let upstream = Engine::start(
        Pipeline::from_yaml(&upstream_yaml, &upstream_registry).expect("upstream loads"),
        Box::new(
            upstream_nats
                .source(&fixture.source_params())
                .expect("upstream source"),
        ),
        1,
        Metrics::noop(),
        no_state(),
    )
    .expect("upstream starts");

    let out = fixture.client.get_stream(&fixture.out_stream);
    fixture
        .client
        .create_pull_consumer(&out, "downstream", AckPolicy::Explicit);
    let downstream_nats = Nats::new(Metrics::noop()).expect("nats runtime");
    let sinks = MemorySinks::new();
    let mut registry = Registry::new();
    registry.register_sink("sink.memory", sinks.clone());
    let downstream = Engine::start(
        Pipeline::from_yaml("nodes:\n  - id: out\n    type: sink.memory\n", &registry)
            .expect("downstream loads"),
        Box::new(
            downstream_nats
                .source(&SourceParams {
                    url: Some(url()),
                    stream: fixture.out_stream.clone(),
                    consumer: "downstream".to_owned(),
                    tenant_prefix: fusion_nats::config::DEFAULT_TENANT_PREFIX.to_owned(),
                    dlq_prefix: fixture.dlq_prefix.clone(),
                })
                .expect("downstream source"),
        ),
        1,
        Metrics::noop(),
        no_state(),
    )
    .expect("downstream starts");

    let payload = r#"{"id": 42, "body": "two hops", "observed_time_unix_nano": 5}"#;
    fixture
        .client
        .publish(&fixture.in_subject("acme"), 42, payload);

    assert!(
        wait_until(SETTLE_TIMEOUT, || sinks.outgoing("out").len() == 1),
        "the record crosses both pipelines"
    );
    let (_, upstream_headers) = fixture
        .client
        .first_message(&fixture.out_stream, &fixture.out_subject)
        .expect("upstream published");
    let upstream_time: u64 = upstream_headers
        .get(INGESTION_TIME)
        .expect("time header")
        .as_str()
        .parse()
        .expect("decimal");
    let written = &sinks.outgoing("out")[0];
    assert_eq!(
        written.record,
        Record::from_json(payload).expect("record parses"),
        "neither pipeline wrote into the record"
    );
    assert_eq!(
        upstream_headers.get(RECORD_ID).map(|v| v.as_str()),
        Some("42"),
        "the upstream sink carries the record id"
    );
    assert_eq!(
        written.meta.record_id,
        RecordId(42),
        "from Fusion-Record-Id"
    );
    assert_eq!(&*written.meta.tenant, "acme", "from Fusion-Tenant");
    assert_eq!(
        written.meta.ingestion_time,
        IngestionTime::Reported(upstream_time),
        "the first pipeline's ingestion time, not the second publish time"
    );

    upstream_nats.shutdown();
    upstream.join().expect("upstream shuts down");
    downstream_nats.shutdown();
    downstream.join().expect("downstream shuts down");
}

/// A producer cannot move its record to another tenant with a header, and a header that does
/// not parse is counted and ignored without a nak.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn the_subject_beats_a_spoofed_tenant_header_and_a_bad_header_is_counted_not_nakked() {
    let fixture = Fixture::new("spoof");
    let recorder = InMemoryRecorder::new();
    let nats = Nats::new(Metrics::new(recorder.clone())).expect("nats runtime");
    let sinks = MemorySinks::new();
    let mut registry = Registry::new();
    registry.register_sink("sink.memory", sinks.clone());
    let engine = Engine::start(
        Pipeline::from_yaml("nodes:\n  - id: out\n    type: sink.memory\n", &registry)
            .expect("pipeline loads"),
        Box::new(nats.source(&fixture.source_params()).expect("source")),
        1,
        Metrics::noop(),
        no_state(),
    )
    .expect("engine starts");

    let mut headers = async_nats::HeaderMap::new();
    headers.insert(RECORD_ID, "42");
    headers.insert(TENANT, "beta");
    headers.insert(INGESTION_TIME, "soon");
    headers.insert(INGESTION_TIME_KIND, "reported");
    fixture.client.publish_with_headers(
        &fixture.in_subject("acme"),
        headers,
        r#"{"id": 42, "body": "spoofed"}"#,
    );

    assert!(
        wait_until(SETTLE_TIMEOUT, || sinks.outgoing("out").len() == 1),
        "the record is walked"
    );
    let written = &sinks.outgoing("out")[0];
    assert_eq!(&*written.meta.tenant, "acme");
    assert!(
        matches!(written.meta.ingestion_time, IngestionTime::Reported(t) if t > 1_000),
        "the publish time stands: {:?}",
        written.meta.ingestion_time
    );
    assert_eq!(
        recorder.counter(CounterMetric::SourceInvalidHeaders, &[("tenant", "acme")]),
        1
    );
    assert!(
        wait_until(SETTLE_TIMEOUT, || fixture.consumer_settled()),
        "acked, not nakked"
    );
    assert_eq!(fixture.consumer_info().num_redelivered, 0);

    nats.shutdown();
    engine.join().expect("clean shutdown");
}

/// A message whose subject names no tenant and which has no `Fusion-Tenant` header is
/// `unknown`, whatever `resource.tenant.id` its payload carries: the pipeline never reads its
/// tenant from the record.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn a_payload_tenant_is_never_read_when_the_transport_names_none() {
    let fixture = Fixture::new("notenant");
    let recorder = InMemoryRecorder::new();
    let nats = Nats::new(Metrics::noop()).expect("nats runtime");
    let sinks = MemorySinks::new();
    let mut registry = Registry::new();
    registry.register_sink("sink.memory", sinks.clone());
    let engine = Engine::start(
        Pipeline::from_yaml("nodes:\n  - id: out\n    type: sink.memory\n", &registry)
            .expect("pipeline loads"),
        Box::new(nats.source(&fixture.source_params()).expect("source")),
        1,
        Metrics::new(recorder.clone()),
        no_state(),
    )
    .expect("engine starts");

    // `{prefix}.acme` has no token after the tenant, so it names none.
    let payload = r#"{"id": 42, "body": "who", "resource": {"tenant.id": "acme"}}"#;
    fixture
        .client
        .publish(&format!("{}.acme", fixture.tenant_prefix), 42, payload);

    assert!(
        wait_until(SETTLE_TIMEOUT, || sinks.outgoing("out").len() == 1),
        "the record is walked"
    );
    let written = &sinks.outgoing("out")[0];
    assert_eq!(&*written.meta.tenant, "unknown");
    assert_eq!(
        written.record,
        Record::from_json(payload).expect("record parses"),
        "the payload keeps its own tenant, untouched"
    );
    assert_eq!(
        recorder.counter(
            CounterMetric::RecordsIn,
            &[("tenant", "acme"), ("stage", "out")]
        ),
        0
    );

    nats.shutdown();
    engine.join().expect("clean shutdown");
}

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn source_fails_fast_when_the_dlq_stream_is_missing_or_does_not_cover_the_prefix() {
    let fixture = Fixture::new("dlq_missing");
    let nats = Nats::new(Metrics::noop()).expect("nats runtime");

    let nowhere = SourceParams {
        dlq_prefix: unique("nowhere").to_ascii_lowercase(),
        ..fixture.source_params()
    };
    let err = nats.source(&nowhere).expect_err("no dead-letter stream");
    assert!(
        matches!(err, NatsError::DeadLetterStreamMissing { .. }),
        "{err}"
    );
    assert!(err.to_string().contains(&nowhere.dlq_prefix), "{err}");

    let narrow_prefix = unique("narrow").to_ascii_lowercase();
    let narrow = unique("DLQ_NARROW");
    fixture
        .client
        .create_stream(&narrow, &[&format!("{narrow_prefix}.acme")]);
    let one_tenant = SourceParams {
        dlq_prefix: narrow_prefix,
        ..fixture.source_params()
    };
    let err = nats.source(&one_tenant);
    fixture.client.delete_stream(&narrow);
    let err = err.expect_err("a stream for one tenant only");
    assert!(
        matches!(err, NatsError::DeadLetterNotCovered { .. }),
        "{err}"
    );
    assert!(err.to_string().contains(&narrow), "{err}");
}

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn source_fails_fast_when_the_consumer_has_no_max_deliver() {
    let fixture = Fixture::new("unbounded");
    let nats = Nats::new(Metrics::noop()).expect("nats runtime");
    let input = fixture.client.get_stream(&fixture.in_stream);
    for (name, max_deliver) in [("forever", -1), ("unset", 0)] {
        fixture.client.create_consumer(
            &input,
            pull::Config {
                durable_name: Some(name.to_owned()),
                ..consumer_config(max_deliver)
            },
        );
        let params = SourceParams {
            consumer: name.to_owned(),
            ..fixture.source_params()
        };

        let err = nats
            .source(&params)
            .expect_err("unbounded delivery rejected");

        assert!(
            matches!(err, NatsError::UnboundedDelivery { .. }),
            "{name}: {err}"
        );
        assert!(err.to_string().contains(name), "{err}");
    }
}

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn source_fails_fast_when_the_consumer_sets_backoff() {
    let fixture = Fixture::new("backoff");
    let nats = Nats::new(Metrics::noop()).expect("nats runtime");
    let input = fixture.client.get_stream(&fixture.in_stream);
    fixture.client.create_consumer(
        &input,
        pull::Config {
            durable_name: Some("backs_off".to_owned()),
            backoff: vec![Duration::from_secs(1), Duration::from_secs(2)],
            ..consumer_config(5)
        },
    );
    let params = SourceParams {
        consumer: "backs_off".to_owned(),
        ..fixture.source_params()
    };

    let err = nats.source(&params).expect_err("backoff rejected");

    assert!(matches!(err, NatsError::ConsumerBackoff { .. }), "{err}");
    assert!(err.to_string().contains("backs_off"), "{err}");
}

/// Issue #10: a message that fails every delivery is published, as it arrived, to its
/// tenant's dead-letter subject with the failure named, then terminated; the earlier
/// deliveries publish nothing, and JetStream delivers it no more.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn a_message_that_fails_every_delivery_is_dead_lettered_and_terminated() {
    let fixture = Fixture::with_max_deliver("dlq", 3);
    let recorder = InMemoryRecorder::new();
    let events = InMemoryEventLog::new();
    let traces = InMemoryTraceSink::new();
    let signals = Signals::new(Metrics::new(recorder.clone()))
        .with_events(events.clone())
        .with_traces(traces.clone(), TraceSampling::default());
    let nats = std::sync::Arc::new(Nats::new(signals.clone()).expect("nats runtime"));
    let sinks = MemorySinks::new();
    sinks.fail_writes_to("out");
    let mut terminated = fixture.advisories("MSG_TERMINATED");
    let mut exhausted = fixture.advisories("MAX_DELIVERIES");
    let engine = fixture.start(&nats, TO_MEMORY, &sinks, signals);
    let payload = br#"{"id": 50, "body": "never lands",   "extra": [1, 2]}"#;
    let mut produced = async_nats::HeaderMap::new();
    produced.insert(RECORD_ID, "50");
    produced.insert("traceparent", "00-abc-def-01");

    fixture
        .client
        .publish_bytes(&fixture.in_subject("acme"), produced, payload);

    let advisory = fixture
        .client
        .next_within(&mut terminated, SETTLE_TIMEOUT * 2)
        .expect("the message is terminated");
    let advisory: serde_json::Value =
        serde_json::from_slice(&advisory.payload).expect("advisory is JSON");
    assert_eq!(advisory["deliveries"], 3, "{advisory}");
    assert_eq!(advisory["reason"], "dlq out: sink_error", "{advisory}");

    let letters = fixture.dead_letters();
    assert_eq!(letters.len(), 1, "only the final delivery dead-letters");
    let (bytes, subject, headers) = &letters[0];
    assert_eq!(bytes.as_slice(), payload, "the payload as it arrived");
    assert_eq!(subject, &fixture.dlq_subject("acme"));
    let header = |name: &str| headers.get(name).map(|v| v.as_str().to_owned());
    assert_eq!(
        header("Fusion-Dlq-Reason").as_deref(),
        Some("out: sink write failed: memory sink `out` is set to fail")
    );
    assert_eq!(
        header("Fusion-Dlq-Subject"),
        Some(fixture.in_subject("acme"))
    );
    assert_eq!(
        header(RECORD_ID).as_deref(),
        Some("50"),
        "a replay keeps its id"
    );
    assert_eq!(header(TENANT).as_deref(), Some("acme"));
    assert_eq!(header(INGESTION_TIME_KIND).as_deref(), Some("reported"));
    assert_eq!(header("traceparent").as_deref(), Some("00-abc-def-01"));

    assert!(
        fixture
            .client
            .next_within(&mut exhausted, Duration::from_secs(1))
            .is_none(),
        "a terminated message never runs out of deliveries"
    );
    assert!(fixture.consumer_settled(), "{:?}", fixture.consumer_info());
    assert_eq!(
        recorder.counter(
            CounterMetric::Dlq,
            &[
                ("tenant", "acme"),
                ("stage", "out"),
                ("reason", "sink_error")
            ]
        ),
        1
    );
    assert_eq!(
        recorder
            .samples(HistogramMetric::DlqPublishDuration, &[("tenant", "acme")])
            .len(),
        1
    );
    let letters = events.of_kind(EventKind::DeadLetter);
    assert_eq!(letters.len(), 1, "{letters:?}");
    let letter = &letters[0];
    assert_eq!(letter.record_id, Some(RecordId(50)));
    assert_eq!(&*letter.tenant, "acme");
    assert_eq!(letter.node, "out");
    assert_eq!(letter.failure, Some(FailureKind::SinkError));
    assert_eq!(letter.delivery_count, 3);
    assert!(letter.stream_sequence.is_some());
    assert!(letter.message.contains("set to fail"), "{}", letter.message);
    let context = TraceKey::new(RecordId(50), "acme").delivery_context(3);
    assert_eq!(letter.trace, Some(context));
    assert!(
        traces
            .traces()
            .iter()
            .any(|t| t.trace_id == context.trace_id && t.span_id == context.span_id),
        "the dead letter names the final delivery's kept trace"
    );
    assert_eq!(
        events.of_kind(EventKind::Nak).len(),
        3,
        "the engine logs every nak"
    );
    assert_eq!(events.of_kind(EventKind::Redelivery).len(), 2);
    // Past the longest nak delay the message could still be waiting out: no fourth try.
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(
        recorder.counter(
            CounterMetric::RecordsIn,
            &[("tenant", "acme"), ("stage", "out")]
        ),
        3
    );

    nats.shutdown();
    engine.join().expect("clean shutdown");
}

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn an_undecodable_payload_is_dead_lettered_after_max_deliver() {
    let fixture = Fixture::with_max_deliver("dlq_garbage", 2);
    let recorder = InMemoryRecorder::new();
    let metrics = Metrics::new(recorder.clone());
    let nats = std::sync::Arc::new(Nats::new(metrics.clone()).expect("nats runtime"));
    let sinks = MemorySinks::new();
    let mut terminated = fixture.advisories("MSG_TERMINATED");
    let engine = fixture.start(&nats, TO_MEMORY, &sinks, metrics);

    fixture.client.publish_with_headers(
        &fixture.in_subject("acme"),
        async_nats::HeaderMap::new(),
        "this is not json",
    );

    assert!(
        fixture
            .client
            .next_within(&mut terminated, SETTLE_TIMEOUT)
            .is_some(),
        "the garbage is terminated"
    );
    let letters = fixture.dead_letters();
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].0, b"this is not json");
    let reason = letters[0]
        .2
        .get("Fusion-Dlq-Reason")
        .map(|v| v.as_str().to_owned());
    assert!(
        reason.as_deref().is_some_and(|r| r.starts_with("source: ")),
        "{reason:?}"
    );
    assert_eq!(
        recorder.counter(
            CounterMetric::Dlq,
            &[
                ("tenant", "acme"),
                ("stage", "source"),
                ("reason", "undecodable")
            ]
        ),
        1
    );

    nats.shutdown();
    engine.join().expect("clean shutdown");
}

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn a_failed_delivery_that_is_not_the_last_publishes_nothing_and_a_retry_succeeds() {
    let fixture = Fixture::with_max_deliver("dlq_retry", 2);
    let nats = std::sync::Arc::new(Nats::new(Metrics::noop()).expect("nats runtime"));
    let sinks = MemorySinks::new();
    let yaml = "nodes:\n  - id: flaky\n    type: fails_first\n  - id: out\n    type: sink.memory\n";
    let engine = fixture.start(&nats, yaml, &sinks, Metrics::noop());

    fixture.client.publish(
        &fixture.in_subject("acme"),
        51,
        r#"{"id": 51, "body": "second time"}"#,
    );

    assert!(
        wait_until(SETTLE_TIMEOUT, || !sinks.records("out").is_empty()),
        "the second delivery reaches the sink"
    );
    assert!(
        wait_until(SETTLE_TIMEOUT, || fixture.consumer_settled()),
        "{:?}",
        fixture.consumer_info()
    );
    assert!(fixture.dead_letters().is_empty());

    nats.shutdown();
    engine.join().expect("clean shutdown");
}

/// When the dead-letter publish keeps failing, the message is not terminated: its final nak
/// has no delay, so JetStream gives up on it at once and says so, and it stays in the
/// input stream to be found by its sequence.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn a_failed_dlq_publish_naks_without_delay_and_is_not_terminated() {
    let fixture = Fixture::build("dlq_refused", 3, |dlq| dlq.max_message_size = 16);
    let recorder = InMemoryRecorder::new();
    let events = InMemoryEventLog::new();
    let metrics = Metrics::new(recorder.clone());
    let nats = std::sync::Arc::new(
        Nats::new(Signals::new(metrics.clone()).with_events(events.clone())).expect("nats runtime"),
    );
    let sinks = MemorySinks::new();
    sinks.fail_writes_to("out");
    let mut terminated = fixture.advisories("MSG_TERMINATED");
    let mut exhausted = fixture.advisories("MAX_DELIVERIES");
    let engine = fixture.start(&nats, TO_MEMORY, &sinks, metrics);

    fixture.client.publish(
        &fixture.in_subject("acme"),
        52,
        r#"{"id": 52, "body": "far larger than the dead-letter stream takes"}"#,
    );

    assert!(
        wait_until(SETTLE_TIMEOUT * 2, || {
            recorder.counter(CounterMetric::DlqPublishErrors, &[("tenant", "acme")]) == 1
        }),
        "the dead-letter publish failed"
    );
    let failed_at = Instant::now();
    assert!(
        fixture
            .client
            .next_within(&mut exhausted, SETTLE_TIMEOUT)
            .is_some(),
        "JetStream gives up on the message"
    );
    // The last nak would otherwise wait out `nak_delay(3)`, four seconds.
    assert!(
        failed_at.elapsed() < Duration::from_millis(1500),
        "{:?}",
        failed_at.elapsed()
    );
    assert!(
        fixture
            .client
            .next_within(&mut terminated, Duration::from_millis(500))
            .is_none(),
        "not terminated"
    );
    assert!(fixture.dead_letters().is_empty());
    assert_eq!(fixture.client.messages(&fixture.in_stream).len(), 1);
    assert_eq!(
        recorder.counter(
            CounterMetric::Dlq,
            &[
                ("tenant", "acme"),
                ("stage", "out"),
                ("reason", "sink_error")
            ]
        ),
        0
    );
    let timed = recorder.samples(HistogramMetric::DlqPublishDuration, &[("tenant", "acme")]);
    assert_eq!(timed.len(), 1, "one sample covers every try");
    // Four tries, with 250 ms, 500 ms and 1 s between them.
    assert!(timed[0] >= 1.75, "{timed:?}");
    let failed = events.of_kind(EventKind::DeadLetterFailed);
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(failed[0].record_id, Some(RecordId(52)));
    assert_eq!(failed[0].node, "out");
    assert!(failed[0].stream_sequence.is_some());
    assert!(events.of_kind(EventKind::DeadLetter).is_empty());

    nats.shutdown();
    engine.join().expect("clean shutdown");
}

/// A message dead-lettered twice (a retry of a publish the server stored but whose `PubAck`
/// was lost) is stored once: the dead letter's id is the message's stream and sequence.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn a_duplicate_dead_letter_is_stored_once() {
    let fixture = Fixture::with_max_deliver("dlq_dupe", 1);
    let recorder = InMemoryRecorder::new();
    let metrics = Metrics::new(recorder.clone());
    let nats = std::sync::Arc::new(Nats::new(metrics.clone()).expect("nats runtime"));
    let sinks = MemorySinks::new();
    sinks.fail_writes_to("out");
    let mut earlier = async_nats::HeaderMap::new();
    earlier.insert("Nats-Msg-Id", format!("{}:1", fixture.in_stream).as_str());
    fixture.client.publish_bytes(
        &fixture.dlq_subject("acme"),
        earlier,
        b"an earlier dead letter",
    );
    let mut terminated = fixture.advisories("MSG_TERMINATED");
    let engine = fixture.start(&nats, TO_MEMORY, &sinks, metrics);

    fixture.client.publish(
        &fixture.in_subject("acme"),
        53,
        r#"{"id": 53, "body": "again"}"#,
    );

    assert!(
        fixture
            .client
            .next_within(&mut terminated, SETTLE_TIMEOUT)
            .is_some(),
        "terminated: a duplicate is still durably stored"
    );
    let letters = fixture.dead_letters();
    assert_eq!(letters.len(), 1, "{letters:?}");
    assert_eq!(letters[0].0, b"an earlier dead letter");
    assert_eq!(
        recorder.counter(
            CounterMetric::Dlq,
            &[
                ("tenant", "acme"),
                ("stage", "out"),
                ("reason", "sink_error")
            ]
        ),
        1
    );

    nats.shutdown();
    engine.join().expect("clean shutdown");
}

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn two_tenants_dead_letter_to_their_own_subjects_in_one_stream() {
    let fixture = Fixture::with_max_deliver("dlq_tenants", 1);
    let nats = std::sync::Arc::new(Nats::new(Metrics::noop()).expect("nats runtime"));
    let sinks = MemorySinks::new();
    sinks.fail_writes_to("out");
    let engine = fixture.start(&nats, TO_MEMORY, &sinks, Metrics::noop());

    fixture.client.publish(
        &fixture.in_subject("acme"),
        54,
        r#"{"id": 54, "body": "a"}"#,
    );
    fixture.client.publish(
        &fixture.in_subject("beta"),
        55,
        r#"{"id": 55, "body": "b"}"#,
    );

    assert!(
        wait_until(SETTLE_TIMEOUT, || fixture.dead_letters().len() == 2),
        "{:?}",
        fixture.dead_letters()
    );
    let mut subjects: Vec<String> = fixture
        .dead_letters()
        .into_iter()
        .map(|(_, subject, _)| subject)
        .collect();
    subjects.sort();
    assert_eq!(
        subjects,
        [fixture.dlq_subject("acme"), fixture.dlq_subject("beta")]
    );

    nats.shutdown();
    engine.join().expect("clean shutdown");
}
