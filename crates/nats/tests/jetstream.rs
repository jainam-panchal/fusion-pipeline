//! Source and sink against a live JetStream server, through the `Source` and `Sink` traits
//! and the engine. Ignored by default; run with a server at `NATS_URL` (default
//! `nats://127.0.0.1:4222`, which `deploy/compose.yaml` provides):
//!
//!     cargo test -p fusion-nats --test jetstream -- --ignored

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_nats::jetstream::consumer::pull;
use async_nats::jetstream::{self, stream};
use fusion_core::engine::Engine;
use fusion_core::memory::MemorySinks;
use fusion_core::pipeline::Pipeline;
use fusion_core::record::Record;
use fusion_core::registry::Registry;
use fusion_nats::config::{SinkParams, SourceParams, url_from_env};
use fusion_nats::{Nats, NatsError};
use futures::StreamExt;

const WAIT: Duration = Duration::from_secs(10);

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

/// Test-side JetStream access, independent of the crate under test.
struct Server {
    rt: tokio::runtime::Runtime,
    js: jetstream::Context,
}

impl Server {
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

    fn create_pull_consumer(&self, stream: &stream::Stream, name: &str) {
        self.rt
            .block_on(stream.create_consumer(pull::Config {
                durable_name: Some(name.to_owned()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
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

    /// The first message on `stream`'s `subject`, as a string, or `None` within `WAIT`.
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
            let message = tokio::time::timeout(WAIT, messages.next()).await.ok()??;
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
    server: Server,
    in_stream: String,
    out_stream: String,
    consumer: String,
    tenant_prefix: String,
    out_subject: String,
}

impl Fixture {
    fn new(tag: &str) -> Self {
        let server = Server::connect();
        let in_stream = unique(&format!("LOGS_{tag}"));
        let out_stream = unique(&format!("PROCESSED_{tag}"));
        let consumer = "pipeline".to_owned();
        let tenant_prefix = unique("logs").to_ascii_lowercase();
        let out_subject = format!("{}.out", unique("processed").to_ascii_lowercase());
        let input = server.create_stream(&in_stream, &[&format!("{tenant_prefix}.>")]);
        server.create_pull_consumer(&input, &consumer);
        server.create_stream(&out_stream, &[&out_subject]);
        Self {
            server,
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
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self
            .server
            .rt
            .block_on(self.server.js.delete_stream(&self.in_stream));
        let _ = self
            .server
            .rt
            .block_on(self.server.js.delete_stream(&self.out_stream));
    }
}

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn sink_write_returns_once_the_record_is_in_the_stream() {
    let f = Fixture::new("sink");
    let nats = Nats::new().expect("nats runtime");
    let sink = nats.sink(&f.sink_params()).expect("sink builds");
    let record = Record::from_json(r#"{"id": 7, "body": "hello"}"#).expect("record parses");

    fusion_core::io::Sink::write(&sink, std::slice::from_ref(&record)).expect("write acked");

    let payload = f
        .server
        .first_payload(&f.out_stream, &f.out_subject)
        .expect("record is in the sink stream");
    assert_eq!(
        Record::from_json(&payload).expect("sink emits a record"),
        record
    );
}

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn sink_fails_fast_when_its_stream_is_missing() {
    let nats = Nats::new().expect("nats runtime");
    let params = SinkParams {
        url: Some(url()),
        stream: unique("MISSING"),
        subject: "processed.nowhere".to_owned(),
    };

    let err = match nats.sink(&params) {
        Ok(_) => panic!("missing stream must be rejected"),
        Err(err) => err,
    };

    assert!(matches!(err, NatsError::StreamMissing { .. }), "{err}");
    assert!(err.to_string().contains(&params.stream), "{err}");
}

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn source_fails_fast_when_stream_or_consumer_is_missing() {
    let f = Fixture::new("src_missing");
    let nats = Nats::new().expect("nats runtime");

    let no_stream = SourceParams {
        stream: unique("MISSING"),
        ..f.source_params()
    };
    let err = nats
        .source(&no_stream)
        .expect_err("missing stream rejected");
    assert!(matches!(err, NatsError::StreamMissing { .. }), "{err}");
    assert!(err.to_string().contains(&no_stream.stream), "{err}");

    let no_consumer = SourceParams {
        consumer: "nobody".to_owned(),
        ..f.source_params()
    };
    let err = nats
        .source(&no_consumer)
        .expect_err("missing consumer rejected");
    assert!(matches!(err, NatsError::ConsumerMissing { .. }), "{err}");
    assert!(err.to_string().contains("nobody"), "{err}");
}

#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn connect_fails_fast_when_the_server_is_unreachable() {
    let nats = Nats::new().expect("nats runtime");
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
/// from the subject and the consumer shows it acknowledged.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn source_stamps_tenant_from_subject_and_acks_after_the_sink() {
    let f = Fixture::new("src");
    let nats = Nats::new().expect("nats runtime");
    let sinks = MemorySinks::new();
    let mut registry = Registry::new();
    registry.register_sink("sink.memory", sinks.clone());
    let pipeline = Pipeline::from_yaml("nodes:\n  - id: out\n    type: sink.memory\n", &registry)
        .expect("pipeline loads");
    let source = nats.source(&f.source_params()).expect("source builds");
    let engine = Engine::start(pipeline, Box::new(source), 2).expect("engine starts");

    f.server.publish(
        &f.in_subject("acme"),
        r#"{"id": 42, "body": "no tenant here"}"#,
    );

    assert!(
        wait_until(WAIT, || !sinks.records("out").is_empty()),
        "record reaches the memory sink"
    );
    let records = sinks.records("out");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].tenant(), Some("acme"));
    assert_eq!(records[0].id.map(|id| id.0), Some(42));

    assert!(
        wait_until(WAIT, || {
            let info = f.server.consumer_info(&f.in_stream, &f.consumer);
            info.num_ack_pending == 0 && info.num_pending == 0
        }),
        "consumer shows the message acknowledged"
    );
    let info = f.server.consumer_info(&f.in_stream, &f.consumer);
    assert_eq!(info.num_redelivered, 0);

    nats.shutdown();
    engine.join().expect("clean shutdown");
}

/// Source into the NATS sink whose stream has been deleted: the write fails, the record is
/// nak'd, and JetStream redelivers it.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn sink_failure_naks_the_source_message_and_jetstream_redelivers() {
    let f = Fixture::new("nak");
    let nats = std::sync::Arc::new(Nats::new().expect("nats runtime"));
    let mut registry = Registry::new();
    nats.register(&mut registry);
    let yaml = format!(
        "nodes:\n  - id: out\n    type: sink.nats\n    url: {}\n    stream: {}\n    subject: {}\n",
        url(),
        f.out_stream,
        f.out_subject
    );
    let pipeline = Pipeline::from_yaml(&yaml, &registry).expect("pipeline loads");
    let source = nats.source(&f.source_params()).expect("source builds");
    let engine = Engine::start(pipeline, Box::new(source), 1).expect("engine starts");

    f.server.delete_stream(&f.out_stream);
    f.server.publish(
        &f.in_subject("acme"),
        r#"{"id": 43, "body": "sink is gone"}"#,
    );

    assert!(
        wait_until(WAIT * 3, || {
            f.server
                .consumer_info(&f.in_stream, &f.consumer)
                .num_redelivered
                > 0
        }),
        "consumer shows the message redelivered"
    );

    nats.shutdown();
    engine.join().expect("clean shutdown");
}
