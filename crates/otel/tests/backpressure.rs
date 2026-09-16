//! Telemetry never holds up a record (ADR 0006): with the exporter stuck, the event log and
//! the trace sink keep accepting from the caller's thread, and what does not fit in their
//! bounded queue is dropped. An exporter that blocks until released stands in for a
//! collector that has stopped answering.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime};

use fusion_core::events::{Event, EventKind, EventLog};
use fusion_core::record::RecordId;
use fusion_core::trace::{NodeSpan, RecordTrace, Settlement, SpanResult, TraceKey, TraceSink};
use fusion_otel::{OtlpEventLog, OtlpTraceSink};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{LogBatch, LogExporter};
use opentelemetry_sdk::trace::{SpanData, SpanExporter};

/// The queue both sinks get: also the most items one export carries.
const QUEUE: usize = 8;

/// How long the caller may take to hand over everything while the exporter is stuck. The
/// handover is non-blocking, so it takes milliseconds; the margin only keeps a slow machine
/// from failing the test.
const HANDOVER: Duration = Duration::from_secs(5);

/// An exporter that says when it has been entered, then waits until released, and counts
/// what it was given.
#[derive(Debug, Clone)]
struct Stuck {
    entered: SyncSender<()>,
    released: Arc<(Mutex<bool>, Condvar)>,
    exported: Arc<AtomicUsize>,
}

impl Stuck {
    fn new() -> (Self, Receiver<()>) {
        let (entered, on_entry) = mpsc::sync_channel(1);
        let stuck = Self {
            entered,
            released: Arc::new((Mutex::new(false), Condvar::new())),
            exported: Arc::new(AtomicUsize::new(0)),
        };
        (stuck, on_entry)
    }

    fn release(&self) {
        let (released, wake) = &*self.released;
        *released.lock().expect("not poisoned") = true;
        wake.notify_all();
    }

    fn exported(&self) -> usize {
        self.exported.load(Ordering::SeqCst)
    }

    /// Runs on the processor's own thread.
    fn wait_then_count(&self, items: usize) {
        let _ = self.entered.try_send(());
        let (released, wake) = &*self.released;
        let mut open = released.lock().expect("not poisoned");
        while !*open {
            open = wake.wait(open).expect("not poisoned");
        }
        self.exported.fetch_add(items, Ordering::SeqCst);
    }
}

impl SpanExporter for Stuck {
    fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        self.wait_then_count(batch.len());
        async { Ok(()) }
    }
}

impl LogExporter for Stuck {
    fn export(
        &self,
        batch: LogBatch<'_>,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        self.wait_then_count(batch.iter().count());
        async { Ok(()) }
    }
}

/// Hand items over with `push` until the exporter is stuck in its first export, then hand
/// over 1000 more from another thread, which must finish within [`HANDOVER`]. Returns how
/// many were handed over in all.
fn hand_over_while_stuck(
    on_entry: &Receiver<()>,
    push: impl Fn(u64) + Send + Sync + Clone + 'static,
) -> u64 {
    let mut before = 0;
    let deadline = Instant::now() + HANDOVER;
    while on_entry.try_recv().is_err() {
        assert!(Instant::now() < deadline, "the exporter was never called");
        push(before);
        before += 1;
    }
    let (done, finished) = mpsc::channel();
    let pushing = push.clone();
    std::thread::spawn(move || {
        for n in 0..1000 {
            pushing(before + n);
        }
        let _ = done.send(());
    });
    assert!(
        finished.recv_timeout(HANDOVER).is_ok(),
        "the caller was held up by a stuck exporter"
    );
    before + 1000
}

fn trace(id: u64) -> RecordTrace {
    let key = TraceKey::new(RecordId(id), "acme");
    let now = SystemTime::now();
    RecordTrace {
        trace_id: key.trace_id(),
        span_id: key.delivery_span_id(1),
        record_id: RecordId(id),
        tenant: Arc::from("acme"),
        delivery_count: 1,
        settlement: Settlement::Ack,
        start: now,
        end: now,
        spans: vec![NodeSpan {
            span_id: key.span_id(1, 1),
            parent_span_id: key.delivery_span_id(1),
            node: "out".to_owned(),
            start: now,
            end: now,
            result: SpanResult::Written,
        }],
    }
}

#[test]
fn a_stuck_trace_exporter_neither_blocks_the_caller_nor_queues_without_bound() {
    let (stuck, on_entry) = Stuck::new();
    let sink = Arc::new(OtlpTraceSink::with_queue(
        stuck.clone(),
        fusion_otel::resource(),
        QUEUE,
    ));

    let exporting = Arc::clone(&sink);
    let traces = hand_over_while_stuck(&on_entry, move |id| exporting.export(trace(id)));
    stuck.release();
    sink.force_flush().expect("flushes once released");

    // Two spans per trace. At most the export that was stuck, the queue behind it and one
    // more batch can have been kept; everything else was dropped.
    let spans = usize::try_from(traces * 2).expect("fits");
    let exported = stuck.exported();
    assert!(exported >= 1, "the stuck export itself counts");
    assert!(exported <= 3 * QUEUE, "{exported} of {spans} spans kept");
    sink.shutdown().expect("shuts down");
}

#[test]
fn a_stuck_log_exporter_neither_blocks_the_caller_nor_queues_without_bound() {
    let (stuck, on_entry) = Stuck::new();
    let log = OtlpEventLog::with_queue(stuck.clone(), fusion_otel::resource(), QUEUE);

    let emitting = log.clone();
    let events = hand_over_while_stuck(&on_entry, move |_| {
        emitting.emit(Event::new(
            EventKind::Redelivery,
            Arc::from("acme"),
            "source",
            2,
        ));
    });
    stuck.release();
    log.force_flush().expect("flushes once released");

    let exported = stuck.exported();
    assert!(exported >= 1, "the stuck export itself counts");
    assert!(exported <= 3 * QUEUE, "{exported} of {events} records kept");
    log.shutdown().expect("shuts down");
}
