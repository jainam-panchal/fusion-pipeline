---
status: accepted
date: 2026-09-16
---

# Logs and traces are closed-set seams in core, and a record's trace is kept or dropped when it settles

Issue #12 asks for structured logs of every stage error and nak, a trace per record with a span per node, and a way to jump between the two in Grafana. Traces keep 1% of passing records and every record that errors or naks. Three decisions here are hard to undo: they fix what the engine emits, how Loki, Tempo and Grafana link, and what a trace id means.

## Decision

**Two more seams beside the recorder.** Core declares `EventLog`, which takes one `Event` from a closed set of kinds (`stage_error`, `nak`, `redelivery`, `dead_letter`, `dead_letter_failed`) with fixed fields: record id, tenant, node, failure kind, delivery count, message. It also declares `TraceSink`, which takes one `RecordTrace` per kept delivery. Each seam has an in-memory fake next to `InMemoryRecorder`, and the OTLP implementations live in `fusion-otel`. The engine and the NATS source reach all three through one `Signals` handle. We did not use `tracing` macros with an OpenTelemetry bridge. That would add a second logging API and a global subscriber, the fields would be free-form, and a test could only scrape text. Here a missing `record.id` does not compile, and the harness asserts every field.

**The keep decision is taken at settlement, not at the start.** Head sampling decides before the walk, so it cannot keep "every record that errors", because nothing has failed yet. Tail sampling in the collector means exporting every span of every record, and story 46 says we must not trace everything. So the walk writes cheap span drafts into a buffer each worker reuses: a node id borrowed from the pipeline, `Instant`s the engine already takes for `stage_duration_seconds`, and closed-set outcomes. When the record settles, the engine keeps the trace if any branch failed, if the delivery count is above one, or if the record's trace key falls in the configured share (`OTEL_TRACES_SAMPLER_ARG`, default 0.01). Only a kept trace is turned into owned spans with wall-clock times, anchored on one `SystemTime` reading per walk. A passing, unsampled record allocates nothing for tracing.

**A trace id is a function of the record.** `trace key = mix(record id ^ fnv1a(tenant) ^ salt)` and `trace id = key << 64 | record id`. Span ids are derived from the key, the delivery count and the visit index, and are never zero. The same key decides sampling, so a redelivered record gets the same answer. Because the id is derived:

- every delivery of one record lands in one trace, and a redelivery's spans (new span ids) sit beside the failure that caused it;
- a log line carries the trace id in its OTLP trace context, and so does the NATS source's dead-letter line, which knows only the record id, the tenant and the delivery count;
- two tenants that reuse an id get different traces;
- the salt keeps record id 0 from giving the invalid all-zero trace id.

The OpenTelemetry SDK's tracer always generates its own ids and has no end time on its span builder. So the OTLP trace sink does not use a tracer: it builds `SpanData` itself and hands it to a `BatchSpanProcessor`.

**Telemetry never holds up a record.** Logs and spans go through the SDK's batch processors, which `try_send` onto a bounded queue and drop when it is full. During a sink outage or a state-store pause, every delivery writes a `stage_error` line, a `nak` line and a trace. What does not fit is lost, not waited for.

## Consequences

- A record without an id has no trace; its `nak` line is the only link.
- A record whose producer reuses an id within one tenant shares a trace with the earlier one. Delivery count and time tell the two apart.
- Changing the trace id function breaks links to traces already stored. Changing the salt or the hash does the same.
- Drops are not logged. `records_dropped_total` answers "where did my logs go", and a line per drop would flood Loki.
