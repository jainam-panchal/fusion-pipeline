---
status: accepted
date: 2026-09-16
---

# The pipeline decides from `Meta`, beside the record, and never writes it into the record

Four values decide how a record is handled: its id, its tenant, its kind and its ingestion time. Before this decision all four lived inside the record, where any stage can move them, so each needed a guard: `id` and `kind` were read-only in core's field paths, the tenant was refused by `edit` and checked by `lua`, and ingestion time had no guard at all, so `dedupe` recomputed it from a record a `lua` script may have stamped with `now_ns()` (issue #45, found on PR #39). A fifth value, the delivery count, had no home: the NATS source read it, counted a redelivery under the subject's tenant, and threw it away.

They lived in the record because the NATS source put them there: it stamped the subject's tenant into `resource.tenant.id` and the JetStream publish time into `observed_time_unix_nano`. That is the pipeline writing its own values into the customer's data, in fields the customer owns, where no reader can tell them from what the producer sent.

## Decision

**Two lanes, end to end.** The record is the customer's data. `Meta` is the pipeline's view of it: record id, tenant, ingestion time, delivery count. **The two are never merged, inside the pipeline or on the wire.** The pipeline never writes a value of its own into a record. A record changes only as the pipeline's config asks, through `edit`, `redact`, `extract` and `lua`, and leaves as the last stage left it. `Meta` travels beside it: on the `Context` inside the engine, and as message headers on the wire.

This follows how established pipelines separate the two. Vector keeps event metadata apart from the event and writes it out only when a remap copies it in. Fluent Bit routes on tags, which are not part of the record. CloudEvents' binary mode carries its attributes in protocol headers and leaves the payload as the application's. Loki and Mimir take the tenant from the `X-Scope-OrgID` header, never from log content. Stream processors distinguish event time (the producer's clock), ingestion time (assigned once as the event enters) and processing time (the worker's clock).

OpenTelemetry's log data model does expect a collector to fill `observed_time_unix_nano`. This pipeline does that only when the operator asks, with `edit copy {from: meta.ingestion_time, to: observed_time_unix_nano}`. An internal value entering customer data unasked is the stricter harm.

**One rule for decisions.** No decision the pipeline takes for itself reads the payload. Metric labels, state keys, windows, the end-to-end histogram, the record id a stateful stage stores and a Lua run's log line all read `Meta`. The engine builds it once per record at intake and hands it to every stage as `&Meta` on the `Context`, so no stage can write it. Every record a stage emits, a route branch or a `lua` split, continues under its parent's `Meta`.

**Where `Meta` comes from: the arrival, and only the arrival.** A source puts what its transport knows on the envelope as an `Arrival`: a tenant, an ingestion time (an `IngestionTime`, below) and the delivery count, which defaults to one. `Meta::resolve` in `core::meta` turns the arrival and the record into a `Meta` or a rejection. The tenant and the ingestion time come from the arrival and never from the record, not even as a fallback. The record is read for two things only, `id` and `kind`, which decide whether it is walked at all:

- **record id:** the record's `id`. None is a rejection (`missing_id`, nakked), as before.
- **kind:** the record's `kind`. Anything but `log` is a rejection (`invalid_record`, acked), as before.
- **tenant:** the arrival's, else `unknown`. A tenant is a metric label, a state-key segment and a header value, so an arrival tenant that is empty or holds a control character counts as none. The transport's word is authenticated: NATS permissions decide who may publish on `logs.acme.>`, while any producer can write any `tenant.id`. A record carrying `tenant.id: beta` on `logs.acme.x` is labelled `acme`, and its payload keeps `beta`; a record carrying `tenant.id: beta` on a subject that names no tenant, with no `Fusion-Tenant` header, is `unknown`.
- **ingestion time:** the arrival's, else the worker clock. Ingestion time is the pipeline's reading, assigned when the message entered the transport, so the NATS source always has one and the producer's clock never decides a window. The clock is reached only from a source that gives no time, the in-memory one tests use.

Amended 2026-09-17 (issue #50, ADR 0007): the record is no longer read for anything. The record id and the kind come from the arrival too, which the NATS source fills from the `Fusion-Record-Id` and `Fusion-Record-Kind` headers the producer sets: no id is `missing_id`, a kind other than `log` is `invalid_record` and is checked first, and an absent kind is `log`. `Meta::resolve` takes the arrival alone. The payload's `id` and `kind` are the producer's data, never read, not even as a fallback.

`Meta.ingestion_time` is an `IngestionTime`: `Reported(nanos)` when a transport said it, `Clock(nanos)` when only the worker clock did; `TimeKind` is the closed set of the two spellings, `reported` and `clock`. The two are never combined, and the end-to-end histogram is observed only for `Reported`. An arrival carries an `IngestionTime` rather than a bare number, so a clock reading an upstream pipeline passed on stays a clock reading.

"Only `kind: log` is processed" and "a record without an id is nakked" are decisions about the record as it arrived, taken once in `Meta::resolve`. They are not invariants of what the sink writes.

**`Meta` on the wire: NATS headers.** The NATS sink publishes each record with three headers, and nothing is stamped into the payload:

| Header | Value |
|---|---|
| `Fusion-Tenant` | `Meta.tenant` |
| `Fusion-Ingestion-Time` | the ingestion time in nanoseconds since the Unix epoch, decimal |
| `Fusion-Ingestion-Time-Kind` | `reported` or `clock` |

The record id is not a header, since it is in the payload as it arrived or as a stage left it. The delivery count is not a header, since it counts this pipeline's own consumer's deliveries and means nothing downstream. The `Nats-` prefix belongs to the server; `Fusion-` is ours.

Amended 2026-09-17 (issue #50, ADR 0007): the record id is a header now. The sink writes `Fusion-Record-Id` from `Meta.record_id`, and the source reads it back, so a downstream pipeline keeps the first pipeline's id whatever a stage did to the payload's `id`. No kind header is written, since every record a sink writes was walked as a log.

**What the NATS source trusts.** It fills the `Arrival` from the subject, the headers and the JetStream message info:

- **tenant:** the subject's `{tenant_prefix}.{tenant}.>` token (`tenant_prefix` is a source parameter, `logs` by default), else the `Fusion-Tenant` header. Only a subject that starts with the prefix and has a token after the tenant names one, so `processed.logs` and `processed.logs.v2` name none. The subject wins because NATS permissions back it and a producer can set any header. The header is used only when the subject names no tenant, as when a downstream pipeline consumes `processed.logs`.
- **ingestion time:** the `Fusion-Ingestion-Time` header with its kind, else the JetStream publish time as `Reported`. The header wins so the first pipeline's ingestion time survives every hop. The publish time does not change on redelivery, so a redelivered message gets the same time.
- **delivery count:** the JetStream delivery count.

A `Fusion-*` header that does not parse (a time that is not a decimal `u64`, a kind that is not `reported` or `clock`, a tenant that is empty or holds a control character, a header given twice, one time header without the other) is ignored as if absent, reported on stderr and counted on `source_invalid_headers_total{tenant}`, under the tenant the record's `Meta` gets (the arrival's). Each such header counts once, for its own problem: a time header given twice is not also counted as unpaired, and the partner it leaves behind is ignored, counted only if its own value does not parse. A `Fusion-Tenant` is only read when the subject names no valid tenant, so a header the subject overrides is never counted. The payload is still a valid record, so the message is not nakked for it.

**Sinks receive `Meta`.** `Sink::write` takes a batch of `Outgoing` values, each a `&Meta` and a `&Record`. The NATS sink maps `Meta` to the headers above; the in-memory sink keeps both, so harness tests assert the untouched record and its `Meta` together. Every future sink says how it carries `Meta`: a header, a column, or not at all.

**Config reads `Meta` through a read-only `meta` path root.** `meta.id`, `meta.tenant`, `meta.ingestion_time` and `meta.delivery_count` are field paths:

- `filter` and `route` conditions decide on them, so routing by tenant is `meta.tenant == "acme"`, not a read of the payload.
- `edit copy {from: meta.<field>, to: <record field>}` is the one way a pipeline value enters a record, and it is the operator's config that puts it there.
- `dedupe` and `sample` key fields, `extract`'s `field` and a `lua` script may read them too. A script receives them as a second argument, `process(record, meta)`, a table whose writes raise a runtime error.
- Any op that writes or removes `meta.*` (`set`, `rename` from or to it, `copy` to it, `hash`, `delete`, a `redact` field) is refused at load: `meta` is the pipeline's.

**Every record field is payload.** Once `Meta` is built, nothing the pipeline decides depends on the record's fields, so no field needs a guard:

- `id` and `kind` are writable and removable in core's field paths, within their types (`id` a non-negative integer, `kind` one of `log`, `metric`, `span`, a removed `kind` being the wire default `log`). `FieldPath::remove` cannot fail on a record field.
- `edit` accepts any op on any record field, `id`, `kind` and `resource.tenant.id` included. `hash` on `id` is refused at load by type, because a digest is a string.
- `redact` refuses a field at load only when the field cannot take a string, which core's write rules decide (`id`, `kind`, `severity_number`, the time fields, and `meta.*`).
- `lua`'s output check types every field and requires none: `id` may change or go, `kind` may be any of the three, the tenant and the time fields may change. An empty table is still neither a record nor a list.
- The spec's warning that a write into a time field moves downstream windows, and the redelivery exception it created for a script stamping `now_ns()`, are gone with the hazard. Two exceptions to "a redelivered record gets the same answer" remain: `every_nth` and a script's upvalues.

What follows is the operator's to configure, and the pipeline does not second-guess it:

- A stage may set `kind` to `metric` or `span`. The record is still walked, counted on `records_out_total` and delivered, since it was a log when the pipeline decided to process it.
- A stage may remove `id`. The sink writes the record without one, and a pipeline consuming that output naks it as `missing_id`, exactly as it would a producer's record without one.
- A stage may change `id`, and a `lua` split may give its records different ids. Every one of them keeps the message's `Meta`, so the dedupe holder, the state keys and the labels use the id the message arrived with, and the ack settles the one message.

What an operator configures a stage to read from the record stays on the record: a condition on `attributes.http.status`, a `dedupe` key on `body`. Those stages exist to act on the data. An operator who wants a decision on the pipeline's values names `meta.*`.

**Redeliveries are counted by the engine.** `source_redeliveries_total` is counted once per record whose `Arrival` says it was delivered before, under `Meta`'s tenant, so a record's redeliveries and its other series agree. A payload the NATS source cannot decode has no record; the source counts its redelivery and its nak under the tenant its arrival gives, which is the tenant `Meta` would have had.

**The delivery count.** `Meta.delivery_count` reaches every stage and `meta.delivery_count` every config. `every_nth` does not use it: it counts records across the tenant, which a per-message count cannot replace, and skipping redeliveries would change the 2026-09-15 behaviour for a gain nobody has asked for. `every_nth` stays the stated exception to "a redelivered record gets the same answer".

**Shape.** `Meta` is on the `Context`, not on the envelope, because its record id is guaranteed and a source cannot guarantee it; the envelope carries the `Arrival`. The tenant is an `Arc<str>`: the state handle is rebuilt per node per record and clones it each time. `UNKNOWN_TENANT` lives in `core::meta`, beside the rule that produces it.

## Rejected

- **Stamping the record and letting it diverge from `Meta`** (the issue's option 3). It keeps the wire shape, but the pipeline's tenant and time sit in customer fields, indistinguishable from the producer's. An earlier draft of this ADR chose it.
- **Stamping in the engine for every source.** One spelling of the rule, and the harness sees the wire shape, but it is the same mixing.
- **`Meta` only, with nothing carried on the wire** (the issue's option 1). The payload stays clean, but a downstream consumer and a chained pipeline lose the tenant and the original ingestion time.
- **`Meta` projected back onto the record by the sink** (the issue's option 2). A script's repair is overwritten at the sink, and it is still mixing.
- **Read-only time fields.** One refusal list instead of three, but it keeps guarding a payload no decision reads.
- **Record-first resolution.** It let a producer's `tenant.id` override an authenticated subject, and a producer's clock decide a window.
- **The record as a fallback when the arrival names nothing.** Meant for test sources, it also reached a NATS message on a subject outside the tenant prefix with no `Fusion-Tenant` header, which is the same mixing by another door. A test source says what a transport would, through `Arrival`. This reverses the fallback step of #45's locked decision D2, as issue #47 records.

## Consequences

- ADR 0004's window is measured in `Meta`'s ingestion time, which for NATS is the JetStream publish time of the first pipeline's message. The producer's clock no longer decides windows.
- The spec's record-model, field-path, write-rule, `edit`, `redact`, `dedupe`, `lua`, source-and-sink, wire-format and telemetry paragraphs stand as written, each with a dated issue #45 amendment after it saying what changed.
- `Context.record_id`, `Metrics::tenant_of`, `Metrics::UNKNOWN_TENANT`, `PathError::ReadOnly` on record fields, `FieldPath::is_writable`, `edit`'s tenant refusal, `lua`'s id, kind and tenant checks, the NATS source's `stamp_tenant` and `stamp_observed_time`, and the NATS `TENANT_KEY` are gone. `Envelope` gains `arrival`; `MemoryInput::push_arrival` lets the harness play a transport. `Sink::write` takes `Outgoing`, and `MemorySinks` returns each record with its `Meta`.
- **The wire output changes.** A record whose producer left out `resource.tenant.id` or both time fields leaves without them. A consumer that read the tenant or the time from the payload of `processed.logs` reads the `Fusion-*` headers instead, or the operator adds an `edit copy` from `meta.*`. `deploy/nats-smoke.sh` asserts the header.
- A record's payload id, kind, tenant or time may disagree with its `Meta`. That is the point: the payload is the customer's, the headers are the pipeline's.
- One metric joins the closed set: `source_invalid_headers_total{tenant}`.
- CLAUDE.md's "Only `kind: log` is processed" names intake as where it is decided, and its "Decisions read `Meta`" and "Windows" invariants follow this ADR.

## Amended 2026-09-18 (issue #79, ADR 0008)

The record is any JSON value, so four of the consequences above name types that no longer
exist. "Every record field is payload" holds more strongly than when it was written; what
changes is that no field has a type to be written *within*:

- `id` and `kind` are writable and removable like any other key, with no type to satisfy: `id`
  takes a UUID, `kind` takes anything. A payload with no `kind` leaves with none, where the
  sink used to write `log`.
- `hash` on `id` is no longer refused at load. Whether a value can be hashed is known only
  when there is one, so `hash` is unapplied on the record with cause `type` instead.
- `redact` refuses a field at load only when it names `meta.*`. There is no type to check, and
  a field that does not hold text on a given record is skipped at run time, as before.
- `lua`'s output check types nothing. It refuses a value with no JSON form, a returned
  boolean, an empty table, a table nested too deep, and strings past the output cap.

Everything this ADR decided stands: `Meta` lives beside the record, is resolved once at intake
from the arrival alone, is read-only through `meta.*`, and every decision reads it rather than
the payload. Making the record free-form is what that separation was for — with no decision
depending on a record field, no field needed a type.
