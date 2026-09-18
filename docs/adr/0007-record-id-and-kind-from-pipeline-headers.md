---
status: accepted
date: 2026-09-17
---

# The record id and the kind come from pipeline headers, never from the payload

ADR 0005 separated the record (the customer's data) from `Meta` (the pipeline's view of it), but left one crossing: `Meta::resolve` still read the payload's `id` and `kind` at intake. Both describe the message, not the log line. The id is the pipeline's identity and idempotency key (the dedupe holder, state, traces, event lines), and a log line has none: whoever sends the message gives it one. The kind is the signal type, which OTLP itself keeps outside the record, with separate endpoints and message types for logs, metrics and traces. Reading them from the payload also meant the pipeline depended on customer fields to decide whether to walk a record at all (issue #50).

## Decision

**Two more pipeline headers, set by the producer.** They follow the pattern of `Fusion-Tenant` and `Fusion-Ingestion-Time`:

| Header | Value | Absent |
|---|---|---|
| `Fusion-Record-Id` | the record id, a decimal `u64` (digits only) | no record id: the message is nakked as `missing_id`, and dead-lettered on its final delivery |
| `Fusion-Record-Kind` | `log`, `metric` or `span` | `log`; a value that does not parse, or two values, is not `log` |

The NATS source reads both into the `Arrival` (`record_id`, and `kind` as an `ArrivalKind`: unnamed, named, or unreadable), and `Meta::resolve` takes the arrival alone, with no record parameter, so the payload is not read by construction. A message whose arrival does not say it is a log (a kind other than `log`, or one the source could not read) is rejected first (`invalid_record`, acked), whether or not the message has an id. The NATS source checks this before it decodes the payload and does not decode a rejected message's payload at all, so the rejection holds whatever the payload is. Otherwise a message without an id is `missing_id`. The id goes on `Meta.record_id` for every decision after intake, as before.

A header that does not parse (an id that is not a decimal `u64`, a kind outside the three, either one given twice) is reported on stderr and counted on `source_invalid_headers_total`, like every other pipeline header. A malformed id is ignored as if absent, so it is `missing_id`. A malformed kind is not taken as absent, because absent means `log` and a producer that set the header did not say `log`: it is unreadable, and the message is rejected as `invalid_record`. Failing closed keeps "a message that is not a log is never walked" true for a producer that gets the header wrong.

JetStream stores a message's headers with it and redelivers them unchanged, so a redelivered message has the same id and kind.

**The payload's `id` and `kind` are the producer's data.** The pipeline never reads them for a decision, not even as a fallback, and never writes them. The `Record` type keeps both fields with their types: a stage may change them within core's write rules, and the sink writes what the last stage left. A stage that sets `id: 99` changes the payload, not `Meta`.

**The sink writes `Fusion-Record-Id` from `Meta`,** next to the tenant and time headers, so a pipeline consuming another's output keeps the first pipeline's id, and its record trace. It writes no kind header: every record it writes was walked, so a log, and absent is `log`.

**A dead letter keeps the id and kind the arrival gave.** `for_dead_letter` strips every producer `Fusion-*` header and writes the arrival's values back, as it does for the tenant and time, so a replay keeps its id and a replayed non-log is still rejected. The dead letter's own `Nats-Msg-Id` (`{stream}:{sequence}`) is not a `u64` and is never read as a record id, so the two do not collide.

## Rejected

- **The JetStream stream sequence, when no header is given.** Header ids and sequence numbers share the `u64` space, so header id 5 and sequence 5 would be one record to `dedupe` (whose holder check takes a matching id for a redelivery of the holder), to traces and to `sample`. Sequences also restart when a stream is recreated, and a producer retry the stream did not deduplicate gets a new one.
- **Header or sequence chosen per source in config.** No current producer needs it, and it can be added later without changing this decision.
- **`Nats-Msg-Id`.** It is the server's duplicate-detection key, producers often put a UUID there, and the dead-letter queue already uses it for `{stream}:{sequence}`.
- **The kind from the source config or the subject.** A header keeps one pattern for every value of the arrival and lets one source carry more than one signal.
- **The payload as a fallback.** ADR 0005's argument for the tenant applies unchanged: a fallback is a second source of truth, and a stage can rewrite the payload.
- **A `kind` field on `Meta`.** Every walked record is a log, so it would be a constant.

## Consequences

- Every producer sets `Fusion-Record-Id`. `deploy/nats-smoke.sh` and `deploy/metrics-check.sh` do; the #13 loghub producer, not yet written, must. A shipper that cannot set headers (OTel Collector, Vector, Fluent Bit) needs a relay in front that adds it; until then its messages are dead-lettered as `missing_id`.
- `missing_id` and its failure kind keep their meaning and a live producer: a message without the header.
- The `Record` type still types the payload's `id` (a `u64`, or its decimal text) and `kind` (one of the three), so a log whose payload has `"id": "3f2a-..."` or `"kind": "event"` does not decode and is dead-lettered as `undecodable`, whatever its id header says. The pipeline decides nothing from those values; loosening their types is a follow-up if a producer needs it.
- A message whose kind is not `log` is never dead-lettered: its payload is not decoded, so a payload that is not a record is rejected like any other. The engine is handed an empty record for it and never reads it. A dead letter therefore always carries `log` or no kind.
- The stream-sequence fallback that issue #50 proposed is turned down (see Rejected): a message without the id header stays `missing_id`, which issue #50 records as decided.
- Every record of a `lua` split keeps the message's `Meta`, so the sink writes the same `Fusion-Record-Id` on each. A downstream pipeline sees the siblings as one record id: one record trace, and a `dedupe` holder that passes siblings sharing a key as the holder redelivered. A per-record id would need an id the pipeline makes up and a redelivery reproduces, which is a new decision.
- The in-memory source says the id and kind through `push_arrival`, as it does the tenant. A bare `MemoryInput::push` has no record id and is nakked. The pipeline test harness plays a producer that sends the record's `id` in the header too, in one place, and the tests about where the id comes from set the two apart.
- ADR 0005's "read for two things only" and "the record id is not a header" are amended. CLAUDE.md's `Meta` and kind invariants follow this ADR.

## Amended 2026-09-18 (issue #79, ADR 0008)

The third consequence above no longer holds. The record is any JSON value, so the `Record`
type no longer types the payload's `id` or `kind`, and a log whose payload has
`"id": "3f2a-..."` or `"kind": "event"` decodes and is walked like any other, acked rather
than dead-lettered as `undecodable`. The "loosening their types is a follow-up" this ADR named
is that follow-up, done.

Everything else here stands. Both values still come from the message's headers and never from
the payload; the header rules, the rejection of a message whose kind is not `log`, the
`missing_id` nak, the dead letter's headers and the sink's `Fusion-Record-Id` are unchanged.
`RecordId` and `Kind` remain in core as the types of the arrival and of `Meta` — what they
stop being is fields of the record type.
