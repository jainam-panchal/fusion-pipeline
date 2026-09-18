---
status: accepted
date: 2026-09-18
---

# The record is any JSON, and a path addresses all of it

The pipeline accepted only data already shaped the way it wanted, and changed the rest without
saying so. A record was a fixed struct — `id`, `kind`, `body`, `severity_text`,
`severity_number`, the two time fields, `attributes`, `resource`, `scope`, `trace_id`,
`span_id` — and everything else about a payload was treated as a mistake:

- A key outside that list was deleted while the message was decoded. A vendor emitting
  `{"level":"error","msg":"auth failure","rhost":"10.0.0.1","host":"web-1"}` reached the sink
  as `{"kind":"log"}`, acked as a success, with nothing counted and nothing logged.
- A payload without a `kind` was written out with `kind: log` added.
- A field whose value did not match its declared type (`"severity_number": "high"`, a `id`
  that is a UUID) failed to decode, so the message was nakked and dead-lettered after
  `max_deliver`, even when no stage read that field.
- A raw log line is not a JSON object, so every producer had to wrap its lines as
  `{"body": "<line>"}` first.

Addressing was restricted the same way. `attributes`, `resource` and `scope` were flat maps,
so `attributes.http.status` meant the key `http.status` and nothing below a key could be
named. `{"test": 12, "test2": {"key1": "ans1", "key2": 123}}` could not be edited at all.

This was measured, not guessed (issue #79): an agent given only the user guide and vendor log
files got text lines working and was blocked outright on JSON lines, and each of the four
behaviours above was reproduced through the guide example runner. Every comparable product —
Vector, Fluent Bit, Fluentd, Logstash, Cribl Stream, the OpenTelemetry Collector, Elastic
Agent — accepts a free-form event and keeps unknown fields. They differ on syntax, not on that.

## Decision

**One rule for the whole pipeline: the pipeline changes nothing it was not told to change.**

**A record is any JSON value.** `Record` is a newtype over `serde_json::Value`. No field list,
no declared types, no defaults. An object, an array, a string and a number all decode; nothing
is dropped at decode and nothing is added on the way out. The sink writes the record as the
last stage left it.

**A path is a list of segments walked over that value.** `level` names a top-level key,
`test2.key2` a key inside an object, `attributes.0.value.intValue` a list position and then
keys below it, `resource."log.format"` a key whose name holds a dot. `.` names the whole
record, and a leading dot names the record only, which is how a root that is not a bare word
is written (`."log.format"`, `.0`) and how a payload key spelled `meta` is reached.

**A read that matches nothing is null, never an error, and a write makes its path exist.**
Walking for a write, a missing key is created; an existing list position is used; anything
else in the way — a scalar, a `null`, a list with no such position — is replaced by an object.
So a write through a record path cannot fail. `FieldPath::writable` proves that once, at load,
by returning a `WritePath`, and the one refusal left in the path module is a `meta` path.

**Raw bytes are a record.** The NATS source takes `codec: json|text` and the sink
`encoding: json|text`, both `json` by default. `codec: text` makes the payload's bytes the
record, one JSON string. `encoding: text` writes a string record as its bytes, and any other
record as its compact JSON, so a stage that turned a line into an object still delivers.

**`Meta` does not move.** It stays beside the record and out of the payload, exactly as ADR
0005 says, resolved at intake from the arrival alone, as ADR 0007 says. `meta.*` is read-only,
and `edit copy {from: meta.<field>, ...}` is still the only way a pipeline value enters a
record. `RecordId` and `Kind` remain, as the types of the arrival and of `Meta`; what they
stop being is fields of the record type.

**Stages keep their shape.** `filter`, `route`, `edit`, `extract`, `redact`, `dedupe`, `sample`
and `lua` keep their keys and their meaning. `extract` gains `into` (default `attributes`),
because `attributes` is no longer a special place to put groups. `lua` receives the record as
a plain JSON value: a table for an object or a list, a string for a text record.

## Rejected

- **A `schema:` block declaring paths and types**, to win back load-time refusal. It is worth
  having and it is a separate ticket; it must not gate the free-form record, which is the
  thing every producer needs first.
- **Refusing a write whose parent is a scalar or a short list.** `extract` and `redact` report
  a write refusal as a stage error, which is a nak and a dead letter after `max_deliver` — a
  data-dependent nak at line rate for a shape mismatch in customer data. The alternative, a
  silent no-op in those two stages, hides it instead. A total write keeps both stages unable
  to fail on data and keeps one rule in the guide instead of three.
- **A sigil for meta (`$meta`)**, to free the payload key `meta`. It would change every
  existing config for a key nobody has; the leading dot already reaches it.
- **Keeping the flat-map reading of a dotted path** (`attributes.http.status` as one key).
  Then nothing below a key would be addressable, which is the whole point.
- **Enabling `serde_json`'s `preserve_order`.** It would make the output byte-faithful to the
  input, but a `Map` would stop being a `BTreeMap`, so the canonical JSON that `dedupe` and
  `sample` hash a nested key by would depend on key order, and a redelivered record could get
  a different answer — ADR 0004's invariant. Sorted keys are the price.

## Consequences

Each of these is accepted, not overlooked.

- **A config mistake is no longer refused at start.** With no field list, `severty_text ==
  "ERROR"` is a valid path that matches nothing, and `set {field: severity_number, value:
  high}` writes a string. What still fails at load: config syntax, unknown node types and
  keys, regex patterns, route labels, window syntax, and any write to `meta.*`. No metric
  tells a path that matches nothing from a key that is genuinely absent; a counter for it is
  its own ticket, and until then the guide carries the symptom table.
- **A dot in a key name needs quotes.** `resource.log.format` used to mean the key
  `log.format`; it now means three levels. Every config, the loghub producer's paths and the
  guide examples are migrated, and a missed one fails silently.
- **A payload with a wrong type for an old field acks where it nakked and dead-lettered.**
  `dlq_total{reason=undecodable}` falls to near zero: `undecodable` now means only a payload
  that is not JSON under `codec: json`, or bytes that are not UTF-8 under `codec: text`.
  Operators alerting on that series should expect the drop.
- **A record leaves at its full size.** Decode used to shrink it by deleting unknown keys. A
  record near the server's `max_payload` that used to fit may now be refused by the sink,
  which is a `sink_error` nak and a dead letter on every delivery. `LOGS` and `PROCESSED` are
  created with the same defaults, so neither stream is stricter than the other; the limit that
  bites is the server's.
- **Object keys come out sorted**, since `serde_json` is built here without `preserve_order`.
  Every key and value survives; the order does not.
- **A write replaces what is in the way.** `set {field: attributes.0.x}` on `attributes: []`
  leaves `attributes: {"0": {"x": ...}}`.
- **An `extract` on a record that is not an object replaces it**, because groups have nowhere
  else to go. The `codec: text` recipe is `edit copy {from: ., to: body}` first.
- **A `lua` record that is an empty list comes back as an empty object.** A returned table
  carrying the record metatable is one record rather than a split, which is what keeps
  `return record` identity for a list record, but an empty table carries no shape.
