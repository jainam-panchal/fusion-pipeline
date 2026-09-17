# fusion-pipeline

An observability pipeline that takes log records from NATS JetStream, runs them through a declared DAG of stages, and delivers them to sinks with end-to-end acknowledgement.

## Language

### Data

**Record**:
The single unit of data flowing through the pipeline: one flat OTLP-shaped log with `id`, `kind`, `body`, `attributes`, `resource`, `scope` and trace fields.
_Avoid_: event, log line, entry

**Message**:
The NATS transport unit carrying one record. A message is what gets acked or nakked; a record is what stages see.
_Avoid_: using "message" for the decoded record

**Envelope**:
A record paired with its arrival and the ack handle that settles its message.

**Arrival**:
What a source's transport says about a message, apart from the record: the record id and kind the producer gave, the tenant it names, when the message entered it (with whether a transport or a clock said so), the delivery count, and the payload's size for `bytes_in_total`. The NATS source takes the record id and kind from the `Fusion-Record-Id` and `Fusion-Record-Kind` pipeline headers, the tenant from the subject, else the `Fusion-Tenant` pipeline header, and the time from the `Fusion-Ingestion-Time` pipeline header, else the JetStream publish time. Everything but the count is optional; the meta comes from it alone: no record id is `missing_id`, no kind is `log` (a kind the source cannot read is not), and no tenant or time is `unknown` and the worker clock. The NATS source builds it from what JetStream handed over for the message (`headers::Received`: subject, headers, publish time, delivery count, payload length), which is the raw input, not the arrival.
_Avoid_: headers (the pipeline headers are one input to it), envelope metadata

**Meta**:
The pipeline's view of one record, resolved once at intake from its arrival alone: record id, tenant, ingestion time, delivery count. Read-only on the stage context and through meta paths, and inherited by every record a stage emits from it. Every decision the pipeline takes for itself reads it, never the payload. It is never written into the record: it leaves on the wire as pipeline headers.
_Avoid_: metadata (ambiguous with the record's own attributes), headers (its wire form, not the thing)

**Payload**:
The record itself, as the customer's data: every field, `id`, `kind`, `resource.tenant.id` and the time fields included. The pipeline reads none of them for itself; stages change them only as the config asks, and the pipeline never writes a value of its own into it.
_Avoid_: body (one field of it), content

**Pipeline header**:
One of the NATS message headers the pipeline reads a message's arrival from: the four that carry a record's meta on the wire, `Fusion-Record-Id` (decimal), `Fusion-Tenant`, `Fusion-Ingestion-Time` (nanoseconds, decimal) and `Fusion-Ingestion-Time-Kind` (`reported` or `clock`), plus `Fusion-Record-Kind` (`log`, `metric` or `span`), which is arrival-only: no meta carries it, a producer sets it and the sink never writes it. The producer sets the record id and kind; the sink writes the rest and the record id; the source reads them all back into the arrival, the subject still winning for the tenant. One that does not parse, is given twice, or is one time header without the other is ignored and counted, once per header, except `Fusion-Record-Kind`: that one is counted too, but the message is then not walked; a `Fusion-Tenant` the subject overrides is not read, so not counted.
_Avoid_: stamp, envelope header, metadata header

**Delivery count**:
How many times the transport has delivered a message, this one included; 1 on the first delivery. Carried on the meta.
_Avoid_: attempt, retry count

**Record id**:
The producer-supplied snowflake a message carries in its `Fusion-Record-Id` header, held on its meta. A message without one is nakked and counted (`missing_id`). The record's `id` field is payload, never read: a stage may change or drop it, and the pipeline keeps the meta's.

**Tenant**:
The owner of a record, as the meta names it: the one its transport names (the NATS subject's `{tenant_prefix}.{tenant}.>` token, else the `Fusion-Tenant` pipeline header), else `unknown`; never read from the record. Never empty and never with a control character: a transport tenant that is counts as none. The record's `resource.tenant.id` is payload, never written by the pipeline unless the config copies `meta.tenant` in, and a stage may rewrite it without changing the tenant.
_Avoid_: customer, org, namespace

**Body**:
The opaque payload of a record. Sources never interpret it; stages parse content out of it into attributes.

### Topology

**Node**:
One entry in the config's `nodes` list: an id, a type, an optional `from`, and type-specific fields. Nodes are the vertices of the DAG.

**Stage**:
The processing implementation behind a non-sink node. One function: record in, one output out.
_Avoid_: transform, processor, step

**Source**:
The reserved node id and the boundary that yields envelopes into the engine.

**Sink**:
A node of type `sink.<kind>` that accepts outgoing records and returns success only on durable acceptance.
_Avoid_: output, destination, exporter

**Outgoing record**:
What a sink writes: a record and its meta, side by side. The sink decides how the meta travels (the NATS sink writes pipeline headers); the record is written as the last stage left it.
_Avoid_: output (a stage's), delivery (the delivery count's)

**Pipeline**:
The immutable, versioned compilation of one config: DAG plus compiled nodes, held behind a swap pointer.
_Avoid_: graph, flow, workflow

**Route**:
A stage with named outputs. Ordered labelled conditions; first match wins; the required default is a label or `drop`.
_Avoid_: switch, dispatcher

**Label**:
The name of one route output. Consumers reference `<route>.<label>`. `drop` is reserved.

**Branch**:
One path a record takes after fan-out. Every branch ends in a sink success, a drop or a failure.

**Fan-out**:
Two or more nodes consuming the same upstream output. The record is copy-on-write across the resulting branches.

**Fan-in**:
One node with a list in `from`, consuming several upstream outputs.

**Field path**:
The one dotted path every stage uses to name a record field: `root ("." segment)*`. Under `attributes`, `resource` or `scope` the segments joined with dots are the flat map key (`attributes.http.status` is the `http.status` key). Segments with characters outside letters, digits, `_` and `-` are double-quoted.
_Avoid_: selector, accessor, bracket path

**Meta path**:
A field path under the `meta` root: `meta.id`, `meta.tenant`, `meta.ingestion_time`, `meta.delivery_count`. Reads the record's meta, not the record. Readable wherever a path is read (conditions, key fields, an `edit` `copy` source); refused at load as the target of any write or removal.
_Avoid_: header path, system field

**Write rules**:
Core's one check of what a field path accepts: a JSON value of the field's type, anything under a map key, nothing under `meta`. Every write runs it first; a stage that needs a type at load asks it instead of writing, and `lua` writes a returned record through it.
_Avoid_: schema, validation, probe

**Condition**:
A `field op literal` expression with `and`, `or`, `not` and parentheses, evaluated against a record. Used by `filter` and `route`.
_Avoid_: predicate, rule, expression language

**Op**:
One entry of an `edit` node's `ops` list: `set`, `rename`, `copy`, `hash` or `delete`, with its fields. Ops run in order on one record. The `op` label of `edit_unapplied_total` is the op's kind.
_Avoid_: operation, action, transform, mutation

**Unapplied op**:
An `edit` op that could not apply to a record: its source read as null (`cause: absent`) or the target refused the value (`cause: type`). The record is unchanged by that op and the op is counted; the node's `on_unapplied` then skips it or drops the record with reason `edit_unapplied`. Never a stage error.
_Avoid_: miss, skip (the policy value, not the event), failure, error

### Outcomes

**Ack**:
The settlement that the message was durably handled or intentionally dropped. Fires once, when every branch has finished.

**Nak**:
The settlement that handling failed and the message should be redelivered, optionally after a delay. Fires once, after all branches finish, if any failed.

**Drop**:
An intentional, counted decision not to forward a record. Always carries a drop reason. A drop is a success for ack purposes.
_Avoid_: discard, filter out, reject (reject means a config was refused at load)

**Drop reason**:
One value from the closed set that labels `records_dropped_total`.

**Stage error**:
A failure inside a stage that is not a drop. Counted separately and makes the record's message nak.

**Failure**:
Why a record's message was nakked: the node that failed first in walk order (`source` before any node ran), its failure kind, the error text, and the `Meta` record id when there is one (so a source can name the record when it logs the dead letter). Carried on the nak; the text is for people, never a label.
_Avoid_: error (a stage error is one kind), cause

**Failure kind**:
One value from the closed set that labels `dlq_total`: `stage_error`, `state_error`, `sink_error`, `panic`, `missing_id`, `undecodable` (set by a source only).

**Final delivery**:
The delivery whose count has reached the consumer's `max_deliver`, read once at startup. A nak on it is not a nak: the message is dead-lettered. A delivery whose count the source could not read is never final.
_Avoid_: last attempt, retry exhausted

**Dead letter**:
A message the source gave up on: published as it arrived to `{dlq_prefix}.{tenant}` (a tenant that is not one subject token written with `%XX` escapes) with `Fusion-Dlq-Reason`, then terminated. One dead-letter stream holds every tenant, one subject each. A dead letter whose publish fails is not terminated; its message stays in its stream.
_Avoid_: poison message, parked message, DLQ record

### Regex

**Facade**:
The two-engine regex wrapper. A pattern compiles on the linear engine first and falls back to PCRE2 only when the syntax needs it.

**Linear engine**:
The Rust `regex` crate path: linear time by construction, cannot backtrack.

**Backtracking engine**:
The PCRE2 path, used only for syntax the linear engine rejects, run under match, depth, heap, work and input-size limits.

**Lint**:
The load-time structural check over the pattern AST for catastrophic shapes.

**Canary**:
The load-time run of a PCRE2-bound pattern against generated adversarial inputs under the runtime limits.

**Non-match**:
A record a regex stage's pattern did not match, or whose field is not a string. Passed on unchanged and counted on `regex_nonmatch_total`; never a drop and never an error.
_Avoid_: miss, extraction failure

**Limit trip**:
A regex limit (match, depth, heap, work, input size) exceeded on one record. A drop with reason `regex_limit`; the pattern keeps serving the next record.
_Avoid_: timeout

### Lua

**Script**:
The Lua source a `lua` node runs: a file named by `script` or text given inline by `source`, defining `process(record)`. Loaded once per worker per node.
_Avoid_: plugin, handler, hook (the instruction counter, not the script)

**Guardrail**:
One of the limits on a script: the instruction budget (`limits.instructions`, per record), the memory cap (`limits.memory_kib`, on the worker's VM for the node, upvalues included; a trip rebuilds the VM) and the output cap (`limits.output_kib`, per returned record). A tripped guardrail is a Lua error of its kind, never a crash of the worker, and a script's `pcall` cannot catch it.
_Avoid_: quota, timeout (nothing is measured in time)

**Lua error**:
A run of `process` that produced no records (`LuaError` in the lua crate, one variant per kind): the budget or cap tripped, the script raised, or the returned record was refused. Counted on `lua_errors_total{kind}` with `kind` from the closed set `instructions`, `memory`, `runtime`, `output`, then handled by the node's `on_error`. A `state.*` call the store could not answer is a state error, not a Lua error.
_Avoid_: exception, script failure, crash

**Error policy**:
A `lua` node's `on_error`: `pass` forwards the record as it entered the node (the default), `drop` drops it with reason `lua_error`, `nak` fails it. Applied by the stage after the error is counted.
_Avoid_: fallback, on_fail

**Output check**:
The validation of what `process` returned before it leaves the stage: every key a record field, strings under the output cap, not an empty table or list, every table read as a list (a marked one, one with keys `1..n`, or the list returned for a split) holding only its positions `1..n`, and every value, once converted from Lua (an integral float to an integer, an `id` given as decimal text to the integer), accepted by core's write rules onto a fresh record. No field is required and none must come back unchanged. A refusal is a Lua error of kind `output` with core's message.
_Avoid_: schema validation, sanitising

**Sandbox**:
The VM a script runs in: `string`, `table`, `math` and `utf8`, plus `state`, `log`, `now_ns()`, a read-only `json` (`json.null`, `json.list`), `record:copy()` on every record table, the read-only `meta` table `process` receives as its second argument, and a `pcall`/`xpcall` that let guardrails and state errors through; no `os`, `io`, `package`, `require`, `load`, `debug` or `print`. A script that names one of those is refused at load.
_Avoid_: jail, container

### Telemetry

**Metric**:
One of the closed set of names the spec's Telemetry section exports, with its fixed label set. `records_dropped_total{tenant, stage, reason}` is the spine.
_Avoid_: counter, gauge, stat (say counter or histogram only for the instrument kind)

**Recorder**:
The seam a metrics backend implements: a counter add, which takes only a counter metric, and a histogram sample, which takes only a histogram metric, each with its labels. In-memory in tests, OTLP in deploy.
_Avoid_: meter, registry, telemetry sink

**Signals**:
The one handle through which the engine and a source reach all three telemetry signals: the metrics, the event log and the trace sink, with the share of passing records to trace.
_Avoid_: telemetry (the crate), observability

**Event**:
One structured log line from the closed set `stage_error`, `nak`, `redelivery`, `dead_letter`, `dead_letter_failed`, with fixed fields: record id, tenant, node, failure kind, delivery count, stream sequence (dead letters only), message, and the record's trace. Drops are not events.
_Avoid_: log line (its wire form), log message, audit record; "event" for a record

**Event log**:
The seam an exporter implements to receive events: in-memory in tests, stderr when no collector is configured, OTLP logs in deploy.
_Avoid_: logger, log sink

**Record trace**:
One kept delivery of one record: a delivery span and one span per node the record visited, each parented on the node it came from. Its ids derive from the trace key, so every delivery of a record is in one trace. Kept when the walk failed, when the record was redelivered, or when the trace key falls in the sampled share; decided when the record settles.
_Avoid_: request trace, sampled trace (every failed one is kept)

**Trace key**:
The record id mixed with the tenant and a salt. Every id of a record trace derives from it, and the sampled share is chosen by it, so a record gets the same answer on every replica and every delivery.
_Avoid_: trace id (derived from it), sampling key

**Trace sink**:
The seam an exporter implements to receive record traces: in-memory in tests, OTLP spans in deploy.
_Avoid_: tracer (the SDK's, which the pipeline does not use), span exporter

**Tenant label**:
The `tenant` label on every metric: the meta's tenant.

**Engine label**:
The `engine` label, a closed set (`linear` or `backtracking`), on every per-node metric of a node whose stage runs a regex, and on no other node. A condition with several patterns reports its worst.

**Stage label**:
The `stage` label on per-node metrics: the node id, or the reserved `source`, itself a node on the metrics (in on hand-over, out on entering the graph, engine rejections as its drops).

### State

**State store**:
The external key-value service behind stateful stages: `set_nx`, `set`, `compare_and_set`, `get`, `incr`, `del`. One keyspace shared by every worker and every replica. Dragonfly in deploy, in-memory in tests.
_Avoid_: cache, Redis (the protocol, not the store)

**Stage environment**:
What a worker holds for every stage it runs: the pipeline name, its state-store connection and the metrics handle, built once at engine start. Core derives each stage's context from it, the record's meta and the node; nothing outside core builds a context.
_Avoid_: runtime, worker context

**State handle**:
What a stage gets on its context: the worker's connection, scoped to one record. Prefixes every key with `{pipeline}:{tenant}:{node}:` and counts every operation.
_Avoid_: client, store (the handle is not the store)

**Pipeline name**:
The top-level `name` in the config, default `pipeline`. First segment of every state key: replicas of one pipeline share state, different pipelines never do.

**Ingestion time**:
When a record entered the pipeline's transport, as the meta holds it: the arrival's (for NATS, an upstream pipeline's `Fusion-Ingestion-Time`, else the JetStream publish time), else the worker clock; never read from the record. Reported when a transport said it, clock otherwise; the two are never combined. Every source must give it from its transport; the clock fallback is for records pushed in tests. Unchanged by redelivery.
_Avoid_: arrival time, processing time; "ingestion time" for the record's time fields, which are payload and are named by their field names (the producer's event time and observed time)

**Window**:
How long a first sighting suppresses repeats, measured in ingestion time. The TTL of the state key, not part of its name.
_Avoid_: bucket, slot

**Holder**:
The record a state key currently names as the owner of its window: for `dedupe`, the id and ingestion time stored in the value. Every verdict is made against the holder.
_Avoid_: owner, winner

**Takeover**:
A record past the holder's window writing itself as the new holder, with a compare-and-set against the holder it read. Refused when another worker wrote first; the record is then judged against that holder instead.
_Avoid_: overwrite, refresh

**Key fields**:
The `key` field paths of a `dedupe` or `consistent` `sample` node, whose values (a missing one is `null`) hashed together say "same content" for `dedupe` and "same group" for `sample`. One parser and one hash, shared.
_Avoid_: dedupe key, group-by

**State error policy**:
A node's `on_state_error`, `pass` or `nak`, applied by the engine when a stage could not reach the store. The stage only reports.
_Avoid_: fallback, degrade

**Share**:
The fraction of records a `sample` node keeps: `percent` for `random` and `consistent`, one in `n` for `every_nth`. A record outside the share drops with reason `sample`.
_Avoid_: rate, ratio

**Sample count**:
The one number behind `every_nth`: per tenant in the state store, advanced by one `incr` per delivery that reaches the node, shared by every worker and replica. Counts 1, n+1, 2n+1, ... are kept. A redelivered message is a new delivery and takes a new count; `every_nth` is the stated exception to "a redelivered record gets the same answer".
_Avoid_: counter (an instrument kind), ticket machine (the explanation, not the term), sequence

**Worker**:
One OS thread that owns a state-store connection and one Lua VM per `lua` node it has seen a record for, and runs stages synchronously.

### Harness

**Expectation**:
What the loghub producer expects of one message it published: the subjects its line must reach, `dedupe` as the drop allowed for a deliberate duplicate, and the attributes the structured CSV says extraction lifts. Written only after the message's `PubAck`, one per line of the expectations file, and never computed by running the pipeline.
_Avoid_: manifest, oracle, golden output

**Duplicate group**:
The ids the producer sent for one line in one cycle: an original and its deliberate duplicates. The verifier judges delivery per group and sink, because `dedupe` keeps whichever copy it sees first: a group is missing from a sink when none of its ids arrived there. Not `dedupe`'s repeat, which is any record whose key fields match the holder's.
_Avoid_: duplicate set, dedupe group

**Extra copy**:
A delivery beyond the first of a duplicate group on one sink, or a second copy of one id: allowed by at-least-once delivery and by `dedupe`'s races, reported and never a failure.
_Avoid_: duplicate delivery, unexpected

