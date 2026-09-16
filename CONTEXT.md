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
A record paired with the ack handle that settles its message.

**Record id**:
The producer-supplied snowflake that is always present on a record. Records without one are nakked and counted.

**Tenant**:
The owner of a record, held at `resource.tenant.id` and stamped by the source from the subject when absent.
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
A node of type `sink.<kind>` that accepts records and returns success only on durable acceptance.
_Avoid_: output, destination, exporter

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
One of the per-record limits on a script: the instruction budget (`limits.instructions`), the memory cap (`limits.memory_kib`) and the output cap (`limits.output_kib`). A tripped guardrail is a Lua error of its kind, never a crash of the worker.
_Avoid_: quota, timeout (nothing is measured in time)

**Lua error**:
A run of `process` that produced no records: the budget or cap tripped, the script raised, or the returned record was refused. Counted on `lua_errors_total{kind}` with `kind` from the closed set `instructions`, `memory`, `runtime`, `output`, then handled by the node's `on_error`. A `state.*` call the store could not answer is a state error, not a Lua error.
_Avoid_: exception, script failure, crash

**Error policy**:
A `lua` node's `on_error`: `pass` forwards the record as it entered the node (the default), `drop` drops it with reason `lua_error`, `nak` fails it. Applied by the stage after the error is counted.
_Avoid_: fallback, on_fail

**Output check**:
The validation of what `process` returned before it leaves the stage: `id` and the tenant unchanged, `kind` still `log`, typed fields typed, the maps flat, no key that is not a record field, strings under the output cap. A refusal is a Lua error of kind `output`.
_Avoid_: schema validation, sanitising

**Sandbox**:
The VM a script runs in: `string`, `table`, `math` and `utf8`, plus `state`, `log` and `now_ns()`; no `os`, `io`, `package`, `require`, `load` or `debug`. A script that names one of those is refused at load.
_Avoid_: jail, container

### Telemetry

**Metric**:
One of the closed set of names the spec's Telemetry section exports, with its fixed label set. `records_dropped_total{tenant, stage, reason}` is the spine.
_Avoid_: counter, gauge, stat (say counter or histogram only for the instrument kind)

**Recorder**:
The boundary a metrics backend implements: a counter add and a histogram sample, each with a metric and its labels. In-memory in tests, OTLP in deploy.
_Avoid_: meter, registry, telemetry sink

**Tenant label**:
The `tenant` label on every metric: `resource.tenant.id`, or `unknown` when the record has none.

**Engine label**:
The `engine` label (`linear` or `backtracking`) on every per-node metric of a node whose stage runs a regex, and on no other node. A condition with several patterns reports its worst.

**Stage label**:
The `stage` label on per-node metrics: the node id, or the reserved `source`, itself a node on the metrics (in on hand-over, out on entering the graph, engine rejections as its drops).

### State

**State store**:
The external key-value service behind stateful stages: `set_nx`, `set`, `compare_and_set`, `get`, `incr`, `del`. One keyspace shared by every worker and every replica. Dragonfly in deploy, in-memory in tests.
_Avoid_: cache, Redis (the protocol, not the store)

**State handle**:
What a stage gets on its context: the worker's connection, scoped to one record. Prefixes every key with `{pipeline}:{tenant}:{node}:` and counts every operation.
_Avoid_: client, store (the handle is not the store)

**Pipeline name**:
The top-level `name` in the config, default `pipeline`. First segment of every state key: replicas of one pipeline share state, different pipelines never do.

**Ingestion time**:
When a record entered: `observed_time_unix_nano`, else `time_unix_nano`, else the worker clock. Every source must stamp `observed_time_unix_nano` at decode from its transport's timestamp when a record has neither (the NATS source uses the JetStream publish time); the worker-clock fallback is for records pushed in tests, not for sources. Unchanged by redelivery.
_Avoid_: arrival time, processing time

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
