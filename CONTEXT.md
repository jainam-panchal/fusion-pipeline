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
The owner of a record, held at `resource["tenant.id"]` and stamped by the source from the subject when absent.
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

### State

**State store**:
The external key-value service behind stateful stages: `set_nx`, `get`, `incr`, `del`. Dragonfly in deploy, in-memory in tests.
_Avoid_: cache, Redis (the protocol, not the store)

**Worker**:
One OS thread that owns a Lua VM and a state-store connection and runs stages synchronously.
