# Observability pipeline POC: spec

Status: draft, 2026-09-08. The decisions below came out of a grilling session. Where something was assumed rather than decided, it is marked (assumed).

## Problem statement

We need to decide whether to build our observability pipeline around a declarative DAG of stages with Lua as the escape hatch, and whether we can promise at-least-once delivery from a NATS source to a NATS sink while keeping stage state outside the process. The atomic feature catalogue (`pipeline_atomic_features.csv`, 148 features across logs, metrics and traces) says what the pipeline must eventually do. It does not say whether the stage model, the ack model, or the language choice hold up. We also have no visibility into a pipeline like this once it runs.

## Solution

A working proof of concept that:

- consumes one JSON log record per JetStream message, runs it through a YAML-declared DAG of stages, and publishes it to one or more JetStream sinks;
- acknowledges the source message only after every sink the record reached has confirmed durable receipt (`PubAck`), and negatively acknowledges on any failure so JetStream redelivers;
- implements the smallest stage set that covers each stage category (stateless transform, stateful transform backed by Dragonfly, sandboxed Lua) plus routing with fan-in and fan-out;
- keeps all stateful stages idempotent under redelivery, so a crash mid-record never turns into data loss;
- ships its own metrics, logs and traces over OTLP into a Grafana stack with two provisioned dashboards, one for tenants and one for us;
- proves the delivery guarantee with a reproducible chaos test that kills the pipeline and pauses the state store mid-run and shows zero missing records;
- measures regex extraction accuracy against loghub ground truth per log format;
- records the Rust-over-Go decision as an ADR with the evidence that settled it.

## User stories

### Pipeline operator

1. As a pipeline operator, I want to declare my pipeline as a YAML file of named nodes, so that I can read and version the topology without reading code.
2. As a pipeline operator, I want each node to declare which node(s) it reads from, so that fan-in is explicit and I can trace where a record came from when debugging.
3. As a pipeline operator, I want to omit `from` on a node and have it default to the previous node in the file, so that a simple linear pipeline stays short.
4. As a pipeline operator, I want a `route` node that exposes named outputs, so that downstream nodes can subscribe to one branch by name.
5. As a pipeline operator, I want a route to require a `default` branch (or explicit `drop`), so that no record silently falls off the end of a router.
6. As a pipeline operator, I want the engine to reject a config at load time if the graph has a cycle, an unreachable node, an unconsumed route label, or no sink, so that misconfiguration fails before it eats traffic.
7. As a pipeline operator, I want a `filter` node with a small condition expression grammar, so that common drop/keep rules need no scripting.
8. As a pipeline operator, I want a `pcre2_extract` node that turns named capture groups into record fields, so that I can parse unstructured bodies with the same PCRE syntax my Splunk and Logstash patterns already use.
9. As a pipeline operator, I want a `redact` node that replaces PCRE matches in named fields in place, so that I can mask secrets before they reach a sink.
10. As a pipeline operator, I want a `sample` node with `random`, `every_nth` and `consistent` modes, so that I can choose between cheap, exact and key-stable sampling.
11. As a pipeline operator, I want a `dedupe` node keyed on chosen fields with a time window, so that repeated records within the window are suppressed.
12. As a pipeline operator, I want a `lua` node that runs a script I provide, so that logic the declarative nodes cannot express still lives in the pipeline.
13. As a pipeline operator, I want to set per-node limits on regex matching (match, depth, heap, work, input size), so that a bad pattern cannot pin a worker (bounded, not eliminated, for one class of pattern: see the amendment under the regex facade).
14. As a pipeline operator, I want to set per-node Lua limits (instruction budget, memory cap), so that a bad script cannot pin or exhaust a worker.
15. As a pipeline operator, I want to choose what happens when a Lua stage errors (`drop`, `pass`, `nak`), so that I control whether bad scripts lose data, pass it untouched, or hold it for retry.
16. As a pipeline operator, I want to choose what happens when the state store is unreachable for a stateful node (`pass` or `nak`), so that I control whether the pipeline degrades or blocks.
17. As a pipeline operator, I want sinks to be nodes with `from` like any other, so that fan-out to two sinks is two nodes naming the same input.
18. As a pipeline operator, I want the pipeline to run as one process with N worker threads, so that I can size it by cores without configuring anything else.
19. As a pipeline operator, I want to start the entire stack with one `docker compose up`, so that the POC is reproducible on any machine.
20. As a pipeline operator, I want the demo to route on a real record property (`log.format`) into per-format parsers, so that the routing feature is shown on a problem that needs it.

### Reliability owner

21. As a reliability owner, I want the source message acknowledged only after every sink it reached has returned `PubAck`, so that "acked" means "durably stored downstream".
22. As a reliability owner, I want any branch failure to nak the whole message, so that a partially delivered record is retried rather than half-lost.
23. As a reliability owner, I want records dropped by a stage (filter, sample, dedupe, Lua drop) to count as successfully handled and be acked, so that intentional drops do not cause redelivery storms.
24. As a reliability owner, I want naks to carry a delay and JetStream to redeliver up to a maximum, so that transient failures retry without hot-looping.
25. As a reliability owner, I want messages that exhaust their delivery attempts to be terminated and published to a per-tenant dead-letter subject, so that nothing is silently discarded.
26. As a reliability owner, I want every stateful stage to be idempotent under redelivery by keying its state on the record id, so that a crash between a state write and the ack never causes a real record to be classified as a duplicate and dropped.
27. As a reliability owner, I want the pipeline to nak any record that arrives without an id, so that the idempotency guarantee has no unguarded path.
28. As a reliability owner, I want a chaos test that kills the pipeline process mid-run and pauses Dragonfly mid-run, so that the guarantee is demonstrated.
29. As a reliability owner, I want the chaos test's verifier to know the expected outcome of every published record, so that "missing" is computed against ground truth rather than against a second run of the pipeline.
30. As a reliability owner, I want duplicates at the sink to be permitted but gaps to fail the test, so that the test asserts at-least-once and nothing stronger.
31. As a reliability owner, I want the fan-out case covered by the chaos test, so that the outstanding-branch ack counter is proven under a real crash and not only in unit tests.

### Tenant viewing the pipeline

32. As a tenant, I want to see how many of my records entered and left the pipeline, so that I know what I am paying for.
33. As a tenant, I want to see how many of my records each stage dropped and why, so that I can answer "where did my logs go" without filing a ticket.
34. As a tenant, I want to see bytes in and out, so that I can size my ingestion.
35. As a tenant, I want to see end-to-end p99 latency, so that I know the pipeline is not adding delay.
36. As a tenant, I want to see none of the pipeline's internal timings or infrastructure state, so that operator concerns are not leaked into my view.

### Internal engineer operating the pipeline

37. As an internal engineer, I want per-stage in/out/dropped/errored counters labelled by tenant and stage, so that I can localise a problem to one node for one customer.
38. As an internal engineer, I want per-stage latency histograms, so that I can find the slow node.
39. As an internal engineer, I want state-store ops, latency and error counts, so that I can tell a Dragonfly problem from a pipeline problem.
40. As an internal engineer, I want Lua error, instruction-limit and memory-limit counts per node, so that I know when a script is misbehaving.
41. As an internal engineer, I want regex limit-trip counts per node, so that I know when a pattern is pathological on real data.
42. As an internal engineer, I want NATS consumer pending, redelivered and ack-lag metrics, so that I can see backpressure forming.
43. As an internal engineer, I want sink `PubAck` latency and failure counts, so that I can see when downstream is the bottleneck.
44. As an internal engineer, I want NATS and Dragonfly scraped into the same Prometheus, so that "is it us or them" is one dashboard.
45. As an internal engineer, I want structured pipeline logs in Loki carrying the record id on every stage error and redelivery event, so that I can follow one record through a failure.
46. As an internal engineer, I want a trace per record with a span per stage, sampled at 1% plus 100% of records that error or nak, so that failures are always traceable without tracing everything.
47. As an internal engineer, I want the pipeline to emit all telemetry over OTLP to a single collector, so that there is one export path and one config to maintain.
48. As an internal engineer, I want both dashboards provisioned from files at compose-up, so that observability exists from the first run.
49. As an internal engineer, I want the chaos-test verifier to emit published/received/missing into the same collector, so that the proof of the delivery guarantee is visible as a panel.

### Lua script author

50. As a Lua script author, I want to write a single `process(record)` function that returns the record, `nil` to drop, or a list to split, so that the contract is obvious.
51. As a Lua script author, I want the record as a plain Lua table with OTLP field names, so that I access `record.attributes["http.path"]` and nothing more exotic.
52. As a Lua script author, I want a `state` API with `get`, `set_nx`, `incr` and `del`, so that stateful logic in Lua uses the same store as native stages.
53. As a Lua script author, I want `log.info`, `log.warn` and `now_ns()`, so that I can debug and timestamp without side channels.
54. As a Lua script author, I want `os`, `io`, `package`, `require`, `load` and `debug` removed, so that I cannot escape the sandbox, by accident or on purpose.
55. As a Lua script author, I want my script validated at load time (parses, defines `process`, touches no forbidden global), so that a broken script fails deploy instead of failing per record.
56. As a Lua script author, I want the engine to validate the record I return (required fields present, correct types, id unchanged, size under cap), so that a script bug cannot corrupt downstream.
57. As a Lua script author, I want my script loaded once per worker and reused across records, so that the per-record cost is one function call.

### Decision maker

58. As a decision maker, I want an ADR recording why Rust was chosen over Go with the Lua-embedding evidence, so that the choice is documented and not merely believed.
59. As a decision maker, I want the ADR to record that PCRE2 JIT is off and why, so that nobody flips it on without re-checking the limit semantics.
60. As a decision maker, I want the POC to demonstrate the stage model on Tier 1/2 features from the catalogue, so that I can judge whether it generalises to the rest.
61. As a decision maker, I want extraction accuracy per log format measured against loghub ground truth, so that I can judge whether regex-based parsing is good enough or dedicated parsers are needed sooner.

## Implementation decisions

### Language and runtime

Rust. The deciding evidence is Lua embedding: `mlua` binds real Lua 5.4; Go's pure-Go option is 5.1 and slow, and its cgo option pays a crossing cost per field access. Recorded in an ADR.

Tokio handles NATS I/O. Stages are synchronous. N worker OS threads (default = cores), each owning one Lua VM and one synchronous Dragonfly connection. Source and sink tasks bridge to workers over bounded channels.

Lua 5.4 via `mlua` with the `lua54` and `vendored` features. No LuaJIT.

Regex is a two-engine facade in our own wrapper crate. Every pattern is first compiled with the Rust `regex` crate, which is linear-time by construction and cannot backtrack. If `regex` rejects the syntax (backreferences, lookaround, atomic or possessive groups, recursion), the pattern falls back to PCRE2 through `pcre2-sys` directly (PCRE2 10.46, bundled). All `unsafe` code lives in the PCRE2 half. JIT is never invoked, so `match_limit` and `depth_limit` behave deterministically on the interpreter path.

Catastrophic-pattern detection happens at config load, in three layers. First, classification: a pattern that compiles on `regex` is `engine=linear` and cannot backtrack; one that needs PCRE2 is `engine=backtracking` and is labelled as such in metrics and logs. Second, a structural lint on the `regex-syntax` AST for nested unbounded quantifiers, overlapping alternation under repetition, and an unbounded quantifier followed by an overlapping suffix, with per-node `on_redos_risk: reject|warn`. Third, a canary, run only for patterns that land on PCRE2: the pattern is run against generated adversarial inputs (repetitions of the pattern's literal alphabet at 1 KiB, 8 KiB and 64 KiB, clamped to `input_bytes`). Every probe runs under the runtime `match_limit` and a work budget of 64 pattern items per byte, anchored at every size and unanchored at the smallest; the node is rejected if either trips. The canary's work budget applies even when the node's runtime `work` limit is off. Runtime limits still apply per record regardless of classification.

Amended 2026-09-09 (issue #2): PCRE2 resets `match_limit` at every start position, so an unanchored non-match whose leading group loop restarts everywhere (`(?:a|b)*(?=c)`) is O(n²) and trips no limit: 1.9 s at 8 KiB, minutes at 64 KiB. A fifth limit, `work`, compiles the pattern with `PCRE2_AUTO_CALLOUT` and counts pattern items across the whole call, tripping its own error variant; it costs 1.5–1.8× on the PCRE2 path and is on by default. Single-character repeats loop inside one item and are not counted; an unanchored PCRE2-only pattern built from them (`(?<=:)\w+\s+\w+`; the plain `\w+\s+\w+` compiles on the linear engine and is unaffected) still costs one scan per start position, about 5 s on a 64 KiB non-matching record, bounded only by `input_bytes` (default 64 KiB). That residual class is a known gap against story 13 and is recorded here rather than solved.

### Record model

One record type, OTLP-semantic but flat: `id`, `kind`, `time_unix_nano`, `observed_time_unix_nano`, `severity_text`, `severity_number`, `body`, `attributes` (map), `resource` (map), `scope` (map), `trace_id`, `span_id`.

`kind` is `log` for the POC. `metric` and `span` variants exist in the enum, are rejected by the engine with a counter, and are otherwise unimplemented.

`id` is a producer-supplied snowflake and is always present. Records without one are nak'd and counted. (assumed) The field is named `id` at the top level of the JSON payload.

Tenant lives at `resource.tenant.id`. The NATS source reads the tenant from the subject (`logs.{tenant}.>`) and stamps it if absent. (assumed) Fewer than 100 tenants, so tenant is a label on every metric including histograms.

Amended 2026-09-11 (issue #20): `attributes`, `resource` and `scope` are flat maps with OTel-style dotted keys (`http.status`, `tenant.id`, `k8s.pod-name`) and scalar values; nothing is nested below a key.

Records are copy-on-write across fan-out: shared until a branch mutates.

### Wire format and test data

The NATS payload is one JSON object per message using OTLP field names. The sink emits the same shape. OTLP protobuf sources are a later source implementation; the engine never sees wire format.

The OTLP shape structures only the envelope. `body` is opaque to the source. Sources decode transport and framing into the envelope and never interpret `body`; stages parse content out of `body` into `attributes`. Dedicated parser nodes (`parse.syslog`, `parse.json`, `parse.kv`, `parse.xml`, `parse.grok`, `parse.timestamp`) are follow-ups. The POC demonstrates the unstructured path with `pcre2_extract` alone.

Test data is the loghub-2.0 `2k_dataset` (https://github.com/logpai/loghub-2.0, cited per its license). Four sets are vendored: Linux, OpenSSH, Apache and Mac. Each has 2,000 raw lines, a `_structured_corrected.csv` with ground-truth fields per line, and a `_templates_corrected.csv` with event templates. The producer replays raw lines as `body`, stamps `resource.log.format` with the set name and `resource.tenant.id` with one tenant per set, and carries the loghub `LineId` in `attributes.loghub.line_id` so any record can be joined back to its ground truth. Windows event logs are in loghub v1 only, not in the 2.0 2k set, and are not used.

Each format has its own `pcre2_extract` node reached through a `route` on `resource.log.format`. The node lifts the columns the structured CSV defines into attributes (for Linux: `Month, Date, Time, Level, Component, PID, Content`; for Apache: `Time, Level, Content`; for OpenSSH: `Date, Day, Time, Component, Pid, Content`). The four extract nodes fan in to `redact`. This is the POC's routing demo.

### Topology

Config is a list of `nodes`, each with `id`, `type`, optional `from` (string or list), and type-specific fields. `source` is a reserved node id. Sinks are nodes of type `sink.<kind>`. `route` nodes declare `routes: {label: condition}` and a required `default: <label>|drop`; downstream nodes reference `router.<label>`.

Load-time validation: acyclic, every node reachable from `source`, every route label consumed or named as default, at least one sink, all `from` targets exist.

Amended 2026-09-11 (issue #4): a default label with no consumer is rejected too, not only an unconsumed route label. Otherwise `default: other` with nothing reading `router.other` would ack and discard unmatched records with no drop reason recorded, which is the silent fall-off story 5 exists to prevent; `default: drop` is the explicit way to say that. The label `drop` is reserved.

The compiled pipeline is versioned and swappable in memory. The loader produces an immutable compiled pipeline (DAG, compiled nodes, version label) held behind an atomic swap pointer. A new version is fully validated, compiled and canaried before it replaces the current one, so a failing config never displaces a running one. Records hold the version they entered on until ack or nak. Per-worker resources (Lua VM, regex match data) rebuild at the next record boundary after a swap. State-store keys are namespaced by node id rather than version, so a config change does not reset windows unless a node is renamed. The POC loads version 1 from a file at startup and never swaps; the trigger (file watch, signal, control-plane fetch) is a follow-up that calls the same load path.

Internally the config becomes a DAG (node ids plus an edge list). The POC config runs filter, route, four per-format `pcre2_extract` nodes, redact, sample, dedupe, lua and a main sink, with a second sink hanging off one route branch to exercise fan-out.

### Condition grammar

`field op literal`, where field is a dotted path into the record, ops are `== != =~ !~ < > <= >=`, combinators are `and or not`, and parentheses group. `=~` and `!~` compile their pattern through the same regex facade with the same limits.

Amended 2026-09-11 (issue #20): the field was "a dotted or bracketed path"; bracket syntax is removed and is a load-time config error naming the node, with the dotted form in the message.

### Field paths

Every stage names a record field with one dotted path, `root ("." segment)*`, and nothing else. The root is a top-level record field. When it is `attributes`, `resource` or `scope`, the segments after it joined with dots are the map key: `attributes.http.status` reads and writes the `http.status` key of `attributes`, and `resource.tenant.id` is the tenant. Map values are scalars; nothing below a key is addressable. `body` is addressed only as a whole, and the scalar fields (`id`, `kind`, `severity_text`, `severity_number`, the time fields, `trace_id`, `span_id`) take no segments. A bare segment is one or more of `[A-Za-z0-9_-]`, so `resource.k8s.pod-name` and `attributes.5xx.count` need no quoting; a segment with any other character is written as a double-quoted string, `attributes."Event ID".code` naming the `Event ID.code` key, with `\"` and `\\` as the only escapes. The root is never quoted.

Core exposes read, write and remove by path so `filter`, `route`, `edit`, `pcre2_extract`, `redact` and `lua` share one path semantics. Writes to `id` and `kind` are refused; `severity_number` takes an integer, `severity_text`, `trace_id` and `span_id` a string, the time fields a non-negative integer, and a map key a scalar. Every refusal is an error value, never a panic, and the record is unchanged on error. Every path parse error is reported at config load, naming the node and saying what to write instead; write and remove refusals are reported by the stage that performs them.

### Stages

Every stage implements one function: record in, and one of `Pass(record)`, `Drop(reason)`, `Routed(label, record)`, `Split(records)` or `Error(error)` out. Stages receive a context carrying the record id, tenant, node id, state-store handle and metrics handle.

Amended 2026-09-11 (issue #6): a sixth output, `StateError { record, error }`, for a stage that could not reach the state store. `Error` carries no record, and the `pass` policy needs the record back unchanged, so the stage hands it over with the error and the engine decides. The context's state handle carries the metrics handle inside it: a stage never emits a metric directly, every state operation is counted by the handle. A stage declares `uses_state` so the engine knows whether to open connections at all.

`filter` takes a condition and `action: drop|keep`.

`route` takes ordered labelled conditions; first match wins, default applies otherwise.

`pcre2_extract` takes `field`, `pattern`, `limits {match, depth, heap_kib, work, input_bytes}` and `on_redos_risk: reject|warn` (default `reject`). Named groups become attributes. The engine is chosen per the facade rules above; `limits.match`, `depth`, `heap_kib` and `work` apply only on the PCRE2 path, `input_bytes` on both. An absent `work` takes the default (10 000 000); `work: 0` turns the count off. (assumed) A non-match is `Pass` unchanged rather than an error.

`redact` takes `fields`, `pattern`, `replace` and the same limits. Replacement is in place.

`sample` takes `mode: random|every_nth|consistent`, `percent` or `n`, and `key` for consistent mode. `every_nth` uses a shared `incr` in the state store guarded by the record id so redelivery does not double-count.

`dedupe` takes `key` fields, `window` and `on_state_error: pass|nak`. The state value is the record id. When `set_nx` fails the stage reads the stored id: the same id means this record is being seen again, so it passes; a different id means a real duplicate, so it drops.

Amended 2026-09-11 (issue #6): the window is measured in ingestion time, not on arrival at the stage. The state value is `"{record id} {ingestion time}"` and `set_nx` answers with it in one step. A record whose claim fails passes when the stored id is its own (redelivery while the key lives), when its ingestion time is older than the holder's (an earlier record coming back after its key expired and a newer duplicate took it), or when its ingestion time is `window` or more past the holder's (the window is over); it drops as a duplicate only when its ingestion time falls inside the holder's window. The arrival-time reading had a hole: a record that passed, whose process died before the ack, redelivered after `ack_wait` with the key expired and a newer duplicate holding it, was dropped, a real record lost, and closing it by rule (`window` longer than every redelivery) would have tied the window's meaning to the consumer's settings. Two imperfections remain, both an extra copy and never a loss: an older record arriving after a newer one with the same content passes, and a duplicate delayed longer than the window passes. Ingestion time is `observed_time_unix_nano`, else `time_unix_nano`, else the worker clock; the NATS source fills `observed_time_unix_nano` from the JetStream publish time when a record has neither. `key` is one or more field paths; a missing field is `null`, so records lacking it dedupe together. The content hash is FNV-1a 64 over the canonical JSON of the key values. `window` is `<integer><ms|s|m|h>`, at least 1 ms (Dragonfly refuses `PX 0`). `on_state_error` defaults to `pass`.

`lua` takes `script`, `limits {instructions, memory}` and `on_error: drop|pass|nak` (default `pass`). The sandbox removes forbidden globals; the instruction budget is a count hook; the memory cap is an allocator limit; output is validated before leaving the stage.

### State store

The interface has four operations: `set_nx(key, value, ttl) -> bool`, `get(key) -> Option<bytes>`, `incr(key, by, ttl) -> i64`, `del(key)`. The Dragonfly implementation speaks the Redis protocol. An in-memory implementation exists for tests.

The engine applies the failure policy from per-node config; the stage does not. Default: `pass` for dedupe and sample, `nak` for anything that would produce data from state.

(assumed) Key namespace is `{pipeline}:{node}:{purpose}:{...}`.

Amended 2026-09-11 (issue #6): `set_nx(key, value, ttl)` returns `None` when it claimed the key and `Some(existing value)` when it did not, one atomic step (`SET NX PX GET`; an `EVAL` with the same meaning where the server refuses the combination), so no caller sees the key vanish between a failed claim and a read. `incr` sets the ttl on every call, as one `MULTI`. The four operations stay the whole interface; the sample node's redelivery guard (#7) will need a plain `set`, to be amended there. Keys are `{pipeline}:{tenant}:{node}:{purpose}:{...}`, the tenant added: the prefix is applied by the state handle, never by a stage, so no configuration can share one tenant's state with another's. `{pipeline}` is the top-level config key `name` (default `pipeline`); replicas of one pipeline share state, two different pipelines on one store need different names. Node ids and the name cannot contain `:`; a tenant that does has it escaped. A record with no tenant is scoped under `unknown`, the same segment a tenant literally named `unknown` gets; the two share state as they share a metric label. The data is shared, the connection is not: the engine opens one synchronous connection per worker at start, only when a node declares that it uses state, with a 5 s connect timeout, 2 s read and write timeouts and a `PING`; any I/O error or timeout drops the connection and the next call reopens it, so a reply arriving after a timeout is never read as the answer to the next command. The store is named by `DRAGONFLY_URL` (default `redis://127.0.0.1:6379`); there is no `state` block in the YAML, as with telemetry. The `state_error` drop reason has no producer: the policies are `pass` and `nak`, neither of which drops. The compose Dragonfly runs with `--maxmemory` and eviction off: an evicted key would silently reopen a window, whereas out-of-memory is a `StateError` the policy handles visibly. Recorded in ADR 0004.

### Source, sink, ack

The source interface yields an envelope: record plus ack handle. The ack handle supports `ack()` and `nak(delay)`. The sink interface accepts a slice of records and returns success only on durable acceptance (`PubAck` for NATS).

The engine keeps an outstanding-branch counter per record. Each branch terminates in a sink success or a drop and decrements; ack fires at zero. Any branch producing `nak` marks the record failed, and the nak fires once all branches have finished.

JetStream pull consumer, explicit ack, `ack_wait` 30s, `max_deliver` 5, backoff on nak. On the final failed delivery the message is terminated and its payload published to `dlq.{tenant}` with the failure reason as a header.

Sink publishes await `PubAck`. (assumed) The sink stream is pre-created by the compose stack, not by the pipeline.

### Telemetry

Metrics, logs and traces all go over OTLP to one collector. The collector fans out to Prometheus, Loki and Tempo. Grafana reads all three.

The spine metric is `records_dropped_total{tenant, stage, reason}`. Reasons are a closed set: `filter`, `route_default_drop`, `sample`, `dedupe`, `lua_drop`, `lua_error`, `regex_limit`, `state_error`, `invalid_record`, `missing_id`. `regex_limit` covers every tripped regex limit: match, depth, heap, work and input size. An engine error that is not a limit (allocation failure, an unexpected PCRE2 code) is a stage error and counts in `records_errored_total`, not as a drop reason.

Other metrics: per-stage `records_in_total`, `records_out_total`, `records_errored_total`, `stage_duration_seconds`; `state_ops_total`, `state_op_duration_seconds`, `state_errors_total`; `lua_errors_total{kind}`; `source_naks_total`, `source_redeliveries_total`, `dlq_total`; `sink_publish_duration_seconds`, `sink_publish_errors_total`; `pipeline_end_to_end_seconds`.

Traces are head-sampled at 1% by record id, with force-sampling on any error or nak.

NATS is scraped through `prometheus-nats-exporter`; Dragonfly is scraped at `:6379/metrics`.

Two dashboards are provisioned. The tenant dashboard shows in/out, dropped by stage and reason, bytes, and e2e p99. The internal dashboard shows everything, plus state store, Lua, regex, NATS and sink panels and the chaos-test coverage panel.

Amended 2026-09-11 (issue #11): The internal dashboard ships with #11; the tenant dashboard is #12. Labels. Every metric carries `tenant`, read from `resource.tenant.id` and `unknown` when absent. The per-stage metrics carry `stage`, the node id; decisions the engine takes before any node runs carry the reserved `stage="source"`, which the metrics treat as a node: every record the source hands over counts on `records_in_total{stage="source"}`, every record that enters the graph on `records_out_total{stage="source"}`, and the engine's rejections are its drops, so intake is one series whatever the first node is. `missing_id` is counted on `records_dropped_total{stage="source", reason="missing_id"}` (the record was not forwarded) and on `source_naks_total` (its message was nakked); `invalid_record` on `records_dropped_total{stage="source"}` only, since rejection acks. Both are counted per delivery: a nakked message that comes back counts again, until the dead-letter queue (#10) ends the retries. `records_out_total` on a sink counts writes that got durable acceptance; on a stage that splits, one per record out. A panic inside a stage or sink counts on `records_errored_total` under that node. `pipeline_end_to_end_seconds` is measured on the ack from `observed_time_unix_nano`, falling back to `time_unix_nano`, and is not observed when the record carries neither or when the record is nakked. Export is OTLP over HTTP/protobuf, configured by the standard `OTEL_EXPORTER_OTLP_*` environment and off when no endpoint is set; there is no telemetry block in the pipeline YAML. `state_ops_total`, `state_op_duration_seconds`, `state_errors_total`, `lua_errors_total` and `dlq_total` are named in the closed set but have no producer until #6, #8 and #10 land, so no series exists in Prometheus until then; the dashboard panels exist and say so. (Amended 2026-09-11, issue #6: the three `state_*` metrics are produced by the state handle from #6 on, one `state_ops_total` and one `state_op_duration_seconds` sample per operation, `state_errors_total` per failed operation, all under the tenant and node of the record being processed.) Worker utilisation has no metric of its own; the panel derives busy worker-seconds from the duration histograms' sums. Process CPU and memory are not pipeline metrics and stay outside the closed set: each process reports its own (ADR 0003). The `regex_limit` drop reason likewise has no producer until the regex stages (#5) land. The exported resource carries `service.instance.id`; the collector's Prometheus exporter turns the resource into the `job` (service name) and `instance` labels, so several pipeline instances behind one consumer are distinct series that the dashboard sums.

### Chaos test

Vendored data lives under `testdata/loghub/<Set>/` (raw log, structured CSV, templates CSV; about 300 KB per set).

Compose services: `nats`, `dragonfly`, `otel-collector`, `prometheus`, `loki`, `tempo`, `grafana`, `pipeline`, `producer`, `verifier`, `nats-exporter`.

The producer is a Rust binary (`--rate`, `--count`, `--datasets Linux,OpenSSH,Apache,Mac`, `--dup-percent`) that replays vendored loghub lines round-robin and is the only data source in the POC. Its output format is what an OTel Collector or Vector agent with a NATS sink would emit, so replacing it with a real agent later changes nothing in the pipeline. The `nats` CLI is used for single-record manual checks (`nats pub`, `nats sub`, `nats consumer info`).

The producer publishes 100k records over about 60s (the 8,000 vendored lines cycled) with snowflake ids, about 30% deliberate duplicates within the dedupe window (the same line re-sent under a new id), and all four formats interleaved. It records the expected outcome for each id: which sinks, or dropped and why, and the expected extracted attributes from the structured CSV.

Chaos script: at t=20s kill the pipeline container, restart at t=25s; at t=40s pause Dragonfly for 5s.

The verifier subscribes to every sink subject, records every id seen per subject, and compares against the producer's expectations for both delivery (which sink) and content (extracted attributes equal the structured-CSV ground truth for that line). It emits `published`, `received`, `missing`, `unexpected` and `extraction_mismatch{format}` as metrics. Pass condition: `missing = 0` and `unexpected = 0`. (assumed) Extraction accuracy per format is reported and not gated; the regex patterns are hand-written, so a few percent of mismatch on a format is a finding about the pattern rather than a test failure.

### Layout

Cargo workspace: core (record, DAG, engine, traits, condition grammar), stages, regex wrapper, NATS source and sink, Dragonfly state store, telemetry wiring, and the binary. Harness (producer, verifier, chaos) and deploy (compose, collector config, dashboards) sit alongside.

## Testing decisions

A good test drives behaviour through a boundary the system already has and asserts on observable output, never on internal structure. Three boundaries are used; nothing below them is tested directly except the unsafe regex wrapper.

The trait boundary is the primary one. In-memory `Source`, `Sink` and `StateStore` implementations exist for it. Tests load a YAML config, push envelopes in, and assert which records reached which sink, in what shape, and which ack handles saw `ack` versus `nak(delay)`. This single seam covers every stage, routing, fan-in and fan-out, the outstanding-branch ack counter, redelivery idempotency (push the same envelope twice), state-store failure policies (the fake store returns errors on demand), Lua guardrails (scripts that loop, allocate, return garbage, or touch `os`), and config validation (cyclic, orphan, unconsumed label, no sink).

Amended 2026-09-11 (issue #11): a fourth boundary, the recorder. `Recorder` is the seam an exporter implements, with an in-memory fake beside the in-memory source and sinks; what the engine emits is asserted through the trait boundary above (push envelopes, read the fake back). Below it, only two things are tested directly: the closed sets of metric names and drop reasons against the spec's spelling, and the OTLP recorder on the SDK's in-memory exporter, as the regex wrapper is tested on its safe API, since instrument names, units and attributes are observable nowhere else short of a live collector.

The regex wrapper boundary gets unit tests on the safe API only: named capture extraction on both engines with identical results for shared syntax; classification (`(?<=x)y` is backtracking, `\w+` is linear); lint verdicts on textbook ReDoS patterns (`(a+)+$`, `(a|aa)*b`) and on benign ones; the canary rejecting `(a+)+$` and accepting `^\d{1,3}$`; each PCRE2 limit tripped by a known-pathological pattern with the right error variant; compile errors surfaced with offset; `Send` and `Sync` compile-time assertions. (assumed) Run under Miri or AddressSanitizer in CI if cheap.

The compose end-to-end boundary is the chaos test above, run as a script that returns non-zero on `missing > 0` or `unexpected > 0`. It is the only test that touches real NATS or Dragonfly.

There is no prior art in this repository; it is empty apart from the feature catalogue.

Tests are written before implementation for each stage and for the engine's ack logic. The chaos test is written alongside the harness, before the pipeline binary is wired.

## Out of scope

- Metrics and traces signals: only `kind: log` is processed.
- Dedicated parser nodes (syslog, JSON, key=value, XML, Grok, timestamp), raw EVTX, dissect, timestamp auto-detection, GeoIP, lookup-table enrichment, log-to-metric, aggregation and windows, rate limiting, OCSF, and everything else in the catalogue not named above.
- A VRL- or OTTL-style expression language. The condition grammar is deliberately small.
- OTLP protobuf sources and sinks; batching more than one record per NATS message.
- Per-key ordering and partitioned consumers.
- A config reload trigger (file watch, signal, control plane). The in-memory swap path exists; nothing calls it after startup.
- Multiple pipelines per process; per-tenant pipeline configs.
- Circuit-breaking a failing Lua stage. The error-rate metric is emitted, so this is one flag later.
- Throughput or latency targets. Performance is observed, not asserted.
- Production hardening: TLS, auth, multi-node, HA state store.

## Further notes

Verified before writing this spec: `async-nats 0.50` has `AckKind::Nak(Option<Duration>)` and `Term`, pull-consumer `max_deliver`, `ack_wait` and `backoff`, and `PublishAckFuture`. `mlua 0.12` has `set_memory_limit`, `HookTriggers::every_nth_instruction`, and `lua54` and `lua55` features; its `sandbox()` is Luau-only, hence the manual global stripping. `redis 1.7` has `SetOptions` with NX and EX. `pcre2-sys 0.2.10` exports `pcre2_set_match_limit_8`, `pcre2_set_depth_limit_8`, `pcre2_set_heap_limit_8`, `pcre2_set_max_pattern_length_8` and `pcre2_set_parens_nest_limit_8`; it does not bind `pcre2_set_callout_8`, which the wrapper declares by hand against the bundled static library. Dragonfly serves Prometheus metrics at `:6379/metrics`. `opentelemetry-otlp 0.32`, `opentelemetry 0.32` and `tracing-opentelemetry 0.33` resolve together. Toolchain present: cargo 1.96.1, docker 29.5, compose v5.1, gcc 13, `nats` CLI.

Not yet verified: the `prometheus-nats-exporter` flags for JetStream consumer metrics. To be confirmed at build time.

Amended 2026-09-11 (issue #11): verified. `prometheus-nats-exporter 0.15.0` with `-varz -connz -jsz=all` exports `jetstream_consumer_num_pending`, `jetstream_consumer_num_redelivered`, `jetstream_consumer_num_ack_pending`, `jetstream_consumer_delivered_stream_seq` and `jetstream_consumer_ack_floor_stream_seq` per `stream_name`/`consumer_name`; ack lag is the difference of the last two.

`irgx` (a Zig linear-time engine with PCRE2 fallback, shipped as a prebuilt static archive per target) was evaluated and rejected. It has the same linear-first, backtracking-fallback shape as `regex` plus PCRE2, but is an opaque foreign binary with no static ReDoS analysis and no maturity signals. We know of no sound Rust static ReDoS analyzer; the three-layer load-time check is the substitute.

The `pcre2` safe crate was considered. A fork exposing `match_limit` and `depth_limit` exists at `inxbit/rust-pcre2`, branch `expose-match-and-depth-limits`, upstream PR 56. We chose to own the wrapper over `pcre2-sys` so that all limits, including heap and compile-time guards, are available without waiting on upstream.

Vector's topology (`inputs:` per component, `route` with named outputs, end-to-end acknowledgements) is the reference model. Ours differs in choosing Lua over VRL, a `regex`-first facade with PCRE2 fallback over `regex` alone, external state over in-memory, and JetStream redelivery over disk buffers.
