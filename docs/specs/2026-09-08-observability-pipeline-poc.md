# Observability pipeline POC: spec

Status: draft, 2026-09-08. The decisions below came out of a grilling session. Where something was assumed rather than decided, it is marked (assumed).

## Problem statement

We need to decide whether to build our observability pipeline around a declarative DAG of stages with Lua as the escape hatch, and whether we can promise at-least-once delivery from a NATS source to a NATS sink while keeping stage state outside the process. The atomic feature catalogue (`pipeline_atomic_features.csv`, 148 features across logs, metrics and traces) says what the pipeline must eventually do. It does not say whether the stage model, the ack model, or the language choice hold up. We also have no visibility into a pipeline like this once it runs.

## Solution

A working proof of concept that:

- consumes one JSON log record per JetStream message, runs it through a YAML-declared DAG of stages, and publishes it to one or more JetStream sinks;
- acknowledges the source message only after every sink the record reached has confirmed durable receipt (`PubAck`), and negatively acknowledges on any failure so JetStream redelivers;
- implements the smallest stage set that covers each stage category (stateless transform, declarative field editing, stateful transform backed by Dragonfly, sandboxed Lua) plus routing with fan-in and fan-out;
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

Amended 2026-09-16 (issue #8): one Lua VM per worker per `lua` node, not one per worker; the Lua paragraph under Stages says why.

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

Internally the config becomes a DAG (node ids plus an edge list). The POC config runs filter, route, four per-format `pcre2_extract` nodes, edit, redact, sample, dedupe, lua and a main sink, with a second sink hanging off one route branch to exercise fan-out.

Amended 2026-09-15 (issue #7): the compose pipeline, `deploy/pipeline.yaml`, carries no sample node, because its smoke test and metrics check expect every record that passes `filter` and `dedupe` to reach the sink. The sample node of the POC config sits on the archive branch of `deploy/pipeline-routing.yaml`, in `consistent` mode on `resource.host`, where dropping half the hosts is the branch's purpose.

Amended 2026-09-16 (issue #21): the compose pipeline does carry one dropping node, on a fan-out branch so the sentence above still holds for the main sink: `only_parsed`, an `edit` node with `on_unapplied: drop` reading from the extract node, renames `attributes.Content` to `attributes.message` and drops every record the syslog pattern did not parse, into a second sink on `processed.parsed`. It is the live producer of the `edit_unapplied` drop reason, which the metrics check requires; a dropped copy on that branch is a success for the ack, and the main branch delivers the same record unchanged.

### Condition grammar

`field op literal`, where field is a dotted path into the record, ops are `== != =~ !~ < > <= >=`, combinators are `and or not`, and parentheses group. `=~` and `!~` compile their pattern through the same regex facade with the same limits.

Amended 2026-09-11 (issue #20): the field was "a dotted or bracketed path"; bracket syntax is removed and is a load-time config error naming the node, with the dotted form in the message.

Amended 2026-09-14 (issue #5): the literal after `=~` or `!~` must be a string, the pattern; anything else is a parse error with the literal's offset, reported at load naming the node. A field that is not a string makes `=~` false and `!~` true, as `!=` is on a type mismatch. `and` and `or` short-circuit, so a pattern on the side that is not reached is not run.

### Field paths

Every stage names a record field with one dotted path, `root ("." segment)*`, and nothing else. The root is a top-level record field. When it is `attributes`, `resource` or `scope`, the segments after it joined with dots are the map key: `attributes.http.status` reads and writes the `http.status` key of `attributes`, and `resource.tenant.id` is the tenant. Map values are scalars; nothing below a key is addressable. `body` is addressed only as a whole, and the scalar fields (`id`, `kind`, `severity_text`, `severity_number`, the time fields, `trace_id`, `span_id`) take no segments. A bare segment is one or more of `[A-Za-z0-9_-]`, so `resource.k8s.pod-name` and `attributes.5xx.count` need no quoting; a segment with any other character is written as a double-quoted string, `attributes."Event ID".code` naming the `Event ID.code` key, with `\"` and `\\` as the only escapes. The root is never quoted.

Core exposes read, write and remove by path so `filter`, `route`, `edit`, `pcre2_extract`, `redact` and `lua` share one path semantics. Writes to `id` and `kind` are refused; `severity_number` takes an integer, `severity_text`, `trace_id` and `span_id` a string, the time fields a non-negative integer, and a map key a scalar. Every refusal is an error value, never a panic, and the record is unchanged on error. Every path parse error is reported at config load, naming the node and saying what to write instead; write and remove refusals are reported by the stage that performs them.

Amended 2026-09-16 (issue #21): "reported by the stage that performs them" reads two ways. In `extract` and `redact` a write refusal cannot arise from data (they write strings they just read as strings), so it stays a stage error, an invariant check. In `edit` it can (`copy body -> severity_number` where one tenant's `body` is an object) and is decided by the record's shape, so a nak would only redeliver the same refusal; there it is a counted unapplied op under the node's `on_unapplied`, never a stage error.

### Stages

Every stage implements one function: record in, and one of `Pass(record)`, `Drop(reason)`, `Routed(label, record)`, `Split(records)` or `Error(error)` out. Stages receive a context carrying the record id, tenant, node id, state-store handle and metrics handle.

Amended 2026-09-11 (issue #6): a sixth output, `StateError { record, error }`, for a stage that could not reach the state store. `Error` carries no record, and the `pass` policy needs the record back unchanged, so the stage hands it over with the error and the engine decides. The context's state handle carries the metrics handle inside it: a stage never emits a metric directly, every state operation is counted by the handle. A stage declares `uses_state` so the engine knows whether to open connections at all.

Amended 2026-09-14 (issue #5): "a stage never emits a metric directly" reads: never without a handle the engine built for it. The context carries a second narrow handle, for the metrics a stage owns (`regex_nonmatch_total`); the per-node series (`records_in_total` and the rest) stay the engine's and are not reachable from a stage.

`filter` takes a condition and `action: drop|keep`.

`route` takes ordered labelled conditions; first match wins, default applies otherwise.

`pcre2_extract` takes `field`, `pattern`, `limits {match, depth, heap_kib, work, input_bytes}` and `on_redos_risk: reject|warn` (default `reject`). Named groups become attributes. The engine is chosen per the facade rules above; `limits.match`, `depth`, `heap_kib` and `work` apply only on the PCRE2 path, `input_bytes` on both. An absent `work` takes the default (10 000 000); `work: 0` turns the count off. (assumed) A non-match is `Pass` unchanged rather than an error.

`redact` takes `fields`, `pattern`, `replace` and the same limits. Replacement is in place.

`edit` takes an ordered `ops` list and `on_unapplied: skip|drop` (default `skip`). Each entry is one op: `set {field, value}` writes a literal (string, number, bool or null); `rename {from, to}` moves a value; `copy {from, to}` duplicates one; `hash {field}` replaces a value with its lowercase hex SHA-256 (a string as its bytes, a number or bool as the canonical JSON text the key hash uses; a stable join key, not anonymisation, since an unsalted digest of a low-entropy field is dictionary-reversible); `delete {fields}` removes each listed path, an absent one being nothing to do. Ops run in order on the same record; `rename` and `copy` overwrite `to`; no templates, defaults or conditions, a conditional edit being a `route` branch with its own `edit` node. An op is unapplied when its source reads as null (absent, or JSON `null`) or the target refuses the value; only `rename`, `copy` and `hash` can be. The record is unchanged by that op (`rename` writes `to` before it removes `from`), the op counts on `edit_unapplied_total{tenant, stage, op, field, cause}` with `op` the op's kind, `field` its source path in canonical form and `cause` one of `absent`, `type`, and then `skip` runs the next op while `drop` drops the record with reason `edit_unapplied`. The node never naks or errors at runtime: the outcome is fixed by the record's shape, so a redelivery would repeat it. Load rejects, naming the node and the op's position: an unknown op, an entry that is not exactly one op, an empty `ops` or `fields`, a missing or unknown key, a malformed path, an op naming `id` or `kind` where it writes or removes, any op that writes or removes `resource.tenant.id` (the engine fixes the tenant once per record for every label and state key; `copy` may read it), a `set` literal that is a map or list or of the wrong type for its field, a `hash` target that does not take a string, `from` equal to `to` as parsed paths, and an `on_unapplied` outside the two values. The type checks are core's own write rules run on an empty record at load. A `set`, `copy` or `rename` into a time field changes ingestion time for every downstream stateful node and the end-to-end histogram; allowed, deterministic, and noted here.

Amended 2026-09-15 (issue #5): the node type is `extract`, not `pcre2_extract`. The name says what the node does, as `filter`, `route`, `redact` and `dedupe` do; which engine a pattern lands on is the facade's decision and is reported on the `engine` label, so an engine in the type name would be wrong for every linear pattern. Every `pcre2_extract` above reads `extract`.

Amended 2026-09-14 (issue #5): `replace` is literal text; `$1`, `$name` and `\1` are written as they are, since the two engines expand them differently and one config must give one output whichever engine the pattern lands on. Every match in each listed field is replaced, following the `regex` crate's rule for empty matches on both engines. A field that is not a string is skipped by `redact` and is a non-match for `extract`. A record no listed field matched, or whose `field` did not match, passes unchanged and counts once on `regex_nonmatch_total`. `redact` refuses `id` and `kind` in `fields` at load. `filter` and `route` take the same `limits` and `on_redos_risk` for the patterns in their conditions; a tripped limit in a condition drops the record with reason `regex_limit` whatever the `action` or the label would have been, and any other engine failure is a stage error. On every regex stage a limit that trips on one record leaves the pattern serving the next. `input_bytes` bounds the text a pattern runs on, not the record: the `field` of `extract`, each listed field of `redact` on its own, the field of each regex leaf in a condition.

`sample` takes `mode: random|every_nth|consistent`, `percent` or `n`, and `key` for consistent mode. `every_nth` uses a shared `incr` in the state store guarded by the record id so redelivery does not double-count.

Amended 2026-09-15 (issue #7): `every_nth` is 1 in n of the deliveries that reach the node, per tenant, across every worker and replica: one shared `incr` per delivery on `sample:count` under the state handle's prefix, and the first of each n is kept (counts 1, n+1, 2n+1, ...), so a tenant with fewer than n records still gets one through. The guard by record id is dropped: a message JetStream redelivers is a new delivery and takes a new count. Keeping the guard meant one state key per record, kept for longer than any redelivery, plus a second round trip per record; the pipeline tools that offer 1-in-N sampling do not pay that price either. The cost is stated: a kept record whose sink fails is nakked, redelivered, and takes a new count on the way back, so during a sink outage most records the node had chosen are dropped on their retry. The share of deliveries stays 1 in n; the set of records that arrive is not the set first chosen. `every_nth` is therefore the stated exception to "a redelivered record gets the same answer whenever it comes back" (ADR 0004). The sample count lives 24 h, refreshed on every `incr`, so no tenant with traffic sees it restart; a tenant silent for a day restarts at 1 and its first record back is kept. `random` flips its coin on the record id: the id mixed with a hash of the node id through the splitmix64 finalizer, kept when the result is at or below `percent` of the 64-bit space, so a redelivered record lands on the same side and two `random` nodes in series keep independent subsets. `consistent` hashes the `key` field values as `dedupe` does (canonical JSON, FNV-1a 64, a missing field is `null`), mixes without a salt, and compares with the same threshold, so the same key value gets the same verdict on every node and pipeline and a key kept at a lower `percent` is kept at any higher one; records missing the key share one `null` value, kept or dropped together, and the `percent` at which that value flips is fixed (about 32 for a one-field key). `percent` is a number in `(0, 100]`, `n` at least 1; each field belongs to one mode and is rejected under another, naming the mode it belongs to. `on_state_error` is accepted for `every_nth` only, default `pass`. Only `every_nth` declares `uses_state`. The 2026-09-11 note under State store that `set` would serve "the sample node's redelivery guard" is overtaken: there is no guard, `sample` uses `incr` only, and `set` stays for callers that mean last-writer-wins.

`dedupe` takes `key` fields, `window` and `on_state_error: pass|nak`. The state value is the record id. When `set_nx` fails the stage reads the stored id: the same id means this record is being seen again, so it passes; a different id means a real duplicate, so it drops.

Amended 2026-09-11 (issue #6): the window is measured in ingestion time, not on arrival at the stage. The state value is `"{record id} {ingestion time}"` and `set_nx` answers with it in one step. A record whose claim fails passes when the stored id is its own (redelivery while the key lives), when its ingestion time is older than the holder's (an earlier record coming back after its key expired and a newer duplicate took it), or when its ingestion time is `window` or more past the holder's (the window is over); it drops as a duplicate only when its ingestion time falls inside the holder's window. The arrival-time reading had a hole: a record that passed, whose process died before the ack, redelivered after `ack_wait` with the key expired and a newer duplicate holding it, was dropped, a real record lost, and closing it by rule (`window` longer than every redelivery) would have tied the window's meaning to the consumer's settings. A record past the holder's window takes the key over with a plain `set` (the holder's key would otherwise live on until its server-side TTL ran out and every record of a replayed burst would pass), so the burst behind it dedupes against the new window. Three imperfections remain, all an extra copy and never a loss: an older record arriving after a newer one with the same content passes, a duplicate delayed longer than the window passes, and two records past the window arriving on two workers at once both pass. A stored value the stage cannot parse passes the record for the same reason. Ingestion time is `observed_time_unix_nano`, else `time_unix_nano`, else the worker clock; the NATS source fills `observed_time_unix_nano` from the JetStream publish time when a record has neither. `key` is one or more field paths; a missing field is `null`, so records lacking it dedupe together. The content hash is FNV-1a 64 over the canonical JSON of the key values. `window` is `<integer><ms|s|m|h>`, at least 1 ms (Dragonfly refuses `PX 0`). `on_state_error` defaults to `pass`.

Amended 2026-09-14 (issue #29): the takeover is a compare-and-set, not a plain `set`. A record past the holder's window writes itself only if the key still holds the value it just read (or nothing, the key having expired meanwhile); otherwise the store answers with the current holder and the stage re-runs its verdict against that holder without another round trip, dropping if it is inside the new holder's window and passing otherwise. A verdict of `WindowOver` against the returned holder (an older record claimed the expired key meanwhile) is tried once more against that holder; refused again, the record passes without holding the key, so a busy key cannot hold a worker in a loop, and the next record with that content takes the key over from what it then reads. So two records past the window arriving on two workers at once agree on one holder (the second's takeover is refused and it drops as a repeat of the first), and a slow worker's stale takeover can no longer move the window backwards past a newer holder. Two imperfections remain, both an extra copy and never a loss: an older record arriving after a newer one with the same content passes, and a duplicate delayed longer than the window passes.

`lua` takes `script`, `limits {instructions, memory}` and `on_error: drop|pass|nak` (default `pass`). The sandbox removes forbidden globals; the instruction budget is a count hook; the memory cap is an allocator limit; output is validated before leaving the stage.

Amended 2026-09-16 (issue #8): as built. The script comes from `script` (a file path, read at load) or `source` (the script inline in the YAML), exactly one. `limits` is `{instructions, memory_kib, output_kib}`: the budget is instructions per record (default 1 000 000, checked every 1 000 instructions or at the budget when it is smaller), the cap is the VM's allocator limit in KiB like the regex `heap_kib` (default 16 384, at least 64, since the sandbox itself needs a few tens), and `output_kib` (default 1 024) bounds the bytes of every string in a returned record together, the "size under cap" of story 56. The VM is one per worker per `lua` node, not one per worker: the memory cap is a property of a VM, so per-node limits need per-node VMs, and two scripts sharing a VM would share globals. It is built on the worker's first record through the node and kept in a thread-local keyed by the compiled node, so a swapped pipeline gets fresh VMs at the next record boundary as the swap paragraph asks. The sandbox loads `string`, `table`, `math` and `utf8` only; `os`, `io`, `package` (and so `require`) and `debug` are never loaded, `load`, `loadfile`, `dofile` and `collectgarbage` are removed from the base library, and no coroutines, since the hook is per Lua thread. A script that names one of `os`, `io`, `package`, `require`, `load`, `loadfile`, `dofile`, `loadstring` or `debug` as a free identifier anywhere, inside `process` included, is refused at load with the line: a lexical scan over the source outside strings and comments, coarse on purpose (`local x = os` is refused like `os.exit()`; a name followed by a single `=`, a table key or an assignment target, is not a read; a name built at run time finds `nil` and is a `runtime` error per record). The same scan decides `uses_state`: a script that names `state` gets a store connection, one that does not never opens one. Load also runs the script's top level once in a throwaway sandbox under the same budget and cap, so a syntax error (with the line, under the file path or the node id), a missing `process`, or a top level that loops fails the config. The record crosses as a table with the OTLP field names and the maps as tables of scalars; `id` is an integer, or its decimal text above 2^63. The returned table must carry `id` unchanged and `kind` as `log`, every other field is optional and typed as core's write rules type it, the maps stay flat, a key that is not a record field is refused (a typo would otherwise drop data silently), and `resource.tenant.id` must come back unchanged for the reason `edit` refuses to write it. A list is a table whose `[1]` is set; every entry is checked the same way and keeps the original `id`, so a script cannot mint ids: the split records of one message share its id, as the story asks for a body split by line. An empty table is neither and is refused. The API is `state.get(key)`, `state.set_nx(key, value, ttl_ms)` (`true`, or `false` and the holder), `state.incr(key, by, ttl_ms)` and `state.del(key)` on the record's state handle, so every key is under `{pipeline}:{tenant}:{node}:` and every call is on the `state_*` metrics; `log.info` and `log.warn` (stderr until the logs ticket); `now_ns()`. A `state.*` call the store cannot answer is not a Lua error: the run ends as `StateError { record, error }` and the engine applies `on_state_error`, default `nak` (the stage may produce data from state), `pass` accepted. `lua_errors_total{kind}` is a closed set in core beside the drop reasons: `instructions`, `memory`, `runtime` (the script raised, or indexed `nil`), `output` (the returned record refused). Every kind counts once, then `on_error` decides; `pass` forwards the record as it entered the node. Load-time failures reject the config and never count.

### State store

The interface has four operations: `set_nx(key, value, ttl) -> bool`, `get(key) -> Option<bytes>`, `incr(key, by, ttl) -> i64`, `del(key)`. The Dragonfly implementation speaks the Redis protocol. An in-memory implementation exists for tests.

The engine applies the failure policy from per-node config; the stage does not. Default: `pass` for dedupe and sample, `nak` for anything that would produce data from state.

(assumed) Key namespace is `{pipeline}:{node}:{purpose}:{...}`.

Amended 2026-09-11 (issue #6): `set_nx(key, value, ttl)` returns `None` when it claimed the key and `Some(existing value)` when it did not, one atomic step (`SET NX PX GET`; an `EVAL` with the same meaning where the server refuses the combination), so no caller sees the key vanish between a failed claim and a read. `incr` sets the ttl on every call, as one `MULTI`. A fifth operation, `set(key, value, ttl)`, the unconditional write, is added: `dedupe` needs it to take over a key whose holder's window is over, and the sample node's redelivery guard (#7) will need it too. Keys are `{pipeline}:{tenant}:{node}:{purpose}:{...}`, the tenant added: the prefix is applied by the state handle, never by a stage, so no configuration can share one tenant's state with another's. `{pipeline}` is the top-level config key `name` (default `pipeline`); replicas of one pipeline share state, two different pipelines on one store need different names. Node ids and the name cannot contain `:`; a tenant that does has it escaped. A record with no tenant is scoped under `unknown`, the same segment a tenant literally named `unknown` gets; the two share state as they share a metric label. The data is shared, the connection is not: the engine opens one synchronous connection per worker at start, only when a node declares that it uses state, with a 5 s connect timeout, 2 s read and write timeouts and a `PING`; any I/O error or timeout drops the connection and the next call reopens it, so a reply arriving after a timeout is never read as the answer to the next command. The store is named by `DRAGONFLY_URL` (default `redis://127.0.0.1:6379`); there is no `state` block in the YAML, as with telemetry. The `state_error` drop reason has no producer: the policies are `pass` and `nak`, neither of which drops. The compose Dragonfly runs with `--maxmemory` and eviction off: an evicted key would silently reopen a window, whereas out-of-memory is a `StateError` the policy handles visibly. Recorded in ADR 0004.

Amended 2026-09-14 (issue #29): a sixth operation, `compare_and_set(key, expected, value, ttl)`, the conditional write: it sets the value with the ttl only when the key holds `expected` or nothing, answering `None`, and otherwise leaves the key alone and answers with the current value, one atomic step (one `EVAL` on Dragonfly, one conditional in the memory store). The store compares bytes and knows nothing of what they mean; the caller decides against the holder it is given. `dedupe` takes a key over with it; `set` stays for callers that mean last-writer-wins.

### Source, sink, ack

The source interface yields an envelope: record plus ack handle. The ack handle supports `ack()` and `nak(delay)`. The sink interface accepts a slice of records and returns success only on durable acceptance (`PubAck` for NATS).

The engine keeps an outstanding-branch counter per record. Each branch terminates in a sink success or a drop and decrements; ack fires at zero. Any branch producing `nak` marks the record failed, and the nak fires once all branches have finished.

JetStream pull consumer, explicit ack, `ack_wait` 30s, `max_deliver` 5, backoff on nak. On the final failed delivery the message is terminated and its payload published to `dlq.{tenant}` with the failure reason as a header.

Sink publishes await `PubAck`. (assumed) The sink stream is pre-created by the compose stack, not by the pipeline.

### Telemetry

Metrics, logs and traces all go over OTLP to one collector. The collector fans out to Prometheus, Loki and Tempo. Grafana reads all three.

The spine metric is `records_dropped_total{tenant, stage, reason}`. Reasons are a closed set: `filter`, `route_default_drop`, `sample`, `dedupe`, `lua_drop`, `lua_error`, `regex_limit`, `state_error`, `invalid_record`, `missing_id`, `edit_unapplied`. `regex_limit` covers every tripped regex limit: match, depth, heap, work and input size. An engine error that is not a limit (allocation failure, an unexpected PCRE2 code) is a stage error and counts in `records_errored_total`, not as a drop reason.

Other metrics: per-stage `records_in_total`, `records_out_total`, `records_errored_total`, `stage_duration_seconds`; `state_ops_total`, `state_op_duration_seconds`, `state_errors_total`; `lua_errors_total{kind}`; `source_naks_total`, `source_redeliveries_total`, `dlq_total`; `sink_publish_duration_seconds`, `sink_publish_errors_total`; `pipeline_end_to_end_seconds`.

Amended 2026-09-16 (issue #8): `lua_errors_total{tenant, stage, kind}` has producers, and `kind` is the closed set `instructions`, `memory`, `runtime`, `output` (the Lua paragraph under Stages says which is which). `records_dropped_total{reason="lua_drop"}` and `{reason="lua_error"}` have producers too: a script returning `nil`, and a failed run under `on_error: drop`. The compose pipeline's `split_lines` node produces the metric under `kind="runtime"` from one record the metrics check publishes with a non-numeric `http.status`, and runs with `on_error: pass`, so neither drop reason has a live producer there.

Amended 2026-09-16 (issue #21): one more metric, `edit_unapplied_total{tenant, stage, op, field, cause}`, one per `edit` op that could not apply to a record, and one more drop reason, `edit_unapplied`, produced only by an `edit` node with `on_unapplied: drop`. `op` is the op's kind (`set`, `rename`, `copy`, `hash`, `delete`; only the middle three ever count), `field` the op's source path in canonical form, so a re-quoted path is not a new series and cardinality is bounded by the config, and `cause` is `absent` or `type`. The label values are closed sets in core, next to the drop reasons. Both appear in the per-stage dashboard row and are required by `deploy/metrics-check.sh` from the compose pipeline's two `edit` nodes: `tag_service` on the main branch produces the metric under `skip`, `only_parsed` on the parsed branch produces the drop reason under `drop`.

Amended 2026-09-14 (issue #5): one more metric, `regex_nonmatch_total{tenant, stage, engine}`, counts the records a regex stage's pattern did not match and passed on unchanged, so extraction coverage per node is readable without the harness verifier. The `engine` label: every per-node metric of a node whose stage runs a regex (`extract`, `redact`, and `filter` or `route` when a condition uses `=~` or `!~`) carries `engine="linear"` or `engine="backtracking"`, the facade's classification of its pattern; a condition with several patterns reports `backtracking` if any of them needs PCRE2; nodes without a regex carry no `engine` label, so `sum by (stage)` is unchanged and a regex node can be split by engine. The same value is in the node's load-time log line. `records_dropped_total{reason="regex_limit"}` now has producers: the two regex stages and conditions with regex operators.

Traces are head-sampled at 1% by record id, with force-sampling on any error or nak.

NATS is scraped through `prometheus-nats-exporter`; Dragonfly is scraped at `:6379/metrics`.

Two dashboards are provisioned. The tenant dashboard shows in/out, dropped by stage and reason, bytes, and e2e p99. The internal dashboard shows everything, plus state store, Lua, regex, NATS and sink panels and the chaos-test coverage panel.

Amended 2026-09-11 (issue #11): The internal dashboard ships with #11; the tenant dashboard is #12. Labels. Every metric carries `tenant`, read from `resource.tenant.id` and `unknown` when absent. The per-stage metrics carry `stage`, the node id; decisions the engine takes before any node runs carry the reserved `stage="source"`, which the metrics treat as a node: every record the source hands over counts on `records_in_total{stage="source"}`, every record that enters the graph on `records_out_total{stage="source"}`, and the engine's rejections are its drops, so intake is one series whatever the first node is. `missing_id` is counted on `records_dropped_total{stage="source", reason="missing_id"}` (the record was not forwarded) and on `source_naks_total` (its message was nakked); `invalid_record` on `records_dropped_total{stage="source"}` only, since rejection acks. Both are counted per delivery: a nakked message that comes back counts again, until the dead-letter queue (#10) ends the retries. `records_out_total` on a sink counts writes that got durable acceptance; on a stage that splits, one per record out. A panic inside a stage or sink counts on `records_errored_total` under that node. `pipeline_end_to_end_seconds` is measured on the ack from `observed_time_unix_nano`, falling back to `time_unix_nano`, and is not observed when the record carries neither or when the record is nakked. Export is OTLP over HTTP/protobuf, configured by the standard `OTEL_EXPORTER_OTLP_*` environment and off when no endpoint is set; there is no telemetry block in the pipeline YAML. `state_ops_total`, `state_op_duration_seconds`, `state_errors_total`, `lua_errors_total` and `dlq_total` are named in the closed set but have no producer until #6, #8 and #10 land, so no series exists in Prometheus until then; the dashboard panels exist and say so. (Amended 2026-09-11, issue #6: the three `state_*` metrics are produced by the state handle from #6 on, one `state_ops_total` and one `state_op_duration_seconds` sample per operation, `state_errors_total` per failed operation, all under the tenant and node of the record being processed.) Worker utilisation has no metric of its own; the panel derives busy worker-seconds from the duration histograms' sums. Process CPU and memory are not pipeline metrics and stay outside the closed set: each process reports its own (ADR 0003). The `regex_limit` drop reason likewise has no producer until the regex stages (#5) land. The exported resource carries `service.instance.id`; the collector's Prometheus exporter turns the resource into the `job` (service name) and `instance` labels, so several pipeline instances behind one consumer are distinct series that the dashboard sums.

### Chaos test

Vendored data lives under `testdata/loghub/<Set>/` (raw log, structured CSV, templates CSV; about 300 KB per set).

Amended 2026-09-14 (issue #5): Linux, Apache and OpenSSH are vendored with the regex stages; Mac lands with the harness (#13). `testdata/loghub/README.md` carries the loghub-2.0 licence notice (research and academic use, citation required, not the workspace's Apache-2.0), the upstream commit and per-file checksums, and the three rules for comparing extracted attributes with the structured CSV (a `N.0` cell is the integer `N`, an empty cell means the attribute is absent, `Content` is trimmed of trailing whitespace on both sides, since some raw lines end in a space the CSV does not keep, and every other column compares exactly). The patterns lift the text as it is in the line, trailing space included; the normalisation is the comparer's.

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
- Within `edit`: templates, defaults, conditional ops, casts, case changes, a salted `hash`, and `on_unapplied: tag` (marking a record for a downstream route). A conditional edit is a `route` branch with its own `edit` node.
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
