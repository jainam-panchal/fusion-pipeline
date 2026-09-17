# fusion-pipeline

Observability pipeline POC in Rust: a YAML-declared DAG of stages, NATS JetStream in and out, records acked only after every branch ends in a sink `PubAck` or an intentional drop.

Read first, in this order:

1. `README.md`: crate map, commands, minimal config, routing example.
2. `CONTEXT.md`: the glossary. Use its terms in code, tests, issues and commit messages.
3. `docs/specs/2026-09-08-observability-pipeline-poc.md`: the spec. Source of truth for behaviour, limits, metrics and what is out of scope. When a decision changes, amend the spec inline with a dated `Amended YYYY-MM-DD (issue #n):` paragraph rather than rewriting history.
4. `docs/adr/`: decisions that were hard to reverse. Read the ones touching your area before designing.

## Invariants no file confesses

- **Ack discipline.** The engine keeps an outstanding-branch counter per record. Ack fires once every branch has ended in sink success or a drop. Any failed branch means one nak after all branches finish, carrying the walk's first `Failure`. Sinks return success only on durable acceptance; NATS means `PubAck`. The engine knows no dead-letter queue: the NATS source turns a nak on the final delivery into a dead letter, `PubAck`, then terminate.
- **The pipeline never creates streams or consumers.** The compose stack does. Missing stream (the dead-letter stream included), consumer or server is a fail-fast startup error, as is a consumer with no `max_deliver` or with `backoff`.
- **Stages are synchronous and pure over one record.** (`lua` is the one stage with per-worker state: its VM, script and upvalues live in a thread-local of the worker, one per node.) One function: record in, one of `Pass`, `Drop(reason)`, `Routed(label, record)`, `Split`, `Error`, `StateError { record, error }` out. Failure policy for state-store errors is applied by the engine from node config, never inside the stage: the stage hands the record back with the error and the engine reads the stage's `on_state_error`.
- **State is shared, the connection is not.** One Dragonfly keyspace for every worker and every replica; one connection per worker, opened at engine start only when a node declares `uses_state`. Stages reach it through the `State` handle on the context, which prefixes every key with `{pipeline}:{tenant}:{node}:` and counts every op. No stage builds a key without it.
- **Decisions read `Meta`, never the payload, and `Meta` never enters the payload** (ADR 0005). `Meta::resolve` builds `Meta` (record id, tenant, ingestion time, delivery count) once at intake from the source's `Arrival` alone, and the engine hands it to stages read-only on the context. Every value comes from the arrival (ADR 0007): the record id from `Fusion-Record-Id`, the kind from `Fusion-Record-Kind` (absent is `log`), the tenant and the ingestion time as before. No arrival id is `missing_id`, no arrival tenant is `unknown`, no arrival time is the worker clock, and no payload key (`id`, `kind`, `resource.tenant.id`, a record time field) is ever read for them, not even as a fallback. The pipeline reads no payload field for itself. Metric labels, state keys, windows and every stored record id read `Meta`. The pipeline never writes a value of its own into a record: no source stamps, no sink projects. `Meta` leaves on the wire as the `Fusion-Record-Id`, `Fusion-Tenant`, `Fusion-Ingestion-Time` and `Fusion-Ingestion-Time-Kind` headers, and a config reads it through the read-only `meta.*` paths; `edit copy {from: meta.<field>, ...}` is the only way one enters a record. Every record field is payload, `id`, `kind`, the tenant and the time fields included: stages may write or remove any of them within core's write rules, and the sink writes the record as the last stage left it.
- **Windows are measured in ingestion time** (`Meta.ingestion_time`: the arrival's, which for NATS is an upstream `Fusion-Ingestion-Time` header or the JetStream publish time; else the worker clock, marked `Clock` and never mixed with a reported time), not on arrival at the stage, so a redelivered record gets the same answer whenever it comes back. The two stated exceptions to "same answer on redelivery" are `sample`'s `every_nth`, which counts deliveries (spec amendment 2026-09-15, issue #7), and a `lua` script's upvalues, which are per worker VM and reset when a `memory` error rebuilds it (spec amendment 2026-09-16, issue #8).
- **Closed sets are declared through `closed_set!`** (`DropReason`, `FailureKind`, `Metric`, `EngineLabel`, `LuaErrorKind`, `EditOp`, `EditCause`, `Kind`, the path roots), so a value cannot be left out of `ALL`. `DropReason` is the `reason` label on `records_dropped_total`; adding a variant to any spec set is a spec amendment. A metric is declared with its instrument, and the recorder's counter and histogram calls take only their own kind.
- **Only core builds a stage `Context`**, from the worker's stage environment, the record's `Meta` and the node. Stage behaviour is tested through the harness.
- **Records are copy-on-write across route branches.** Shared until a branch mutates.
- **Reserved names.** `source` is the reserved node id. `drop` is the reserved route label. Every declared route label, the default included, must have a consumer or the config is rejected at load.
- **`unsafe` lives only in `crates/regex`, in the PCRE2 half.** JIT is never invoked. Every pattern compiles on the linear `regex` engine first and falls back to PCRE2 only when the syntax needs it.
- **Only `kind: log` is processed, decided at intake from `Fusion-Record-Kind`.** A message the transport says is a `metric` or `span`, or whose `Fusion-Record-Kind` does not parse or is given twice, is counted and rejected before any stage runs, before its record id is checked, and the NATS source does not decode its payload. The payload's `kind` is never read: a stage may set another kind, and the sink writes it (ADR 0005, ADR 0007).

## How to test

- Drive behaviour through the trait boundary: load a YAML config, push envelopes through the in-memory `Source`, assert which records reached which in-memory `Sink` and which ack handles saw `ack` versus `nak`. The source, sink and state fakes live in `crates/core/src/memory.rs`; the event and trace fakes live beside their seams.
- Tests that touch real NATS are `#[ignore]` and need `deploy/compose.yaml` up. Commands are in the README.
- The loghub harness (`crates/harness`) is tested on its pure modules, `loghub`, `plan`, `expect`, `verdict` and `cli`: what a run publishes and how a report judges it are not observable through `Source`/`Sink`. Its route table is checked against `deploy/pipeline-poc.yaml` through the harness in `deploy_configs.rs`, and the live run is `deploy/loghub-check.sh`.
- Tests below the trait boundary, on the safe API only: the regex crate, the core parsers `path` and `condition`, and each stage's config parser (`Filter::from_node`, `Route::from_node`, `Dedupe::from_node`), because their error variants, offsets and hints are not observable through `Source`/`Sink` beyond "config rejected". Load-time rejections and accepted edge values are asserted there; behaviour over records goes through the harness. The same holds for the NATS transport's pure functions, `fusion_nats::subject`, `fusion_nats::headers` and the delivery arithmetic in `fusion_nats::source` (`nak_delay`, `delivery_limit`, `is_final_delivery`): which subject names a tenant, which pipeline header is refused, what a dead letter carries and which delivery is final are not observable through the in-memory `Source`, so they are asserted on their public functions, and the live round trip is an `#[ignore]` test. So are two telemetry functions: `fusion_otel::sampling`, the `OTEL_TRACES_SAMPLER_ARG` parser, whose refusal is a startup error, and `TraceKey`'s id derivation for record id 0, whose trace and span ids the salt keeps non-zero; the `0 → 1` guard behind the salt is unreachable from any input and untested.
- Metrics are a seam of their own: `Recorder` in `crates/core/src/metrics.rs`, with `InMemoryRecorder` as the fake. Assert what the engine emits through the harness (`h.counter`, `h.samples`); below it only the closed sets (metric names, drop reasons, failure kinds), the label sets of the metrics a transport emits outside the engine (the dead-letter metrics, on `InMemoryRecorder`), and the OTLP recorder on the SDK's in-memory exporter.
- Events and record traces are seams too (ADR 0006): `EventLog` in `crates/core/src/events.rs` with `InMemoryEventLog`, and `TraceSink` in `crates/core/src/trace.rs` with `InMemoryTraceSink`, both reached through `Signals`. Assert what the engine logs and traces through the harness (`h.events`, `h.events_of`, `h.traces`); below it only their closed sets, and the OTLP event log and trace sink on the SDK's in-memory exporters, plus one test exporter that blocks until released, which shows their queues drop rather than wait. The NATS source's dead-letter events are asserted in the `#[ignore]` JetStream tests.
- State is the fourth seam: `StateStore` in `crates/core/src/state.rs`, with `MemoryStateStore` as the fake (fake clock, `fail_all`, `keys`). Stage behaviour over state is asserted through the harness (`h.state`); below it only the store contract itself, once on the fake and once, `#[ignore]`, on Dragonfly.

## Workflow

- Work happens on a branch named `feat/<issue>-<slug>` (or `fix/`, `refactor/`), pushed and merged by PR. Never commit on `main`.
- Commits are Conventional Commits scoped by crate: `feat(core): ...`, `fix(nats): ...`, `docs(spec): ...`; `deploy` for the compose stack.
- One ticket per `/implement` session. Clear context between tickets.

## Agent skills

### Issue tracker

Issues live in this repo's GitHub Issues via the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Default vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `CONTEXT.md` at the root and ADRs under `docs/adr/`. See `docs/agents/domain.md`.
