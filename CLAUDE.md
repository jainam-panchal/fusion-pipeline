# fusion-pipeline

Observability pipeline POC in Rust: a YAML-declared DAG of stages, NATS JetStream in and out, records acked only after every branch ends in a sink `PubAck` or an intentional drop.

Read first, in this order:

1. `README.md`: crate map, commands, minimal config, routing example.
2. `CONTEXT.md`: the glossary. Use its terms in code, tests, issues and commit messages.
3. `docs/specs/2026-09-08-observability-pipeline-poc.md`: the spec. Source of truth for behaviour, limits, metrics and what is out of scope. When a decision changes, amend the spec inline with a dated `Amended YYYY-MM-DD (issue #n):` paragraph rather than rewriting history.
4. `docs/adr/`: decisions that were hard to reverse. Read the ones touching your area before designing.

## Invariants no file confesses

- **Ack discipline.** The engine keeps an outstanding-branch counter per record. Ack fires once every branch has ended in sink success or a drop. Any failed branch means one nak after all branches finish. Sinks return success only on durable acceptance; NATS means `PubAck`.
- **The pipeline never creates streams or consumers.** The compose stack does. Missing stream, consumer or server is a fail-fast startup error.
- **Stages are synchronous and pure over one record.** One function: record in, one of `Pass`, `Drop(reason)`, `Routed(label, record)`, `Split`, `Error` out. Failure policy for state-store errors is applied by the engine from node config, never inside the stage.
- **`DropReason` is a closed set** and is the `reason` label on `records_dropped_total`. Adding a variant is a spec amendment.
- **Records are copy-on-write across route branches.** Shared until a branch mutates.
- **Reserved names.** `source` is the reserved node id. `drop` is the reserved route label. Every declared route label, the default included, must have a consumer or the config is rejected at load.
- **`unsafe` lives only in `crates/regex`, in the PCRE2 half.** JIT is never invoked. Every pattern compiles on the linear `regex` engine first and falls back to PCRE2 only when the syntax needs it.
- **Only `kind: log` is processed.** Metric and span variants exist, are counted and rejected.

## How to test

- Drive behaviour through the trait boundary: load a YAML config, push envelopes through the in-memory `Source`, assert which records reached which in-memory `Sink` and which ack handles saw `ack` versus `nak`. Fakes live in `crates/core/src/memory.rs`.
- Tests that touch real NATS are `#[ignore]` and need `deploy/compose.yaml` up. Commands are in the README.
- Tests below the trait boundary, on the safe API only: the regex crate, and the core parsers `path` and `condition`, whose error variants, offsets and hints are not observable through `Source`/`Sink` beyond "config rejected".
- Metrics are a seam of their own: `Recorder` in `crates/core/src/metrics.rs`, with `InMemoryRecorder` as the fake. Assert what the engine emits through the harness (`h.counter`, `h.samples`); below it only the closed sets (metric names, drop reasons) and the OTLP recorder on the SDK's in-memory exporter.

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
