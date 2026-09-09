# fusion-pipeline

Observability pipeline proof of concept: a YAML-declared DAG of stages with Lua as the escape hatch, NATS JetStream in and out with ack-after-PubAck, stage state in Dragonfly, telemetry over OTLP into Grafana.

Spec: `docs/specs/2026-09-08-observability-pipeline-poc.md`

Feature catalogue the spec was derived from: `pipeline_atomic_features.csv`

## Layout

Cargo workspace under `crates/`:

| Crate | Contents |
|---|---|
| `core` | record model, config loader, DAG validation, engine, `Source`/`Sink`/`AckHandle` traits, in-memory fakes, condition grammar |
| `stages` | built-in stages (`filter` so far) |
| `regex` | two-engine regex facade: linear `regex` first, PCRE2 fallback with configurable limits, load-time ReDoS lint and canary; the only crate with `unsafe` |
| `nats` | NATS JetStream source and sink (placeholder) |
| `state` | state store implementations (placeholder) |
| `otel` | OTLP telemetry wiring (placeholder) |
| `pipeline` | the binary and the default stage registry |

```sh
cargo test --workspace
cargo clippy --workspace --all-targets
```

The regex crate wraps C (PCRE2 10.46, bundled by `pcre2-sys`), so Miri cannot run its
tests. Leak-check it with AddressSanitizer on nightly instead:

```sh
RUSTFLAGS=-Zsanitizer=address cargo +nightly test -p fusion-regex --target x86_64-unknown-linux-gnu
```

A minimal config:

```yaml
workers: 4          # optional, defaults to one per core
nodes:
  - id: keep_errors
    type: filter    # reads from `source` because it is first
    condition: severity_text == "ERROR"
    action: keep
  - id: out
    type: sink.memory   # reads from keep_errors, the previous node
```
