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
| `nats` | NATS JetStream source and sink (placeholder) |
| `state` | state store implementations (placeholder) |
| `otel` | OTLP telemetry wiring (placeholder) |
| `pipeline` | the binary and the default stage registry |

```sh
cargo test --workspace
cargo clippy --workspace --all-targets
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
