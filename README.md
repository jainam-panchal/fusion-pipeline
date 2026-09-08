# fusion-pipeline

Observability pipeline proof of concept: a YAML-declared DAG of stages with Lua as the escape hatch, NATS JetStream in and out with ack-after-PubAck, stage state in Dragonfly, telemetry over OTLP into Grafana.

Spec: `docs/specs/2026-09-08-observability-pipeline-poc.md`

Feature catalogue the spec was derived from: `pipeline_atomic_features.csv`

## Layout

| Crate | Path | Holds |
|---|---|---|
| `pipeline-core` | `crates/core` | record, config loader and DAG validation, condition grammar, `Source`/`Sink`/`AckHandle`/`Stage` traits, engine, in-memory fakes |
| `pipeline-stages` | `crates/stages` | built-in stages (`filter` so far) |
| `pipeline-regex` | `crates/regex` | regex facade, `regex` first with PCRE2 fallback (placeholder) |
| `pipeline-nats` | `crates/nats` | JetStream source and sink (placeholder) |
| `pipeline-state` | `crates/state` | state store implementations (placeholder) |
| `pipeline-otel` | `crates/otel` | OTLP telemetry wiring (placeholder) |
| `fusion-pipeline` | `crates/pipeline` | the binary and the stage registry; `fusion-pipeline check <config.yaml>` validates a config |

```sh
cargo test --workspace
cargo run -p fusion-pipeline -- check examples/linear.yaml
```
