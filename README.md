# fusion-pipeline

Observability pipeline proof of concept: a YAML-declared DAG of stages with Lua as the escape hatch, NATS JetStream in and out with ack-after-PubAck, stage state in Dragonfly, telemetry over OTLP into Grafana.

Spec: `docs/specs/2026-09-08-observability-pipeline-poc.md`

Feature catalogue the spec was derived from: `pipeline_atomic_features.csv`

## Layout

Cargo workspace under `crates/`:

| Crate | Contents |
|---|---|
| `core` | record model, field paths (read, write, remove), config loader, DAG validation, engine, `Source`/`Sink`/`AckHandle` traits, in-memory fakes, condition grammar |
| `stages` | built-in stages: `filter`, `route` |
| `regex` | two-engine regex facade: linear `regex` first, PCRE2 fallback with configurable limits, load-time ReDoS lint and canary; the only crate with `unsafe` |
| `nats` | NATS JetStream source (pull consumer, explicit ack) and sink (returns after `PubAck`); tenant stamped from the subject; `NATS_URL` overrides configured URLs |
| `state` | state store implementations (placeholder) |
| `otel` | OTLP metrics exporter: one instrument per spec metric behind core's `Recorder` boundary, HTTP/protobuf to the collector, configured by `OTEL_EXPORTER_OTLP_*` |
| `pipeline` | the `pipelined` binary and the default stage registry |

```sh
cargo test --workspace
cargo clippy --workspace --all-targets
```

## Running against NATS

`deploy/compose.yaml` brings up JetStream plus a one-shot `nats-init` that creates the
`LOGS` (`logs.>`) and `PROCESSED` (`processed.>`) streams and the `pipeline` pull consumer
(explicit ack, `ack_wait` 30s, `max_deliver` 5). The pipeline never creates streams itself
and fails fast at startup when the server, stream or consumer is missing.

```sh
docker compose -f deploy/compose.yaml up -d
cargo run -p fusion-pipeline -- --config deploy/pipeline.yaml

# in another shell
nats sub processed.logs --count 1 &
nats pub logs.acme.syslog '{"id": 1, "body": "disk full"}'
nats consumer info LOGS pipeline        # 0 pending, 0 redelivered
```

The record arrives with `resource.tenant.id` set to `acme`, read from the subject. A sink
that cannot get its `PubAck` (delete `PROCESSED` to see it) makes the engine nak the source
message and JetStream redeliver it. `NATS_URL` overrides the `url` of the source and every
sink. `deploy/nats-smoke.sh` runs these checks end to end and exits non-zero on any failure.

The live tests are ignored by default and need the compose stack:

```sh
cargo test -p fusion-nats --test jetstream -- --ignored
cargo test -p fusion-pipeline --test cli -- --ignored
```

The regex crate wraps C (PCRE2 10.46, bundled by `pcre2-sys`), so Miri cannot run its
tests. Leak-check it with AddressSanitizer on nightly instead:

```sh
RUSTFLAGS=-Zsanitizer=address cargo +nightly test -p fusion-regex --target x86_64-unknown-linux-gnu
```

## Metrics

The full stack, pipeline included, is one command; the internal dashboard is provisioned
from `deploy/grafana` and the pipeline's metrics reach Prometheus through the collector:

```sh
docker compose -f deploy/compose.yaml up -d --build
open http://127.0.0.1:3000/d/fusion-internal     # Grafana, no login
nats pub logs.acme.syslog '{"id": {{Count}}, "body": "disk full"}' --count 1000
deploy/metrics-check.sh                          # traffic in; every metric with a producer present, labels checked, exit non-zero otherwise
```

The pipeline exports over OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` (or the metrics-specific
variable) is set and records nothing otherwise, so `cargo run` against the compose NATS works
as before; point it at the compose collector with `OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318`.
`OTEL_METRIC_EXPORT_INTERVAL` (milliseconds) sets the cadence; compose uses 5000.

Every metric carries `tenant`; per-node metrics carry `stage` (the node id, `source` for the
engine's own decisions) and `records_dropped_total` carries `reason` from the spec's closed
set, and the collector adds `job="fusion-pipeline"` and `instance=<hostname>` from the
resource, so `--scale pipeline=3` gives three series that the dashboard sums. NATS is scraped through `prometheus-nats-exporter`
(`jetstream_consumer_*` for pending, redelivered and ack floor), Dragonfly at
`:6379/metrics`. Each process reports its own CPU and memory: the pipeline exports OTel's
`process.cpu.time`, `process.memory.usage` and `process.thread.count`, NATS its `varz`,
Dragonfly its own metrics (ADR 0003 says why not cAdvisor). `deploy/nats-smoke.sh` runs its own `pipelined` on the host and
stops the compose one first.

The dashboard is timeseries only, no stat tiles: an Overview row (throughput, latency, backlog,
failures, CPU, memory) with Last/Max/Mean in every legend, then one row per stage that
repeats for every `stage` the pipeline has reported (records, p50/p95/p99, drops by reason,
state-store ops), and a collapsed Internals row. `Tenant` and `Stage` variables filter
everything.

A minimal config:

```yaml
workers: 4          # optional, defaults to one per core
source:             # optional in tests, required by the binary
  type: nats
  stream: LOGS
  consumer: pipeline
nodes:
  - id: keep_errors
    type: filter    # reads from `source` because it is first
    condition: severity_text == "ERROR"
    action: keep
  - id: out
    type: sink.nats     # reads from keep_errors, the previous node
    stream: PROCESSED
    subject: processed.logs
```

## Field paths

Every stage names a record field with one dotted path: write what the JSON shows, outer
field, dot, key. Under `attributes`, `resource` and `scope` the segments after the root,
joined with dots, are the flat map key, so `attributes.http.status` reads the `http.status`
key. A segment is letters, digits, `_` and `-`; quote it for anything else:
`attributes."Event ID".code`. `body` and the scalar fields take no segments. Brackets are
not accepted; every path error is a load-time error that names the node and says what to
write instead.

```yaml
condition: attributes.http.status >= 500 and resource.tenant.id == "acme"
condition: resource.k8s.pod-name == "web-0" and attributes."something something" == 1
```

## Routing

A `route` node has named outputs. Consumers read `<route>.<label>`; two nodes naming the
same label fan out, and a node with `from: [a, b]` fans in. Every declared label, the default
included, must have a consumer, or the config is rejected at load. The source message is
acked once every branch has ended in a sink success or a drop, and nakked if any branch
failed. Records are copy-on-write across branches.

```yaml
nodes:
  - id: by_format
    type: route
    routes:                                 # ordered; first match wins
      linux: resource.log.format == "Linux"
      apache: resource.log.format == "Apache"
    default: other                          # a label, or `drop`
  - id: linux_out
    type: sink.nats
    from: by_format.linux
  - id: linux_archive
    type: sink.nats
    from: by_format.linux                   # fan-out: same label twice
  - id: rest
    type: sink.nats
    from: [by_format.apache, by_format.other]   # fan-in
```
