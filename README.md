# fusion-pipeline

Observability pipeline proof of concept: a YAML-declared DAG of stages with Lua as the escape hatch, NATS JetStream in and out with ack-after-PubAck, stage state in Dragonfly, telemetry over OTLP into Grafana.

Spec: `docs/specs/2026-09-08-observability-pipeline-poc.md`

Feature catalogue the spec was derived from: `pipeline_atomic_features.csv`

## Layout

Cargo workspace under `crates/`:

| Crate | Contents |
|---|---|
| `core` | record model, field paths (read, write, remove), config loader, DAG validation, engine, `Source`/`Sink`/`AckHandle` traits, in-memory fakes, condition grammar |
| `stages` | built-in stages: `filter`, `route`, `dedupe`, `extract`, `redact`, `sample`, `edit` |
| `regex` | two-engine regex facade: linear `regex` first, PCRE2 fallback with configurable limits, load-time ReDoS lint and canary; the only crate with `unsafe` |
| `nats` | NATS JetStream source (pull consumer, explicit ack) and sink (returns after `PubAck`); tenant stamped from the subject; `NATS_URL` overrides configured URLs |
| `state` | Dragonfly state store over the Redis protocol: one sync connection per worker, timeouts and reconnect, `DRAGONFLY_URL` |
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
sink. The compose pipeline has a `dedupe` node, so it also needs the compose Dragonfly:
`DRAGONFLY_URL` names it (default `redis://127.0.0.1:6379`), and a config with no stateful
node never touches it. `deploy/nats-smoke.sh` runs these checks end to end and exits
non-zero on any failure.

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
engine's own decisions), the metrics of a regex node carry `engine`, and
`records_dropped_total` carries `reason` from the spec's closed set, and the collector adds `job="fusion-pipeline"` and `instance=<hostname>` from the
resource, so `--scale pipeline=3` gives three series that the dashboard sums. NATS is scraped through `prometheus-nats-exporter`
(`jetstream_consumer_*` for pending, redelivered and ack floor), Dragonfly at
`:6379/metrics`. Each process reports its own CPU and memory: the pipeline exports OTel's
`process.cpu.time`, `process.memory.usage` and `process.thread.count`, NATS its `varz`,
Dragonfly its own metrics (ADR 0003 says why not cAdvisor). `deploy/nats-smoke.sh` runs its own `pipelined` on the host and
stops the compose one first.

The dashboard is timeseries only, no stat tiles: an Overview row (throughput, latency, backlog,
failures, CPU, memory) with Last/Max/Mean in every legend, a Source row (handed over,
entered the graph, rejected by reason), then one row per
stage that repeats for every node the pipeline has reported (records, p50/p95/p99, drops by
reason, state-store ops, latency and errors), and a collapsed Internals row. A node added to the config gets
its row on first record, no dashboard edit. `Tenant` and `Stage` variables filter
everything. Latency charts go blank for a minute with no records, since a quantile of
nothing is undefined; the grey records/s bars in each show what the line is based on.

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

## State and dedupe

Stateful nodes keep their state in Dragonfly, one keyspace shared by every worker and every
replica; each worker holds its own connection, opened at startup with a ping when some node
uses state, so an unreachable store fails fast. Every key is `{pipeline}:{tenant}:{node}:...`:
`name:` at the top of the config (default `pipeline`) is the first segment, so replicas of
one pipeline share state and two different pipelines on one Dragonfly must be given
different names. The tenant segment means no node can dedupe one tenant against another;
records with no tenant fall under `unknown`, like their metrics.

```yaml
name: ingest
nodes:
  - id: dedupe_body
    type: dedupe
    key: [body, resource.host]   # one or more field paths; a missing field is null
    window: 10s                  # ms | s | m | h; at least 1ms
    on_state_error: pass         # pass (default) | nak
  - id: out
    type: sink.memory
```

`dedupe` keeps one key per distinct content (the hash of the `key` values) holding the id
and ingestion time of the record that claimed it, with `window` as its TTL. The first record
passes; a different record with the same content inside the window drops with reason
`dedupe` and is acked; after the window the next one passes and takes the key over, so the
records behind it dedupe against the new window even while the old key's TTL is still
running. The takeover is a compare-and-set against the holder the record read, so two
workers past the window at once agree on one new holder and the other drops as its
repeat. The window is measured in ingestion time (`observed_time_unix_nano`, which the NATS source
fills from the JetStream publish time when a record has no timestamp), so a record
redelivered after a crash carries the same time it had before and is recognised as itself
however long the redelivery took, even if a newer duplicate claimed the key meanwhile.

When Dragonfly cannot answer (2 s per operation, then the connection is reopened on the
next call) the stage hands the record back and the engine applies `on_state_error`: `pass`
forwards it un-deduped, `nak` fails it so JetStream redelivers. Either way the operation is
on `state_errors_total`. Live keys are unique contents per window, about 200 bytes each;
the Dragonfly panel shows the memory, `state_ops_total` the call rate per node.

## Sampling

A `sample` node keeps a share of the records and drops the rest with reason `sample`; a
drop is acked, never redelivered. `random` scrambles the record id (mixed with the node id)
and keeps it when the result is at or below `percent`, so a redelivered record gets the same
verdict and two `random` nodes in series keep independent subsets. `consistent` scrambles
the `key` field values instead, with no node id mixed in, so every record of a host is kept
or dropped together on every node and every pipeline, and a host kept at 20% is kept at 50%;
records missing the key field share one `null` value and are kept or dropped together. `every_nth` keeps
1 in `n` of the deliveries that reach the node, per tenant, exact across every worker and
replica: one shared sample count in Dragonfly, one `incr` per record, counts 1, n+1, 2n+1,
... kept, so a tenant with fewer than `n` records still gets one through. It is the only mode
that touches the store and the only one that takes `on_state_error`.

```yaml
nodes:
  - id: keep_tenth
    type: sample
    mode: random               # random | every_nth | consistent
    percent: 10                # random, consistent: (0, 100]
  - id: one_in_ten
    type: sample
    mode: every_nth
    n: 10
    on_state_error: pass       # pass (default) | nak
  - id: half_the_hosts
    type: sample
    mode: consistent
    percent: 50
    key: [resource.host]
```

`every_nth` counts deliveries, not records: a message JetStream redelivers takes a new
count. So a kept record whose sink failed is nakked, comes back, and usually loses its
place; during a sink outage most of the records the node had chosen are dropped on their
retry while the 1-in-`n` share of deliveries stays right. Remembering every record would
cost a state key per record, which is why the guard is not there (issue #7 records the
decision). Do not fan the same record into an `every_nth` node twice: each arrival counts.
The count lives 24 h, refreshed on every record, so a tenant quieter than that restarts at 1
and its first record back is kept.

## Editing fields

An `edit` node runs a short list of plain ops in order on one record: `set` a literal,
`rename` or `copy` a value (both overwrite `to`), `hash` a value to lowercase hex SHA-256,
`delete` fields. No templates, no conditions: a conditional edit is a `route` branch with
its own `edit` node. An op whose source reads as null or whose target refuses the value
(`copy body -> severity_number` with a string body) is unapplied: the record is unchanged by
that op and the op counts on `edit_unapplied_total{op, field, cause}`, then `on_unapplied`
says whether the record goes on (`skip`, the default) or drops with reason `edit_unapplied`.
The node never naks: the outcome is fixed by the record's shape. What load can check, it
refuses, naming the node and the op's position: paths, `id`, `kind` and the tenant, a `set`
literal of the wrong type, a `hash` target that takes no string, `from` equal to `to`.

```yaml
nodes:
  - id: normalise
    type: edit
    on_unapplied: skip         # skip (default) | drop
    ops:
      - set:    { field: resource.env, value: prod }
      - rename: { from: attributes.http.path, to: attributes.http.route }
      - copy:   { from: body, to: attributes.raw }
      - hash:   { field: attributes.user.email }
      - delete: { fields: [attributes.debug] }
```

`hash` is a stable join key, not anonymisation: an unsalted digest of an email is
dictionary-reversible.

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

## Regex stages

`extract` lifts a pattern's named groups out of one string field into `attributes`;
`redact` replaces every match in the listed fields in place, with `replace` taken literally.
Both, and any `filter` or `route` condition using `=~` or `!~`, compile through the regex
facade: linear engine first, PCRE2 only when the syntax needs it, with per-node `limits`
(`match`, `depth`, `heap_kib`, `work`, `input_bytes`) and `on_redos_risk: reject|warn` for
the load-time lint and canary. A non-match passes the record unchanged and counts on
`regex_nonmatch_total`; a tripped limit drops it with reason `regex_limit` and the next
record is served. Every metric of a regex node carries `engine=linear|backtracking`.

```yaml
nodes:
  - id: parse_linux
    type: extract
    field: body
    pattern: '^(?<Month>[A-Z][a-z]{2}) +(?<Date>\d{1,2}) (?<Time>\d{2}:\d{2}:\d{2}) (?<Level>\S+) (?<Component>[^\[:]+)(?:\[(?<PID>\d+)\])?: (?<Content>.*)$'
    limits: { input_bytes: 8192 }
    on_redos_risk: reject
  - id: mask_ips
    type: redact
    fields: [body, attributes.Content]
    pattern: '\b\d{1,3}(?:\.\d{1,3}){3}\b'
    replace: '[ip]'
  - id: keep_auth
    type: filter
    condition: attributes.Component =~ "^sshd"
    action: keep
```

The extraction accuracy tests replay every 20th line of the vendored loghub sets
(`testdata/loghub/`, licence and normalisation rules in its README) and compare the
attributes with the structured CSV:

```sh
cargo test -p fusion-pipeline --test extract_loghub
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
