# fusion-pipeline

Observability pipeline proof of concept: a YAML-declared DAG of stages with Lua as the escape hatch, NATS JetStream in and out with ack-after-PubAck, stage state in Dragonfly, telemetry over OTLP into Grafana.

Spec: `docs/specs/2026-09-08-observability-pipeline-poc.md`

Feature catalogue the spec was derived from: `pipeline_atomic_features.csv`

## Layout

Cargo workspace under `crates/`:

| Crate | Contents |
|---|---|
| `core` | record model, field paths (read, write, remove), config loader, DAG validation, engine, `Source`/`Sink`/`AckHandle` traits, in-memory fakes, condition grammar |
| `stages` | built-in stages: `filter`, `route`, `dedupe`, `extract`, `redact`, `sample`, `edit` (`lua` has its own crate) |
| `regex` | two-engine regex facade: linear `regex` first, PCRE2 fallback with configurable limits, load-time ReDoS lint and canary; the only crate with `unsafe` |
| `nats` | NATS JetStream source (pull consumer, explicit ack) and sink (returns after `PubAck`); `Meta` in and out as `Fusion-*` headers, never in the record; `NATS_URL` overrides configured URLs |
| `state` | Dragonfly state store over the Redis protocol: one sync connection per worker, timeouts and reconnect, `DRAGONFLY_URL` |
| `otel` | OTLP metrics exporter: one instrument per spec metric behind core's `Recorder` boundary, HTTP/protobuf to the collector, configured by `OTEL_EXPORTER_OTLP_*` |
| `lua` | the `lua` stage: Lua 5.4 through `mlua` (vendored), one sandboxed VM per worker per node, instruction budget, memory cap, output check, `state`/`log`/`now_ns` API |
| `pipeline` | the `pipelined` binary and the default stage registry |

```sh
cargo test --workspace
cargo clippy --workspace --all-targets
```

## Running against NATS

`deploy/compose.yaml` brings up JetStream plus a one-shot `nats-init` that creates the
`LOGS` (`logs.>`), `PROCESSED` (`processed.>`) and `DLQ` (`dlq.>`) streams and the
`pipeline` pull consumer (explicit ack, `ack_wait` 30s, `max_deliver` 5, no `backoff`). The
pipeline never creates streams itself and fails fast at startup when the server, stream or
consumer is missing, when the consumer has no `max_deliver` or sets `backoff`, and when no
stream captures every `dlq.<tenant>` subject.

```sh
docker compose -f deploy/compose.yaml up -d
cargo run -p fusion-pipeline -- --config deploy/pipeline.yaml

# in another shell
nats sub processed.logs --count 1 &
nats pub logs.acme.syslog '{"id": 1, "body": "disk full"}'
nats consumer info LOGS pipeline        # 0 pending, 0 redelivered
```

The record arrives exactly as published, and the message carries the pipeline's view of it
as headers: `Fusion-Tenant: acme` (from the subject), `Fusion-Ingestion-Time` (the JetStream
publish time, in nanoseconds) and `Fusion-Ingestion-Time-Kind: reported`. The pipeline never
writes those into the record; a config that wants the tenant in the payload says so with
`edit copy {from: meta.tenant, to: resource.tenant.id}`. A pipeline reading `processed.logs`
takes the tenant and ingestion time back from the headers (the subject's tenant, when the
subject names one, wins over the header). Only a subject of the form
`{tenant_prefix}.{tenant}.>` names a tenant; the source's `tenant_prefix` is `logs` unless the
config says otherwise, so `processed.logs` names none. A sink
that cannot get its `PubAck` (delete `PROCESSED` to see it) makes the engine nak the source
message and JetStream redeliver it. `NATS_URL` overrides the `url` of the source and every
sink.

A message that fails its last delivery is dead-lettered: the source publishes it as it
arrived (payload and the producer's headers, minus any `Nats-*`) to `dlq.{tenant}`, waits
for the `PubAck` and terminates it. The dead letter carries `Fusion-Dlq-Reason` (the node that
failed and its error, `source` for a payload that is not a record or a record without an
`id`), `Fusion-Dlq-Subject` (where it arrived), the tenant and ingestion time headers, so
republishing it to its subject replays it with the same `Meta`, and `Nats-Msg-Id`
(`{stream}:{sequence}`), so a second dead letter of one message is dropped. It counts
`dlq_total{tenant, stage, reason}`, `reason` being `stage_error`, `state_error`,
`sink_error`, `panic`, `missing_id` or `undecodable`. `DLQ` is one stream with a subject per
tenant, capped per subject; `dlq_prefix` on the source moves the subjects (default `dlq`).
When the publish fails four times the message is not terminated: its last nak has no delay,
JetStream gives up on it at once, `dlq_publish_errors_total` counts it, and the message stays
in `LOGS` under the stream sequence the pipeline logs.

```sh
nats pub logs.acme.syslog '{"body": "no id"}'   # fails every delivery
nats sub 'dlq.>' --count 1                       # about 15 s later, with Fusion-Dlq-Reason
```
 The compose pipeline has a `dedupe` node, so it also needs the compose Dragonfly:
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

Every metric carries `tenant`, the record's `Meta` tenant (the subject's, else the
`Fusion-Tenant` header, else `unknown`; never the record's `resource.tenant.id`); per-node metrics carry `stage` (the node id, `source` for the
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
repeat. The window is measured in the record's ingestion time, which the engine fixes at
intake from the transport (for NATS, the JetStream publish time, or an upstream pipeline's
`Fusion-Ingestion-Time`), not from the producer's clock, so a record redelivered after a crash carries the same time
it had before and is recognised as itself however long the redelivery took, even if a newer
duplicate claimed the key meanwhile. A stage upstream rewriting the time fields changes what
the sink writes, not the window (ADR 0005).

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
refuses, naming the node and the op's position: paths, a `set` literal of the wrong type,
a `hash` target that takes no string, `from` equal to `to`, any write to a `meta.*` path.
Any record field may be edited, `id`, `kind` and `resource.tenant.id` included: the pipeline
decides from the record's `Meta`, fixed at intake, so an edit changes what the sink writes
and nothing else (ADR 0005). `copy` may read a `meta.*` path, which is how a pipeline value
enters a record: `copy {from: meta.tenant, to: resource.tenant.id}`.

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

## Lua scripts

A `lua` node runs a script defining `process(record)`. The record is a plain table with
OTLP field names; return it to pass, `nil` to drop (reason `lua_drop`), or a list of
records to split. It does what `edit` cannot: derive a value, split a body, keep a counter.

```yaml
nodes:
  - id: split_lines
    type: lua
    script: scripts/split_lines.lua      # or `source: |` with the script inline
    limits: { instructions: 1000000, memory_kib: 16384, output_kib: 1024 }
    on_error: pass                       # pass (default) | drop | nak
    on_state_error: nak                  # nak (default) | pass; only if the script uses `state`
```

```lua
local seen = 0                           -- upvalues persist across records on one worker

function process(record, meta)   -- meta: id, tenant, ingestion_time, delivery_count
  seen = seen + 1
  local status = record.attributes["http.status"]
  if status ~= nil and status ~= json.null then
    record.attributes["http.status_class"] = string.format("%dxx", status // 100)
  end
  if type(record.body) ~= "string" or not record.body:find("\n") then return record end
  local out = {}
  for line in record.body:gmatch("[^\n]+") do
    local r = record:copy()              -- a deep copy of every field
    r.body = line
    out[#out + 1] = r
  end
  if #out == 0 then return record end    -- only newlines: nothing to split
  return out
end
```

A script may change or drop any field, `id`, `kind`, the tenant and the time fields
included; every returned record continues under the incoming record's `Meta`, so labels,
state keys and windows do not move. Every returned field goes through core's write rules,
the same ones `edit` uses (`id`, `severity_number` and the time fields integers, `18 / 2`
counting as one and an `id` given as decimal text too; `kind` one of `log`, `metric`,
`span`, and `log` when left out), a key that is not a record field is refused, and the
strings together stay under `output_kib`. `meta` is read-only: writing to it is a `runtime`
error. Anything else is a Lua error of kind `output`. A record the script leaves alone, or
copies, comes back unchanged: a JSON list stays a list even when empty, and a JSON `null` in
a list or a map is `json.null`, which is truthy, so test it with `== json.null`.
`json.list(t)` makes a table the script builds a list, so a field set to `json.list()`
leaves as `[]`; returned as the whole result, an empty list is refused like an empty table.
A list, the one `process` returns for a split included, may hold only its positions `1..n`:
write `json.null`, not `nil`, for a null entry. A field set to `json.null` is left out, as
with `nil`. A script that loops is stopped by the instruction budget (`instructions`, per
record), one that allocates without bound by the memory cap (`memory_kib`, at least 64, on
the worker's VM as a whole, upvalues included), a script that raises is `runtime`; each
counts on `lua_errors_total{kind}` and then `on_error` decides: `pass` forwards the record
as it came in, `drop` drops it with reason `lua_error`, `nak` fails it so JetStream
redelivers. `nak` is for failures a retry can cure; a `runtime` or `output` error repeats on
redelivery until the consumer's `max_deliver`, so under `nak` a bad script sends its
records to the dead-letter queue. The next record is served either way:
the VM survives a budget or runtime error, and a `memory` error rebuilds it, upvalues
included, since a script whose upvalues grow would otherwise fail every record from then on.
`pcall` and `xpcall` catch the script's own errors and nothing else: a budget or cap trip
and a state error go through them.

The sandbox has `string`, `table`, `math` and `utf8`, plus `state.get(key)`,
`state.set_nx(key, value, ttl_ms)` (`true`, or `false` and the holder), `state.incr(key, by,
ttl_ms)` and `state.del(key)` on the node's state handle (every key under
`{pipeline}:{tenant}:{node}:`, every call on the `state_*` metrics), `log.info`, `log.warn`,
`now_ns()`, a read-only `json` (`json.null`, `json.list(t)`) and `record:copy()`. `os`,
`io`, `package`, `require`, `load`, `debug` and `print` are not there, and a script that
names one of them anywhere is refused when the config loads, with the line; so is a script
that does not parse or does not define `process`. A `state.*` call the store cannot answer
is handled by `on_state_error`, not `on_error`; its default is `nak` where `dedupe` and
`sample` default to `pass`, because a record forwarded past a script that did not run may be
unredacted, whereas an un-deduped one is only a copy. One VM per worker per node, the script
loaded once, so a counter in its upvalues persists across the records that worker sees; with
`workers: 4` there are four counters, and a redelivered record may land on another. A
`script:` path is read relative to the process working directory, not the config file.

## Field paths

Every stage names a record field with one dotted path: write what the JSON shows, outer
field, dot, key. Under `attributes`, `resource` and `scope` the segments after the root,
joined with dots, are the flat map key, so `attributes.http.status` reads the `http.status`
key. A segment is letters, digits, `_` and `-`; quote it for anything else:
`attributes."Event ID".code`. `body` and the scalar fields take no segments. Brackets are
not accepted; every path error is a load-time error that names the node and says what to
write instead. `meta.id`, `meta.tenant`, `meta.ingestion_time` and `meta.delivery_count` read
the pipeline's view of the record rather than the record: a condition on the tenant reads
`meta.tenant`, since `resource.tenant.id` is whatever the producer or a stage put there. A
`meta.*` path can be read anywhere and written nowhere.

```yaml
condition: attributes.http.status >= 500 and meta.tenant == "acme"
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
