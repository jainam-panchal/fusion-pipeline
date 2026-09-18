# fusion-pipeline

Observability pipeline proof of concept: a YAML-declared DAG of stages with Lua as the escape hatch, NATS JetStream in and out with ack-after-PubAck, stage state in Dragonfly, telemetry over OTLP into Grafana.

Spec: `docs/specs/2026-09-08-observability-pipeline-poc.md`

Feature catalogue the spec was derived from: `pipeline_atomic_features.csv`

New here: follow [Quick start](#quick-start), then read [What the POC concluded](#what-the-poc-concluded)
and [Out of scope and still open](#out-of-scope-and-still-open). To write configs, read the
[user guide](https://jainam-panchal.github.io/fusion-pipeline/).

## Quick start

**Prerequisites.** Rust 1.85 or newer with cargo, Docker with Compose v2, the
[`nats` CLI](https://github.com/nats-io/natscli), curl, jq and make. A nightly toolchain only
for the AddressSanitizer check of the regex crate.

**1. Bring the stack up.** NATS with its streams and consumer, Dragonfly, the collector,
Prometheus, Loki, Tempo, Grafana, and the pipeline, built from this checkout:

```sh
docker compose -f deploy/compose.yaml up -d --build
```

The compose pipeline runs `deploy/pipeline.yaml`, the demo config: drop `TRACE` records, drop a body
repeated within 10s, parse syslog-shaped bodies into attributes, tag the service, mask IPv4
addresses, run a small Lua script, write everything to `processed.logs`, and on a fan-out
write only the records whose body the syslog pattern parsed to `processed.parsed`. Its header comment says
what each node is there to show. A port another stack already holds moves with
`GRAFANA_PORT`, `DRAGONFLY_PORT`, `LOKI_PORT` or `TEMPO_PORT`.

**2. Send one record.**

```sh
nats sub 'processed.>' --count 2 & sleep 1     # let the subscription start
nats pub logs.acme.syslog '{"body": "Jun 14 15:16:01 combo sshd[19939]: authentication failure; rhost=218.188.2.4"}' \
  -H 'Fusion-Record-Id:1'
nats consumer info LOGS pipeline        # Unprocessed Messages: 0, Redelivered Messages: 0
```

Two messages arrive, both with the headers `Fusion-Record-Id: 1`, `Fusion-Tenant: acme` (from
the subject), `Fusion-Ingestion-Time` and `Fusion-Ingestion-Time-Kind: reported`:

- on `processed.logs`, the record with `Month` … `Content` extracted, `attributes.service:
  sshd`, `attributes.pipeline: compose`, and `rhost=[ip]` in the body and the content;
- on `processed.parsed`, the same line parsed but not masked or tagged, with `Content`
  renamed `message` (that branch leaves the DAG before the redact node).

A body the syslog pattern does not parse, `'{"body": "disk full"}'`, reaches `processed.logs`
with only `attributes.pipeline: compose` added, and is dropped on the parsed branch with
reason `edit_unapplied`. For what the headers mean, dead letters and a sink failure, see
[Running against NATS](#running-against-nats).

**3. Look at it.**

| URL | What |
|---|---|
| http://127.0.0.1:3000/d/fusion-internal | Grafana, internal dashboard: throughput, latency, backlog, failures, one row per node, the harness's *Coverage* row. No login. |
| http://127.0.0.1:3000/d/fusion-tenant | Grafana, one tenant's view: records in and out, where the drops went, dead letters |
| http://127.0.0.1:3000/explore | Grafana *Explore*: the pipeline's events in Loki, record traces in Tempo |
| http://127.0.0.1:9090 | Prometheus |
| http://127.0.0.1:8889/metrics | the collector's Prometheus endpoint, the pipeline's own metrics |
| http://127.0.0.1:7777/metrics | `prometheus-nats-exporter`: JetStream consumer pending, redelivered, ack floor |
| http://127.0.0.1:8222 | NATS monitoring |
| http://127.0.0.1:3100, http://127.0.0.1:3200 | Loki and Tempo APIs |

The Grafana, Loki and Tempo ports follow `GRAFANA_PORT`, `LOKI_PORT` and `TEMPO_PORT`.
`deploy/metrics-check.sh` sends traffic and checks that every metric, an event and a record
trace arrived. Details: [Metrics, logs and traces](#metrics-logs-and-traces).

**4. Run the loghub check and the chaos run.**

```sh
make loghub     # 100k loghub records through deploy/pipeline-poc.yaml, judged by the verifier
make chaos      # the same, with the pipeline killed at 20s (back at 25s) and Dragonfly paused at 40s for 5s
```

Both switch the stack to `deploy/pipeline-poc.yaml`, the full DAG with every node type, and
exit 0 on a pass. The coverage panel on the internal dashboard climbs while they run. What a
pass is, the last report and the extraction accuracy: [Loghub harness](#loghub-harness).

**5. Put the compose pipeline's config back, or stop.**

```sh
docker compose -f deploy/compose.yaml up -d      # back to pipeline.yaml after make loghub/chaos
docker compose -f deploy/compose.yaml down       # stop; add -v to drop the streams and data
```

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
| `harness` | the loghub harness: `loghub-producer` replays the vendored loghub sets into NATS, `loghub-verifier` follows the run and judges what the pipeline delivered |

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

To run the pipeline from the checkout rather than in compose, stop the compose one first;
otherwise both pull from `LOGS/pipeline`, and a message may go to either process:

```sh
docker compose -f deploy/compose.yaml up -d
docker compose -f deploy/compose.yaml stop pipeline
cargo run -p fusion-pipeline -- --config deploy/pipeline.yaml

# in another shell
nats sub processed.logs --count 1 &
nats pub logs.acme.syslog '{"body": "disk full"}' -H 'Fusion-Record-Id:1'
nats consumer info LOGS pipeline        # Unprocessed Messages: 0, Redelivered Messages: 0
```

A producer names each message's record id in the `Fusion-Record-Id` header (a decimal
`u64`) and, for anything but a log, its kind in `Fusion-Record-Kind` (`log`, `metric` or
`span`; absent is `log`). The pipeline reads neither from the payload: a payload `id` or
`kind` is the producer's data (ADR 0007). A message without the id header is nakked
(`missing_id`); one whose kind header is not `log`, including one that does not parse or is
given twice, is dropped (`invalid_record`) without its payload being decoded.

The record arrives exactly as published, and the message carries the pipeline's view of it as
headers: `Fusion-Record-Id: 1`, `Fusion-Tenant: acme` (from the subject),
`Fusion-Ingestion-Time` (the JetStream publish time, in nanoseconds) and
`Fusion-Ingestion-Time-Kind: reported`. The pipeline never writes those into the record; a
config that wants the tenant in the payload says so with
`edit copy {from: meta.tenant, to: resource.tenant.id}`. A pipeline reading `processed.logs`
takes the record id, tenant and ingestion time back from the headers (the subject's tenant,
when the subject names one, wins over the header). Only a subject of the form
`{tenant_prefix}.{tenant}.>` names a tenant; the source's `tenant_prefix` is `logs` unless the
config says otherwise, so `processed.logs` names none. A sink that cannot get its `PubAck`
(delete `PROCESSED` to see it) makes the engine nak the source message and JetStream redeliver
it. `NATS_URL` overrides the `url` of the source and every sink.

A message that fails its last delivery is dead-lettered: the source publishes it as it arrived
(payload and the producer's headers, minus any `Nats-*` or `Fusion-*`) to `dlq.{tenant}`, waits
for the `PubAck` and terminates it. The dead letter carries `Fusion-Dlq-Reason` (the node that
failed and its error, `source` for a payload that is not a record or a message without a
`Fusion-Record-Id`), `Fusion-Dlq-Subject` (where it arrived), the record id, kind, tenant and
ingestion time headers, so republishing it to its subject replays it with the same `Meta`, and
`Nats-Msg-Id` (`{stream}:{sequence}`), so a second dead letter of one message is dropped. It
counts `dlq_total{tenant, stage, reason}`, `reason` being `stage_error`, `state_error`,
`sink_error`, `panic`, `missing_id` or `undecodable`. `DLQ` is one stream with a subject per
tenant, capped per subject; `dlq_prefix` on the source moves the subjects (default `dlq`). When
the publish fails four times the message is not terminated: its last nak has no delay, JetStream
gives up on it at once, `dlq_publish_errors_total` counts it, and the message stays in `LOGS`
under the stream sequence the pipeline logs.

```sh
nats pub logs.acme.syslog '{"body": "no id header"}'   # fails every delivery
nats sub 'dlq.>' --count 1                             # about 15 s later, with Fusion-Dlq-Reason
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

## Metrics, logs and traces

The full stack, pipeline included, is one command. The dashboards and the Prometheus, Loki and
Tempo datasources are provisioned from `deploy/grafana`, and the pipeline's metrics, logs and
record traces all reach them through the one collector:

```sh
docker compose -f deploy/compose.yaml up -d --build
open http://127.0.0.1:3000/d/fusion-internal     # Grafana, no login
open http://127.0.0.1:3000/d/fusion-tenant       # one tenant's view
nats pub logs.acme.syslog '{"body": "disk full"}' -H 'Fusion-Record-Id:{{Count}}' --count 1000
deploy/metrics-check.sh                          # traffic in; every metric, the log lines and a trace checked, exit non-zero otherwise
```

Ports that clash with another stack move with `GRAFANA_PORT`, `DRAGONFLY_PORT`, `LOKI_PORT` and
`TEMPO_PORT`; `metrics-check.sh` reads `GRAFANA_PORT`, `LOKI_PORT` and `TEMPO_PORT`.

The pipeline exports a signal over OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` (or that signal's
`OTEL_EXPORTER_OTLP_{METRICS,LOGS,TRACES}_ENDPOINT`) is set. A signal with no endpoint is off:
no metrics are recorded, events go to stderr, no trace is exported. So `cargo run` against the
compose NATS works as before; point it at the compose collector with
`OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318`. `OTEL_METRIC_EXPORT_INTERVAL`
(milliseconds) sets the metrics cadence (compose uses 5000). `OTEL_BLRP_*` and `OTEL_BSP_*` size
the log and span queues; a full queue drops rather than holds up a worker.

**Logs** are events from a closed set: `stage_error`, `nak`, `redelivery`, `dead_letter` and
`dead_letter_failed`. Each one carries `record.id` (stored in Loki as `record_id`), `tenant`,
`node`, `reason` (the failure kind) and `delivery_count`, and the error text is the line. Drops
are not logged: `records_dropped_total` says where they went. In Grafana, *Explore* → Loki:

```logql
{service_name="fusion-pipeline"} | record_id="1000001"
{service_name="fusion-pipeline"} | event="nak" | tenant="acme"
```

**Traces**: one trace per record, one `delivery` span per delivery and one span per node it
visited, parented on the node the record came from. The pipeline decides whether to keep a
trace when the record settles (ADR 0006): it keeps every trace whose walk failed or whose record
was redelivered, and `OTEL_TRACES_SAMPLER_ARG` (default `0.01`) of the rest, chosen by record id
and tenant. The trace id is derived from them too, so every delivery of a record lands in one
trace. A log line's *Open trace* button opens the trace in Tempo, and a span's *Logs for this
span* opens the record's lines in Loki.

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

**The tenant dashboard** (`fusion-tenant`) shows one tenant, chosen at the top:

- records in and records written by each sink;
- *Where did my logs go*: drops by stage and reason, from `records_dropped_total` alone;
- dead letters by reason;
- bytes in and written (`bytes_in_total`, `bytes_out_total`);
- end-to-end p99.

It shows no stage timings and nothing about the state store, NATS, Lua or the process.

**The internal dashboard** is timeseries only, no stat tiles: an Overview row (throughput, latency, backlog,
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

## Writing configs

The user guide at <https://jainam-panchal.github.io/fusion-pipeline/> is the reference for config authors. Its source is `docs/guide/`, an mdBook whose examples are tested (see below).

| Topic | Guide page |
|---|---|
| How a config is laid out, `from`, fan-out and fan-in | [Writing a config](https://jainam-panchal.github.io/fusion-pipeline/writing-a-config.html) |
| Field paths and `meta.*` | [Field paths](https://jainam-panchal.github.io/fusion-pipeline/field-paths.html) |
| The condition grammar | [Conditions](https://jainam-panchal.github.io/fusion-pipeline/conditions.html) |
| Regex engines, `limits`, `on_redos_risk` | [Regex limits](https://jainam-panchal.github.io/fusion-pipeline/regex-limits.html) |
| State in Dragonfly, `on_state_error`, windows | [State and failure policy](https://jainam-panchal.github.io/fusion-pipeline/state-and-failure.html) |
| `filter`, `route`, `dedupe` | [filter](https://jainam-panchal.github.io/fusion-pipeline/stages/filter.html), [route](https://jainam-panchal.github.io/fusion-pipeline/stages/route.html), [dedupe](https://jainam-panchal.github.io/fusion-pipeline/stages/dedupe.html) |
| `edit` | [edit](https://jainam-panchal.github.io/fusion-pipeline/stages/edit/index.html) |
| `sample` | [sample](https://jainam-panchal.github.io/fusion-pipeline/stages/sample/index.html) |
| `extract` and `redact` | [extract and redact](https://jainam-panchal.github.io/fusion-pipeline/stages/regex/index.html) |
| `lua` | [lua](https://jainam-panchal.github.io/fusion-pipeline/stages/lua/index.html) |
| NATS keys, headers and dead letters | [NATS](https://jainam-panchal.github.io/fusion-pipeline/nats.html) |

Build and check the guide locally with [mdBook](https://rust-lang.github.io/mdBook/) 0.5:

```sh
mdbook serve docs/guide                     # http://localhost:3000
cargo test -p fusion-pipeline --test guide_examples --test guide_pages
```

Each example is a folder under `docs/guide/examples/` with `pipeline.yaml`, `input.yaml` and `expected.yaml`; `guide_examples` runs every one through the engine, and `guide_pages` fails when a stage type has no page, an include is missing, or an example is shown nowhere.

The extraction accuracy tests replay every 20th line of the vendored loghub sets
(`testdata/loghub/`, licence and normalisation rules in its README) and compare the
attributes with the structured CSV:

```sh
cargo test -p fusion-pipeline --test extract_loghub
```


## Loghub harness

The end-to-end check of the POC pipeline (`deploy/pipeline-poc.yaml`, every node type: filter,
dedupe, route by log format, one extract node per format, fan-in to redact and edit, a
consistent sample and a lua node before the main sink, a Linux audit fan-out). It needs the
compose stack, the `nats` CLI, curl, jq and cargo:

```sh
make loghub                                                 # deploy/loghub-check.sh: 100k records over ~60s
make chaos                                                  # the same with a kill and a pause
PRODUCER_ARGS="--count 20000 --rate 1000" deploy/loghub-check.sh
```

The script brings the stack up with `PIPELINE_CONFIG=pipeline-poc.yaml`, purges `LOGS`,
`PROCESSED` and `DLQ`, removes the last run's expectations, and runs the two binaries from
`crates/harness` side by side:

- `loghub-producer` (`--rate`, `--count`, `--datasets Linux,OpenSSH,Apache,Mac`,
  `--dup-percent`, `--seed`, `--dedupe-window`, `--expectations`) publishes each distinct
  loghub line raw in `body` on `logs.<tenant>.loghub` (one tenant per set) with
  `Fusion-Record-Id`, its `LineId` and replay cycle in `attributes.loghub.line_id` and
  `attributes.loghub.cycle`, about 30% of them sent twice within 500ms under a new id, and
  writes one expectation per acked message to `target/loghub/expectations.jsonl`: which
  subjects (no main subject for a line the `sample` node leaves out in that cycle), `drop:
  dedupe` for a duplicate, the line's row of the structured CSV and what the config's `edit`
  and `lua` nodes write. It fails the run when a publish needed a retry or its timing fell
  behind the plan, and says how it ended in `expectations.jsonl.done`.
- `loghub-verifier` follows the run: it reads the expectations as they are written and every
  `processed.>` and `dlq.>` subject as the pipeline writes it, and every 5s judges what it has
  and exports it to the collector (the internal dashboard's *Coverage* row, where `reached`,
  the line and subject pairs some copy reached, climbs to `expected`; `published` counts
  messages and `received` record and subject pairs, so those two never meet). Once the producer is done, the `pipeline` consumer has settled and
  both streams are read to their end, it prints the final report. A line's copies count
  together: it is missing when none reached a subject it should have, and a second copy is an
  extra copy, allowed as long as `dedupe` dropped at least 80% of the planned duplicates.
  `edit`'s and `lua`'s writes are checked on the main subject; a main copy of a sampled-out
  line is unexpected.

`make chaos` (`deploy/loghub-check.sh --chaos`) kills the pipeline container at 20s after the
producer starts and starts it at 25s, and pauses Dragonfly at 40s for 5s. Records in flight at
the kill come back when their ack wait runs out; records `dedupe_body` handles during the pause
pass un-deduped (`on_state_error: pass`), counted on `state_errors_total` and never as a
`dedupe` drop or a nak. After the verdict the script checks that the chaos landed: the consumer
redelivered messages, `dedupe_body` counted state errors, and nothing was nakked. The chaos run
of 2026-09-17:

```
published      100000
received       80694
expected       80513
missing        0
unexpected     0
dead_lettered  0
edit_mismatch  0
lua_mismatch   0
sampled_out    7091
extra_copies   181
repeated_on_every_subject 36
dedupe         dropped 27550 of 27695 planned duplicates (at least 80%: held)
extraction by set:
  Apache   100.000% of 15771 groups, 0 mismatched
  Linux    100.000% of 15738 groups, 0 mismatched
  Mac      100.000% of 15745 groups, 0 mismatched
  OpenSSH  100.000% of 15738 groups, 0 mismatched
verdict: PASS

== did the chaos land?
redelivered messages          8   (the kill at 20s: more than 0)
dedupe_body state errors      8   (the pause at 40s: more than 0)
naks                          0   (on_state_error: pass: 0)
verdict: PASS under chaos
```

The run without chaos that day had 22 extra copies and 4 groups repeated on both Linux
subjects (dedupe races), and dropped 27677 of the planned duplicates. The chaos checks read
each counter's highest value since the producer started, so what the killed process counted
survives its restart; a command the script runs that fails exits 2, and the schedule needs a
run longer than 45s. The first 100k run of
#13 found nine Linux lines with more than one space before the component or after the colon
(`kernel:   HighMem zone: ...`); the Linux pattern now starts `Component` and `Content` at the
first non-space, as the CSV does.

Extraction accuracy of that chaos run (2026-09-17), per set against the loghub structured CSV:

| Set | Distinct lines | Groups checked | Mismatched | Accuracy |
|---|---:|---:|---:|---:|
| Apache | 1,461 | 15,771 | 0 | 100.000% |
| Linux | 2,000 | 15,738 | 0 | 100.000% |
| Mac | 1,991 | 15,745 | 0 | 100.000% |
| OpenSSH | 2,000 | 15,738 | 0 | 100.000% |

Each set's 2,000-line sample is replayed as its distinct lines (Apache and Mac repeat some
word for word). A group is one line in one replay cycle, each line coming round about nine
times (about twelve for Apache), and only its first copy on the main subject is compared,
over the set's CSV columns under the rules in `testdata/loghub/README.md`; sampled-out groups
are not checked. So the table measures four hand-written patterns against 1,461 to 2,000
distinct lines each, not 63k independent samples, and the patterns
were tuned on these same lines (the Linux fix above). `cargo test -p fusion-pipeline --test
extract_loghub` checks every 20th line of each set without the stack.

`deploy/loghub-check.sh` exits 0 on a pass (nothing missing, unexpected, dead-lettered or wrongly written by `edit`
or `lua`, and at least 80% of the planned duplicates dropped), 1 on a fail or, under
`--chaos`, when any message was nakked, 2 when the run
could not be judged (the producer failed, the pipeline did not settle, or under `--chaos` the
chaos did not land). Extraction accuracy is reported, never gated; the mismatching `LineId`s
are listed. The stack keeps running the POC config afterwards;
`docker compose -f deploy/compose.yaml up -d` puts `pipeline.yaml` back.

## What the POC concluded

Each point answers one of the spec's decision-maker stories (58-61), or for delivery its
reliability stories (28-31), and names its evidence and its limits.

- **Rust over Go**, decided by Lua embedding: `mlua` embeds real Lua 5.4 with the memory cap
  and instruction hook the `lua` stage needs, where Go offers Lua 5.1 in pure Go or a cgo
  crossing per field access. [ADR 0001](docs/adr/0001-rust-over-go.md).
- **PCRE2 JIT stays off** and every pattern tries the linear `regex` engine first, so PCRE2's
  limits behave the same on every run; a pattern that needs PCRE2 passes a structural lint
  and a canary at load. [ADR 0002](docs/adr/0002-regex-first-facade-pcre2-jit-off.md). One
  class of slow PCRE2-only pattern is still bounded only by `input_bytes` ([Limits and guarantees](https://jainam-panchal.github.io/fusion-pipeline/limits.html)).
- **The stage model, as built.** Eight stages (`filter`, `route`, `dedupe`, `extract`,
  `redact`, `sample`, `edit`, `lua`), covering four of the five log features
  `pipeline_atomic_features.csv` puts in `Priority Tier` 1 (not log-to-metric), five of its
  ten Tier 2 ones (static tags, rename, scripted transform,
  dedupe, route; not lookups, GeoIP, event aggregation, JSON extraction or rate limiting)
  and a few Tier 3 ones (`edit`'s hash and delete), are each one synchronous function from a
  record to an outcome, with state behind one handle and failure policy in the engine. `deploy/pipeline-poc.yaml` uses all of them in one DAG with fan-out and fan-in. That
  is the evidence for judging whether the model generalises; the catalogue features left out
  are listed on [Limits and guarantees](https://jainam-panchal.github.io/fusion-pipeline/limits.html) and none was tried.
- **Delivery under failure, in one run.** The 2026-09-17 chaos run (100k published, the
  pipeline killed and restarted, Dragonfly paused for 5s) ended with nothing missing,
  unexpected or dead-lettered, 8 messages redelivered, 8 state errors on `dedupe_body` and no
  naks. That is one run: the kill was not forced to land between the two sinks of a fan-out
  (36 groups repeated on both Linux subjects against 4 without chaos suggest it did), and the nak path was not exercised under chaos, since `on_state_error: pass` forwards
  records during the pause. The ack and nak rules themselves are covered by the engine tests.
- **Regex extraction on these four formats.** Hand-written patterns matched the loghub ground
  truth on every checked group of the Apache, Linux, Mac and OpenSSH samples (2,000 lines
  each, 1,461 to 2,000 of them distinct; table under [Loghub harness](#loghub-harness)),
  after one fix to the Linux pattern found on the same data. For these formats regex-based
  parsing was good enough, so the POC gives no reason to bring dedicated parsers forward for
  them; for any other format it gives no answer either way.

What the POC does not conclude:

- throughput or latency: performance is observed, not asserted. The engine's throughput on
  the compose stages is printed by
  `cargo test --release -p fusion-pipeline --test throughput -- --ignored --nocapture`;
- that the outstanding-branch ack counter survives a crash between the two sinks of a
  fan-out: the chaos run does not force the kill to land there (story 31 is met only as
  far as the hint above);
- anything about formats other than the four loghub sets, or about dedicated parsers;
- multi-node NATS, an HA state store, TLS or auth;
- whether a real agent can feed it: OTel Collector, Vector and Fluent Bit cannot set
  `Fusion-Record-Id`, so they need a relay in front, and until then their messages are
  dead-lettered as `missing_id` (ADR 0007).

## Out of scope and still open

What the pipeline guarantees, what it does not, what is out of scope and the known limits are on the guide's [Limits and guarantees](https://jainam-panchal.github.io/fusion-pipeline/limits.html) page, taken from the spec's *Out of scope* section and its amendments. In short: only logs are processed, delivery is at least once, and production hardening (TLS, auth, several nodes, a highly available state store) is not built. `gh issue list` has the open work.
