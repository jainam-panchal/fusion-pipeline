# Troubleshooting

## The pipeline does not start

The pipeline prints the reason and exits. Most reasons start with `pipelined: `, and the table leaves that part out. In the compose stack, the container restarts and prints the same line again, so check `docker compose -f deploy/compose.yaml logs pipeline`.

| Message starts with | Cause | Fix |
|---|---|---|
| `` unknown argument ``, `--config needs a path` or `usage: pipelined --config <path>`, with no `pipelined: ` in front | the command line is wrong (exit code 2) | Run `pipelined --config <file>`. |
| `` could not read config `<path>` `` | the file is missing or unreadable | Check the path. In compose, `PIPELINE_CONFIG` is relative to `deploy/`. |
| `config is not valid YAML` | a YAML mistake, or an unknown top-level key | See [Writing a config](writing-a-config.md#what-the-pipeline-refuses). |
| `` config has no `source` block `` | no `source:` | Add one. See [NATS](nats.md#the-source). |
| `` node `<id>`: ``, `` node id ``, `` pipeline name `` and a config problem | a key or value a node does not take, an unknown `type`, or a bad id or name | See the node's page, [Writing a config](writing-a-config.md#what-the-pipeline-refuses), and [Field paths](field-paths.md#what-the-pipeline-refuses), [Conditions](conditions.md#what-the-pipeline-refuses) and [Regex limits](regex-limits.md#what-the-pipeline-refuses). |
| `` node `<id>` reads ``, `` route `<id>` label ``, `pipeline has no sink node` and other graph messages | how the nodes connect | See [Writing a config](writing-a-config.md#what-the-pipeline-refuses) and [route](stages/route.md#what-the-pipeline-refuses). |
| `` node `<id>`: could not connect to NATS `` | NATS is not reachable | Check the server and `NATS_URL`. |
| `` node `<id>`: stream `<stream>` does not exist `` | a stream is missing | Create it. See [NATS](nats.md#what-the-server-must-have). |
| `` node `<id>`: stream `<stream>` at <url> does not capture subject `` | a sink's stream does not store its subject | Add the subject to the stream, or change the sink. |
| `` node `source`: consumer `<consumer>` does not exist `` | the consumer is missing | Create it: pull, explicit ack, `max_deliver`, no `backoff`. |
| `` node `source`: consumer ... has ack policy `` | the ack policy is not `explicit` | Recreate the consumer with `--ack explicit`. |
| `` node `source`: consumer ... has no `max_deliver` `` | no delivery limit | Set `--max-deliver`, for example 5. |
| `` node `source`: consumer ... sets `backoff` `` | the consumer has a backoff | Remove it. The pipeline sets its own waits. |
| `` node `source`: no stream at <url> captures the dead-letter subjects `` | no dead-letter stream | Create one with subjects `dlq.>` (or your `dlq_prefix`). |
| `` node `source`: stream `<stream>` at <url> captures some `` | the dead-letter stream covers only some tenants | Use `dlq.>` or `dlq.*` as its subject. |
| `` node `<id>`: JetStream request failed `` | another JetStream error, such as a push consumer | Read the reason. The consumer must be a pull consumer. |
| `state store: invalid state store url` | `DRAGONFLY_URL` is malformed | Fix the URL, for example `redis://127.0.0.1:6379`. |
| `could not open the state store for worker` | Dragonfly is not reachable, and a node uses state | Start Dragonfly, or check `DRAGONFLY_URL`. |
| `` `OTEL_TRACES_SAMPLER_ARG` is `` | the sampler value is not a number from 0 to 1 | Fix the variable. |
| `could not build the OTLP` | an OpenTelemetry endpoint setting is wrong | Check the `OTEL_EXPORTER_OTLP_*` variables. |
| `could not start the NATS I/O runtime`, `could not spawn`, `could not set up the Ctrl-C handler` | the machine refused a thread or a signal handler | Check the process limits and try again. |

## Messages keep coming back

`nats consumer info <stream> <consumer>` shows `Redelivered Messages`. A message comes back when its record failed. Each failure is logged as an event: in Loki when the stack sends logs there, or on standard error when no log endpoint is set. Common causes:

- The message has no valid `Fusion-Record-Id`. Every delivery fails. Fix the producer.
- The payload is not a JSON object, or a field has the wrong type. See [Concepts](concepts.md#message-and-record).
- A sink cannot store: the output stream is missing, full, or slow to confirm.
- Dragonfly failed on a node with `on_state_error: nak`. See [State and failure policy](state-and-failure.md).
- A `lua` script failed with `on_error: nak`. A `runtime` or `output` error usually fails again on every delivery.

After the last delivery the message becomes a dead letter.

## Dead letters appear

Read one:

```sh
nats sub 'dlq.>' --count 1
```

`Fusion-Dlq-Reason` names the node that failed and why. `source` means the message had no record id or its payload was not a record. `Fusion-Dlq-Subject` is where it came from. After fixing the cause, publish the dead letter back to that subject, with its headers, to process it again. It keeps its record id, tenant and ingestion time. A dead letter from `source` fails again unless the replay fixes it: add a `Fusion-Record-Id`, or correct the payload. See [NATS](nats.md#redelivery-and-dead-letters).

## Records are missing from the output

A record that is dropped is acked and does not reach a sink. The `reason` label on `records_dropped_total` says why:

| Reason | Cause |
|---|---|
| `filter` | a [filter](stages/filter.md) dropped it |
| `route_default_drop` | no [route](stages/route.md) condition matched and `default` is `drop` |
| `sample` | [sample](stages/sample/README.md) did not keep it |
| `dedupe` | [dedupe](stages/dedupe.md) saw a repeat |
| `regex_limit` | a pattern tripped a [limit](regex-limits.md) |
| `edit_unapplied` | an [edit](stages/edit/README.md) op could not apply and `on_unapplied` is `drop` |
| `lua_drop` | a [lua](stages/lua/README.md) script returned `nil` |
| `lua_error` | a lua script failed and `on_error` is `drop` |
| `invalid_record` | `Fusion-Record-Kind` was not `log`, or was unreadable |
| `missing_id` | the message had no record id; the message is also nakked |

Also check for a branch that ends at a stage with no sink after it. See [Writing a config](writing-a-config.md#a-branch-that-ends-at-a-stage).

## The same record arrives twice

This is expected now and then: the pipeline delivers at least once. It happens after a redelivery, and when one branch of a fan-out failed and the others had already written. Use [dedupe](stages/dedupe.md) to remove most repeats, and make readers of the output streams cope with the rest.

## The tenant is `unknown`

The subject did not name a tenant and there was no valid `Fusion-Tenant` header. Check that the subject is `<tenant_prefix>.<tenant>.<something>`, with at least three words, and that `tenant_prefix` matches. See [Concepts](concepts.md#tenant).

## A field is missing or changed in the output

- Nothing is dropped for being unexpected, and nothing is added. If a key is missing, a stage removed it.
- `extract` writes every value as text, so a number such as a PID comes out as `"4711"`.
- Check the `edit` ops, which run in order.
- A write makes its path exist and replaces what is in the way, so `set {field: body.parsed}` on a `body` that was text leaves an object and the text is gone. See [field paths](field-paths.md#reading-and-writing).
- Object keys come back sorted. Every key and value survives; the order does not.

## A stage keeps everything, or nothing, or drops everything

Most often the path names nothing. A path that matches nothing is not an error: it reads as null on every record, so a whole stage gets one answer.

| What you see | Likely cause |
|---|---|
| `filter action: keep` keeps nothing | The condition's path is misspelled, or a dotted name needs quotes. |
| `filter action: drop` keeps everything | The same. |
| Every record takes a `route` default | No label's condition can be true. |
| `dedupe` drops everything after the first | Every record hashes the same, because the key names nothing. |
| `sample mode: consistent` keeps all or none | The same. |
| `edit` counts every record on `edit_unapplied_total` with cause `absent` | The op's `from` names nothing. |

Check the path against a real message. A key whose name holds a dot needs quotes: `resource."log.format"`, not `resource.log.format`. See [field paths](field-paths.md#a-path-that-matches-nothing-is-not-an-error).

## A condition or pattern does not match

- `503` and `"503"` are different. Check how the producer sends numbers.
- `attributes."http.status"` is the key `http.status`; `attributes.http.status` is the key `status` inside the key `http`. Quotes decide which.
- `=~` searches anywhere in the text. Use `^` and `$` to match the whole text.
- In a condition, write `"\\d"` for `\d`. In `pattern:`, use single YAML quotes and write `\d`.
- A field that is not text never matches `=~`.

See [Conditions](conditions.md).

## Lines on standard error

| Line starts with | Meaning |
|---|---|
| `` pipeline: node `<id>` pattern= `` and `type=` | the pattern each regex node loaded, and its engine |
| `` pipeline: lua `<id>` record <id>: `` | a lua script failed on a record |
| `` pipeline: lua `<id>` record <id> info: `` or `warn:` | a script called `log.info` or `log.warn` |
| `nats source: ignored a pipeline header` | a `Fusion-*` header did not fit its format |
| `nats source: nak of undecodable message` | a payload was not a record |
| `nats source: not walked: an unreadable pipeline header` | a bad `Fusion-Record-Kind`; the message is dropped |
| `` pipeline: node `<id>` on_redos_risk=warn `` | a pattern loaded under `warn` despite a finding |
| `pipeline: ` and an event | a pipeline event, when no log endpoint is set |
