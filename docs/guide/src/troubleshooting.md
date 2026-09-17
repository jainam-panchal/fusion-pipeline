# Troubleshooting

## The pipeline does not start

The pipeline prints `pipelined: ` and the reason, then exits. In the compose stack, the container restarts and prints the same line again, so check `docker compose -f deploy/compose.yaml logs pipeline`.

| Message starts with | Cause | Fix |
|---|---|---|
| `usage: pipelined --config <path>` | no `--config`, or an unknown argument | Run `pipelined --config <file>`. |
| `` could not read config `<path>` `` | the file is missing or unreadable | Check the path. In compose, `PIPELINE_CONFIG` is relative to `deploy/`. |
| `config is not valid YAML` | a YAML mistake, or an unknown top-level key | See [Writing a config](writing-a-config.md#what-the-pipeline-refuses). |
| `` config has no `source` block `` | no `source:` | Add one. See [NATS](nats.md#the-source). |
| `` node `<id>`: `` and a config problem | a key or value a node does not take | See the node's page, and [Field paths](field-paths.md#what-the-pipeline-refuses), [Conditions](conditions.md#what-the-pipeline-refuses) and [Regex limits](regex-limits.md#what-the-pipeline-refuses). |
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
| `invalid state store url` | `DRAGONFLY_URL` is malformed | Fix the URL, for example `redis://127.0.0.1:6379`. |
| `could not open the state store for worker` | Dragonfly is not reachable, and a node uses state | Start Dragonfly, or check `DRAGONFLY_URL`. |
| `` `OTEL_TRACES_SAMPLER_ARG` is `` | the sampler value is not a number from 0 to 1 | Fix the variable. |
| `could not build the OTLP` | an OpenTelemetry endpoint setting is wrong | Check the `OTEL_EXPORTER_OTLP_*` variables. |

## Messages keep coming back

`nats consumer info <stream> <consumer>` shows `Redelivered Messages`. A message comes back when its record failed. The pipeline writes a line to standard error for each failure. Common causes:

- **No record id.** The message has no valid `Fusion-Record-Id`. Every delivery fails. Fix the producer.
- **The payload is not a record.** It is not a JSON object, or a field has the wrong type. See [Concepts](concepts.md#message-and-record).
- **A sink cannot store.** The output stream is missing, full, or slow to confirm.
- **Dragonfly failed** on a node with `on_state_error: nak`. See [State and failure policy](state-and-failure.md).
- **A `lua` script failed** with `on_error: nak`. A `runtime` or `output` error usually fails again on every delivery.

After the last delivery the message becomes a dead letter.

## Dead letters appear

Read one:

```sh
nats sub 'dlq.>' --count 1
```

`Fusion-Dlq-Reason` names the node that failed and why. `source` means the message had no record id or its payload was not a record. `Fusion-Dlq-Subject` is where it came from. After fixing the cause, publish the dead letter back to that subject, with its headers, to process it again. It keeps its record id, tenant and ingestion time. See [NATS](nats.md#redelivery-and-dead-letters).

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

- A top-level payload key that is not a record field is dropped. Move it under `attributes`.
- `kind: log` is added when the payload had no `kind`.
- `extract` writes every value as text, so `"4711"`, not `4711`.
- Check the `edit` ops, which run in order.

## A condition or pattern does not match

- `503` and `"503"` are different. Check how the producer sends numbers.
- `attributes.http.status` is the flat key `http.status`, not a nested object.
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
