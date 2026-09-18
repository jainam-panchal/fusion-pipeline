# State and failure policy

Some stages remember things between records. They keep that memory in Dragonfly, a Redis-compatible store, so every worker and every copy of the pipeline shares it.

| Stage | Uses Dragonfly | `on_state_error` default |
|---|---|---|
| [`dedupe`](stages/dedupe.md) | always | `pass` |
| [`sample`](stages/sample/README.md) | only with `mode: every_nth` | `pass` |
| [`lua`](stages/lua/README.md) | when the script uses `state` | `nak` |

Other stages and modes do not take `on_state_error`.

## Connecting

The pipeline reads the address from `DRAGONFLY_URL`, default `redis://127.0.0.1:6379`. There is no key for it in the config.

When some node uses Dragonfly, each worker connects at start. If Dragonfly cannot be reached then, the pipeline does not start. When no node uses it, the pipeline never connects, and its start line says `state unused`.

## When Dragonfly fails while running

Each call to Dragonfly has 2 seconds to answer. A call that fails or runs out of time is a state error. After a connection error or a timeout, the next call opens a fresh connection, with up to 5 seconds to connect. The node's `on_state_error` decides what happens to the record:

| `on_state_error` | The record | The message |
|---|---|---|
| `pass` | goes on unchanged, as if the node were not there | acked when its branches finish |
| `nak` | stops | nakked, and delivered again later |

`pass` keeps records flowing. The cost is that `dedupe` lets duplicates through and `every_nth` keeps records it did not count. `nak` holds records back until Dragonfly answers. Each nak waits 1, 2, 4, 8 seconds and so on, up to 30 seconds, before the next delivery. After the last delivery, the message becomes a dead letter. With the compose stack's 5 deliveries, the waits add up to 15 seconds, plus the time each failed call takes. A longer outage sends messages to the dead-letter stream ([issue #30](https://github.com/jainam-panchal/fusion-pipeline/issues/30) would give these naks a longer wait).

`lua` defaults to `nak` because a record that skips a script may skip work such as masking data. A record that skips `dedupe` is only a duplicate.

The examples in this guide cannot make Dragonfly fail, so this section has no example.

## Keys

The pipeline puts `<pipeline name>:<tenant>:<node id>:` in front of every key a stage uses. So:

- Copies of one pipeline share their state. Two different pipelines on one Dragonfly need different `name`s.
- Tenants never see each other's state.
- Records with no tenant share the tenant `unknown`.

For this reason, node ids and the pipeline name cannot contain `:`.

## Windows use ingestion time

`dedupe` measures its window in ingestion time: the time the message entered NATS (or the `Fusion-Ingestion-Time` header, when an upstream pipeline set it). It does not use the payload's time fields, or the time the record reaches the stage. So a record that comes back later gets the same answer as the first time.

Here the second message arrives 5 seconds after the first and is dropped. The third arrives 12 seconds after the first, outside the 10 second window, so it passes and starts a new window:

```yaml
# messages in
{{#include ../examples/state/window/input.yaml}}
```

```yaml
# config
{{#include ../examples/state/window/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/state/window/expected.yaml}}
```

A message that comes back with the same record id is not a duplicate of itself:

```yaml
# messages in
{{#include ../examples/state/same-id/input.yaml}}
```

```yaml
# config
{{#include ../examples/state/same-id/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/state/same-id/expected.yaml}}
```

Two things can give a different answer on a redelivery. `sample` with `every_nth` counts deliveries, see [every_nth](stages/sample/every-nth.md). A `lua` script's values kept between records belong to one worker, see [lua](stages/lua/script-api.md#values-that-live-between-records). The [spec](https://github.com/jainam-panchal/fusion-pipeline/blob/main/docs/specs/2026-09-08-observability-pipeline-poc.md#stages) lists both.

Dragonfly expires a `dedupe` key by its own clock. If the pipeline falls behind by more than the window, a duplicate can find the key gone and pass ([issue #54](https://github.com/jainam-panchal/fusion-pipeline/issues/54)). A record that arrives after a newer copy of itself also passes. Both cost an extra copy, never a lost record.

## What the pipeline refuses

```yaml
# config
{{#include ../examples/state/rejected-policy-value/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/state/rejected-policy-value/expected.yaml}}
```

There is no `drop` policy. Use `pass` or `nak`.

```yaml
# config
{{#include ../examples/state/rejected-policy-random/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/state/rejected-policy-random/expected.yaml}}
```

Only `every_nth` uses Dragonfly, so only it takes `on_state_error`. A `lua` script that never uses `state` refuses the key too, with ``node `<id>`: `on_state_error` is given but the script never uses `state` ``.

```yaml
# config
{{#include ../examples/state/rejected-window-unit/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/state/rejected-window-unit/expected.yaml}}
```

Write the window with a unit: `ms`, `s`, `m` or `h`, as in `10s`.
