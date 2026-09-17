# sample: every_nth

```yaml
- id: one_in_ten
  type: sample
  mode: every_nth
  n: 10
  on_state_error: pass
```

| Key | Default | What it does |
|---|---|---|
| `n` | required | Keep one record in `n`. A whole number, at least 1. |
| `on_state_error` | `pass` | What to do when Dragonfly does not answer: `pass` or `nak`. See [State and failure policy](../../state-and-failure.md). |

## How it decides

The node keeps a counter in Dragonfly, one per tenant. Each record adds 1. The records at counts 1, n+1, 2n+1 and so on are kept. So the first record of a tenant is always kept, even when the tenant sends fewer than `n`.

```yaml
# messages in
{{#include ../../../examples/stages/sample/every-nth/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/sample/every-nth/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/sample/every-nth/expected.yaml}}
```

The counter is shared by every worker and every copy of the pipeline, so the count is exact. A counter that is not touched for 24 hours starts again at 1.

Each tenant counts on its own:

```yaml
# messages in
{{#include ../../../examples/stages/sample/every-nth-per-tenant/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/sample/every-nth-per-tenant/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/sample/every-nth-per-tenant/expected.yaml}}
```

## Deliveries, not records

`every_nth` counts deliveries. When NATS delivers a message again, that delivery takes the next count, and it may now be dropped. Over time, one delivery in `n` is kept, but the records that get through are not always the ones picked the first time. This is a known exception to "a redelivered record gets the same answer". The [spec](https://github.com/jainam-panchal/fusion-pipeline/blob/main/docs/specs/2026-09-08-observability-pipeline-poc.md#stages) has the reasoning (amendment 2026-09-15, issue #7).

The examples cannot redeliver a message, but three messages with the same record id show the same thing. `every_nth` does not look at the id:

```yaml
# messages in
{{#include ../../../examples/stages/sample/every-nth-same-id/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/sample/every-nth-same-id/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/sample/every-nth-same-id/expected.yaml}}
```

Compare this with [`random`](random.md), where the same id always gets the same answer.

## When Dragonfly is down

With `on_state_error: pass`, records go on without being counted, and none are dropped. With `nak`, the message is nakked and comes back later. The pipeline connects to Dragonfly through `DRAGONFLY_URL` (default `redis://127.0.0.1:6379`).
