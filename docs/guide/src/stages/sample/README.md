# sample

`sample` keeps a share of the records and drops the rest. A dropped record is acked, and it counts under the drop reason `sample`.

It has three modes. Pick one with `mode`:

| Mode | Keeps | Keys | Uses Dragonfly |
|---|---|---|---|
| [`random`](random.md) | about `percent` of the records | `percent` | no |
| [`every_nth`](every-nth.md) | exactly one record in `n`, per tenant | `n`, `on_state_error` | yes |
| [`consistent`](consistent.md) | about `percent` of the values of `key`, with every record of a kept value | `percent`, `key` | no |

`mode` is required. A key that belongs to another mode is an error, so a `random` node cannot have `n`.

Which one to use:

- `random` is the cheapest. A record keeps its answer when NATS delivers it again.
- `every_nth` gives an exact count, but it talks to Dragonfly for every record, and a redelivered message can get a different answer.
- `consistent` keeps or drops whole groups, for example every line from the same host.

A simple example:

```yaml
# messages in
{{#include ../../../examples/stages/sample/random-keep-all/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/sample/random-keep-all/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/sample/random-keep-all/expected.yaml}}
```

`percent: 100` keeps everything. The mode pages show real sampling.

For what the pipeline refuses, see [Errors](errors.md).
