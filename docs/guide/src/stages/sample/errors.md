# sample errors

The pipeline refuses these configs at start. The message follows `pipelined: `.

## No `mode`

```yaml
# config
{{#include ../../../examples/stages/sample/rejected-no-mode/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/sample/rejected-no-mode/expected.yaml}}
```

## A key from another mode

```yaml
# config
{{#include ../../../examples/stages/sample/rejected-wrong-mode-key/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/sample/rejected-wrong-mode-key/expected.yaml}}
```

## `percent` out of range

```yaml
# config
{{#include ../../../examples/stages/sample/rejected-percent-zero/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/sample/rejected-percent-zero/expected.yaml}}
```

## `n` of 0

```yaml
# config
{{#include ../../../examples/stages/sample/rejected-n-zero/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/sample/rejected-n-zero/expected.yaml}}
```

## `consistent` without `key`

```yaml
# config
{{#include ../../../examples/stages/sample/rejected-no-key/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/sample/rejected-no-key/expected.yaml}}
```

## A key `sample` does not know

```yaml
# config
{{#include ../../../examples/stages/sample/rejected-unknown-key/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/sample/rejected-unknown-key/expected.yaml}}
```

## A bad `on_state_error`

```yaml
# config
{{#include ../../../examples/stages/sample/rejected-bad-policy/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/sample/rejected-bad-policy/expected.yaml}}
```

## All messages

Each message starts with ``node `<id>`: ``.

| Problem | Message |
|---|---|
| no `mode` | `` `mode` is required: `random`, `every_nth` or `consistent` `` |
| unknown mode | `` unknown mode `weighted`: use `random`, `every_nth` or `consistent` `` |
| a key from another mode | `` `n` belongs to `mode: every_nth`, not `mode: random` `` |
| no `percent` | `` `percent` is required: the share kept, above 0 and at most 100 `` |
| `percent` of 0 or less, or above 100 | `` `percent` must be above 0 and at most 100, not 0 `` |
| no `n` | `` `n` is required: keep one record in n `` |
| `n: 0` | `` `n` must be at least 1 `` |
| `on_state_error` other than `pass` or `nak` | `` unknown variant `drop`, expected `pass` or `nak` `` |
| `n` above 9223372036854775807 | `` `n` is too large `` |
| no `key` | `` `key` is required: the field paths records are kept or dropped together by `` |
| `key: []` | `` `key` needs at least one field path `` |
| a `key` path that does not parse | ``key `<path>`: `` and the reason, see [Field paths](../../field-paths.md) |
| a key `sample` does not know | `` unknown field `seed`, expected one of `mode`, `percent`, `n`, `key`, `on_state_error` `` |
