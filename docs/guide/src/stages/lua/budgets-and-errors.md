# lua budgets and errors

## Limits

```yaml
limits:
  instructions: 1000000   # Lua instructions per record, at least 1
  memory_kib: 16384       # memory for the worker's copy of the script, at least 64
  output_kib: 1024        # text in one returned record, at least 1
```

Every key is optional, and the values above are the defaults.

- `instructions` is counted again for every record. A record that uses them up fails, and the next record starts fresh.
- `memory_kib` covers everything the script keeps, values between records included. When it runs out, the pipeline throws away that worker's copy of the script and builds a new one for the next record.
- `output_kib` applies to each returned record on its own.

A script that loops forever fails with the `instructions` limit:

```yaml
# messages in
{{#include ../../../examples/stages/lua/budget-trip/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/budget-trip/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/budget-trip/expected.yaml}}
```

## When a script fails

A run fails when:

- it uses up `instructions` (kind `instructions`)
- it runs out of `memory_kib` (kind `memory`)
- it raises an error, calls something that is not there, or writes to `meta` (kind `runtime`)
- it returns something the pipeline refuses (kind `output`)

The kind is counted on `lua_errors_total`, and one line with the reason goes to standard error. Then `on_error` decides what happens to the record:

| `on_error` | The record | The message |
|---|---|---|
| `pass` (default) | goes on as it came into the node | acked when its branches finish |
| `drop` | dropped, reason `lua_error` | acked |
| `nak` | stops | nakked, and redelivered |

The same script with each setting. Record 1 works. Record 2 has a `http.status` that is not a number, so `//` raises an error:


### `on_error: pass`

```yaml
# messages in
{{#include ../../../examples/stages/lua/on-error-pass/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/on-error-pass/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/on-error-pass/expected.yaml}}
```

### `on_error: drop`

```yaml
# messages in
{{#include ../../../examples/stages/lua/on-error-drop/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/on-error-drop/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/on-error-drop/expected.yaml}}
```

### `on_error: nak`

```yaml
# messages in
{{#include ../../../examples/stages/lua/on-error-nak/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/on-error-nak/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/on-error-nak/expected.yaml}}
```

With `nak`, think about the next delivery. A script that fails on a record will fail again on every redelivery, so the message ends as a dead letter after the last one. A `state` failure is different: it is handled by `on_state_error`, see [State API](state-api.md).

## What the pipeline refuses

The pipeline loads each script when it starts and runs the code outside `process` once. These configs stop it with the message shown.

### A name from outside the sandbox

```yaml
# config
{{#include ../../../examples/stages/lua/rejected-os/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/rejected-os/expected.yaml}}
```

The message gives the node id and the line in the script. `print` gets the hint `; use log.info`.

### No `process` function

```yaml
# config
{{#include ../../../examples/stages/lua/rejected-no-process/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/rejected-no-process/expected.yaml}}
```

### A syntax error

```yaml
# config
{{#include ../../../examples/stages/lua/rejected-syntax/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/rejected-syntax/expected.yaml}}
```

### Both `script` and `source`

```yaml
# config
{{#include ../../../examples/stages/lua/rejected-both/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/rejected-both/expected.yaml}}
```

### A limit below its minimum

```yaml
# config
{{#include ../../../examples/stages/lua/rejected-memory/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/rejected-memory/expected.yaml}}
```

### `on_state_error` without `state`

```yaml
# config
{{#include ../../../examples/stages/lua/rejected-state-policy/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/rejected-state-policy/expected.yaml}}
```

Other messages, each after ``node `<id>`: ``:

| Problem | Message |
|---|---|
| neither `script` nor `source` | ``give `script` (a file path) or `source` (the script inline)`` |
| a `script` file that cannot be read | ``` `script`: cannot read `<path>`: ``` and the reason |
| code outside `process` that loops forever | `instruction budget exceeded` |
| code outside `process` that calls `state` | ``the pipeline API is only available inside `process` `` |
