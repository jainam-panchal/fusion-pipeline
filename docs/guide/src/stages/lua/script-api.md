# lua script API

## The record table

Each field of the record is a key in the table, under its own name: `id`, `kind`, `time_unix_nano`, `observed_time_unix_nano`, `severity_text`, `severity_number`, `body`, `attributes`, `resource`, `scope`, `trace_id` and `span_id`. See [Concepts](../../concepts.md) for their types.

- A field the record does not have is `nil`. `kind` is always there.
- `attributes`, `resource` and `scope` are always tables, maybe empty. Their keys are the full flat keys: `record.attributes["http.status"]`.
- `id` is the payload's `id`. The record id the pipeline uses is `meta.id`.

## The meta table

`meta.id`, `meta.tenant`, `meta.ingestion_time` (nanoseconds) and `meta.delivery_count`. Writing to `meta` is an error. To keep a value, copy it into the record.

## Return values

Return the record to pass it on. The script can change any field, add fields under `attributes`, `resource` or `scope`, and remove fields by setting them to `nil`.

Return `nil` to drop the record. The message is still acked.

```yaml
# messages in
{{#include ../../../examples/stages/lua/drop-debug/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/drop-debug/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/drop-debug/expected.yaml}}
```

Only `nil` drops. `false` is an error:

```yaml
# messages in
{{#include ../../../examples/stages/lua/return-false/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/return-false/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/return-false/expected.yaml}}
```

Here `on_error: pass` forwards the record as it came in. See [Budgets and errors](budgets-and-errors.md).

Return a list of records to split one record into several. `record:copy()` makes a full copy to start each one from:

```yaml
# messages in
{{#include ../../../examples/stages/lua/split-lines/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/split-lines/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/split-lines/expected.yaml}}
```

Every record from a split keeps the Meta of the message, so all three carry record id 1. The message is acked once, after all three are written. An empty list is an error: return `nil` to drop instead.

## What the pipeline checks on the way out

Before a returned record leaves the stage, the pipeline checks it:

- Every top-level key must be a record field. A key such as `colour` is an error.
- Each field must have the right type. `kind` must be `log`, `metric` or `span`, and `id` a whole number that is not negative.
- A whole number written as a float, such as `18 / 2`, is turned into the number `9`. For `id` only, a text of digits such as `"7"` becomes the number.
- The text values in one record, added up, must fit in `limits.output_kib`.
- Tables may nest at most 128 levels.

```yaml
# messages in
{{#include ../../../examples/stages/lua/output-refused/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/output-refused/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/output-refused/expected.yaml}}
```

## JSON values

A Lua table with keys `1..n` and nothing else becomes a JSON list. A table with no such keys becomes an object. A table with `1..n` and other keys is refused. Two helpers cover the cases Lua cannot say on its own:

- `json.null` is JSON `null`. Use it inside a list or object. A field set to `json.null` is removed, the same as `nil`. `json.null` counts as true in an `if`, so compare with `== json.null`.
- `json.list(t)` marks `t` as a list, so an empty table stays `[]`. `json.list()` makes a new empty list.

```yaml
# messages in
{{#include ../../../examples/stages/lua/json-null/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/json-null/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/json-null/expected.yaml}}
```

A list may only have the positions `1..n`. For a gap, write `json.null`, not `nil`.

## What a script can call

- The Lua libraries `string`, `table`, `math` and `utf8`.
- The basic functions `assert`, `error`, `getmetatable`, `ipairs`, `next`, `pairs`, `rawequal`, `rawget`, `rawlen`, `rawset`, `select`, `setmetatable`, `tonumber`, `tostring`, `type` and `warn`, and the values `_G` and `_VERSION`.
- `pcall` and `xpcall`. They catch errors the script raises itself. They do not catch a budget or memory trip or a `state` failure.
- `record:copy()`, a full copy of a record.
- `log.info(text)` and `log.warn(text)`. Each call writes one line to the pipeline's standard error, with the node id and record id. The lines are not rate limited ([issue #42](https://github.com/jainam-panchal/fusion-pipeline/issues/42)).
- `now_ns()`, the current time in nanoseconds.
- `state`, see [State API](state-api.md).
- `json.null` and `json.list`.

A script that names `os`, `io`, `package`, `require`, `load`, `loadfile`, `dofile`, `loadstring`, `debug` or `print` is refused at start. `collectgarbage` and `coroutine` are not there either, so using them is an error: at start when the code outside `process` uses them, or when a record runs otherwise.

## Values that live between records

Each worker has its own copy of the script. A global or local defined outside `process` keeps its value across the records that worker handles. So with 4 workers, a counter kept this way counts to four different numbers, and a redelivered message may land on a different worker and see a different value. The [spec](https://github.com/jainam-panchal/fusion-pipeline/blob/main/docs/specs/2026-09-08-observability-pipeline-poc.md#stages) lists this as a known exception to "a redelivered record gets the same answer" (amendment 2026-09-16, issue #8).

A memory trip throws the copy away and starts a fresh one, so these values go back to their start. For a count that is shared and survives restarts, use [`state`](state-api.md).
