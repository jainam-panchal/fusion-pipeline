# lua script API

## The record

`process` gets the record as the JSON it is. There is no field list: whatever keys the producer sent are the keys you get.

- A JSON object is a table, and a list is a table too.
- A record that is one piece of text, as a [`codec: text`](../../nats.md) source gives it, is a Lua string. A number or a bool crosses as itself.
- **Nothing is there unless it was sent.** A key the record does not have is `nil`, including `attributes`, `kind` and `id`. Before adding to a table that may be absent, write `record.attributes = record.attributes or {}`.
- A key whose name holds a dot is just a key: `record.resource["log.format"]`.
- `record.id` is the payload's own `id`, if it has one. The record id the pipeline uses is `meta.id`.
- `record:copy()` is a method on the record table, so it exists only when the record is an object or a list. A record that is text, a number or a bool is a Lua value you can copy by assigning it.

## The meta table

`meta.id`, `meta.tenant`, `meta.ingestion_time` (nanoseconds) and `meta.delivery_count`. Writing to `meta` is an error. To keep a value, copy it into the record.

## Return values

Return the record to pass it on. The script can change any key, add keys anywhere it likes — there is no field list — and remove one by setting it to `nil`. A record the script leaves alone comes back exactly as it went in, whatever its shape.

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

- Every value must have a JSON form. A function, a coroutine, userdata, a number that is not finite, or text that is not UTF-8 is an error.
- The text values in one record, added up, must fit in `limits.output_kib`.
- Tables may nest at most 127 levels below the record table (128 counting it).
- A returned boolean is an error, unless the record itself arrived as a boolean: return `nil` to drop a record.

No key and no type is checked, because a record is any JSON. A key such as `colour` is fine.

A returned boolean is the one shape rule: `return false` almost always means an author wanted
to drop the record, so it is refused and says to return `nil` instead. A record that *is* a
boolean is exempt, so an identity script on one still works.

The value you return is read as **one record or a split**, so a table you build with positions
`1..n` is a split, not a list record. To pass a list record on, return `record` or
`record:copy()`; both keep the record's own shape.

Setting a key to `nil` removes it. Setting it to `json.null` keeps it as an explicit `null`.

A whole number larger than 2^63 does not survive a script: Lua holds it as a floating-point
number, so it comes back as one. `meta.id` is the exception, handed over as text when it is
that large.

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

- `json.null` is JSON `null`. Use it anywhere a value goes. A key set to `json.null` keeps an explicit `null`; set it to `nil` to remove it. `json.null` counts as true in an `if`, so compare with `== json.null`.
- `json.list(t)` marks `t` as a list, so an empty table stays `[]`. `json.list()` makes a new empty list. Use it for a value inside the record; at the top level a marked list is a split.

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
