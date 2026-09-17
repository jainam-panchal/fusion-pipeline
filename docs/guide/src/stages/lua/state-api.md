# lua state API

`state` stores values in Dragonfly. Every worker and every copy of the pipeline share them. The pipeline connects to Dragonfly only when a script uses the global `state`. A field such as `record.state`, or the word inside a string or comment, does not count. Each worker then has one connection, shared by all its nodes that use state. It finds Dragonfly through `DRAGONFLY_URL` (default `redis://127.0.0.1:6379`).

| Call | Returns | What it does |
|---|---|---|
| `state.get(key)` | text or `nil` | Reads a value. `nil` when the key is missing or has expired. |
| `state.set_nx(key, value, ttl_ms)` | `true`, or `false` and the stored value | Stores `value` only if the key is missing, and sets it to expire after `ttl_ms` milliseconds. |
| `state.incr(key, by, ttl_ms)` | the new number | Adds `by` to a number, starting from 0. The expiry is reset on every call. |
| `state.del(key)` | nothing | Removes a key. A missing key is fine. |

Keys are private to the node and the tenant. The pipeline puts `<pipeline name>:<tenant>:<node id>:` in front of every key, so two tenants never see each other's values. The pipeline name is the config's `name`, `pipeline` by default. A `:` or `%` in the tenant is written as `%3A` or `%25`.

A counter per body:

```yaml
# messages in
{{#include ../../../examples/stages/lua/state-counter/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/state-counter/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/state-counter/expected.yaml}}
```

Let the first record with a body through and drop the rest:

```yaml
# messages in
{{#include ../../../examples/stages/lua/first-seen/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/first-seen/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/first-seen/expected.yaml}}
```

## When Dragonfly does not answer

If Dragonfly cannot be reached when the pipeline starts, the pipeline does not start. The rest of this section is about calls that fail while it runs.

A `state` call that fails stops the script. `on_error` does not apply, and `pcall` does not catch it. The node's `on_state_error` decides instead:

- `nak` (the default): the message is nakked and comes back later.
- `pass`: the record goes on as it came into the node, as if the script had not run.

The default is `nak` because a script may build its output from what it stores, and a record that skipped the script may be missing work it needed. `dedupe` and `sample` default to `pass`, since a record that skips them is only an extra copy. See [State and failure policy](../../state-and-failure.md).

Calling `state` in the code outside `process` stops the pipeline at start.
