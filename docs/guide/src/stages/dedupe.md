# dedupe

`dedupe` drops a record when a different record with the same key values came in shortly before.

```yaml
- id: no_repeats
  type: dedupe
  key: [body]
  window: 10s
  on_state_error: pass
```

| Key | Default | What it does |
|---|---|---|
| `key` | required | A list of [field paths](../field-paths.md), at least one. Records with the same values for all of them count as repeats. `meta.*` paths work too. |
| `window` | required | How long a record blocks its repeats: a whole number and a unit, `ms`, `s`, `m` or `h`. At least `1ms`. |
| `on_state_error` | `pass` | What to do when Dragonfly does not answer: `pass` or `nak`. See [State and failure policy](../state-and-failure.md). |

A dropped record counts under the reason `dedupe`, and the message is acked. `dedupe` keeps its memory in Dragonfly, so every worker and every copy of the pipeline shares it.

## Repeats inside the window

The first record with a key passes and holds the key. A different record with the same key, less than `window` later, is dropped:

```yaml
# messages in
{{#include ../../examples/stages/dedupe/repeat/input.yaml}}
```

```yaml
# config
{{#include ../../examples/stages/dedupe/repeat/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/dedupe/repeat/expected.yaml}}
```

## Time is ingestion time

"Later" is measured in ingestion time: when the message entered NATS. The payload's time fields play no part. A record that arrives `window` or more after the holder passes and becomes the new holder. The repeats after it are checked against it:

```yaml
# messages in
{{#include ../../examples/stages/dedupe/new-window/input.yaml}}
```

```yaml
# config
{{#include ../../examples/stages/dedupe/new-window/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/dedupe/new-window/expected.yaml}}
```

Record 2 comes exactly 10 seconds after record 1, so it is outside the window and starts a new one. Record 3 is 2 seconds after record 2 and is dropped.

The window does not slide: a dropped repeat does not make it longer.

## The key

All listed fields count together:

```yaml
# messages in
{{#include ../../examples/stages/dedupe/several-fields/input.yaml}}
```

```yaml
# config
{{#include ../../examples/stages/dedupe/several-fields/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/dedupe/several-fields/expected.yaml}}
```

A missing field counts as `null`, so all records without it share one key:

```yaml
# messages in
{{#include ../../examples/stages/dedupe/missing-field/input.yaml}}
```

```yaml
# config
{{#include ../../examples/stages/dedupe/missing-field/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/dedupe/missing-field/expected.yaml}}
```

Each tenant has its own keys:

```yaml
# messages in
{{#include ../../examples/stages/dedupe/tenants/input.yaml}}
```

```yaml
# config
{{#include ../../examples/stages/dedupe/tenants/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/dedupe/tenants/expected.yaml}}
```

## Redeliveries and late records

A message that comes back with the same record id is not a repeat of itself, so it passes again. See [State and failure policy](../state-and-failure.md#windows-use-ingestion-time) for an example.

A record that came in before the current holder passes too. This happens when a message is redelivered after its key was taken over:

```yaml
# messages in
{{#include ../../examples/stages/dedupe/older-late/input.yaml}}
```

```yaml
# config
{{#include ../../examples/stages/dedupe/older-late/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/dedupe/older-late/expected.yaml}}
```

## Known gaps

Dragonfly removes a key `window` after it was stored, by its own clock. If the pipeline falls behind by more than the window, for example after a restart or while Dragonfly is paused, a repeat can find the key gone and pass ([issue #54](https://github.com/jainam-panchal/fusion-pipeline/issues/54)). This, and the late record above, can let an extra copy through. `dedupe` never drops a record that is not a repeat.

Each key in Dragonfly takes about 200 bytes. The number of keys is the number of different key values seen in one window.

## When Dragonfly fails

With `on_state_error: pass`, records go on without the check, so repeats get through. With `nak`, the message is nakked and comes back later. If Dragonfly cannot be reached at start, the pipeline does not start. See [State and failure policy](../state-and-failure.md).

## What the pipeline refuses

```yaml
# config
{{#include ../../examples/stages/dedupe/rejected-empty-key/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/dedupe/rejected-empty-key/expected.yaml}}
```

```yaml
# config
{{#include ../../examples/stages/dedupe/rejected-no-window/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/dedupe/rejected-no-window/expected.yaml}}
```

The other messages, each after `` node `<id>`:  ``:

| Problem | Message |
|---|---|
| `window` without a unit | `` window `10`: needs a unit: `ms`, `s`, `m` or `h` `` |
| an unknown unit | `` window `10sec`: unknown unit `sec`; use `ms`, `s`, `m` or `h` `` |
| no number, or a fraction such as `1.5s` | `` window `ten`: write `<integer><ms\|s\|m\|h>`, e.g. `10s` `` (a fraction gives the unknown unit message) |
| `0s` | `` window `0s`: must be at least 1ms `` |
| a bad `key` path | `` key `<path>`:  `` and the reason, see [Field paths](../field-paths.md#what-the-pipeline-refuses) |
| `on_state_error` other than `pass` or `nak` | `` unknown variant `drop`, expected `pass` or `nak` `` |
