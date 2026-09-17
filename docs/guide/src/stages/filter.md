# filter

`filter` keeps or drops each record by a [condition](../conditions.md).

```yaml
- id: keep_errors
  type: filter
  condition: severity_text == "ERROR"
  action: keep
```

| Key | Default | What it does |
|---|---|---|
| `condition` | required | The test. See [Conditions](../conditions.md). |
| `action` | required | `keep`: records that match go on, the rest are dropped. `drop`: records that match are dropped, the rest go on. |
| `limits` | see [Regex limits](../regex-limits.md) | Limits for `=~` and `!~` in the condition. |
| `on_redos_risk` | `reject` | What to do with a risky pattern. See [Regex limits](../regex-limits.md). |

A dropped record counts under the reason `filter`, and the message is acked.

## Keep

```yaml
# messages in
{{#include ../../examples/stages/filter/keep-errors/input.yaml}}
```

```yaml
# config
{{#include ../../examples/stages/filter/keep-errors/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/filter/keep-errors/expected.yaml}}
```

Record 3 has no `severity_text`, so it does not match, and `keep` drops it.

## Drop

```yaml
# messages in
{{#include ../../examples/stages/filter/drop-debug/input.yaml}}
```

```yaml
# config
{{#include ../../examples/stages/filter/drop-debug/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/filter/drop-debug/expected.yaml}}
```

With `drop`, record 3 does not match either, so it goes on. Mind missing fields when you pick `keep` or `drop`.

## More than one test

```yaml
# messages in
{{#include ../../examples/stages/filter/server-errors/input.yaml}}
```

```yaml
# config
{{#include ../../examples/stages/filter/server-errors/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/filter/server-errors/expected.yaml}}
```

## Patterns

With `=~` or `!~` in the condition, a record that goes over a limit is dropped with the reason `regex_limit`, whatever `action` says. See [Regex limits](../regex-limits.md) for an example.

## What the pipeline refuses

```yaml
# config
{{#include ../../examples/stages/filter/rejected-no-action/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/filter/rejected-no-action/expected.yaml}}
```

`action` has no default. Write `keep` or `drop`.

```yaml
# config
{{#include ../../examples/stages/filter/rejected-bad-action/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/filter/rejected-bad-action/expected.yaml}}
```

A key `filter` does not know:

```yaml
# config
{{#include ../../examples/stages/filter/rejected-unknown-key/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/filter/rejected-unknown-key/expected.yaml}}
```

Mistakes in the condition are listed on [Conditions](../conditions.md#what-the-pipeline-refuses) and [Field paths](../field-paths.md#what-the-pipeline-refuses).
