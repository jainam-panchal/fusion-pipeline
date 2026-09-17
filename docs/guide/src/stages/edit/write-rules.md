# edit write rules

Every op that writes a field follows the same rules as the rest of the pipeline. See [Field paths](../../field-paths.md#what-a-write-accepts) for the table of which field takes which values.

The short version:

- `attributes.*`, `resource.*`, `scope.*` and `body` take any value.
- `id` and the time fields take a whole number, 0 or more.
- `severity_number` takes a whole number.
- `severity_text`, `trace_id` and `span_id` take text.
- `kind` takes `log`, `metric` or `span`.
- `meta.*` takes nothing. No op can write or remove it.

`null` removes `id`, a time field, `severity_text`, `severity_number`, `trace_id` or `span_id`. Under a map key or in `body`, `null` is kept as a value.

## Checked at start

- `set`: the value against the field.
- `hash`: that the field takes text.
- Every op: that it writes or removes no `meta.*` path. Only `copy` may read one.

## Checked on each record

`copy` and `rename` can only check when the value is known. A value the target does not take makes the op unapplied with cause `type`, and the record keeps its old value:

```yaml
# messages in
{{#include ../../../examples/stages/edit/type-clash/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/edit/type-clash/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/type-clash/expected.yaml}}
```

`severity_number` does not take the text `high`, and a list cannot be hashed. Both ops are counted as unapplied, and with `skip` the record goes on unchanged.
