# edit recipes

## Replace an email with a join key

```yaml
# messages in
{{#include ../../../examples/stages/edit/recipe-pseudonymise/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/edit/recipe-pseudonymise/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/recipe-pseudonymise/expected.yaml}}
```

The digest is the same for every record with the same email, so records can still be matched. See [hash](ops.md#hash) for what it does not hide.

## Put the real tenant into the record

```yaml
# messages in
{{#include ../../../examples/stages/edit/recipe-tenant-field/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/edit/recipe-tenant-field/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/recipe-tenant-field/expected.yaml}}
```

The producer sent its own `resource.tenant.id`. `copy` replaces it with the tenant the pipeline knows, so the record can be trusted downstream.

## Keep only records a parser handled

Run [`extract`](../regex/extract.md) first, then an `edit` with `on_unapplied: drop` that renames a field only the parser writes. Records the parser did not match are dropped:

```yaml
# messages in
{{#include ../../../examples/stages/edit/unapplied-drop/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/edit/unapplied-drop/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/unapplied-drop/expected.yaml}}
```

`deploy/pipeline.yaml` does this on a branch of its own, so the main branch still keeps every record.
