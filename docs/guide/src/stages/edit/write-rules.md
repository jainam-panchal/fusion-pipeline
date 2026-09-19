# edit write rules

A record is any JSON, so no field has a type and no value is refused for being the wrong one. See [Field paths](../../field-paths.md#reading-and-writing) for the whole picture.

The short version:

- Every path takes any value. `set {field: severity_number, value: high}` writes the text `high`.
- A write makes its path exist. A missing name is created; a value already in the way is replaced, so writing `body.parsed` when `body` is text leaves an object and the text is gone.
- A write into a list position the list already has keeps the list. A position it does not have is replaced by an object.
- Anything can be removed. Removing a list position closes the gap. Removing a path that is not there does nothing.
- `meta.*` takes nothing. No op can write or remove it. Only `copy` may read one.

## Checked at start

- Every path parses.
- No op writes or removes a `meta.*` path.
- `set`'s value is text, a number, a bool or `null`, not an object or a list.
- `from` and `to` of a `rename` or `copy` are different paths.

Nothing else is checked at start, because nothing else can be known without a record. A path that names nothing is not an error.

## Unapplied on a record

`rename`, `copy` and `hash` need a value to work on, so they are the ops that can be unapplied. See [errors](errors.md) for what `on_unapplied` then does.

| Cause | When |
|---|---|
| `absent` | The source path reads as null: nothing is there, or the value is `null`. |
| `type` | `hash` was given an object or a list, which has no text to hash. |

`set` and `delete` are never unapplied: a write always lands, and removing something that is not there is nothing to do.

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
