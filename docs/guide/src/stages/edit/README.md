# edit

`edit` changes fields with a list of ops.

```yaml
- id: normalise
  type: edit
  on_unapplied: skip
  ops:
    - set: {field: resource.env, value: prod}
    - rename: {from: attributes.http.path, to: attributes.http.route}
    - copy: {from: body, to: attributes.raw}
    - hash: {field: attributes.user.email}
    - delete: {fields: [attributes.debug]}
```

| Key | Default | What it does |
|---|---|---|
| `ops` | required | The ops, at least one. They run in order on each record. |
| `on_unapplied` | `skip` | What to do when an op cannot apply: `skip` or `drop`. |

| Op | What it does |
|---|---|
| `set: {field, value}` | Writes a fixed value. |
| `rename: {from, to}` | Moves a value to another field. |
| `copy: {from, to}` | Copies a value. `from` can be a `meta.*` path. |
| `hash: {field}` | Replaces a value with its SHA-256, in hex. |
| `delete: {fields}` | Removes fields. |

Each entry in `ops` holds one op. The details are on [Ops](ops.md).

```yaml
# messages in
{{#include ../../../examples/stages/edit/set/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/edit/set/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/set/expected.yaml}}
```

`edit` has no conditions. To edit only some records, send them down their own branch with a [`route`](../route.md) and put the `edit` there.

## When an op cannot apply

`rename`, `copy` and `hash` read a field first. An op cannot apply when that field is missing or `null` (cause `absent`), or when the value does not fit (cause `type`): the target does not take it, or `hash` got a list or object. The record is left as it was by that op, and the op is counted on `edit_unapplied_total`. Then `on_unapplied` decides:

- `skip`: go on with the next op.
- `drop`: drop the record with the reason `edit_unapplied`. Later ops do not run. The drop counts as done for the ack.

With `skip`, the second record goes on without the rename:

```yaml
# messages in
{{#include ../../../examples/stages/edit/unapplied-skip/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/edit/unapplied-skip/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/unapplied-skip/expected.yaml}}
```

The same ops with `drop`. The second record is dropped:

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

`set` and `delete` always apply. `edit` never naks a record.

## Pages

- [Ops](ops.md): each op in detail.
- [Write rules](write-rules.md): which values each field takes.
- [Recipes](recipes.md): common edits.
- [Errors](errors.md): what the pipeline refuses.
