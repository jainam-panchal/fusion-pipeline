# edit ops

Ops run from top to bottom, and each one sees what the ones before it did.

```yaml
# messages in
{{#include ../../../examples/stages/edit/order/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/edit/order/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/order/expected.yaml}}
```

The rename moves the value of `a` to `b`, then `set` replaces it.

## set

`set: {field, value}` writes `value` to `field`, creating or replacing it. `value` is text, a number, `true`, `false` or `null`. Lists and objects are not allowed. The pipeline checks at start that the field takes the value, so `set` always applies.

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

## rename

`rename: {from, to}` writes the value of `from` to `to`, replacing what `to` held, then removes `from`.

```yaml
# messages in
{{#include ../../../examples/stages/edit/rename/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/edit/rename/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/rename/expected.yaml}}
```

If `to` does not take the value, nothing changes, and `from` stays. `from` and `to` must be different fields, and neither can be a `meta.*` path.

## copy

`copy: {from, to}` writes the value of `from` to `to`, replacing what `to` held. `from` stays.

```yaml
# messages in
{{#include ../../../examples/stages/edit/copy/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/edit/copy/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/copy/expected.yaml}}
```

`from` can be a `meta.*` path. It is the only op that can put a Meta value into a record. A [`lua`](../lua/README.md) script can do it too.

```yaml
# messages in
{{#include ../../../examples/stages/edit/copy-meta/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/edit/copy-meta/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/copy-meta/expected.yaml}}
```

`meta.tenant` is text. `meta.id`, `meta.ingestion_time` and `meta.delivery_count` are numbers.

The pipeline does not check at start that `to` takes the value `from` will have. A `copy` from `meta.tenant` to `severity_number` loads, and then is unapplied with cause `type` on every record.

## hash

`hash: {field}` replaces the value of `field` with its SHA-256 digest, as 64 lowercase hex characters.

```yaml
# messages in
{{#include ../../../examples/stages/edit/hash/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/edit/hash/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/hash/expected.yaml}}
```

- Text is hashed as its bytes. A number or `true`/`false` is hashed as its JSON text, so `42` and `"42"` give the same digest.
- A list or object cannot be hashed (cause `type`). A missing field is cause `absent`.
- The field must take text, so `id`, `kind`, `severity_number` and the time fields cannot be hashed.

The digest has no salt. It works as a join key: the same input always gives the same digest. It does not hide values that are easy to guess, such as short numbers or known email addresses.

## delete

`delete: {fields}` removes each listed field. A field that is not there is fine, so `delete` always applies.

```yaml
# messages in
{{#include ../../../examples/stages/edit/delete/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/edit/delete/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/delete/expected.yaml}}
```

## Fields you can change

Every record field is yours: `id`, `kind` and `resource.tenant.id` too. Changing them changes what the sink writes, and nothing else. The pipeline keeps using the record's Meta for the tenant, the record id and its decisions.

```yaml
# messages in
{{#include ../../../examples/stages/edit/kind-and-id/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/edit/kind-and-id/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/kind-and-id/expected.yaml}}
```

The sink still writes `Fusion-Record-Id: "1"`. The pipeline decided the record is a log when the message arrived, from its `Fusion-Record-Kind` header, and it keeps processing it. Later stages that read `kind` see `span`. A removed `kind` reads as `log`.
