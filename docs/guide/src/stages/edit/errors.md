# edit errors

The pipeline refuses these configs at start. Ops are counted from 0, so `op 0` is the first.

## Writing to Meta

```yaml
# config
{{#include ../../../examples/stages/edit/rejected-meta-target/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/rejected-meta-target/expected.yaml}}
```

## A value the field does not take

No field has a type, so no value is refused at start. `set` still refuses an object or a list
as its `value`; write those with a [`lua`](../lua/README.md) script.

Hashing something with no text, such as a list, is not a start-up error either: the op is
unapplied on that record with cause `type`. See [write rules](write-rules.md#unapplied-on-a-record).

## `from` and `to` the same

```yaml
# config
{{#include ../../../examples/stages/edit/rejected-same-field/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/rejected-same-field/expected.yaml}}
```

## An unknown op

```yaml
# config
{{#include ../../../examples/stages/edit/rejected-unknown-op/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/rejected-unknown-op/expected.yaml}}
```

## Two ops in one entry

```yaml
# config
{{#include ../../../examples/stages/edit/rejected-two-ops/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/rejected-two-ops/expected.yaml}}
```

## A missing key

```yaml
# config
{{#include ../../../examples/stages/edit/rejected-no-to/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/rejected-no-to/expected.yaml}}
```

## A bad `on_unapplied`

```yaml
# config
{{#include ../../../examples/stages/edit/rejected-bad-policy/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/edit/rejected-bad-policy/expected.yaml}}
```

## Other messages

Each follows ``node `<id>`: ``.

| Problem | Message |
|---|---|
| no ops | `` `ops` needs at least one op `` |
| `set` with a list or object | `` op <n> (`set`): `value` must be a string, number, bool or null `` |
| `delete` with an empty list | `` op <n> (`delete`): `fields` needs at least one field path `` |
| a bad path | `` op <n> (`<op>`): `<key>`: `` and the reason, see [Field paths](../../field-paths.md#what-the-pipeline-refuses) |
| a key an op does not know | `` op <n> (`<op>`): unknown field `<key>`, expected ... `` |
| a node key `edit` does not know | `` unknown field `<key>`, expected `ops` or `on_unapplied` `` |
