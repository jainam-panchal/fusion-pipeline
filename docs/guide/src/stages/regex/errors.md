# extract and redact errors

The pipeline refuses these configs at start. The message follows `pipelined: `.

## A pattern that does not parse

```yaml
# config
{{#include ../../../examples/stages/regex/rejected-open-group/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/rejected-open-group/expected.yaml}}
```

Close the group: `user=(?<user>\w+)`. A pattern neither engine can read is reported by PCRE2, so the message says `backtracking engine` even when the pattern uses no PCRE2-only syntax.

## A risky pattern

```yaml
# config
{{#include ../../../examples/stages/regex/rejected-redos/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/rejected-redos/expected.yaml}}
```

A repeat inside a repeat can take very long on some inputs. Rewrite it with one repeat, for example `^(?<words>[\w\s]*)$`, or set `on_redos_risk: warn` if you have checked the pattern. See [Regex limits](../../regex-limits.md#checks-at-start).

## Masking a field that does not take text

```yaml
# config
{{#include ../../../examples/stages/regex/rejected-redact-id/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/rejected-redact-id/expected.yaml}}
```

`redact` writes text, so every listed field must take text. To hide an id, copy it to an attribute first with [`edit`](../edit/README.md).

## A missing key

```yaml
# config
{{#include ../../../examples/stages/regex/rejected-no-replace/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/rejected-no-replace/expected.yaml}}
```

`replace` has no default. Give the text to write, which can be empty: `replace: ''`.

## A key the stage does not have

```yaml
# config
{{#include ../../../examples/stages/regex/rejected-into/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/rejected-into/expected.yaml}}
```

`extract` always writes to `attributes.<group name>`. To move a value elsewhere, follow it with an [`edit`](../edit/README.md).

## Other messages

Each follows ``node `<id>`: ``.

| Problem | Message |
|---|---|
| a bad `field` or `fields` path | `` field `<path>`: `` and the reason, see [Field paths](../../field-paths.md#what-the-pipeline-refuses) |
| `fields: []` | `` `fields` needs at least one field path `` |
| a group name that cannot be an attribute key | `` group `<name>` cannot name an attribute: `` and the reason |
| a canary trip | `` pattern `<pattern>`: canary tripped: `` and the limit hit, with the shape and size of the test input |
| other pattern problems | see [Regex limits](../../regex-limits.md#what-the-pipeline-refuses) |
