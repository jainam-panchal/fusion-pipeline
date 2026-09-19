# redact

```yaml
- id: mask_ips
  type: redact
  fields: [body, attributes.peer]
  pattern: '\b\d{1,3}(?:\.\d{1,3}){3}\b'
  replace: '[ip]'
```

| Key | Default | What it does |
|---|---|---|
| `fields` | required | The [field paths](../../field-paths.md) to mask, at least one. Any path but `meta.*`, which is read-only. A field that does not hold text on a given record is skipped. |
| `pattern` | required | The regular expression. |
| `replace` | required | The text that replaces each match. |
| `limits` | see [Regex limits](../../regex-limits.md) | Limits for this pattern, applied to each field. |
| `on_redos_risk` | `reject` | What to do with a risky pattern. |

## What it does

Every match in every listed field is replaced. Fields that are not listed stay as they are. A listed field that is missing, or is not text, is skipped:

```yaml
# messages in
{{#include ../../../examples/stages/regex/redact-ips/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/regex/redact-ips/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/redact-ips/expected.yaml}}
```

`port` is a number, so it is skipped, and `host` is not listed. A record with no match in any field goes on unchanged and counts once on `regex_nonmatch_total`.

A pattern that can match empty text also matches at the start, at the end and between characters, and `replace` is inserted there. Make sure the pattern needs at least one character.

## replace is plain text

`$1`, `$name` and `\1` are written as they are. The two engines expand them differently, so the pipeline expands neither:

```yaml
# messages in
{{#include ../../../examples/stages/regex/redact-literal/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/regex/redact-literal/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/redact-literal/expected.yaml}}
```

To keep a label and mask only what follows it, match only the secret with a lookbehind. See [Recipes](recipes.md).

## All fields or none

`redact` checks every listed field before it changes any. If any field trips a limit, the whole record is dropped with the reason `regex_limit`, so a record is never half masked:

```yaml
# messages in
{{#include ../../../examples/stages/regex/redact-limit/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/regex/redact-limit/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/redact-limit/expected.yaml}}
```

Record 1's body is 13 bytes, over the limit of 12, so the record is dropped, even though `attributes.peer` was short enough. The drop still counts as done, so the message is acked.
