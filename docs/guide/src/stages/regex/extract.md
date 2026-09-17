# extract

```yaml
- id: parse
  type: extract
  field: body
  pattern: '^(?<level>[A-Z]+): (?<message>.+)$'
```

| Key | Default | What it does |
|---|---|---|
| `field` | required | The [field path](../../field-paths.md) to read. It can be any field, a `meta.*` path included. |
| `pattern` | required | The regular expression. Named groups, `(?<name>...)`, say what to keep. |
| `limits` | see [Regex limits](../../regex-limits.md) | Limits for this pattern. |
| `on_redos_risk` | `reject` | What to do with a risky pattern. |

## What it writes

For each named group that matched, `extract` writes `attributes.<name>` as text. Numbers stay text too, so `pid` below is `"4711"`. The field it read is left as it was.

```yaml
# messages in
{{#include ../../../examples/stages/regex/extract-basic/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/regex/extract-basic/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/extract-basic/expected.yaml}}
```

- Record 2 has no `pid=` part. The group did not take part in the match, so nothing is written for it.
- Record 3 does not match. It goes on unchanged and counts on `regex_nonmatch_total`. It is not dropped.
- A group that matches empty text writes `""`.

Only the first match in the field is used. Groups without a name, such as `(...)` and `(?:...)`, write nothing. A pattern with no named groups loads, and writes nothing.

## Reading other fields

An existing attribute with the same name is replaced. A field that is not text, or is missing, counts as no match:

```yaml
# messages in
{{#include ../../../examples/stages/regex/extract-attribute/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/regex/extract-attribute/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/extract-attribute/expected.yaml}}
```

Record 3 has a number in `attributes.msg`, so it passes unchanged, even though its body would match.

## PCRE2 patterns

Lookaround and back references work, and run on PCRE2:

```yaml
# messages in
{{#include ../../../examples/stages/regex/extract-lookbehind/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/regex/extract-lookbehind/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/extract-lookbehind/expected.yaml}}
```
