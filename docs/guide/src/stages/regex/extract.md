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

For each named group that matched, `extract` writes `attributes.<name>` as text. Numbers stay text too, so `pid` below is `"4711"`. The field it read is left as it was, unless it is the attribute a group writes to.

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
- Record 3 does not match. It goes on unchanged and counts as a non-match on `regex_nonmatch_total`.

Only the first match in the field is used. Groups without a name, such as `(...)` and `(?:...)`, write nothing.

A group that takes part but matches no text writes `""`:

```yaml
# messages in
{{#include ../../../examples/stages/regex/extract-empty-group/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/regex/extract-empty-group/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/extract-empty-group/expected.yaml}}
```

A pattern with no named groups loads and writes nothing:

```yaml
# messages in
{{#include ../../../examples/stages/regex/extract-no-groups/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/regex/extract-no-groups/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/extract-no-groups/expected.yaml}}
```

## Reading other fields

An existing attribute with the same name as a group is replaced:

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

A field that is not text, or is missing, is a non-match, even when another field would match:

```yaml
# messages in
{{#include ../../../examples/stages/regex/extract-not-text/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/regex/extract-not-text/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/extract-not-text/expected.yaml}}
```

## PCRE2 patterns

Lookaround and back references work, and run on PCRE2. Here `\1` finds a word written twice:

```yaml
# messages in
{{#include ../../../examples/stages/regex/extract-backref/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/regex/extract-backref/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/extract-backref/expected.yaml}}
```
