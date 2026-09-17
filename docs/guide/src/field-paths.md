# Field paths

A field path names one field of a record. Every stage uses the same form: `filter` and `route` conditions, `dedupe` and `sample` keys, `extract` and `redact` fields, and `edit` ops.

## The form

A path starts with a field name:

| Path | Names |
|---|---|
| `id`, `kind`, `body`, `severity_text`, `severity_number`, `time_unix_nano`, `observed_time_unix_nano`, `trace_id`, `span_id` | that whole field |
| `attributes.<key>`, `resource.<key>`, `scope.<key>` | one key in that map |
| `meta.id`, `meta.tenant`, `meta.ingestion_time`, `meta.delivery_count` | the record's [Meta](concepts.md#meta), read-only |

Under `attributes`, `resource` and `scope`, everything after the first dot is the key, dots included. So `attributes.http.status` is the key `http.status`. It does not look inside a nested object:

```yaml
# messages in
{{#include ../examples/paths/flat-key/input.yaml}}
```

```yaml
# config
{{#include ../examples/paths/flat-key/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/paths/flat-key/expected.yaml}}
```

Record 3 has no `http.status` key, so the filter drops it. A path cannot reach inside a value.

## Names with other characters

Letters, digits, `_` and `-` can be written as they are: `resource.k8s.pod-name`, `attributes.5xx.count`. Put anything else, such as a space or a colon, in double quotes: `attributes."Event ID"`. Inside the quotes, write `\"` for a quote and `\\` for a backslash.

A condition with a quoted path needs no extra YAML quotes:

```yaml
# messages in
{{#include ../examples/paths/quoted/input.yaml}}
```

```yaml
# config
{{#include ../examples/paths/quoted/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/paths/quoted/expected.yaml}}
```

Brackets, as in `attributes["http.status"]`, are not allowed.

## Meta paths

`meta.*` reads the pipeline's own facts about the record. Here the filter keeps the record from the `acme` subject, whatever the payload says:

```yaml
# messages in
{{#include ../examples/paths/meta-tenant/input.yaml}}
```

```yaml
# config
{{#include ../examples/paths/meta-tenant/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/paths/meta-tenant/expected.yaml}}
```

Meta paths can be read anywhere a path is read. They can never be written or removed. To keep a Meta value in the record, copy it with [`edit`](stages/edit/README.md).

## What a write accepts

Stages that change a record, `edit`, `extract`, `redact` and `lua`, go through the same rules:

| Path | Accepts |
|---|---|
| `attributes.*`, `resource.*`, `scope.*`, `body` | any JSON value |
| `id`, `time_unix_nano`, `observed_time_unix_nano` | a whole number from 0 to 18446744073709551615 |
| `severity_number` | a whole number from -2147483648 to 2147483647 |
| `severity_text`, `trace_id`, `span_id` | text |
| `kind` | `log`, `metric` or `span` |
| `meta.*` | nothing |

Writing `null` to `id`, a time field, `severity_text`, `severity_number`, `trace_id` or `span_id` removes it. `kind` refuses `null`. Under a map key or in `body`, `null` is kept as a value. (A `lua` script is different: a top-level field it sets to `json.null` is removed.) Any field can be removed. A removed `kind` is written as `log`. A write that is refused leaves the record as it was.

## What the pipeline refuses

```yaml
# config
{{#include ../examples/paths/rejected-unknown-field/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/paths/rejected-unknown-field/expected.yaml}}
```

```yaml
# config
{{#include ../examples/paths/rejected-brackets/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/paths/rejected-brackets/expected.yaml}}
```

```yaml
# config
{{#include ../examples/paths/rejected-body-field/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/paths/rejected-body-field/expected.yaml}}
```

`body` is one value. Use [`extract`](stages/regex/extract.md) to pull parts of it into attributes first.

```yaml
# config
{{#include ../examples/paths/rejected-meta-write/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/paths/rejected-meta-write/expected.yaml}}
```

Every message, with the fix:

| Message | Fix |
|---|---|
| `` `<name>` is not a record field; instead use one of ... `` | Use one of the listed names. |
| `` `<path>` has an empty segment; instead use `<field>.<key>` with one dot between names `` | Remove the extra or trailing dot. |
| `` `<segment>` has `<char>`; instead use `<path>` `` | Do as the message shows: quote the segment, or inside quotes write `\\` for a backslash. |
| `` `<path>` has an unclosed quote; instead close it: `attributes."some key"` `` | Close the quote. |
| `` brackets are not allowed; instead use `<path>` `` | Use the dotted form the message shows. |
| `` `<path>` is not a meta field; instead use one of `meta.id`, `meta.tenant`, `meta.ingestion_time`, `meta.delivery_count` `` | Use one of those four. |
| `` `<field>` is one value and has no fields; instead use `<field>` `` | Use the field alone. For `body`, extract into attributes first. |
| `` `<map>` needs a key; instead use `<map>.<key>` `` | Add the key. |
| `` `<path>` is the pipeline's and cannot be written or removed; ... `` | Write to a record field. Use `copy {from: meta.<field>, to: <field>}` to keep a Meta value. |
| `` `<field>` takes <type>, not <type> `` | Give a value of the listed type. |

The node id and the setting come first, for example ```node `keep`: condition `...`: ``` or ```node `n`: key `...`: ```. In a condition, the message ends with the offset of the path.
