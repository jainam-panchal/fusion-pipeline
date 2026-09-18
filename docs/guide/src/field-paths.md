# Field paths

A field path names part of a record. Every stage uses the same form: `filter` and `route` conditions, `dedupe` and `sample` keys, `extract` and `redact` fields, and `edit` ops.

A record is whatever JSON the producer sent. There is no field list and no declared type, so a path can name anything in it.

## The form

A path is names joined with dots, read left to right:

| Path | Names |
|---|---|
| `.` | the whole record |
| `level` | a top-level key |
| `test2.key2` | a key inside an object |
| `resource."log.format"` | a key whose name contains a dot |
| `attributes.0.value.intValue` | a list position, then keys below it |
| `meta.id`, `meta.tenant`, `meta.ingestion_time`, `meta.delivery_count` | the record's [Meta](concepts.md#meta), read-only |

So this record:

```json
{"test": 12, "test2": {"key1": "ans1", "key2": 123}}
```

is addressed by `test`, `test2`, `test2.key1` and `test2.key2`.

```yaml
# messages in
{{#include ../examples/paths/nested/input.yaml}}
```

```yaml
# config
{{#include ../examples/paths/nested/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/paths/nested/expected.yaml}}
```

## Names with other characters

Letters, digits, `_` and `-` can be written as they are: `resource.env`, `attributes.retry-count`. Put anything else in double quotes: a space, a colon, or a dot that belongs to the name. Inside the quotes, write `\"` for a quote and `\\` for a backslash.

A dot inside quotes is part of the name; a dot outside them goes one level down. These are two different paths:

| Path | Names |
|---|---|
| `resource."log.format"` | the key `log.format` of `resource` |
| `resource.log.format` | the key `format`, inside the key `log`, of `resource` |

Producers that follow OpenTelemetry send keys like `log.format` and `http.status`, so those need the quotes.

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

## Lists

A whole number names a position in a list, counting from zero. An OTLP-shaped payload is addressed like this:

```yaml
# messages in
{{#include ../examples/paths/list-position/input.yaml}}
```

```yaml
# config
{{#include ../examples/paths/list-position/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/paths/list-position/expected.yaml}}
```

Picking a list item by a key it holds, as in `attributes[key=="db.port"]`, is not supported.

## The whole record, and paths that start with a dot

`.` names the record itself. That is what a raw line from a [`codec: text`](nats.md) source is: one string, with no keys.

A path may also start with a dot. It then names the record and nothing else, which is how you write a first name that is not a plain word:

| Path | Names |
|---|---|
| `."log.format"` | the top-level key `log.format` |
| `.0` | the first position, when the record is a list |
| `.meta` | the payload's own key `meta` |

Without the leading dot, `meta` is the pipeline's, not the payload's. Everywhere else the dot changes nothing: `.level` and `level` are the same path.

In a condition, a path that does not start with a letter or `_` must use the leading dot, so write `."log.format" == "Linux"` and `.0 == 5`.

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

## Reading and writing

**A path that matches nothing reads as null.** It is not an error. `level == null` is true for a record with no `level`, and so is `a.b.c.d == null`.

**A write makes its path exist.** A missing name is created:

```yaml
- set: {field: attributes.stage, value: parsed}   # attributes is created if it was not there
```

A value already in the way is replaced. Writing `body.parsed` when `body` is the string `"a line"` leaves `body` as `{"parsed": ...}`, and the line is gone. Writing `attributes.0.x` when `attributes` is an empty list leaves `attributes` as `{"0": {"x": ...}}`. A write into a position the list already has keeps the list.

**Any value fits any path.** No path has a type, so `set {field: severity_number, value: high}` writes the text `high`.

**Anything can be removed.** Removing a list position closes the gap. Removing a path that is not there does nothing. Removing `.` leaves a record of `null`.

Only `meta.*` refuses a write or a removal.

## A path that matches nothing is not an error

This is the cost of taking any JSON. A misspelled path, or one that misses the quotes a dotted name needs, loads and runs and quietly matches nothing. Nothing is counted and nothing is logged.

What that looks like in each stage:

| Stage | A path that matches nothing |
|---|---|
| `filter` | The condition is false, so `action: keep` drops every record and `action: drop` keeps every record. |
| `route` | No label matches, so every record takes the default. |
| `dedupe` | Every record hashes to the same key, so everything after the first in the window is dropped as a duplicate. |
| `sample` in `consistent` mode | Every record hashes to the same key, so all of them are kept or none are. |
| `extract`, `redact` | The field is not a string, so the record passes unchanged and counts on `regex_nonmatch_total`. |
| `edit` `rename`, `copy`, `hash` | The op is unapplied with cause `absent`, counted on `edit_unapplied_total`. |

A misspelled filter, keeping nothing:

```yaml
# messages in
{{#include ../examples/paths/matches-nothing/input.yaml}}
```

```yaml
# config
{{#include ../examples/paths/matches-nothing/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/paths/matches-nothing/expected.yaml}}
```

If a stage is doing one of those things to everything, check the path against a real message first. [Troubleshooting](troubleshooting.md) has the same list from the symptom's side.

## What the pipeline refuses

Only the shape of the path itself, at start-up.

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
{{#include ../examples/paths/rejected-meta-write/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/paths/rejected-meta-write/expected.yaml}}
```

Every message, with the fix:

| Message | Fix |
|---|---|
| `` `<path>` has an empty segment; instead use `<name>.<name>` with one dot between names `` | Remove the extra or trailing dot. |
| `` `<segment>` has `<char>`; instead use `<path>` `` | Do as the message shows: quote the name, or inside quotes write `\\` for a backslash. |
| `` `<path>` has an unclosed quote; instead close it: `attributes."some key"` `` | Close the quote. |
| `` brackets are not allowed; instead use `<path>` `` | Use the dotted form the message shows. |
| `` `<path>` is not a meta field; instead use one of `meta.id`, `meta.tenant`, `meta.ingestion_time`, `meta.delivery_count` `` | Use one of those four, or write `.meta...` for a payload key of that name. |
| `` `<path>` is the pipeline's and cannot be written or removed; ... `` | Write to a record field. Use `copy {from: meta.<field>, to: <field>}` to keep a Meta value. |

The node id and the setting come first, for example ```node `keep`: condition `...`: ``` or ```node `n`: key `...`: ```. In a condition, the message ends with the offset of the path.
