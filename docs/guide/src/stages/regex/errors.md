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

## A field that holds no text

`redact` is not refused for any field at start: no field has a type. A field that does not
hold text at run time is skipped, and a record no listed field matched counts once on
`regex_nonmatch_total`.

## Other messages

Each follows ``node `<id>`: ``.

| Problem | Message |
|---|---|
| a bad `field` or `fields` path | `` field `<path>`: `` and the reason, see [Field paths](../../field-paths.md#what-the-pipeline-refuses) |
| `fields: []` | `` `fields` needs at least one field path `` |
| a group name that cannot be an attribute key | `` group `<name>` cannot name an attribute: `` and the reason |
| a canary trip | `` pattern `<pattern>`: canary tripped: `` and the limit hit, with the shape and size of the test input |
| other pattern problems | see [Regex limits](../../regex-limits.md#what-the-pipeline-refuses) |
