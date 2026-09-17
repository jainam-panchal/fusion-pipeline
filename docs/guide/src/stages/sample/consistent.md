# sample: consistent

```yaml
- id: half_the_hosts
  type: sample
  mode: consistent
  percent: 50
  key: [resource.host]
```

| Key | Required | What it does |
|---|---|---|
| `percent` | yes | The share of key values to keep. Above 0 and at most 100. |
| `key` | yes | A list of [field paths](../../field-paths.md), at least one. Records with the same values are kept or dropped together. `meta.*` paths work too. |

## How it decides

The answer comes from the values of the `key` fields and nothing else. So every record from `web-2` gets the same answer, on every worker, in every copy of the pipeline, and on every delivery. It uses no Dragonfly.

```yaml
# messages in
{{#include ../../../examples/stages/sample/consistent/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/sample/consistent/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/sample/consistent/expected.yaml}}
```

`web-2` is kept, so both of its records are kept. `web-1` is dropped.

Two more things follow from this:

- A value kept at a lower `percent` is also kept at any higher one. Hosts kept at 20% are all kept at 50%.
- The node id does not change the answer. Two `consistent` nodes with the same `key` and `percent` make the same choice.

## Missing fields

A missing field counts as `null`. All records without it share one value, so they are all kept or all dropped:

```yaml
# messages in
{{#include ../../../examples/stages/sample/consistent-missing-key/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/sample/consistent-missing-key/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/sample/consistent-missing-key/expected.yaml}}
```

For a key with one field, the missing value is kept when `percent` is about 32 or more, and dropped below that. If many records lack the field, that one choice moves a lot of records.
