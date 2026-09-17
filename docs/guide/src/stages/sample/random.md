# sample: random

```yaml
- id: keep_tenth
  type: sample
  mode: random
  percent: 10
```

| Key | Required | What it does |
|---|---|---|
| `percent` | yes | The share to keep. A number above 0 and at most 100. Fractions such as `0.5` work. |

## How it decides

The answer comes from the record id (the `Fusion-Record-Id` header) and the node id. Nothing else goes in, so:

- A record gets the same answer every time it arrives. A redelivered message is kept or dropped again, the same as the first time.
- Two `random` nodes in a row keep different records. Two 10% nodes in a row keep about 1%.
- The share is close to `percent` over many records. Over a handful, it can be far off.

Here record 1 is kept, record 2 is dropped, and record 1 is kept again when it comes back:

```yaml
# messages in
{{#include ../../../examples/stages/sample/random-same-id/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/sample/random-same-id/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/sample/random-same-id/expected.yaml}}
```

A payload `id` field plays no part. Only the header counts.
