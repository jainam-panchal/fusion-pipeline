# Writing a config

A config is one YAML file. The pipeline reads it once at start. If anything in it is wrong, the pipeline prints the problem and exits before it reads a single message.

## Top level

```yaml
name: checkout-logs     # optional
workers: 4              # optional
source:                 # required
  type: nats
  stream: LOGS
  consumer: pipeline
nodes:                  # required
  - ...
```

| Key | Default | What it does |
|---|---|---|
| `name` | `pipeline` | Names the pipeline. Stages that remember things (such as `dedupe`) store them under this name, so two different pipelines that share a Dragonfly need different names. Copies of the same pipeline should keep the same name. No `:` allowed. |
| `workers` | one per CPU core | How many records the pipeline works on at the same time. |
| `source` | none | Where messages come from. The only type is `nats`. See [NATS](nats.md). |
| `nodes` | none | The stages and sinks, in a list. |

Any other key is an error.

## Nodes

Every node has an `id` and a `type`, and may have `from`. All its other keys belong to its type.

```yaml
- id: keep_errors            # your name for this node
  type: filter               # what it is
  from: source               # optional: where its records come from
  condition: severity_text == "ERROR"
  action: keep
```

- `id` must be unique. It cannot be `source`, and it cannot contain `.` or `:`.
- `type` is a stage, such as `filter`, `route`, `dedupe`, `edit`, `sample`, `extract`, `redact` or `lua`, or the sink `sink.nats`. Each stage has its own page under Stages. `sink.nats` is on the [NATS](nats.md) page.
- A sink writes records out and passes nothing on. Every config needs at least one.

## How records flow

Without `from`, a node reads from the node above it. The first node reads from `source`. So a plain list is a chain:

```yaml
# config
{{#include ../examples/config/chain/pipeline.yaml}}
```

```yaml
# messages in
{{#include ../examples/config/chain/input.yaml}}
```

```yaml
# result
{{#include ../examples/config/chain/expected.yaml}}
```

Record 1 stops at `drop_debug`, record 2 at `keep_checkout`, and record 3 reaches `out`.

### Fan-out

When two nodes read from the same place, each gets its own copy of the record. Here `all` and `errors` both read from `source`:

```yaml
# config
{{#include ../examples/config/fan-out/pipeline.yaml}}
```

```yaml
# messages in
{{#include ../examples/config/fan-out/input.yaml}}
```

```yaml
# result
{{#include ../examples/config/fan-out/expected.yaml}}
```

`errors_out` has no `from`, so it reads from `errors`, the node above it.

Changes a stage makes on one path do not show up on another path.

### Fan-in

A list in `from` makes a node read from several places. The node gets a record once for each path that reaches it:

```yaml
# config
{{#include ../examples/config/fan-in/pipeline.yaml}}
```

```yaml
# messages in
{{#include ../examples/config/fan-in/input.yaml}}
```

```yaml
# result
{{#include ../examples/config/fan-in/expected.yaml}}
```

Record 3 is an error and slow, so it passes both filters and `out` writes it twice.

### A path that ends at a stage

If nothing reads from a stage, records that pass it go nowhere. The message is still acked. Here `errors` has no node after it:

```yaml
# config
{{#include ../examples/config/dead-end/pipeline.yaml}}
```

```yaml
# messages in
{{#include ../examples/config/dead-end/input.yaml}}
```

```yaml
# result
{{#include ../examples/config/dead-end/expected.yaml}}
```

The pipeline does not warn about this, so make sure each path you care about ends in a sink.

### Routes

A [`route`](stages/route.md) sends each record down one of several named outputs. Nodes read an output with `from: <route id>.<label>`, for example `from: by_level.errors`. The route page has the rules.

## What the pipeline refuses

A config with no sink:

```yaml
{{#include ../examples/config/rejected-no-sink/pipeline.yaml}}
```

```yaml
{{#include ../examples/config/rejected-no-sink/expected.yaml}}
```

A `from` with a typo:

```yaml
{{#include ../examples/config/rejected-unknown-from/pipeline.yaml}}
```

```yaml
{{#include ../examples/config/rejected-unknown-from/expected.yaml}}
```

A node below a sink. The sink passes nothing on, so `drop_debug` never gets a record:

```yaml
{{#include ../examples/config/rejected-after-sink/pipeline.yaml}}
```

```yaml
{{#include ../examples/config/rejected-after-sink/expected.yaml}}
```

The pipeline prints the message after `pipelined: `. The full list:

| Problem | Message |
|---|---|
| No sink | `pipeline has no sink node` |
| `from` names a node that does not exist | ``node `out` reads from `x`, which does not exist`` |
| A node no record can reach | ``node `x` is unreachable from `source` `` |
| Nodes that read from each other in a loop | ``node `x` is part of a cycle`` |
| Two nodes with one id | ``node id `x` is declared more than once`` |
| A node called `source` | ``node id `source` is reserved`` |
| An id with `.` or `:` | ``node id `x.y` contains a dot; ...`` or ``node id `x:y` contains a colon; ...`` |
| A `name` with `:` | ``pipeline name `x:y` contains a colon; ...`` |
| A `type` that does not exist | ``node `x` has unknown type `y` `` |
| A wrong key or value for the type | ``node `x`: `` followed by what is wrong |
| Not valid YAML, or an unknown top-level key | `config is not valid YAML: ...` |
| No `source` block | ``config has no `source` block; ...`` |

Route outputs have rules of their own. They are on the [route](stages/route.md) page.
