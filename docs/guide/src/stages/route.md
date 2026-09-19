# route

`route` sends each record down one named output, called a label.

```yaml
- id: by_format
  type: route
  routes:
    linux: resource."log.format" == "Linux"
    apache: resource."log.format" == "Apache"
  default: other
```

| Key | Default | What it does |
|---|---|---|
| `routes` | required | Labels and their [conditions](../conditions.md), at least one. They are checked in the order written. |
| `default` | required | The label for records that match no condition, or `drop`. |
| `limits` | see [Regex limits](../regex-limits.md) | Limits for `=~` and `!~` in the conditions. |
| `on_redos_risk` | `reject` | What to do with a risky pattern. See [Regex limits](../regex-limits.md). |

## Reading a label

A node reads a label with `from: <route id>.<label>`. Each record goes to the first label whose condition matches, and to no other.

```yaml
# messages in
{{#include ../../examples/stages/route/by-format/input.yaml}}
```

```yaml
# config
{{#include ../../examples/stages/route/by-format/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/route/by-format/expected.yaml}}
```

Record 3 matches neither condition, so it takes the `default` label `other`.

## First match wins

```yaml
# messages in
{{#include ../../examples/stages/route/first-match/input.yaml}}
```

```yaml
# config
{{#include ../../examples/stages/route/first-match/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/route/first-match/expected.yaml}}
```

Record 1 matches both conditions. `errors` comes first, so only `errors_out` gets it. Put the most specific conditions first.

## default: drop

`drop` is a reserved label. With `default: drop`, records that match nothing are dropped with the reason `route_default_drop`, which counts as done for the ack. `drop` needs no node to read it.

```yaml
# messages in
{{#include ../../examples/stages/route/default-drop/input.yaml}}
```

```yaml
# config
{{#include ../../examples/stages/route/default-drop/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/route/default-drop/expected.yaml}}
```

A rule cannot be called `drop`, and no node can read `<route id>.drop`.

## Every label needs a consumer

A node that reads a label is its consumer. Each label, the `default` label included, must have at least one consumer. Otherwise the pipeline does not start. This keeps records from going nowhere by mistake.

A label can have several consumers, and each gets its own copy. One node can read several labels by listing them in `from`:

```yaml
# messages in
{{#include ../../examples/stages/route/fan-out-in/input.yaml}}
```

```yaml
# config
{{#include ../../examples/stages/route/fan-out-in/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/route/fan-out-in/expected.yaml}}
```

The message for record 1 is acked once, after both `archive` and `search` have stored it. A consumer does not have to be a sink. Any stage can read a label.

## Patterns

With `=~` or `!~` in a condition, a record that goes over a limit is dropped with the reason `regex_limit` before any label is picked. See [Regex limits](../regex-limits.md).

## What the pipeline refuses

A label nothing reads:

```yaml
# config
{{#include ../../examples/stages/route/rejected-unconsumed/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/route/rejected-unconsumed/expected.yaml}}
```

A node that reads the route without a label. A node with no `from` reads the node above it, which is the route here:

```yaml
# config
{{#include ../../examples/stages/route/rejected-no-label/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/route/rejected-no-label/expected.yaml}}
```

Add `from: by_level.errors` to `out`.

A rule called `drop`:

```yaml
# config
{{#include ../../examples/stages/route/rejected-reserved/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/route/rejected-reserved/expected.yaml}}
```

No `default`:

```yaml
# config
{{#include ../../examples/stages/route/rejected-no-default/pipeline.yaml}}
```

```yaml
# result
{{#include ../../examples/stages/route/rejected-no-default/expected.yaml}}
```

The other messages:

| Problem | Message |
|---|---|
| `routes` is empty | `` node `<id>`: `routes` must name at least one label `` |
| a label or condition that is not text | `` node `<id>`: route labels must be strings `` or `` node `<id>`: route `<label>` must map to a condition string `` |
| `from` names a label the route does not have | `` node `<node>` reads label `<label>` from route `<route>`, which does not declare it `` |
| `from` names a label of a node that is not a route | `` node `<node>` reads label `<label>` from `<target>`, which is not a route `` |
| a bad condition | `` node `<id>`: route `<label>` `<condition>`:  `` and the reason, see [Conditions](../conditions.md#what-the-pipeline-refuses) |
