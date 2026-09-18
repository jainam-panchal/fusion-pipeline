# Concepts

The exact meaning of each term is in the glossary, [`CONTEXT.md`](https://github.com/jainam-panchal/fusion-pipeline/blob/main/CONTEXT.md). This page covers what a config author needs.

## Message and record

A **message** is what NATS delivers: a subject, headers and a payload. The payload is a JSON **record**, one log in OTLP shape. Stages see and change the record. NATS acks or naks the message.

A record has these top-level fields, all optional:

| Field | Holds |
|---|---|
| `id` | a whole number |
| `kind` | `log`, `metric` or `span` |
| `time_unix_nano`, `observed_time_unix_nano` | whole numbers, nanoseconds |
| `severity_text` | text, such as `ERROR` |
| `severity_number` | a whole number |
| `body` | any JSON value, usually text |
| `attributes`, `resource`, `scope` | objects with any keys and values |
| `trace_id`, `span_id` | text |

Put your own fields under `attributes` or `resource`. Two things to know:

- Any other top-level key is dropped when the message is read. It is not in the record, and the sink does not write it.
- The sink always writes `kind`. When the payload had none, it writes `kind: log`, which means the same thing.

```yaml
# messages in
{{#include ../examples/concepts/record-fields/input.yaml}}
```

```yaml
# config
{{#include ../examples/concepts/record-fields/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/concepts/record-fields/expected.yaml}}
```

A payload that is not a JSON object, or a field that holds the wrong type, cannot be read at all. The message is nakked, and after the last delivery it becomes a dead letter:

```yaml
# messages in
{{#include ../examples/concepts/bad-field/input.yaml}}
```

```yaml
# config
{{#include ../examples/concepts/bad-field/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/concepts/bad-field/expected.yaml}}
```

## Meta

The pipeline also keeps its own facts about each record. These are called **Meta**:

| Meta | Where it comes from |
|---|---|
| record id | the `Fusion-Record-Id` header, a whole number |
| tenant | the subject `logs.<tenant>.<anything>`, else the `Fusion-Tenant` header, else `unknown` |
| ingestion time | the `Fusion-Ingestion-Time` header, else the time NATS stored the message |
| delivery count | NATS: 1 the first time, 2 on the first redelivery, and so on |

Meta never comes from the payload. The pipeline does not use a payload `id` or `resource."tenant.id"` for anything. It also never puts Meta into the record. It sends Meta along as headers on every message it writes.

Here the payload has `id: 99` and a tenant of its own. The pipeline uses id 7 from the header and tenant `acme` from the subject, and leaves the payload's values alone.

```yaml
# messages in
{{#include ../examples/concepts/meta-not-payload/input.yaml}}
```

```yaml
# config
{{#include ../examples/concepts/meta-not-payload/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/concepts/meta-not-payload/expected.yaml}}
```

Stages can read Meta through `meta.id`, `meta.tenant`, `meta.ingestion_time` and `meta.delivery_count`. See [Field paths](field-paths.md). To put the tenant into the record, copy it with an [`edit`](stages/edit/README.md) stage.

### Tenant

The subject wins over the header. The header only counts when the subject does not name a tenant, for example when a pipeline reads another pipeline's output from `processed.logs`.

```yaml
# messages in
{{#include ../examples/concepts/tenant/input.yaml}}
```

```yaml
# config
{{#include ../examples/concepts/tenant/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/concepts/tenant/expected.yaml}}
```

The first word of the subject is `logs` unless the source sets `tenant_prefix`. The subject needs at least one word after the tenant: `logs.acme.app` names `acme`, `logs.acme` names nobody.

## Only logs

The pipeline processes logs only. A producer that sends anything else sets `Fusion-Record-Kind` to `metric` or `span`. No header means `log`. The pipeline acks and drops every message whose header is not `log`, before any stage runs. That includes a value it cannot read and a header given twice. It does not look at the payload of those messages.

```yaml
# messages in
{{#include ../examples/concepts/not-a-log/input.yaml}}
```

```yaml
# config
{{#include ../examples/concepts/not-a-log/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/concepts/not-a-log/expected.yaml}}
```

A `kind` field inside the payload does not decide this. It is part of your data.

## Ack, nak, drop

A message is acked when the pipeline is done with it: every sink that should have the record has stored it. A drop also counts as done. A stage such as `filter` drops a record on purpose, and the message is acked.

A message is nakked when something failed, and NATS delivers it again later. When the last delivery fails too, the pipeline writes the message to the dead-letter subject `dlq.<tenant>` and tells NATS to stop delivering it. The compose stack allows 5 deliveries. [NATS](nats.md) covers dead letters in full.

A message without a record id is a failure, so it is nakked:

```yaml
# messages in
{{#include ../examples/concepts/missing-id/input.yaml}}
```

```yaml
# config
{{#include ../examples/concepts/missing-id/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/concepts/missing-id/expected.yaml}}
```

## Branches

When a record goes down more than one branch, the message is acked only after every branch has finished. If any branch fails, the message is nakked once, after all of them finish. A sink on a branch that worked then gets the record again on the next delivery, so whatever reads the sink's stream should cope with duplicates. [Writing a config](writing-a-config.md) shows how a record splits into branches.
