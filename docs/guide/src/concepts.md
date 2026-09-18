# Concepts

The exact meaning of each term is in the glossary, [`CONTEXT.md`](https://github.com/jainam-panchal/fusion-pipeline/blob/main/CONTEXT.md). This page covers what a config author needs.

## Message and record

A **message** is what NATS delivers: a subject, headers and a payload. The payload is the **record**. Stages see and change the record. NATS acks or naks the message.

**A record is whatever JSON the producer sent.** There is no field list and no declared type. An object, a list, a piece of text and a number are all records. The pipeline keeps what it was given, changes only what the config tells it to change, and the sink writes what the last stage left.

So a vendor that emits one JSON object per line goes through whole:

```yaml
# messages in
{{#include ../examples/concepts/free-form/input.yaml}}
```

```yaml
# config
{{#include ../examples/concepts/free-form/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/concepts/free-form/expected.yaml}}
```

Both records come out as they went in. The second one holds an `id` that is not a number and a `severity_number` that is text; earlier versions refused those, and nothing refuses them now.

Names like `body`, `severity_text` and `attributes` come from OpenTelemetry and are a good default if you are choosing. They are ordinary keys: nothing in the pipeline treats them specially, and nothing adds them. A payload with no `kind` comes out with no `kind`.

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

The top-level `host` is kept beside the one under `attributes`: nothing is dropped for being unexpected.

Two things the pipeline still refuses to read, both from the [source](nats.md):

- Under `codec: json`, a payload that is not JSON at all.
- Under `codec: text`, bytes that are not UTF-8.

Either one is nakked, and after the last delivery it becomes a dead letter.

Object keys come back sorted. Every key and value survives; the order they were written in does not.

To name part of a record, see [field paths](field-paths.md).

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
