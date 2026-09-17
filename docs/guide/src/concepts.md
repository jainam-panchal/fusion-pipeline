# Concepts

The exact meaning of each term is in the glossary, [`CONTEXT.md`](https://github.com/jainam-panchal/fusion-pipeline/blob/main/CONTEXT.md). This page covers what a config author needs.

## Message and record

A **message** is what NATS delivers: a subject, headers and a payload. The payload is a JSON **record**, one log in OTLP shape:

```json
{
  "severity_text": "ERROR",
  "body": "disk full",
  "attributes": {"http.status": 500},
  "resource": {"service.name": "checkout"}
}
```

Stages see and change the record. NATS acks or naks the message.

## Meta

The pipeline also keeps its own facts about each record. These are called **Meta**:

| Meta | Where it comes from |
|---|---|
| record id | the `Fusion-Record-Id` header, a whole number |
| tenant | the subject `logs.<tenant>.<anything>`, else the `Fusion-Tenant` header, else `unknown` |
| ingestion time | the `Fusion-Ingestion-Time` header, else the time NATS stored the message |
| delivery count | NATS: 1 the first time, 2 on the first redelivery, and so on |

Meta never comes from the payload. A payload field called `id`, or `resource.tenant.id`, is your data, and the pipeline does not read it. The pipeline also never writes Meta into the record. It sends Meta along as headers on every message it writes.

Here the payload has `id: 99` and a tenant of its own. The pipeline uses id 7 from the header and tenant `acme` from the subject, and it writes the payload exactly as it arrived.

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

The pipeline processes logs only. A producer that sends anything else sets `Fusion-Record-Kind` to `metric` or `span`. No header means `log`. The pipeline drops other kinds before any stage runs and acks them. It does not look at their payload.

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

A `kind` field inside the payload does not count. It is part of your data.

## Ack, nak, drop

Each message ends in one of these:

- **ack**: the pipeline is done. Every sink that should have the record has stored it, or a stage dropped it.
- **drop**: a stage decided not to pass the record on, for example a `filter`. A drop still ends in an ack. Every drop has a reason, which shows up in the `records_dropped_total` metric.
- **nak**: something failed. NATS delivers the message again later.
- **dead letter**: when the last delivery fails too, the pipeline writes the message to `dlq.<tenant>` and tells NATS to stop. The compose stack allows 5 deliveries.

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

When a record goes down more than one path, the message is acked only after every path has finished. If any path fails, the message is nakked once, after all of them finish. This means a sink can get the same record again when the message comes back, so sinks should be fine with duplicates. [Writing a config](writing-a-config.md) shows how paths split.
