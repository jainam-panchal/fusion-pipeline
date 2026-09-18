# NATS

The pipeline reads messages from a NATS JetStream consumer and writes records to JetStream streams.

## The source

```yaml
source:
  type: nats
  url: nats://127.0.0.1:4222
  stream: LOGS
  consumer: pipeline
  tenant_prefix: logs
  dlq_prefix: dlq
```

| Key | Default | What it does |
|---|---|---|
| `type` | required | `nats`, the only source type. |
| `url` | `nats://127.0.0.1:4222` | The server. `NATS_URL` replaces it when set and not empty. |
| `stream` | required | The stream to read from. It must exist. |
| `consumer` | required | The consumer on that stream. It must exist. |
| `tenant_prefix` | `logs` | The first word of subjects that name a tenant: `<tenant_prefix>.<tenant>.<anything>`. |
| `dlq_prefix` | `dlq` | Dead letters go to `<dlq_prefix>.<tenant>`. |
| `codec` | `json` | How a payload becomes a record. `json` reads the payload as JSON, whatever shape it is. `text` makes the payload's bytes the record, one piece of text, so a producer can send raw log lines with no JSON around them. |

Any other key is an error. The pipeline reads every message the consumer delivers, whatever its subject.

## The sink

```yaml
- id: out
  type: sink.nats
  url: nats://127.0.0.1:4222
  stream: PROCESSED
  subject: processed.logs
```

| Key | Default | What it does |
|---|---|---|
| `url` | `nats://127.0.0.1:4222` | The server. `NATS_URL` replaces it when set and not empty. |
| `stream` | required | The stream that stores `subject`. It must exist, and its subjects must include `subject`. |
| `subject` | required | Every record is published to this subject. |
| `encoding` | `json` | How a record becomes a payload. `json` writes the record's JSON. `text` writes a record that is text as its bytes, and any other record as its JSON, so a stage that turned a line into an object still gets written rather than failing. |

## Raw lines

With `codec: text` the record is the line itself, with no JSON around it, so a producer that
only has log lines needs no wrapper. The record is then one piece of text and `.` is the whole
of it, so copy it into a field before writing anything beside it:

```yaml
# messages in
{{#include ../examples/nats/text-codec/input.yaml}}
```

```yaml
# config
{{#include ../examples/nats/text-codec/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/nats/text-codec/expected.yaml}}
```

`raw:` in an example is the bytes on the wire, where `payload:` is JSON.

With `encoding: text` on the sink as well, a line goes out as a line:

```yaml
# messages in
{{#include ../examples/nats/text-roundtrip/input.yaml}}
```

```yaml
# config
{{#include ../examples/nats/text-roundtrip/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/nats/text-roundtrip/expected.yaml}}
```

A record that is no longer text, because a stage made it an object, is written as JSON rather
than failing.

The subject is the same for every record. To split records across subjects, use a [`route`](stages/route.md) with one sink per label.

A sink counts a record as written once the stream confirms it has stored it. The sink waits up to 5 seconds for that. If it does not come, the branch fails, and the source message is nakked and delivered again, or becomes a dead letter on its last delivery. Records that other branches already wrote are written again then, so readers of these streams should expect duplicates.

The source and the sinks share one connection per server URL.

The sink writes the record as the last stage left it, and adds nothing. A payload that had no `kind` comes out with no `kind`.

```yaml
# config
{{#include ../examples/nats/rejected-sink-no-subject/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/nats/rejected-sink-no-subject/expected.yaml}}
```

```yaml
# config
{{#include ../examples/nats/rejected-sink-unknown-key/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/nats/rejected-sink-unknown-key/expected.yaml}}
```

## What the server must have

The pipeline never creates streams or consumers. Before it starts, create:

- The input stream (`source.stream`), and on it a durable **pull** consumer (`source.consumer`) with:
  - ack policy `explicit`
  - a `max_deliver` above 0
  - no `backoff`
- A dead-letter stream whose subjects include `<dlq_prefix>.>` or `<dlq_prefix>.*`. Give it a duplicate window, so a message dead-lettered twice is stored once.
- A stream for each sink's `subject`.

The pipeline sets its own wait before each redelivery, which is why the consumer must not set `backoff`. `ack_wait` is not checked; the compose stack uses 30 seconds. The consumer's settings are read once, at start.

The compose stack runs these commands:

```sh
nats stream add LOGS --subjects 'logs.>' --storage file --retention limits --defaults
nats stream add PROCESSED --subjects 'processed.>' --storage file --retention limits --defaults
nats stream add DLQ --subjects 'dlq.>' --storage file --retention limits \
  --discard old --dupe-window 2m --max-age 7d --max-msgs-per-subject 100000 --defaults
nats consumer add LOGS pipeline --pull --ack explicit --wait 30s \
  --max-deliver 5 --deliver all --replay instant --defaults
```

Copies of the same config can share a consumer, and each gets some of the messages. Two different configs must not share one, or each would process only part of the stream.

## Start-up checks

After it has read the config and the Dragonfly and telemetry settings, the pipeline checks the sinks, then the source, and stops at the first problem. It prints ``pipelined: node `<id>`: `` and one of these (the source's id is `source`):

| Problem | Message |
|---|---|
| server not reachable | `could not connect to NATS at <url>: <reason>` |
| stream missing | `` stream `<stream>` does not exist at <url>; create it before starting the pipeline `` |
| sink stream does not store the subject | `` stream `<stream>` at <url> does not capture subject `<subject>` (its subjects are [...]) `` |
| consumer missing | `` consumer `<consumer>` does not exist on stream `<stream>` at <url>; create it (pull, explicit ack) before starting the pipeline `` |
| ack policy not `explicit` | `` consumer `<consumer>` on stream `<stream>` at <url> has ack policy `<policy>`; the pipeline needs `explicit` `` |
| no `max_deliver` | `` consumer `<consumer>` on stream `<stream>` at <url> has no `max_deliver`; the pipeline needs a positive limit to dead-letter a message `` |
| `backoff` set | `` consumer `<consumer>` on stream `<stream>` at <url> sets `backoff`; the pipeline sets each redelivery delay on its nak, so the consumer must not `` |
| no dead-letter stream | `` no stream at <url> captures the dead-letter subjects `<prefix>.<tenant>`; create one (e.g. subjects `<prefix>.>`) before starting the pipeline `` |
| dead-letter stream covers some tenants only | `` stream `<stream>` at <url> captures some `<prefix>.<tenant>` subjects but not every tenant's (its subjects are [...]); use `<prefix>.>` or `<prefix>.*` `` |
| a push consumer, or another JetStream error | `JetStream request failed at <url>: <reason>` |

## Subjects and tenants

The tenant comes from the subject when it has the form `<tenant_prefix>.<tenant>.<at least one more word>`. Otherwise it comes from the `Fusion-Tenant` header, and if that is missing too, the tenant is `unknown`. The payload is never read for it.

```yaml
# messages in
{{#include ../examples/nats/tenant-prefix/input.yaml}}
```

```yaml
# config
{{#include ../examples/nats/tenant-prefix/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/nats/tenant-prefix/expected.yaml}}
```

- Message 2 is on `logs.`, which is not this source's prefix, and has no header.
- Message 3 has only two words, so the subject names nobody and the header counts.

The subject wins over the header because NATS permissions control who can publish on a subject, while any producer can set a header.

## Headers

### What the pipeline reads

| Header | Format | If it is missing |
|---|---|---|
| `Fusion-Record-Id` | a whole number, digits only | the message is nakked, and becomes a dead letter after the last delivery |
| `Fusion-Record-Kind` | `log`, `metric` or `span` | the message is a log |
| `Fusion-Tenant` | any text without control characters, not empty | the tenant is `unknown`, unless the subject names one |
| `Fusion-Ingestion-Time` | nanoseconds since 1970, digits only | the time NATS stored the message is used |
| `Fusion-Ingestion-Time-Kind` | `reported` or `clock` | `Fusion-Ingestion-Time` is ignored too, and the stored time is used |

Names must match exactly, including case.

A header that does not fit its format, or that appears twice, is ignored as if it were missing. The pipeline writes a line to standard error and counts it on `source_invalid_headers_total`. The two time headers only count together: one without the other is ignored. A bad header never makes a nak by itself, with two effects to know:

- A bad `Fusion-Record-Id` leaves the message without a record id, and a message without one is nakked.
- A bad or repeated `Fusion-Record-Kind` means the message is not a known log, so it is dropped and acked. So is a message whose kind is `metric` or `span`.

```yaml
# messages in
{{#include ../examples/nats/invalid-headers/input.yaml}}
```

```yaml
# config
{{#include ../examples/nats/invalid-headers/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/nats/invalid-headers/expected.yaml}}
```

- Message 1: the bad time is ignored, and the time NATS stored the message is used.
- Message 2: the tenant is given twice, so it is ignored.
- Message 3: `+3` is not a record id, so the message has none and is nakked.
- Message 4: `LOG` is not a kind, so the message is dropped.

The delivery count, and the time NATS stored the message, come from JetStream itself.

### What the pipeline writes

Every record the sink writes carries exactly four headers: `Fusion-Record-Id`, `Fusion-Tenant`, `Fusion-Ingestion-Time` and `Fusion-Ingestion-Time-Kind`. Their order is not fixed. The payload is the record as the last stage left it.

A pipeline that reads another pipeline's output gets the same record id, tenant and ingestion time back from these headers:

```yaml
# messages in
{{#include ../examples/nats/downstream/input.yaml}}
```

```yaml
# config
{{#include ../examples/nats/downstream/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/nats/downstream/expected.yaml}}
```

`processed.logs` names no tenant, so the tenant comes from the header. The ingestion time is the first pipeline's, not the time the message was stored in `PROCESSED`. `Fusion-Record-Kind` is not written, since every record written is a log.

## Redelivery and dead letters

When a record fails, the source naks its message with a wait before the next delivery: 1 second after the first delivery, then 2, 4, 8, 16, and 30 seconds from then on. With `max_deliver: 5`, that is 1, 2, 4 and 8 seconds.

On the last delivery (when the delivery count reaches `max_deliver`), a failure makes a dead letter instead of a nak:

1. The source publishes the message to `<dlq_prefix>.<tenant>`, with its payload as it arrived.
2. It waits for the dead-letter stream to confirm it.
3. It tells NATS to stop delivering the message.

In a dead-letter subject, characters that cannot be in a NATS subject word (`.`, `*`, `>`, spaces, non-ASCII) and `%` are written as `%XX`. So the tenant `a.b` gets `dlq.a%2Eb`.

A dead letter carries:

- the producer's own headers, minus any starting with `Nats-` or `Fusion-` (in any case)
- `Fusion-Tenant` with the tenant the pipeline gave the message (`unknown` if none), `Fusion-Ingestion-Time` and `Fusion-Ingestion-Time-Kind` with its ingestion time, `Fusion-Record-Id` when the message had a valid one, and `Fusion-Record-Kind` when it had a valid one
- `Fusion-Dlq-Reason`: the node that failed and its error, at most 1024 bytes. The node is `source` for a message with no record id or a payload that is not a record.
- `Fusion-Dlq-Subject`: the subject the message arrived on
- `Nats-Msg-Id`: `<stream>:<sequence>`, so the dead-letter stream drops a second copy of the same message within its duplicate window

Publishing a dead letter back to its `Fusion-Dlq-Subject` replays it with the same record id, tenant and time.

If the dead-letter publish fails four times, the pipeline naks the message with no wait. NATS then stops delivering it, but the message stays in the input stream. The pipeline logs its sequence number and counts it on `dlq_publish_errors_total`. If the pipeline dies during the last delivery, NATS gives up on the message after `ack_wait`, and it stays in the input stream too.

The examples in this guide always deliver a message once, so they cannot show redelivery or dead letters. Against the compose stack, a message without a record id shows a dead letter:

```sh
nats pub logs.acme.syslog '{"body": "no id header"}'   # fails every delivery
nats sub 'dlq.>' --count 1                             # about 15 seconds later
```

## Stopping

The first Ctrl-C (`SIGINT`) stops reading new messages and lets the workers finish the ones they have. The compose stack stops the pipeline with `SIGINT`. Other signals, such as `SIGTERM`, end the process without that step. Messages the consumer had fetched but not handed over yet are delivered again after `ack_wait`. A second Ctrl-C exits at once.

## Environment variables

| Variable | What it does |
|---|---|
| `NATS_URL` | The NATS server for the source and every sink. |
| `DRAGONFLY_URL` | The state store. See [State and failure policy](state-and-failure.md). |
| `OTEL_EXPORTER_OTLP_ENDPOINT` and the per-signal `OTEL_EXPORTER_OTLP_*_ENDPOINT` | Where metrics, logs and traces go. |
| `OTEL_METRIC_EXPORT_INTERVAL`, `OTEL_TRACES_SAMPLER_ARG`, `OTEL_BLRP_*`, `OTEL_BSP_*` | Export timing, trace sampling and queue sizes. |

The [README](https://github.com/jainam-panchal/fusion-pipeline#metrics-logs-and-traces) covers the telemetry variables.
