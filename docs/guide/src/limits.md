# Limits and guarantees

The [spec](https://github.com/jainam-panchal/fusion-pipeline/blob/main/docs/specs/2026-09-08-observability-pipeline-poc.md) is the full source for what this page says. If the two disagree, the spec is right.

## What the pipeline guarantees

- **At least once.** A message is acked only after every branch of its record has ended in a sink that confirmed storage, or in a drop. If any branch fails, the message is nakked once, after all branches finish. A record can therefore reach a sink more than once: after a redelivery, after a lost ack, or after a crash. Readers of the output streams should expect duplicates.
- **Meta from the transport only.** The record id, tenant, ingestion time and delivery count come from the message's headers, subject and JetStream. The payload is never read for them, and the pipeline never writes them into the payload. See [Concepts](concepts.md#meta).
- **The same answer on redelivery.** A redelivered message carries the same Meta, so conditions, `dedupe`, `random` and `consistent` sampling decide the same way again. Two exceptions: `sample` with `every_nth` counts deliveries (spec amendment 2026-09-15, issue #7), and values a `lua` script keeps between records belong to one worker and reset on a memory trip (spec amendment 2026-09-16, issue #8).
- **Windows in ingestion time.** `dedupe` windows use the time the message entered NATS, or the upstream `Fusion-Ingestion-Time` header.
- **A bad config never starts.** Every config and server check runs before the first message is read.

## What it does not guarantee

- **Order.** Workers handle messages at the same time, so records can reach a sink in a different order than they were published.
- **Exactly once.** See "at least once" above. `dedupe` removes most repeats but has known gaps ([issue #54](https://github.com/jainam-panchal/fusion-pipeline/issues/54)).
- **Anything but logs.** Only messages whose `Fusion-Record-Kind` is `log` or missing are processed. Others are dropped.

## Things to know about records

- A record has a fixed set of top-level fields. Any other top-level key in a payload is dropped when the message is read. Put your own fields under `attributes` or `resource`.
- The sink always writes `kind`. A payload without one comes out with `kind: log`.
- One NATS message carries one record.

## Fixed numbers

| What | Value |
|---|---|
| sink wait for storage | 5 s |
| NATS connect timeout | 5 s |
| Dragonfly call timeout | 2 s, and 5 s to connect |
| wait before a redelivery | 1, 2, 4, 8, 16, then 30 s |
| dead-letter publish tries | 4 |
| `Fusion-Dlq-Reason` length | 1024 bytes |
| longest regex pattern | 8192 bytes |
| regex parentheses depth | 250 |
| `lua` table nesting in a returned record | 128 |

Configurable limits and their defaults are on [Regex limits](regex-limits.md) and [lua budgets](stages/lua/budgets-and-errors.md).

## Out of scope

These are not built:

- metrics and traces as input
- dedicated parsers (syslog, JSON, key=value, XML, Grok, timestamps), raw EVTX, dissect, GeoIP, lookups, log-to-metric, aggregation, rate limiting, OCSF
- a VRL- or OTTL-style language; the condition grammar is small on purpose
- in `edit`: templates, defaults, conditional ops, casts, case changes, a salted `hash`, `on_unapplied: tag`
- OTLP protobuf sources and sinks
- per-key ordering and partitioned consumers
- reloading a config while running
- more than one pipeline per process, and a config per tenant
- stopping a `lua` stage that keeps failing
- throughput or latency targets
- production hardening: TLS, authentication, several nodes, a highly available state store

## Known limits

- A PCRE2-only pattern with no anchor, built from single-character repeats such as `(?<=:)\w+\s+\w+`, can take about 5 seconds on a 64 KiB text that does not match. Only `input_bytes` bounds it.
- `hash` in `edit` has no salt. It is a join key, not a way to hide values.
- A state error nakked with `on_state_error: nak` uses the normal redelivery waits, so a Dragonfly outage longer than those waits (at least 15 seconds with 5 deliveries) sends messages to the dead-letter stream ([issue #30](https://github.com/jainam-panchal/fusion-pipeline/issues/30)).
- A failed sink publish is not retried inside the sink; the whole message is nakked ([issue #36](https://github.com/jainam-panchal/fusion-pipeline/issues/36)).
- A `lua` `script` path is read from the working directory, not the config's directory ([issue #41](https://github.com/jainam-panchal/fusion-pipeline/issues/41)).
- `lua` error lines and `log.*` calls are not rate limited ([issue #42](https://github.com/jainam-panchal/fusion-pipeline/issues/42)).

[Open issues](https://github.com/jainam-panchal/fusion-pipeline/issues) has the current list.
