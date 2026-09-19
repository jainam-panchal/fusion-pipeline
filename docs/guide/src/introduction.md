# How to read this guide

This guide is for people who write pipeline configs. It covers what each stage does, the keys it takes, and what happens to a record that goes through it.

The pipeline reads log records from NATS, runs them through the stages in your YAML file, and writes them back to NATS.

## Examples

Every example has three parts: the messages that go in, the config, and what comes out. A test runs every example in this guide, so what you see here is what the pipeline does.

Here is a filter that keeps only `ERROR` records.

### Messages in

Each message has a subject, headers and a JSON payload. The tenant (`acme`) comes from the subject, and the record id comes from the `Fusion-Record-Id` header.

```yaml
{{#include ../examples/intro/keep-errors/input.yaml}}
```

### Config

```yaml
{{#include ../examples/intro/keep-errors/pipeline.yaml}}
```

Most examples leave out the `source` block to stay short. A real config needs one. See [NATS](nats.md).

### Result

```yaml
{{#include ../examples/intro/keep-errors/expected.yaml}}
```

`acks` lists what happened to each message, in order. `ack` means the pipeline is done with it. `nak` means it failed: NATS delivers it again, and after the last try the pipeline writes it to the dead-letter stream. Record 2 was dropped by the filter, which still counts as done, so it is acked and never reaches `out`.

Some examples show a config the pipeline refuses. Those have no messages, and the result is the error the pipeline prints.

`sinks` lists what each sink wrote. The pipeline adds the `Fusion-*` headers to each record it writes. The payload is the record as the last stage left it, with nothing added.

## Words

The guide uses the terms from the project glossary, [`CONTEXT.md`](https://github.com/jainam-panchal/fusion-pipeline/blob/main/CONTEXT.md). If this guide and the [spec](https://github.com/jainam-panchal/fusion-pipeline/blob/main/docs/specs/2026-09-08-observability-pipeline-poc.md) disagree, the spec is right and the guide needs a fix.
