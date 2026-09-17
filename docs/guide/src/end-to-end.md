# End to end

This page builds one config in five steps. Each step adds a stage, and each step is a tested example. All steps use the same three messages:

```yaml
# messages in
{{#include ../examples/end-to-end/1-filter/input.yaml}}
```

## 1. Drop debug records

```yaml
# config
{{#include ../examples/end-to-end/1-filter/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/end-to-end/1-filter/expected.yaml}}
```

The `DEBUG` record is dropped and its message acked. See [filter](stages/filter.md).

## 2. Parse the body

Add an `extract` after the filter. Its named groups become attributes.

<details><summary>Messages in (the same as step 1)</summary>

```yaml
{{#include ../examples/end-to-end/2-extract/input.yaml}}
```

</details>

```yaml
# config
{{#include ../examples/end-to-end/2-extract/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/end-to-end/2-extract/expected.yaml}}
```

See [extract](stages/regex/extract.md).

## 3. Mask IP addresses

Add a `redact` after the parser, so it can mask the new attribute too.

<details><summary>Messages in (the same as step 1)</summary>

```yaml
{{#include ../examples/end-to-end/3-redact/input.yaml}}
```

</details>

```yaml
# config
{{#include ../examples/end-to-end/3-redact/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/end-to-end/3-redact/expected.yaml}}
```

See [redact](stages/regex/redact.md).

## 4. Put the tenant into the record

The tenant `acme` comes from the subject and travels as a header. Copy it into the record for readers that only see the payload.

<details><summary>Messages in (the same as step 1)</summary>

```yaml
{{#include ../examples/end-to-end/4-edit/input.yaml}}
```

</details>

```yaml
# config
{{#include ../examples/end-to-end/4-edit/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/end-to-end/4-edit/expected.yaml}}
```

See [edit](stages/edit/README.md).

## 5. Send errors to their own subject

Replace the single sink with a `route` and two sinks.

<details><summary>Messages in (the same as step 1)</summary>

```yaml
{{#include ../examples/end-to-end/5-route/input.yaml}}
```

</details>

```yaml
# config
{{#include ../examples/end-to-end/5-route/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/end-to-end/5-route/expected.yaml}}
```

Every label of the route has a consumer, and each message is acked once its record is stored. See [route](stages/route.md).

This last config was also run against the compose stack. The error record arrived on `processed.errors`, the other on `processed.logs`, and the consumer had nothing left to deliver.

## Running it

Save the config under `deploy/` and start it as in [Getting started](getting-started.md):

```sh
PIPELINE_CONFIG=my-pipeline.yaml docker compose -f deploy/compose.yaml up -d pipeline
nats sub 'processed.>' &
nats pub logs.acme.auth '{"severity_text": "ERROR", "body": "ERROR: login failed from 10.0.0.12"}' -H 'Fusion-Record-Id:2'
```

A fuller config with every stage type is in `deploy/pipeline-poc.yaml`.
