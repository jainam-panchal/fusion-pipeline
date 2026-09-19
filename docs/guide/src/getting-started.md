# Getting started

This page runs a small config against a local NATS and sends it two messages.

You need Docker with Compose v2 and the [`nats` CLI](https://github.com/nats-io/natscli). Run the commands from the repository root.

## 1. Start the stack

```sh
docker compose -f deploy/compose.yaml up -d --build
```

This starts NATS with the streams and consumer the pipeline reads from and writes to, and the pipeline itself, built from your checkout. It runs the demo config `deploy/pipeline.yaml` for now.

The stack also starts Dragonfly, Grafana, Prometheus, Loki and Tempo. If one of their ports is already taken on your machine, set `GRAFANA_PORT`, `DRAGONFLY_PORT`, `LOKI_PORT` or `TEMPO_PORT` before the command. The other ports are fixed and must be free: 4222 and 8222 (NATS), 4317, 4318 and 8889 (the collector), 7777 (the NATS exporter) and 9090 (Prometheus).

## 2. The config

```yaml
{{#include ../examples/getting-started/first-pipeline/pipeline.yaml}}
```

- `source` reads from the `LOGS` stream through the consumer `pipeline`. The stack created both.
- `drop_debug` drops records whose `severity_text` is `DEBUG`.
- `out` writes everything else to the subject `processed.logs` and waits until the `PROCESSED` stream has stored it.

Inside the stack, `NATS_URL` replaces the `url` above, so you can leave it as it is.

## 3. Run it

```sh
PIPELINE_CONFIG=../docs/guide/examples/getting-started/first-pipeline/pipeline.yaml \
  docker compose -f deploy/compose.yaml up -d pipeline
docker compose -f deploy/compose.yaml logs pipeline
```

The log ends with a line like `pipelined: running with 8 workers ...`. If the config is wrong, the log shows `pipelined: ` and the reason instead. The stack restarts the pipeline, so the same error repeats until you fix the file and run the command again.

`PIPELINE_CONFIG` is a path relative to `deploy/`. For your own config, put the file in `deploy/` and pass its name, for example `PIPELINE_CONFIG=my-pipeline.yaml`.

## 4. Send two messages

```sh
nats sub processed.logs &
nats pub logs.acme.checkout '{"severity_text": "DEBUG", "body": "cart loaded"}' -H 'Fusion-Record-Id:1'
nats pub logs.acme.checkout '{"severity_text": "WARN", "body": "payment retry"}' -H 'Fusion-Record-Id:2'
```

Every message needs a `Fusion-Record-Id` header with a number. The subject `logs.acme.checkout` tells the pipeline the record belongs to the tenant `acme`.

In this guide's examples, the same two messages look like this:

```yaml
{{#include ../examples/getting-started/first-pipeline/input.yaml}}
```

## 5. What comes out

Only the `WARN` record reaches `processed.logs`:

```text
[#1] Received on "processed.logs" with reply "_INBOX..."
Fusion-Record-Id: 2
Fusion-Tenant: acme
Fusion-Ingestion-Time: 1789636753651713591
Fusion-Ingestion-Time-Kind: reported

{"severity_text":"WARN","body":"payment retry"}
```

As an example result:

```yaml
{{#include ../examples/getting-started/first-pipeline/expected.yaml}}
```

The headers may come in another order. The ingestion time is the moment NATS stored the message, so yours will differ. The examples in this guide fix it to one value.

Check that nothing is left waiting:

```sh
nats consumer info LOGS pipeline
```

`Unprocessed Messages` and `Redelivered Messages` should both be `0`.

## 6. Clean up

Put the demo config back, or stop the stack:

```sh
docker compose -f deploy/compose.yaml up -d pipeline   # back to deploy/pipeline.yaml
docker compose -f deploy/compose.yaml down             # stop everything
```

## Without Docker for the pipeline

With Rust 1.85 or newer, you can run the pipeline from the checkout while the rest of the stack stays in Docker. Stop the one in the stack first, or both will read the same consumer:

```sh
docker compose -f deploy/compose.yaml stop pipeline
cargo run -p fusion-pipeline -- --config docs/guide/examples/getting-started/first-pipeline/pipeline.yaml
```

Next, [Concepts](concepts.md) explains the headers and what `ack` and `nak` mean.
