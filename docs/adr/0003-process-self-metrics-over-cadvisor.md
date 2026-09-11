---
status: accepted
date: 2026-09-11
---

# Process CPU and memory are self-reported, not read from the container runtime

The internal dashboard needs the pipeline's CPU and resident memory next to its throughput and latency, and NATS's and Dragonfly's next to those, so an operator can tell a slow pipeline from a starved one. Two ways were open: cAdvisor in the compose stack, reading every container's cgroup through the Docker daemon, or each process reporting itself. cAdvisor was tried first (one container, no pipeline code, per-container CPU, RSS and network for the whole stack) and rejected: its Docker handler resolves a container's read-write layer through the classic `image/overlay2/layerdb` store, and on a daemon running the containerd snapshotter (`driver-type: io.containerd.snapshotter.v1`, the default on new Docker 25+ installs and on the machine this POC is built on) it logs `failed to identify the read-write layer ID` for every container and exports nothing. Versions 0.49.1 and 0.52.1 both fail the same way. Its containerd handler would register the containers but without the compose labels that name them, so the dashboard could not group by service.

We chose self-reporting. The pipeline exports OpenTelemetry's `process.cpu.time`, `process.memory.usage` and `process.thread.count` from `/proc/self/schedstat` and `/proc/self/status` on every export interval, next to the spec's metrics and under the same resource; NATS reports CPU and memory through `prometheus-nats-exporter -varz`; Dragonfly reports resident memory on its own `/metrics`. This works wherever the pipeline runs, including `cargo run` on a laptop against the compose NATS, needs no Docker socket in a container, and keeps the compose stack to services the spec names.

## Consequences

- The `process.*` names sit outside the spec's closed metric set; the spec says so, and `Metric` does not list them. They are OpenTelemetry semantic conventions, not pipeline vocabulary, and the closed-set test does not cover them.
- Per-process, not per-container: a process that forks (none does) would report only itself, and the collector's own footprint is visible only through its self-metrics.
- Dragonfly does not export CPU, so the "what does it cost" panel shows its memory only.
- Network I/O per service, which cAdvisor would have given for free, is not reported. NATS's exporter carries bytes in and out on the server side, which is the number an operator cares about.
- If cAdvisor gains snapshotter support, container-level metrics can be added as a scrape job without touching the pipeline; the self-reported metrics would stay, since they cover the non-compose case.
