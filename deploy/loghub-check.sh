#!/usr/bin/env bash
# The loghub harness end to end (issue #13): the compose stack running
# deploy/pipeline-poc.yaml, the producer replaying the vendored loghub sets into LOGS, and the
# verifier judging what reached PROCESSED and DLQ against the producer's expectations.
#
#   deploy/loghub-check.sh
#   PRODUCER_ARGS="--count 20000 --rate 1000" deploy/loghub-check.sh
#
# Steps: bring the stack up with PIPELINE_CONFIG=pipeline-poc.yaml (the pipeline container is
# recreated when it ran another config), purge LOGS, PROCESSED and DLQ so every message read
# is this run's, run the producer (100k records over about 60s by default; PRODUCER_ARGS adds
# flags), then the verifier, which waits for the pipeline to settle, prints the summary and
# exports it to the collector. Dragonfly is not flushed: the dedupe window is 2s of ingestion
# time, so a previous run's keys are already out of it.
#
# Exits with the verifier's code: 0 when nothing is missing, unexpected or dead-lettered, 1
# when something is, 2 when the run could not be judged. Needs docker compose, the `nats`
# CLI and cargo. Host ports follow the compose overrides (DRAGONFLY_PORT, GRAFANA_PORT, ...).
# The stack keeps running the POC config afterwards; `docker compose -f deploy/compose.yaml
# up -d` puts pipeline.yaml back.
set -euo pipefail

cd "$(dirname "$0")/.."
export PIPELINE_CONFIG=pipeline-poc.yaml
COMPOSE=(docker compose -f deploy/compose.yaml)
EXPECTATIONS=${EXPECTATIONS:-target/loghub/expectations.jsonl}
read -r -a PRODUCER_FLAGS <<<"${PRODUCER_ARGS:-}"
unset NATS_URL

fail() { echo "FAIL: $*" >&2; exit 2; }
step() { echo; echo "== $*"; }

# wait_for <seconds> <description> <command...>: poll until the command succeeds.
wait_for() {
    local seconds=$1 what=$2; shift 2
    for ((i = 0; i < seconds * 4; i++)); do
        if "$@" >/dev/null 2>&1; then return 0; fi
        sleep 0.25
    done
    fail "timed out after ${seconds}s waiting for $what"
}

runs_poc() {
    "${COMPOSE[@]}" exec -T pipeline cat /etc/pipeline/pipeline.yaml | grep -q '^name: poc$'
}

step "build the harness"
cargo build --release -q -p fusion-harness

step "compose up with $PIPELINE_CONFIG"
"${COMPOSE[@]}" up -d --build --wait --wait-timeout 300 2>&1 | tail -3
runs_poc || fail "the pipeline container does not run $PIPELINE_CONFIG"
wait_for 30 "the LOGS/pipeline consumer" nats consumer info LOGS pipeline

step "purge LOGS, PROCESSED and DLQ"
for stream in LOGS PROCESSED DLQ; do
    nats stream purge "$stream" -f >/dev/null
done

step "produce"
target/release/loghub-producer --expectations "$EXPECTATIONS" "${PRODUCER_FLAGS[@]}" \
    || fail "the producer did not publish the plan"

step "verify"
status=0
OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://127.0.0.1:4318} \
    target/release/loghub-verifier --expectations "$EXPECTATIONS" || status=$?
exit "$status"
