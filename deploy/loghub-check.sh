#!/usr/bin/env bash
# The loghub harness end to end (issues #13 and #14): the compose stack running
# deploy/pipeline-poc.yaml, the producer replaying the vendored loghub sets into LOGS, and the
# verifier judging what reached PROCESSED and DLQ against the producer's expectations.
#
#   deploy/loghub-check.sh
#   deploy/loghub-check.sh --chaos                  # or `make chaos`
#   PRODUCER_ARGS="--count 20000 --rate 1000" deploy/loghub-check.sh
#
# Steps: bring the stack up with PIPELINE_CONFIG=pipeline-poc.yaml (the pipeline container is
# recreated when it ran another config), purge LOGS, PROCESSED and DLQ and remove the last
# run's expectations so every message read is this run's, then start the verifier, which
# follows the run and exports what it has every 5s, and the producer (100k records over about
# 60s by default; PRODUCER_ARGS adds flags). Once the producer is done and the pipeline has
# settled, the verifier prints the summary. Dragonfly is not flushed: the dedupe window is 2s
# of ingestion time, so a previous run's keys are already out of it.
#
# --chaos adds the spec's chaos schedule, timed from the producer's start: at 20s the pipeline
# container is killed (`docker kill`) and at 25s started again (`docker start`); at 40s
# Dragonfly is paused (`docker pause`) and at 45s unpaused. The pipeline and the collector are
# restarted before the run, so every pipeline counter read afterwards counts this run alone.
# After the verifier, the run must show that the chaos landed: the source consumer redelivered
# messages (its deliveries grew by more than the messages published), `dedupe_body` counted
# state errors (`state_errors_total`), and no message was nakked (`source_naks_total`), since
# `on_state_error: pass` forwards a record the store could not answer for rather than failing
# it.
#
# Exits 0 when nothing is missing, unexpected, dead-lettered or wrongly written, 1 when
# something is (the verifier's verdict), 2 when the run could not be judged: the producer
# failed, the pipeline did not settle, or under --chaos the chaos did not land. Needs docker
# compose, the `nats` CLI, curl, jq and cargo. Host ports follow the compose overrides
# (DRAGONFLY_PORT, GRAFANA_PORT, ...). The stack keeps running the POC config afterwards;
# `docker compose -f deploy/compose.yaml up -d` puts pipeline.yaml back.
set -euo pipefail

cd "$(dirname "$0")/.."
export PIPELINE_CONFIG=pipeline-poc.yaml
COMPOSE=(docker compose -f deploy/compose.yaml)
PROM=${PROM_URL:-http://127.0.0.1:9090}
EXPECTATIONS=${EXPECTATIONS:-target/loghub/expectations.jsonl}
read -r -a PRODUCER_FLAGS <<<"${PRODUCER_ARGS:-}"
unset NATS_URL

# The chaos schedule, in seconds from the producer's start.
KILL_AT=20
START_AT=25
PAUSE_AT=40
UNPAUSE_AT=45
# The loghub tenants, whose counters the chaos evidence reads.
TENANTS='tenant=~"linux|openssh|apache|mac"'

CHAOS=0
case "${1:-}" in
    --chaos) CHAOS=1 ;;
    "") ;;
    *) echo "usage: $0 [--chaos]" >&2; exit 2 ;;
esac

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

# delivered: the source consumer's delivery count and the last stream sequence it delivered.
delivered() {
    nats consumer info LOGS pipeline --json | jq -r '"\(.delivered.consumer_seq) \(.delivered.stream_seq)"'
}

# prom_sum <selector>: the sum of the matching series now, 0 when there is none.
prom_sum() {
    curl -sf --get "$PROM/api/v1/query" --data-urlencode "query=sum($1) or vector(0)" \
        | jq -r '.data.result[0].value[1]'
}

state_of() { docker inspect -f "{{.State.$2}}" "$1"; }
running() { [[ "$(state_of "$1" Running)" == true ]]; }

# at <seconds>: sleep until that long after the producer started.
at() {
    local delay
    delay=$(awk -v t0="$T0" -v t="$1" -v now="$EPOCHREALTIME" \
        'BEGIN { d = t0 + t - now; printf "%.3f", (d > 0 ? d : 0) }')
    sleep "$delay"
    printf '   t=%2ss  ' "$1"
}

# The chaos schedule; exits 2 when a step did not take.
chaos() {
    at "$KILL_AT"
    docker kill "$PIPELINE" >/dev/null
    echo "pipeline killed"
    sleep 1
    ! running "$PIPELINE" || fail "the killed pipeline is running again before t=${START_AT}s"
    at "$START_AT"
    docker start "$PIPELINE" >/dev/null
    echo "pipeline started"
    at "$PAUSE_AT"
    docker pause "$DRAGONFLY" >/dev/null
    echo "dragonfly paused"
    [[ "$(state_of "$DRAGONFLY" Paused)" == true ]] || fail "dragonfly did not pause"
    at "$UNPAUSE_AT"
    docker unpause "$DRAGONFLY" >/dev/null
    echo "dragonfly unpaused"
}

# Whatever happens, leave the stack running.
restore() {
    if [[ -n "${DRAGONFLY:-}" && "$(state_of "$DRAGONFLY" Paused 2>/dev/null)" == true ]]; then
        docker unpause "$DRAGONFLY" >/dev/null || true
    fi
    if [[ -n "${PIPELINE:-}" ]] && ! running "$PIPELINE" 2>/dev/null; then
        docker start "$PIPELINE" >/dev/null || true
    fi
    if [[ -n "${VERIFIER:-}" ]]; then kill "$VERIFIER" 2>/dev/null || true; fi
    if [[ -n "${PRODUCER:-}" ]]; then kill "$PRODUCER" 2>/dev/null || true; fi
}
trap restore EXIT

step "build the harness"
cargo build --release -q -p fusion-harness

step "compose up with $PIPELINE_CONFIG"
"${COMPOSE[@]}" up -d --build --wait --wait-timeout 300 2>&1 | tail -3
runs_poc || fail "the pipeline container does not run $PIPELINE_CONFIG"
PIPELINE=$("${COMPOSE[@]}" ps -q pipeline)
DRAGONFLY=$("${COMPOSE[@]}" ps -q dragonfly)
if ((CHAOS)); then
    step "restart the pipeline and the collector, so counters start at zero"
    "${COMPOSE[@]}" restart otel-collector pipeline 2>&1 | tail -2
fi
wait_for 30 "the LOGS/pipeline consumer" nats consumer info LOGS pipeline

step "purge LOGS, PROCESSED and DLQ"
for stream in LOGS PROCESSED DLQ; do
    nats stream purge "$stream" -f >/dev/null
done
rm -f "$EXPECTATIONS" "$EXPECTATIONS.done"
read -r CONSUMER_SEQ_BEFORE STREAM_SEQ_BEFORE < <(delivered)

step "produce and verify$( ((CHAOS)) && echo ", with chaos" )"
OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://127.0.0.1:4318} \
    target/release/loghub-verifier --expectations "$EXPECTATIONS" &
VERIFIER=$!
T0=$EPOCHREALTIME
target/release/loghub-producer --expectations "$EXPECTATIONS" "${PRODUCER_FLAGS[@]}" &
PRODUCER=$!
if ((CHAOS)); then chaos; fi
producer_status=0
wait "$PRODUCER" || producer_status=$?
PRODUCER=
((producer_status == 0)) || fail "the producer did not publish the plan"
status=0
wait "$VERIFIER" || status=$?
VERIFIER=
((status == 0)) || exit "$status"
((CHAOS)) || exit 0

step "did the chaos land?"
read -r consumer_seq stream_seq < <(delivered)
redelivered=$(((consumer_seq - CONSUMER_SEQ_BEFORE) - (stream_seq - STREAM_SEQ_BEFORE)))
# One pipeline export (5s) and one scrape (5s) after the pipeline settled.
sleep 12
state_errors=$(prom_sum "state_errors_total{stage=\"dedupe_body\",$TENANTS}")
naks=$(prom_sum "source_naks_total{$TENANTS}")
echo "redelivered messages          $redelivered   (the kill at ${KILL_AT}s: more than 0)"
echo "dedupe_body state errors      $state_errors   (the pause at ${PAUSE_AT}s: more than 0)"
echo "naks                          $naks   (on_state_error: pass: 0)"
landed=1
((redelivered > 0)) || { echo "the kill left no message to redeliver" >&2; landed=0; }
[[ "$state_errors" != 0 ]] || { echo "the pause caused no dedupe state error" >&2; landed=0; }
[[ "$naks" == 0 ]] || { echo "messages were nakked" >&2; landed=0; }
((landed)) || fail "the chaos did not land as specified; the run is not judged"
echo "verdict: PASS under chaos"
