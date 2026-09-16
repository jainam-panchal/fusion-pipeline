#!/usr/bin/env bash
# End-to-end check of the NATS source and sink against the compose stack (issue #3).
#
# Brings up deploy/compose.yaml, runs `pipelined --config deploy/pipeline.yaml`, and checks:
#   1. a record published to logs.acme.syslog appears on processed.logs exactly as
#      published, with the pipeline headers Fusion-Tenant: acme, a decimal
#      Fusion-Ingestion-Time and Fusion-Ingestion-Time-Kind: reported, and the consumer shows
#      it acknowledged;
#   2. with the PROCESSED stream deleted, the record is nak'd and JetStream redelivers it;
#   3. NATS_URL overrides the YAML URL for both the source and the sink (a bogus NATS_URL
#      makes startup fail fast; bogus YAML URLs with a real NATS_URL start fine).
# Exits non-zero on the first failure. Needs docker compose, the `nats` CLI, jq and cargo.
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f deploy/compose.yaml)
CONFIG=deploy/pipeline.yaml
LOG=$(mktemp -t pipelined.XXXXXX.log)
SUB_OUT=$(mktemp -d -t nats-sub.XXXXXX)
PIPELINED_PID=""
unset NATS_URL

fail() { echo "FAIL: $*" >&2; exit 1; }
step() { echo; echo "== $*"; }

cleanup() {
    if [[ -n "$PIPELINED_PID" ]] && kill -0 "$PIPELINED_PID" 2>/dev/null; then
        kill -INT "$PIPELINED_PID" 2>/dev/null || true
        wait "$PIPELINED_PID" 2>/dev/null || true
    fi
    rm -rf "$SUB_OUT"
}
trap cleanup EXIT

# consumer_field <field>: one number from `nats consumer info LOGS pipeline`.
consumer_field() { nats consumer info LOGS pipeline --json | jq -r ".$1"; }

# wait_for <seconds> <description> <command...>: poll until the command succeeds.
wait_for() {
    local seconds=$1 what=$2; shift 2
    for ((i = 0; i < seconds * 4; i++)); do
        if "$@" >/dev/null 2>&1; then return 0; fi
        sleep 0.25
    done
    fail "timed out after ${seconds}s waiting for $what"
}

consumer_settled() {
    [[ "$(consumer_field num_ack_pending)" == 0 && "$(consumer_field num_pending)" == 0 ]]
}

start_pipelined() {
    : > "$LOG"
    "$PIPELINED" --config "$CONFIG" 2>"$LOG" &
    PIPELINED_PID=$!
    wait_for 15 "pipelined to start" grep -q "running with" "$LOG"
}

stop_pipelined() {
    kill -INT "$PIPELINED_PID"
    local status=0
    wait "$PIPELINED_PID" || status=$?
    PIPELINED_PID=""
    [[ $status == 0 ]] || fail "pipelined exited with status $status:"$'\n'"$(cat "$LOG")"
}

step "compose up (nats + nats-init; the compose pipeline stays down, this runs its own)"
"${COMPOSE[@]}" stop pipeline >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d --wait nats dragonfly
"${COMPOSE[@]}" run --rm nats-init >/dev/null
wait_for 15 "the LOGS/pipeline consumer" nats consumer info LOGS pipeline

step "build pipelined"
cargo build -q -p fusion-pipeline
PIPELINED=target/debug/pipelined

step "clean streams"
nats stream purge LOGS -f >/dev/null
nats stream purge PROCESSED -f >/dev/null

step "1. pub logs.acme.syslog -> sub processed.logs, record untouched, Meta in headers, consumer acked"
start_pipelined
# --dump writes each message as JSON: {"Subject", "Header": {name: [values]}, "Data": base64}.
timeout 20 nats sub processed.logs --count 1 --dump="$SUB_OUT" >/dev/null &
SUB_PID=$!
sleep 1
nats pub logs.acme.syslog '{"id": 1, "body": "disk full", "severity_text": "ERROR"}'
wait "$SUB_PID" || fail "nats sub processed.logs saw no record"
MSG="$SUB_OUT/1.json"
[[ -f "$MSG" ]] || fail "nats sub wrote no message to $SUB_OUT"
jq . "$MSG"
RECORD=$(jq -r '.Data | @base64d' "$MSG")
echo "$RECORD"
jq -e '.id == 1' <<<"$RECORD" >/dev/null || fail "record on processed.logs has the wrong id"
jq -e '.resource["tenant.id"] == null and .observed_time_unix_nano == null' <<<"$RECORD" >/dev/null \
    || fail "the pipeline wrote a tenant or a time into the record on processed.logs"
jq -e '.Header["Fusion-Tenant"] == ["acme"]' "$MSG" >/dev/null \
    || fail 'message on processed.logs lacks Fusion-Tenant: acme'
jq -e '.Header["Fusion-Ingestion-Time"][0] | test("^[0-9]+$")' "$MSG" >/dev/null \
    || fail 'message on processed.logs lacks a decimal Fusion-Ingestion-Time'
jq -e '.Header["Fusion-Ingestion-Time-Kind"] == ["reported"]' "$MSG" >/dev/null \
    || fail 'message on processed.logs lacks Fusion-Ingestion-Time-Kind: reported'
wait_for 10 "the consumer to show 0 pending" consumer_settled
[[ "$(consumer_field num_redelivered)" == 0 ]] || fail "consumer shows redeliveries after a clean run"
echo "ok: delivered untouched, Meta in headers, 0 pending, 0 redelivered"

step "2. sink stream deleted -> record nak'd -> JetStream redelivers"
nats stream rm PROCESSED -f >/dev/null
nats pub logs.acme.syslog '{"id": 2, "body": "sink is gone"}'
wait_for 30 "num_redelivered > 0" bash -c '[[ "$(nats consumer info LOGS pipeline --json | jq -r .num_redelivered)" -gt 0 ]]'
echo "ok: num_redelivered=$(consumer_field num_redelivered)"
"${COMPOSE[@]}" run --rm nats-init >/dev/null
wait_for 30 "the redelivered record to be acked once the stream is back" consumer_settled
echo "ok: stream recreated, redelivered record acked"
grep -q "was not acknowledged" "$LOG" || fail "pipelined did not report the failed publish:"$'\n'"$(cat "$LOG")"
stop_pipelined

step "3. NATS_URL overrides the YAML url"
if NATS_URL=nats://127.0.0.1:1 timeout 20 "$PIPELINED" --config "$CONFIG" 2>"$LOG"; then
    fail "pipelined started against a bogus NATS_URL"
fi
grep -q "127.0.0.1:1" "$LOG" || fail "startup error does not name the NATS_URL endpoint:"$'\n'"$(cat "$LOG")"
echo "ok: $(head -1 "$LOG")"

BOGUS_CONFIG=$(mktemp -t pipeline-bogus.XXXXXX.yaml)
sed -e 's#url: nats://127.0.0.1:4222#url: nats://127.0.0.1:1#' "$CONFIG" >"$BOGUS_CONFIG"
grep -q "127.0.0.1:1" "$BOGUS_CONFIG" || fail "could not rewrite the config URLs"
: > "$LOG"
NATS_URL=nats://127.0.0.1:4222 "$PIPELINED" --config "$BOGUS_CONFIG" 2>"$LOG" &
PIPELINED_PID=$!
wait_for 15 "pipelined to start with bogus YAML URLs and a real NATS_URL" grep -q "running with" "$LOG"
stop_pipelined
rm -f "$BOGUS_CONFIG"
echo "ok: source and sink both took NATS_URL over the YAML url"

echo
echo "PASS"
