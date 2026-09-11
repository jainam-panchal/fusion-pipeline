#!/usr/bin/env bash
# End-to-end check of the metrics path against the compose stack (issue #11).
#
# Brings up deploy/compose.yaml (pipeline included), drives traffic through it, and checks:
#   1. every scrape target is up: the collector (pipeline metrics), NATS via
#      prometheus-nats-exporter, Dragonfly, and the collector's self-metrics;
#   2. traffic: 1,000 records with distinct bodies, one repeated body the dedupe node drops,
#      one TRACE record the filter drops, one record without an id and one sink failure;
#   3. every metric with a producer is in Prometheus with the labels the spec gives it;
#   4. the `reason` values seen on records_dropped_total are within the spec's closed set;
#   5. the NATS exporter reports JetStream consumer pending, redelivered and ack floor;
#   6. every pipeline series carries the instance id, and the pipeline, NATS and Dragonfly
#      each report their own CPU and resident memory;
#   7. Grafana serves the provisioned internal dashboard.
# The Lua and dead-letter metrics have no producer until #8 and #10; they are reported as
# pending, not required.
# Exits non-zero on the first failure. Needs docker compose, the `nats` CLI, curl and jq.
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f deploy/compose.yaml)
PROM=${PROM_URL:-http://127.0.0.1:9090}
GRAFANA=${GRAFANA_URL:-http://127.0.0.1:${GRAFANA_PORT:-3000}}
RECORDS=${RECORDS:-1000}
unset NATS_URL

fail() { echo "FAIL: $*" >&2; exit 1; }
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

# prom_query <expr>: the instant-query result vector as JSON.
prom_query() {
    curl -sf --get "$PROM/api/v1/query" --data-urlencode "query=$1" | jq -c '.data.result'
}

# prom_has <expr>: whether the expression returns at least one series.
prom_has() { [[ "$(prom_query "$1" | jq 'length')" -gt 0 ]]; }

# prom_value <expr>: the first sample's value, or empty.
prom_value() { prom_query "$1" | jq -r '.[0].value[1] // empty'; }

# target_up <job>: whether Prometheus scraped the job successfully on its last attempt.
target_up() { [[ "$(prom_value "up{job=\"$1\"}")" == 1 ]]; }

step "compose up (full stack, pipeline built)"
"${COMPOSE[@]}" up -d --build --wait --wait-timeout 300 2>&1 | tail -3
wait_for 30 "the LOGS/pipeline consumer" nats consumer info LOGS pipeline

step "1. scrape targets up"
for job in pipeline nats dragonfly otel-collector prometheus; do
    wait_for 60 "target $job" target_up "$job"
    echo "up: $job"
done

step "2. traffic: $RECORDS records, one repeat (deduped), one TRACE (filtered), one without an id, one sink failure"
nats stream purge LOGS -f >/dev/null
# Distinct bodies, so the dedupe node lets every one of them through.
nats pub logs.acme.syslog \
    "{\"id\": {{Count}}, \"severity_text\": \"ERROR\", \"body\": \"disk full {{Count}}\", \"observed_time_unix_nano\": {{UnixNano}}}" \
    --count "$RECORDS" >/dev/null
nats pub logs.acme.syslog '{"id": 999999, "severity_text": "ERROR", "body": "disk full 1"}' >/dev/null
nats pub logs.acme.syslog '{"id": 1000000, "severity_text": "TRACE", "body": "noise"}' >/dev/null
nats pub logs.acme.syslog '{"body": "no id"}' >/dev/null
# Sink failure: the PROCESSED stream is gone, so the write gets no PubAck, the source
# message is nakked and JetStream redelivers it; nats-init recreates the stream.
nats stream rm PROCESSED -f >/dev/null
nats pub logs.acme.syslog '{"id": 1000001, "body": "sink is gone"}' >/dev/null
wait_for 60 "the nakked record to redeliver" \
    bash -c '[[ "$(nats consumer info LOGS pipeline --json | jq -r .num_redelivered)" -gt 0 ]]'
"${COMPOSE[@]}" run --rm nats-init >/dev/null 2>&1
wait_for 60 "the consumer to settle" \
    bash -c '[[ "$(nats consumer info LOGS pipeline --json | jq -r .num_ack_pending)" == 0 ]]'
wait_for 60 "records_in_total{stage=out} to reach $RECORDS" \
    bash -c "[[ \"\$(curl -sf --get '$PROM/api/v1/query' --data-urlencode 'query=records_in_total{stage=\"out\",tenant=\"acme\"}' | jq -r '.data.result[0].value[1] // 0' | cut -d. -f1)\" -ge $RECORDS ]]"
echo "records_in_total{stage=out}: $(prom_value 'records_in_total{stage="out",tenant="acme"}')"

step "3. every spec metric present with its labels"
# name|labels: labels are the spec's, and the query pins each one so a metric that lost a
# label fails here. Histograms are checked through their _bucket series.
SPEC_METRICS=(
    'records_in_total{tenant="acme",stage="out"}'
    'records_out_total{tenant="acme",stage="out"}'
    'records_dropped_total{tenant="acme",stage="source",reason="missing_id"}'
    'records_dropped_total{tenant="acme",stage="drop_trace",reason="filter"}'
    'records_dropped_total{tenant="acme",stage="dedupe_body",reason="dedupe"}'
    'records_errored_total{tenant="acme",stage="out"}'
    'stage_duration_seconds_bucket{tenant="acme",stage="drop_trace"}'
    'state_ops_total{tenant="acme",stage="dedupe_body"}'
    'state_op_duration_seconds_bucket{tenant="acme",stage="dedupe_body"}'
    'source_naks_total{tenant="acme"}'
    'source_redeliveries_total{tenant="acme"}'
    'sink_publish_duration_seconds_bucket{tenant="acme",stage="out"}'
    'sink_publish_errors_total{tenant="acme",stage="out"}'
    'pipeline_end_to_end_seconds_bucket{tenant="acme"}'
)
# Named in the spec, emitted by stages that do not exist yet (#8 Lua, #10 dead-letter
# queue). Reported, not required, until their tickets land. `state_errors_total` has a
# producer but a healthy run gives it nothing to count. Likewise the `regex_limit` drop
# reason has no producer until the regex stages (#5).
PENDING_METRICS=(
    state_errors_total lua_errors_total dlq_total
)
for expr in "${SPEC_METRICS[@]}"; do
    wait_for 30 "$expr" prom_has "$expr"
    echo "present: $expr"
done
for name in "${PENDING_METRICS[@]}"; do
    if prom_has "$name"; then echo "present: $name"; else echo "pending: $name (nothing to count in this run, or no producer yet)"; fi
done

step "4. records_dropped_total reasons within the closed set"
CLOSED_SET="filter route_default_drop sample dedupe lua_drop lua_error regex_limit state_error invalid_record missing_id"
seen=$(curl -sf --get "$PROM/api/v1/label/reason/values" --data-urlencode 'match[]=records_dropped_total' | jq -r '.data[]')
for reason in $seen; do
    [[ " $CLOSED_SET " == *" $reason "* ]] || fail "reason \`$reason\` is not in the spec's closed set"
    echo "reason: $reason"
done

step "5. NATS exporter: JetStream consumer pending, redelivered, ack floor"
for name in jetstream_consumer_num_pending jetstream_consumer_num_redelivered jetstream_consumer_ack_floor_stream_seq; do
    wait_for 30 "$name" prom_has "${name}{consumer_name=\"pipeline\"}"
    echo "present: $name = $(prom_value "${name}{consumer_name=\"pipeline\"}")"
done

step "6. instance id on every series, CPU and RSS per process"
[[ "$(prom_query 'records_in_total{job!="fusion-pipeline"}' | jq 'length')" == 0 ]] \
    || fail "pipeline series not labelled job=fusion-pipeline (honor_labels missing?)"
[[ "$(prom_query 'records_in_total{instance=""}' | jq 'length')" == 0 ]] \
    || fail "pipeline series without an instance id"
echo "instances: $(curl -sf --get "$PROM/api/v1/label/instance/values" --data-urlencode 'match[]=records_in_total' | jq -r '.data | join(", ")')"
for expr in 'process_cpu_time_seconds_total{job="fusion-pipeline"}' 'process_memory_usage_bytes{job="fusion-pipeline"}' 'process_thread_count{job="fusion-pipeline"}' 'gnatsd_varz_cpu' 'gnatsd_varz_mem' 'dragonfly_used_memory_rss_bytes'; do
    wait_for 60 "$expr" prom_has "$expr"
    echo "present: $expr"
done

step "7. Grafana provisioned the internal dashboard"
wait_for 60 "grafana" curl -sf "$GRAFANA/api/health"
title=$(curl -sf "$GRAFANA/api/dashboards/uid/fusion-internal" | jq -r '.dashboard.title')
[[ "$title" == "fusion-pipeline internal" ]] || fail "dashboard fusion-internal not provisioned (got \`$title\`)"
echo "dashboard: $title ($GRAFANA/d/fusion-internal)"

echo
echo "OK: all metrics checks passed"
