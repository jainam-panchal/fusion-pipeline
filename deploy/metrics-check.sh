#!/usr/bin/env bash
# End-to-end check of the telemetry paths against the compose stack (issues #11 and #12).
#
# Brings up deploy/compose.yaml (pipeline included), drives traffic through it, and checks:
#   1. every scrape target is up: the collector (pipeline metrics), NATS via
#      prometheus-nats-exporter, Dragonfly, and the collector's self-metrics;
#   2. traffic: 1,000 records with distinct bodies, one repeated body the dedupe node drops,
#      one TRACE record the filter drops, one syslog line the extract node parses and the
#      redact node masks, one with a non-numeric http.status the lua node raises on, one
#      with a malformed pipeline header, one message without a Fusion-Record-Id header and one
#      sink failure;
#   3. every metric with a producer is in Prometheus with the labels the spec gives it;
#   4. the `reason` values seen on records_dropped_total and dlq_total are within the spec's
#      closed sets;
#   5. the NATS exporter reports JetStream consumer pending, redelivered and ack floor;
#   6. every pipeline series carries the instance id, and the pipeline, NATS and Dragonfly
#      each report their own CPU and resident memory;
#   7. Grafana serves the provisioned internal and tenant dashboards, and every tenant
#      dashboard query runs in Prometheus;
#   8. Loki has the `nak` line of the message without a record id, and the `stage_error` line of the
#      record whose sink failed, whose trace id finds its trace in Tempo.
# The message without a record id fails every delivery and is dead-lettered after the fifth, about
# 15 s after it is published, which is what dlq_total and dlq_publish_duration_seconds
# show; no dead-letter publish fails, so dlq_publish_errors_total is reported as pending.
# Exits non-zero on the first failure. Needs docker compose, the `nats` CLI, curl and jq.
# Host ports follow the compose overrides: GRAFANA_PORT, LOKI_PORT, TEMPO_PORT.
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f deploy/compose.yaml)
PROM=${PROM_URL:-http://127.0.0.1:9090}
GRAFANA=${GRAFANA_URL:-http://127.0.0.1:${GRAFANA_PORT:-3000}}
LOKI=${LOKI_URL:-http://127.0.0.1:${LOKI_PORT:-3100}}
TEMPO=${TEMPO_URL:-http://127.0.0.1:${TEMPO_PORT:-3200}}
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

# loki_lines <logql>: the matching streams of the last hour as JSON. Loki returns each line's
# structured metadata (record_id, trace_id, ...) among its stream's labels.
loki_lines() {
    curl -sf --get "$LOKI/loki/api/v1/query_range" --data-urlencode "query=$1" \
        --data-urlencode "since=1h" --data-urlencode "limit=20" | jq -c '.data.result'
}

# loki_has <logql>: whether the query matches at least one line.
loki_has() { [[ "$(loki_lines "$1" | jq 'length')" -gt 0 ]]; }

# tempo_spans <trace id>: the names of the trace's spans, one per line.
tempo_spans() {
    curl -sf "$TEMPO/api/traces/$1" | jq -r '.batches[].scopeSpans[].spans[].name'
}

# target_up <job>: whether Prometheus scraped the job successfully on its last attempt.
target_up() { [[ "$(prom_value "up{job=\"$1\"}")" == 1 ]]; }

step "compose up (full stack, pipeline built)"
"${COMPOSE[@]}" up -d --build --wait --wait-timeout 300 2>&1 | tail -3
# The collector, Loki and Tempo read their config and Grafana its provisioning only at start,
# and `up` leaves a running container alone when only a mounted file changed.
RESTARTED_NS=$(date +%s%N)
"${COMPOSE[@]}" restart otel-collector loki tempo grafana >/dev/null 2>&1
wait_for 30 "the LOGS/pipeline consumer" nats consumer info LOGS pipeline

step "1. scrape targets up"
for job in pipeline nats dragonfly otel-collector prometheus; do
    wait_for 60 "target $job" target_up "$job"
    echo "up: $job"
done

step "2. traffic: $RECORDS records, one repeat (deduped), one TRACE (filtered), one the lua node raises on, one with a malformed pipeline header, one without a record id, one sink failure"
nats stream purge LOGS -f >/dev/null
nats stream purge DLQ -f >/dev/null
# Distinct bodies, so the dedupe node lets every one of them through.
nats pub logs.acme.syslog \
    "{\"id\": {{Count}}, \"severity_text\": \"ERROR\", \"body\": \"disk full {{Count}}\", \"observed_time_unix_nano\": {{UnixNano}}}" \
    --count "$RECORDS" -H 'Fusion-Record-Id:{{Count}}' >/dev/null
nats pub logs.acme.syslog '{"id": 999999, "severity_text": "ERROR", "body": "disk full 1"}' \
    -H 'Fusion-Record-Id:999999' >/dev/null
nats pub logs.acme.syslog '{"id": 1000000, "severity_text": "TRACE", "body": "noise"}' \
    -H 'Fusion-Record-Id:1000000' >/dev/null
nats pub logs.acme.syslog '{"id": 1000002, "body": "Jun 14 15:16:01 combo sshd(pam_unix)[19939]: authentication failure; rhost=218.188.2.4"}' \
    -H 'Fusion-Record-Id:1000002' >/dev/null
# `"abc" // 100` raises inside the lua node: a `runtime` error, forwarded by `on_error: pass`.
nats pub logs.acme.syslog '{"id": 1000003, "body": "bad status", "attributes": {"http.status": "abc"}}' \
    -H 'Fusion-Record-Id:1000003' >/dev/null
# A pipeline header that does not parse: ignored and counted on
# source_invalid_headers_total, and the record is still walked.
nats pub logs.acme.syslog '{"id": 1000004, "body": "bad header"}' \
    -H 'Fusion-Record-Id:1000004' -H 'Fusion-Ingestion-Time:soon' -H 'Fusion-Ingestion-Time-Kind:reported' >/dev/null
# No Fusion-Record-Id: nakked on every delivery, then dead-lettered to dlq.acme and
# terminated. The payload's own `id` is data and does not count.
nats pub logs.acme.syslog '{"id": 1000005, "body": "no id header"}' >/dev/null
# Sink failure: the PROCESSED stream is gone, so the write gets no PubAck, the source
# message is nakked and JetStream redelivers it; nats-init recreates the stream.
nats stream rm PROCESSED -f >/dev/null
nats pub logs.acme.syslog '{"id": 1000001, "body": "sink is gone"}' \
    -H 'Fusion-Record-Id:1000001' >/dev/null
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
    'records_dropped_total{tenant="acme",stage="only_parsed",reason="edit_unapplied"}'
    'records_errored_total{tenant="acme",stage="out"}'
    'stage_duration_seconds_bucket{tenant="acme",stage="drop_trace"}'
    'records_in_total{tenant="acme",stage="parse_syslog",engine="linear"}'
    'regex_nonmatch_total{tenant="acme",stage="parse_syslog",engine="linear"}'
    'records_out_total{tenant="acme",stage="mask_ips",engine="linear"}'
    'edit_unapplied_total{tenant="acme",stage="tag_service",op="copy",field="attributes.Component",cause="absent"}'
    'lua_errors_total{tenant="acme",stage="split_lines",kind="runtime"}'
    'records_out_total{tenant="acme",stage="split_lines"}'
    'state_ops_total{tenant="acme",stage="dedupe_body"}'
    'state_op_duration_seconds_bucket{tenant="acme",stage="dedupe_body"}'
    'source_naks_total{tenant="acme"}'
    'source_redeliveries_total{tenant="acme"}'
    'source_invalid_headers_total{tenant="acme"}'
    'sink_publish_duration_seconds_bucket{tenant="acme",stage="out"}'
    'sink_publish_errors_total{tenant="acme",stage="out"}'
    'pipeline_end_to_end_seconds_bucket{tenant="acme"}'
    'dlq_total{tenant="acme",stage="source",reason="missing_id"}'
    'dlq_publish_duration_seconds_bucket{tenant="acme"}'
    'bytes_in_total{tenant="acme"}'
    'bytes_out_total{tenant="acme",stage="out"}'
)
# Named in the spec, with a producer, but a healthy run gives them nothing to count:
# `state_errors_total` needs a store failure and `dlq_publish_errors_total` a dead-letter
# publish that fails. So do the `regex_limit`, `lua_drop` and `lua_error` drop reasons: no
# pattern in deploy/pipeline.yaml trips a limit on this traffic, the lua node returns nil for
# nothing, and its `on_error` is `pass`.
PENDING_METRICS=(
    state_errors_total dlq_publish_errors_total
)
for expr in "${SPEC_METRICS[@]}"; do
    wait_for 30 "$expr" prom_has "$expr"
    echo "present: $expr"
done
for name in "${PENDING_METRICS[@]}"; do
    if prom_has "$name"; then echo "present: $name"; else echo "pending: $name (nothing to count in this run, or no producer yet)"; fi
done

step "4. records_dropped_total and dlq_total reasons within the closed sets"
CLOSED_SET="filter route_default_drop sample dedupe lua_drop lua_error regex_limit state_error invalid_record missing_id edit_unapplied"
seen=$(curl -sf --get "$PROM/api/v1/label/reason/values" --data-urlencode 'match[]=records_dropped_total' | jq -r '.data[]')
for reason in $seen; do
    [[ " $CLOSED_SET " == *" $reason "* ]] || fail "reason \`$reason\` is not in the spec's closed set"
    echo "reason: $reason"
done

DLQ_REASONS="stage_error state_error sink_error panic missing_id undecodable"
seen=$(curl -sf --get "$PROM/api/v1/label/reason/values" --data-urlencode 'match[]=dlq_total' | jq -r '.data[]')
for reason in $seen; do
    [[ " $DLQ_REASONS " == *" $reason "* ]] || fail "dlq reason \`$reason\` is not in the spec's closed set"
    echo "dlq reason: $reason"
done
[[ "$(nats stream info DLQ --json | jq -r '.state.messages')" -ge 1 ]] \
    || fail "the message without a Fusion-Record-Id is not on the DLQ stream"
echo "DLQ stream messages: $(nats stream info DLQ --json | jq -r '.state.messages')"

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

step "7. Grafana provisioned both dashboards; every tenant query runs"
wait_for 60 "grafana" curl -sf "$GRAFANA/api/health"
for pair in "fusion-internal|fusion-pipeline internal" "fusion-tenant|fusion-pipeline tenant"; do
    uid=${pair%%|*} want=${pair#*|}
    title=$(curl -sf "$GRAFANA/api/dashboards/uid/$uid" | jq -r '.dashboard.title')
    [[ "$title" == "$want" ]] || fail "dashboard $uid not provisioned (got \`$title\`)"
    echo "dashboard: $title ($GRAFANA/d/$uid)"
done
for uid in loki tempo; do
    [[ "$(curl -sf "$GRAFANA/api/datasources/uid/$uid" | jq -r '.uid')" == "$uid" ]] \
        || fail "datasource $uid not provisioned"
    echo "datasource: $uid"
done
jq -r '.panels[].targets[]?.expr' deploy/grafana/dashboards/tenant.json | while read -r expr; do
    query=${expr//\$tenant/acme}
    query=${query//\$__rate_interval/1m}
    query=${query//\$__range/1h}
    status=$(curl -s --get "$PROM/api/v1/query" --data-urlencode "query=$query" | jq -r '.status')
    [[ "$status" == success ]] || fail "tenant dashboard query does not run: $query"
done
echo "tenant dashboard: every query runs"
echo "where did my logs go (acme): $(prom_query 'sum by (stage, reason) (increase(records_dropped_total{tenant="acme"}[1h])) > 0' | jq -c '[.[] | {stage: .metric.stage, reason: .metric.reason, records: (.value[1] | tonumber | round)}]')"

step "8. logs in Loki, traces in Tempo, linked by trace id"
wait_for 60 "loki" curl -sf "$LOKI/ready"
wait_for 60 "tempo" curl -sf "$TEMPO/ready"
NAK='{service_name="fusion-pipeline"} | event="nak" | reason="missing_id" | node="source"'
wait_for 60 "the nak line of the message without a Fusion-Record-Id" loki_has "$NAK"
echo "present: $NAK"
ERROR='{service_name="fusion-pipeline"} | event="stage_error" | record_id="1000001" | node="out" | reason="sink_error" | tenant="acme"'
wait_for 60 "the stage_error line of the record whose sink failed" loki_has "$ERROR"
echo "present: $ERROR"
trace_id=$(loki_lines "$ERROR" | jq -r '[.[].stream.trace_id // empty][0] // empty')
[[ -n "$trace_id" ]] || fail "the stage_error line carries no trace_id"
wait_for 60 "trace $trace_id in Tempo" curl -sf "$TEMPO/api/traces/$trace_id"
wait_for 30 "the failed sink's span in trace $trace_id" bash -c "$(declare -f tempo_spans); TEMPO='$TEMPO' tempo_spans $trace_id | grep -qx out"
spans=$(tempo_spans "$trace_id" | sort | uniq -c | tr -s ' ' | paste -sd, -)
[[ "$spans" == *delivery* ]] || fail "trace $trace_id has no delivery span: $spans"
echo "trace $trace_id: $spans"
labels=$(curl -sf --get "$LOKI/loki/api/v1/labels" --data-urlencode "start=$RESTARTED_NS" | jq -c '.data')
[[ "$labels" == '["service_name"]' ]] || fail "Loki index labels are $labels, not only service_name"
echo "Loki index labels: $labels"

echo
echo "OK: all telemetry checks passed"
