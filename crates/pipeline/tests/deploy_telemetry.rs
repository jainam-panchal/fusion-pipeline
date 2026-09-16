//! The telemetry half of `deploy/` (issue #12): the collector sends logs to Loki and traces
//! to Tempo, Grafana links the two by trace id and record id, and the tenant dashboard shows
//! one tenant's counts, drops, bytes and end-to-end latency and nothing internal. The live
//! round trip is `deploy/metrics-check.sh`.

mod common;

use std::collections::BTreeSet;

use common::deploy_config;
use serde_json::Value as Json;
use serde_yaml_ng::Value as Yaml;

fn yaml(name: &str) -> Yaml {
    serde_yaml_ng::from_str(&deploy_config(name))
        .unwrap_or_else(|err| panic!("deploy/{name} is YAML: {err}"))
}

/// `value` at the `path` of mapping keys, as text.
fn text<'v>(value: &'v Yaml, path: &[&str]) -> &'v str {
    at(value, path)
        .as_str()
        .unwrap_or_else(|| panic!("`{path:?}` is text"))
}

/// `value` at the `path` of mapping keys.
fn at<'v>(value: &'v Yaml, path: &[&str]) -> &'v Yaml {
    path.iter().fold(value, |value, key| {
        value
            .get(key)
            .unwrap_or_else(|| panic!("`{key}` of `{path:?}` is missing"))
    })
}

fn texts(value: &Yaml) -> Vec<String> {
    value
        .as_sequence()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn compose_runs_loki_and_tempo_with_their_own_configs_and_movable_ports() {
    let compose = yaml("compose.yaml");
    for (service, config, port) in [
        ("loki", "./loki.yaml", "${LOKI_PORT:-3100}:3100"),
        ("tempo", "./tempo.yaml", "${TEMPO_PORT:-3200}:3200"),
    ] {
        let service = at(&compose, &["services", service]);
        assert!(
            texts(at(service, &["volumes"]))
                .iter()
                .any(|volume| volume.starts_with(&format!("{config}:"))),
            "{service:?} mounts {config}"
        );
        assert!(
            texts(at(service, &["ports"])).iter().any(|p| p == port),
            "{service:?} publishes {port}"
        );
        let image = text(service, &["image"]);
        assert!(
            image.contains(':') && !image.ends_with(":latest"),
            "{image} is pinned"
        );
    }
    let collector = texts(at(&compose, &["services", "otel-collector", "depends_on"]));
    assert_eq!(collector, ["loki", "tempo"]);
    let grafana = texts(at(&compose, &["services", "grafana", "depends_on"]));
    for needed in ["prometheus", "loki", "tempo"] {
        assert!(
            grafana.iter().any(|s| s == needed),
            "grafana waits for {needed}"
        );
    }
}

#[test]
fn the_collector_sends_logs_to_loki_over_otlp_and_traces_to_tempo() {
    let collector = yaml("otel-collector.yaml");
    assert_eq!(
        text(&collector, &["exporters", "otlphttp/loki", "endpoint"]),
        "http://loki:3100/otlp"
    );
    assert_eq!(
        text(&collector, &["exporters", "otlp/tempo", "endpoint"]),
        "tempo:4317"
    );
    assert_eq!(
        at(&collector, &["exporters", "otlp/tempo", "tls", "insecure"]).as_bool(),
        Some(true)
    );
    for (pipeline, exporter) in [("logs", "otlphttp/loki"), ("traces", "otlp/tempo")] {
        let pipeline = at(&collector, &["service", "pipelines", pipeline]);
        assert_eq!(texts(at(pipeline, &["receivers"])), ["otlp"]);
        assert_eq!(texts(at(pipeline, &["exporters"])), [exporter]);
    }
    assert_eq!(
        texts(at(
            &collector,
            &["service", "pipelines", "metrics", "exporters"]
        )),
        ["prometheus"]
    );
}

#[test]
fn loki_keeps_structured_metadata_and_tempo_listens_beyond_localhost() {
    let loki = yaml("loki.yaml");
    assert_eq!(
        at(&loki, &["limits_config", "allow_structured_metadata"]).as_bool(),
        Some(true)
    );
    // Loki's default index labels include `service.instance.id`; only the service name is
    // an index label here, so a replica adds no stream.
    let resource = at(
        &loki,
        &["limits_config", "otlp_config", "resource_attributes"],
    );
    assert_eq!(at(resource, &["ignore_defaults"]).as_bool(), Some(true));
    let rules = at(resource, &["attributes_config"])
        .as_sequence()
        .expect("attribute rules");
    let indexed: Vec<String> = rules
        .iter()
        .filter(|rule| rule.get("action").and_then(Yaml::as_str) == Some("index_label"))
        .flat_map(|rule| texts(at(rule, &["attributes"])))
        .collect();
    assert_eq!(indexed, ["service.name"]);
    let tempo = yaml("tempo.yaml");
    assert_eq!(
        text(
            &tempo,
            &[
                "distributor",
                "receivers",
                "otlp",
                "protocols",
                "grpc",
                "endpoint"
            ]
        ),
        "0.0.0.0:4317"
    );
}

#[test]
fn grafana_opens_a_trace_from_its_log_line_and_the_log_lines_from_a_trace() {
    let sources = yaml("grafana/provisioning/datasources/datasources.yaml");
    let by_uid = |uid: &str| {
        at(&sources, &["datasources"])
            .as_sequence()
            .expect("a list")
            .iter()
            .find(|source| source.get("uid").and_then(Yaml::as_str) == Some(uid))
            .unwrap_or_else(|| panic!("a datasource with uid {uid}"))
            .clone()
    };
    let prometheus = by_uid("prometheus");
    assert_eq!(text(&prometheus, &["type"]), "prometheus");

    let loki = by_uid("loki");
    assert_eq!(text(&loki, &["type"]), "loki");
    assert_eq!(text(&loki, &["url"]), "http://loki:3100");
    let derived = at(&loki, &["jsonData", "derivedFields"])
        .as_sequence()
        .expect("derived fields");
    assert_eq!(derived.len(), 1);
    // Loki keeps the OTLP trace context as the structured metadata `trace_id`.
    assert_eq!(text(&derived[0], &["matcherType"]), "label");
    assert_eq!(text(&derived[0], &["matcherRegex"]), "trace_id");
    assert_eq!(text(&derived[0], &["datasourceUid"]), "tempo");

    let tempo = by_uid("tempo");
    assert_eq!(text(&tempo, &["type"]), "tempo");
    assert_eq!(text(&tempo, &["url"]), "http://tempo:3200");
    let to_logs = at(&tempo, &["jsonData", "tracesToLogsV2"]);
    assert_eq!(text(to_logs, &["datasourceUid"]), "loki");
    assert_eq!(at(to_logs, &["customQuery"]).as_bool(), Some(true));
    let query = text(to_logs, &["query"]);
    // Loki stores the OTLP attribute `record.id` as `record_id`; Tempo keeps `record.id`.
    // `$$` is a literal `$` in a provisioning file, which Grafana otherwise reads as an
    // environment variable.
    for needle in [
        r#"record_id="$${__span.tags["record.id"]}""#,
        r#"tenant="$${__span.tags["tenant"]}""#,
    ] {
        assert!(query.contains(needle), "`{needle}` missing from {query}");
    }
}

/// The tenant dashboard's panels, flattened out of rows.
fn tenant_panels() -> Vec<Json> {
    let dashboard: Json = serde_json::from_str(&deploy_config("grafana/dashboards/tenant.json"))
        .expect("tenant.json is JSON");
    assert_eq!(dashboard["uid"], "fusion-tenant");
    let variables = dashboard["templating"]["list"]
        .as_array()
        .expect("variables");
    let tenant = variables
        .iter()
        .find(|v| v["name"] == "tenant")
        .expect("a tenant variable");
    assert_eq!(tenant["multi"], false, "one tenant at a time");
    assert_eq!(tenant["includeAll"], false, "one tenant at a time");
    assert_eq!(
        tenant["query"]["query"],
        r#"label_values(records_in_total{stage="source"}, tenant)"#
    );
    assert_eq!(variables.len(), 1, "no stage or instance variable");
    let mut panels = Vec::new();
    let mut stack: Vec<Json> = dashboard["panels"].as_array().expect("panels").clone();
    while let Some(panel) = stack.pop() {
        if let Some(inner) = panel["panels"].as_array() {
            stack.extend(inner.iter().cloned());
        }
        panels.push(panel);
    }
    panels
}

fn exprs(panel: &Json) -> Vec<String> {
    panel["targets"]
        .as_array()
        .map(|targets| {
            targets
                .iter()
                .filter_map(|t| t["expr"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

/// Every metric name in a PromQL expression, each with its selector (empty when it has
/// none): the identifiers that end in `_total`, `_bucket`, `_count` or `_sum`, outside
/// quoted strings, `{...}` selectors and the label lists of `by`, `without`, `on`,
/// `ignoring`, `group_left` and `group_right`.
fn metric_names(expr: &str) -> Vec<(String, String)> {
    const SUFFIXES: [&str; 4] = ["_total", "_bucket", "_count", "_sum"];
    const GROUPINGS: [&str; 6] = [
        "by",
        "without",
        "on",
        "ignoring",
        "group_left",
        "group_right",
    ];
    let chars: Vec<char> = expr.chars().collect();
    let mut names = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '"' => {
                i += 1;
                while i < chars.len() && chars[i] != '"' {
                    i += if chars[i] == '\\' { 2 } else { 1 };
                }
                i += 1;
            }
            '{' => {
                while i < chars.len() && chars[i] != '}' {
                    i += 1;
                }
                i += 1;
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let name: String = chars[start..i].iter().collect();
                if GROUPINGS.contains(&name.as_str()) {
                    let mut j = i;
                    while chars.get(j).is_some_and(|c| c.is_whitespace()) {
                        j += 1;
                    }
                    if chars.get(j) == Some(&'(') {
                        i = chars[j..]
                            .iter()
                            .position(|c| *c == ')')
                            .map_or(chars.len(), |p| j + p + 1);
                    }
                } else if SUFFIXES.iter().any(|suffix| name.ends_with(suffix)) {
                    let selector = if chars.get(i) == Some(&'{') {
                        let close = chars[i..]
                            .iter()
                            .position(|c| *c == '}')
                            .map_or(chars.len(), |p| i + p);
                        chars[i + 1..close].iter().collect()
                    } else {
                        String::new()
                    };
                    names.push((name, selector));
                }
            }
            _ => i += 1,
        }
    }
    names
}

#[test]
fn metric_names_finds_bare_metrics_and_skips_labels_and_strings() {
    let names = |expr: &str| -> Vec<(String, String)> { metric_names(expr) };
    assert_eq!(
        names(
            r#"sum by (stage) (rate(records_out_total{tenant="$tenant"}[1m]) and on (tenant, stage) bytes_out_total)"#
        ),
        [
            (
                "records_out_total".to_owned(),
                r#"tenant="$tenant""#.to_owned()
            ),
            ("bytes_out_total".to_owned(), String::new()),
        ]
    );
    assert_eq!(
        names(r#"sum by (reason_count) (dlq_total{tenant="$tenant", stage="x_total"})"#),
        [(
            "dlq_total".to_owned(),
            r#"tenant="$tenant", stage="x_total""#.to_owned()
        )]
    );
    assert_eq!(names(r#"label_replace(up, "x", "y_total", "", "")"#), []);
}

#[test]
fn the_tenant_dashboard_reads_only_tenant_facing_metrics_for_the_chosen_tenant() {
    const ALLOWED: [&str; 7] = [
        "records_in_total",
        "records_out_total",
        "records_dropped_total",
        "bytes_in_total",
        "bytes_out_total",
        "pipeline_end_to_end_seconds_bucket",
        "dlq_total",
    ];
    let panels = tenant_panels();
    let mut seen = BTreeSet::new();
    for panel in &panels {
        for expr in exprs(panel) {
            let names = metric_names(&expr);
            assert!(!names.is_empty(), "`{expr}` selects no metric");
            for (name, selector) in names {
                assert!(ALLOWED.contains(&name.as_str()), "`{name}` in `{expr}`");
                assert!(
                    selector.contains(r#"tenant="$tenant""#),
                    "`{name}` is not filtered on the tenant in `{expr}`"
                );
                seen.insert(name);
            }
        }
    }
    assert_eq!(
        seen,
        ALLOWED.iter().map(|n| (*n).to_owned()).collect(),
        "every tenant metric has a panel"
    );
}

#[test]
fn the_tenant_dashboard_answers_where_did_my_logs_go_from_drops_alone() {
    let panels = tenant_panels();
    let drops: Vec<&Json> = panels
        .iter()
        .filter(|p| {
            let exprs = exprs(p);
            !exprs.is_empty()
                && exprs.iter().all(|e| {
                    metric_names(e)
                        .iter()
                        .all(|(name, _)| name == "records_dropped_total")
                })
        })
        .collect();
    assert!(
        drops.iter().any(|p| p["title"]
            .as_str()
            .is_some_and(|t| t.contains("Where did my logs go"))
            && exprs(p)
                .iter()
                .any(|e| e.contains("sum by (stage, reason)"))),
        "a `Where did my logs go` panel sums records_dropped_total by stage and reason"
    );

    let p99 = panels
        .iter()
        .flat_map(exprs)
        .find(|e| e.contains("pipeline_end_to_end_seconds_bucket"))
        .expect("an end-to-end panel");
    assert!(
        p99.starts_with("histogram_quantile(0.99, sum by (le) (rate("),
        "{p99}"
    );
    let outs: Vec<String> = panels
        .iter()
        .flat_map(exprs)
        .filter(|e| e.contains("records_out_total"))
        .collect();
    assert!(!outs.is_empty());
    for out in outs {
        assert!(
            out.contains("and on (tenant, stage)") && out.contains("bytes_out_total"),
            "records out is counted on sinks only: {out}"
        );
    }
}
