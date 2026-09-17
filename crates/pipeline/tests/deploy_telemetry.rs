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

/// One label matcher of a selector: label, operator (`=`, `!=`, `=~`, `!~`) and unquoted
/// value.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Matcher {
    label: String,
    op: String,
    value: String,
}

impl Matcher {
    fn new(label: &str, op: &str, value: &str) -> Self {
        Self {
            label: label.to_owned(),
            op: op.to_owned(),
            value: value.to_owned(),
        }
    }
}

/// One series a PromQL expression reads: its name and the matchers of its selector (none
/// when it has no selector).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Series {
    name: String,
    matchers: Vec<Matcher>,
}

/// Grouping keywords, whose parenthesised list holds label names.
const GROUPINGS: [&str; 6] = [
    "by",
    "without",
    "on",
    "ignoring",
    "group_left",
    "group_right",
];

/// Operators, aggregations (which may be followed by `by (...)` rather than `(`) and the
/// number literals. Matched in any case.
const KEYWORDS: [&str; 20] = [
    "and",
    "or",
    "unless",
    "bool",
    "offset",
    "atan2",
    "sum",
    "avg",
    "min",
    "max",
    "count",
    "group",
    "stddev",
    "stdvar",
    "topk",
    "bottomk",
    "quantile",
    "count_values",
    "nan",
    "inf",
];

/// A position in a PromQL expression, with the few moves both scanners need. Every move
/// that expects a closing character fails rather than running off the end.
struct Cursor {
    chars: Vec<char>,
    at: usize,
}

impl Cursor {
    fn new(text: &str) -> Self {
        Self {
            chars: text.chars().collect(),
            at: 0,
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.at).copied()
    }

    fn skip_spaces(&mut self) {
        self.take_while(char::is_whitespace);
    }

    fn take_while(&mut self, keep: impl Fn(char) -> bool) -> String {
        let start = self.at;
        while self.peek().is_some_and(&keep) {
            self.at += 1;
        }
        self.chars[start..self.at].iter().collect()
    }

    /// An identifier: an ASCII letter or `_`, then letters, digits, `_` and `:`.
    fn identifier(&mut self) -> String {
        self.take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | ':'))
    }

    /// The text of the string opening here (`"`, `'` or a raw backtick string), consumed.
    fn quoted(&mut self) -> Result<String, String> {
        let quote = self.peek().ok_or("expected a string")?;
        let start = self.at;
        self.at += 1;
        let mut text = String::new();
        loop {
            match self.peek() {
                None => return Err(format!("unclosed string at {start}")),
                Some(c) if c == quote => break,
                Some('\\') if quote != '`' => {
                    self.at += 1;
                    text.extend(self.peek());
                }
                Some(c) => text.push(c),
            }
            self.at += 1;
        }
        self.at += 1;
        Ok(text)
    }

    /// The text up to the `close` matching the `open` here, consumed with both ends,
    /// skipping strings.
    fn delimited(&mut self, close: char) -> Result<String, String> {
        let start = self.at;
        self.at += 1;
        loop {
            match self.peek() {
                None => return Err(format!("unclosed `{}` at {start}", self.chars[start])),
                Some(c) if c == close => break,
                Some('"' | '\'' | '`') => {
                    self.quoted()?;
                }
                Some(_) => self.at += 1,
            }
        }
        self.at += 1;
        Ok(self.chars[start + 1..self.at - 1].iter().collect())
    }
}

/// Every series a PromQL expression reads: every identifier that is not a function
/// (followed by `(`), an operator, aggregation or number literal (in any case), part of a
/// number or duration, or a dashboard variable (`$x`, `${x}`), and is not inside a string, a
/// `[...]` range or the label list of a grouping keyword; and every selector with no name
/// in front, named by its exact `__name__` matcher or its quoted name, else by its own text.
///
/// # Errors
///
/// An unclosed string, selector, range or list, unbalanced parentheses, a series named both
/// in front of its selector and inside it, or a character PromQL does not have (a name
/// starting with a non-ASCII letter), so a malformed dashboard query fails the test instead
/// of passing or panicking.
fn metric_names(expr: &str) -> Result<Vec<Series>, String> {
    let mut cursor = Cursor::new(expr);
    let mut found = Vec::new();
    let mut depth = 0_usize;
    while let Some(c) = cursor.peek() {
        match c {
            '"' | '\'' | '`' => {
                cursor.quoted()?;
            }
            '{' => {
                let text = cursor.delimited('}')?;
                let matchers = matchers(&text)?;
                let name = matchers
                    .iter()
                    .find(|m| m.label == "__name__" && m.op == "=")
                    .map_or_else(|| format!("{{{text}}}"), |m| m.value.clone());
                found.push(Series { name, matchers });
            }
            '[' => {
                cursor.delimited(']')?;
            }
            '(' => {
                depth += 1;
                cursor.at += 1;
            }
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| format!("unopened `)` at {}", cursor.at))?;
                cursor.at += 1;
            }
            '$' => {
                cursor.at += 1;
                if cursor.peek() == Some('{') {
                    cursor.delimited('}')?;
                } else {
                    cursor.identifier();
                }
            }
            // A number or a duration (`5m`, `1e-3`), letters and all.
            c if c.is_ascii_digit() || c == '.' => {
                cursor.take_while(|c| c.is_ascii_alphanumeric() || c == '.');
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let name = cursor.identifier();
                let keyword = name.to_ascii_lowercase();
                cursor.skip_spaces();
                let next = cursor.peek();
                if GROUPINGS.contains(&keyword.as_str()) {
                    if next == Some('(') {
                        cursor.delimited(')')?;
                    }
                } else if next == Some('(') || KEYWORDS.contains(&keyword.as_str()) {
                    // A function call, an operator keyword or a number literal.
                } else {
                    let matchers = if next == Some('{') {
                        matchers(&cursor.delimited('}')?)?
                    } else {
                        Vec::new()
                    };
                    if matchers.iter().any(|m| m.label == "__name__") {
                        return Err(format!("`{name}` is named twice"));
                    }
                    found.push(Series { name, matchers });
                }
            }
            c if c.is_whitespace() || "+-*/%^=!<>,@:".contains(c) => cursor.at += 1,
            other => return Err(format!("unexpected `{other}` at {}", cursor.at)),
        }
    }
    if depth > 0 {
        return Err(format!("{depth} unclosed `(`"));
    }
    Ok(found)
}

/// The label matchers of a selector's inside. A quoted string standing alone is the series
/// name (Prometheus 3), read as an exact `__name__` matcher; a label may also be quoted.
///
/// # Errors
///
/// Anything that is not a comma-separated list of matchers.
fn matchers(selector: &str) -> Result<Vec<Matcher>, String> {
    let mut cursor = Cursor::new(selector);
    let mut found = Vec::new();
    loop {
        cursor.skip_spaces();
        let (label, quoted) = match cursor.peek() {
            None => break,
            Some('"' | '\'' | '`') => (cursor.quoted()?, true),
            Some(c) if c.is_ascii_alphabetic() || c == '_' => (cursor.identifier(), false),
            Some(other) => return Err(format!("unexpected `{other}` in `{{{selector}}}`")),
        };
        cursor.skip_spaces();
        let op = cursor.take_while(|c| matches!(c, '=' | '!' | '~'));
        if op.is_empty() {
            if !quoted {
                return Err(format!("`{label}` has no operator in `{{{selector}}}`"));
            }
            found.push(Matcher::new("__name__", "=", &label));
        } else {
            if !["=", "!=", "=~", "!~"].contains(&op.as_str()) {
                return Err(format!("operator `{op}` in `{{{selector}}}`"));
            }
            cursor.skip_spaces();
            if !matches!(cursor.peek(), Some('"' | '\'' | '`')) {
                return Err(format!("unquoted value in `{{{selector}}}`"));
            }
            let value = cursor.quoted()?;
            found.push(Matcher::new(&label, &op, &value));
        }
        cursor.skip_spaces();
        match cursor.peek() {
            None => break,
            Some(',') => cursor.at += 1,
            Some(other) => return Err(format!("unexpected `{other}` in `{{{selector}}}`")),
        }
    }
    Ok(found)
}

/// Whether a selector keeps only the dashboard's chosen tenant: it has `tenant="$tenant"`,
/// and every other matcher on `tenant` is that one.
fn filters_on_the_tenant(matchers: &[Matcher]) -> bool {
    let chosen = Matcher::new("tenant", "=", "$tenant");
    let on_tenant: Vec<&Matcher> = matchers.iter().filter(|m| m.label == "tenant").collect();
    !on_tenant.is_empty() && on_tenant.iter().all(|m| **m == chosen)
}

/// `metric_names` for a dashboard query, failing the test on a malformed one.
fn series_of(expr: &str) -> Vec<Series> {
    metric_names(expr).unwrap_or_else(|err| panic!("`{expr}` does not scan: {err}"))
}

fn names(expr: &str) -> Vec<String> {
    series_of(expr).into_iter().map(|s| s.name).collect()
}

#[test]
fn metric_names_finds_every_series_and_skips_labels_strings_and_variables() {
    assert_eq!(
        names(
            r#"sum by (stage) (rate(records_out_total{tenant="$tenant"}[1m]) and on (tenant, stage) bytes_out_total)"#
        ),
        ["records_out_total", "bytes_out_total"]
    );
    assert_eq!(
        series_of(r#"rate(sink_total{stage="a}b_total"}[1m])"#),
        [Series {
            name: "sink_total".to_owned(),
            matchers: vec![Matcher::new("stage", "=", "a}b_total")],
        }]
    );
    assert_eq!(
        names(r#"sum by (reason_count) (dlq_total{tenant="$tenant", stage="x_total"})"#),
        ["dlq_total"]
    );
    // Any series counts, not only the pipeline's; functions, keywords, strings, ranges and
    // dashboard variables do not.
    assert_eq!(
        names(r#"label_replace(up, "x", 'y_total', "", "") > bool 0"#),
        ["up"]
    );
    assert_eq!(
        names(r#"sum(increase(gnatsd_varz_mem[$__range] offset -5m)) or vector(0) @ start()"#),
        ["gnatsd_varz_mem"]
    );
    assert_eq!(
        names(r#"sum without(x) (a_total[5m:1m]) * on() group_left b_total / ${__range}"#),
        ["a_total", "b_total"]
    );
    assert_eq!(
        names(r"SUM BY (stage) (dlq_total{stage='a}b'}) > NaN OR Inf + 1e-3"),
        ["dlq_total"]
    );
    assert_eq!(
        series_of(r#"x_total{stage="a\"}b"}"#)[0].matchers,
        [Matcher::new("stage", "=", r#"a"}b"#)]
    );
}

#[test]
fn a_selector_with_no_name_in_front_is_named_by_its_name_matcher() {
    assert_eq!(
        names(r#"sum({__name__="state_ops_total", tenant="$tenant"})"#),
        ["state_ops_total"]
    );
    // A quoted name (Prometheus 3) is the series name; a quoted label is a label.
    assert_eq!(
        series_of(r#"{"a.b", "tenant"="$tenant"}"#),
        [Series {
            name: "a.b".to_owned(),
            matchers: vec![
                Matcher::new("__name__", "=", "a.b"),
                Matcher::new("tenant", "=", "$tenant"),
            ],
        }]
    );
    // Without an exact name the selector itself stands as the name, which no allow-list
    // holds.
    assert_eq!(
        names(r#"{__name__=~"state_.*"}"#),
        [r#"{__name__=~"state_.*"}"#]
    );
}

#[test]
fn a_malformed_query_is_an_error_not_a_panic() {
    for bad in [
        "x{",
        "{",
        r#"x{a="b"} or {"#,
        r#"x{a="b}"#,
        r#"x{"a.b", tenant="$tenant}"#,
        "rate(x[5m)",
        "sum by (stage x",
        "ümlaut_total",
        r#"x{a}b="c"}"#,
        "x{a=b}",
        "x{a}",
        r#"x{a=="b"}"#,
        // Parentheses must balance.
        "rate(x[5m]",
        "sum(a_total))",
        ")a_total(",
        // A name in front leaves no room for another inside.
        r#"x_total{"y_total"}"#,
        r#"x_total{__name__="y_total"}"#,
    ] {
        assert!(metric_names(bad).is_err(), "`{bad}` scanned");
    }
    assert!(matchers(r#"a=""#).is_err());
}

#[test]
fn only_an_exact_tenant_matcher_filters_on_the_tenant() {
    let filters = |selector: &str| filters_on_the_tenant(&matchers(selector).expect("parses"));
    assert!(filters(r#"stage="x", tenant="$tenant""#));
    assert!(!filters(r#"xtenant="$tenant""#));
    assert!(!filters(r#"tenant!="$tenant""#));
    assert!(!filters(r#"tenant=~"$tenant""#));
    assert!(!filters(r#"tenant="$tenant", tenant!="$tenant""#));
    assert!(!filters(""));
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
            let series = series_of(&expr);
            assert!(!series.is_empty(), "`{expr}` selects no metric");
            for Series { name, matchers } in series {
                assert!(ALLOWED.contains(&name.as_str()), "`{name}` in `{expr}`");
                assert!(
                    filters_on_the_tenant(&matchers),
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
                && exprs
                    .iter()
                    .all(|e| names(e).iter().all(|name| name == "records_dropped_total"))
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
