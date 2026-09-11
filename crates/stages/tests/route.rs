//! The `route` stage through its public contract: a record in, `Routed(label, record)` or
//! `Drop(route_default_drop)` out.

use fusion_core::config::Config;
use fusion_core::record::{Record, RecordId};
use fusion_core::stage::{Context, DropReason, Stage, StageOutput};
use fusion_stages::Route;

fn route(yaml_params: &str) -> Route {
    let yaml = format!(
        "nodes:\n  - id: by_format\n    type: route\n{yaml_params}  - id: out\n    type: sink.memory\n"
    );
    let config = Config::from_yaml(&yaml).expect("config loads");
    Route::from_node(&config.nodes[0]).expect("route builds")
}

fn record(format: &str) -> Record {
    Record::from_json(&format!(
        r#"{{"id": 1, "body": "x", "resource": {{"log.format": "{format}"}}}}"#
    ))
    .expect("record parses")
}

const CTX: Context<'static> = Context {
    node_id: "by_format",
    record_id: RecordId(1),
};

const BY_FORMAT: &str = r#"    routes:
      linux: resource.log.format == "Linux"
      apache: resource.log.format == "Apache"
    default: other
"#;

#[test]
fn record_goes_down_the_label_whose_condition_matches() {
    let route = route(BY_FORMAT);

    let out = route.process(record("Apache"), &CTX);

    assert!(
        matches!(out, StageOutput::Routed(ref label, ref r) if label == "apache" && r.id == Some(RecordId(1))),
        "{out:?}"
    );
}

#[test]
fn unmatched_record_goes_down_the_default_label() {
    let route = route(BY_FORMAT);

    let out = route.process(record("Mac"), &CTX);

    assert!(
        matches!(out, StageOutput::Routed(ref label, _) if label == "other"),
        "{out:?}"
    );
}

#[test]
fn default_drop_drops_unmatched_records_with_route_default_drop() {
    let route = route(&BY_FORMAT.replace("default: other", "default: drop"));

    let out = route.process(record("Mac"), &CTX);

    assert!(
        matches!(out, StageOutput::Drop(DropReason::RouteDefaultDrop)),
        "{out:?}"
    );
}

#[test]
fn first_matching_route_wins_in_declaration_order() {
    let route = route(
        r#"    routes:
      any: resource.log.format != ""
      linux: resource.log.format == "Linux"
    default: drop
"#,
    );

    let out = route.process(record("Linux"), &CTX);

    assert!(
        matches!(out, StageOutput::Routed(ref label, _) if label == "any"),
        "{out:?}"
    );
}

#[test]
fn condition_that_does_not_parse_is_rejected_naming_the_node_and_label() {
    let yaml = r#"
nodes:
  - id: by_format
    type: route
    routes:
      linux: resource.log.format ==
    default: drop
  - id: out
    type: sink.memory
"#;
    let config = Config::from_yaml(yaml).expect("config loads");

    let err = Route::from_node(&config.nodes[0]).expect_err("bad condition");

    let message = err.to_string();
    assert!(
        message.contains("by_format") && message.contains("linux"),
        "{message}"
    );
}
