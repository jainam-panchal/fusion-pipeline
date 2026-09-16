//! The `route` stage's load-time contract: a condition that does not parse is refused naming
//! the node and the label. Behaviour over records (which label a record takes, the default,
//! first match wins) goes through the engine in the pipeline crate, since only core builds a
//! stage's context.

use fusion_core::config::Config;
use fusion_stages::Route;

#[test]
fn a_well_formed_route_builds() {
    let yaml = r#"
nodes:
  - id: by_format
    type: route
    routes:
      linux: resource.log.format == "Linux"
      acme: meta.tenant == "acme"
    default: drop
  - id: out
    type: sink.memory
"#;
    let config = Config::from_yaml(yaml).expect("config loads");
    Route::from_node(&config.nodes[0]).expect("route builds");
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
