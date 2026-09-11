//! Config loading through the public loader: YAML in, resolved node list or a load error out.

use fusion_core::config::{Config, ConfigError};

#[test]
fn nodes_load_with_explicit_from() {
    let yaml = r#"
nodes:
  - id: keep_errors
    type: filter
    from: source
    condition: severity_text == "ERROR"
    action: keep
  - id: out
    type: sink.memory
    from: keep_errors
"#;

    let config = Config::from_yaml(yaml).expect("config loads");

    let ids: Vec<&str> = config.nodes.iter().map(|n| n.id.as_str()).collect();
    assert_eq!(ids, ["keep_errors", "out"]);
    assert_eq!(config.nodes[0].kind, "filter");
    assert_eq!(config.nodes[0].from, ["source"]);
    assert_eq!(config.nodes[1].from, ["keep_errors"]);
}

#[test]
fn missing_from_defaults_to_previous_node_and_first_node_to_source() {
    let yaml = r#"
nodes:
  - id: keep_errors
    type: filter
    condition: severity_text == "ERROR"
    action: keep
  - id: out
    type: sink.memory
"#;

    let config = Config::from_yaml(yaml).expect("config loads");

    assert_eq!(config.nodes[0].from, ["source"]);
    assert_eq!(config.nodes[1].from, ["keep_errors"]);
}

#[test]
fn from_accepts_a_list_for_fan_in() {
    let yaml = r#"
nodes:
  - id: a
    type: filter
    condition: severity_text == "ERROR"
    action: keep
  - id: b
    type: filter
    from: source
    condition: severity_text == "WARN"
    action: keep
  - id: out
    type: sink.memory
    from: [a, b]
"#;

    let config = Config::from_yaml(yaml).expect("config loads");

    assert_eq!(config.nodes[2].from, ["a", "b"]);
}

#[test]
fn source_is_a_reserved_node_id() {
    let yaml = r#"
nodes:
  - id: source
    type: filter
    condition: severity_text == "ERROR"
    action: keep
  - id: out
    type: sink.memory
"#;

    let err = Config::from_yaml(yaml).expect_err("reserved id rejected");

    assert!(
        matches!(err, ConfigError::ReservedId { ref node } if node == "source"),
        "{err}"
    );
}

#[test]
fn duplicate_node_ids_are_rejected() {
    let yaml = r#"
nodes:
  - id: out
    type: filter
    condition: severity_text == "ERROR"
    action: keep
  - id: out
    type: sink.memory
"#;

    let err = Config::from_yaml(yaml).expect_err("duplicate rejected");

    assert!(
        matches!(err, ConfigError::DuplicateId { ref node } if node == "out"),
        "{err}"
    );
}

#[test]
fn source_block_loads_with_its_type_and_params() {
    let yaml = r#"
source:
  type: nats
  url: nats://localhost:4222
  stream: LOGS
  consumer: pipeline
nodes:
  - id: out
    type: sink.memory
"#;

    let config = Config::from_yaml(yaml).expect("config loads");

    let source = config.source.expect("source block present");
    assert_eq!(source.kind, "nats");
    let params: std::collections::BTreeMap<String, String> =
        source.parse_params().expect("params parse");
    assert_eq!(params["url"], "nats://localhost:4222");
    assert_eq!(params["stream"], "LOGS");
    assert_eq!(params["consumer"], "pipeline");
}

#[test]
fn source_block_is_optional() {
    let yaml = r#"
nodes:
  - id: out
    type: sink.memory
"#;

    let config = Config::from_yaml(yaml).expect("config loads");

    assert!(config.source.is_none());
}

#[test]
fn node_id_with_a_dot_is_rejected_because_from_uses_dots_for_route_labels() {
    let yaml = r#"
nodes:
  - id: by.format
    type: route
    routes:
      linux: resource["log.format"] == "Linux"
    default: drop
  - id: out
    type: sink.memory
    from: by.format.linux
"#;

    let err = Config::from_yaml(yaml).expect_err("dotted id rejected");

    assert!(
        matches!(err, ConfigError::DottedId { ref node } if node == "by.format"),
        "{err}"
    );
}
