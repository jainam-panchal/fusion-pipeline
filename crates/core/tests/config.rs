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
