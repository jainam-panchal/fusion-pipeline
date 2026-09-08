use pipeline_core::config::{load_str, ConfigError};

const LINEAR: &str = r#"
nodes:
  - id: keep_errors
    type: filter
    condition: 'severity_text == "ERROR"'
    action: keep
  - id: out
    type: sink.memory
"#;

#[test]
fn node_without_from_reads_from_previous_node() {
    let cfg = load_str(LINEAR).unwrap();
    assert_eq!(cfg.inputs_of("keep_errors"), vec!["source"]);
    assert_eq!(cfg.inputs_of("out"), vec!["keep_errors"]);
}

#[test]
fn explicit_from_is_kept() {
    let yaml = r#"
nodes:
  - id: a
    type: filter
    condition: 'severity_text == "ERROR"'
    action: keep
  - id: b
    type: filter
    condition: 'severity_text == "WARN"'
    action: keep
    from: source
  - id: out
    type: sink.memory
    from: [a, b]
"#;
    let cfg = load_str(yaml).unwrap();
    assert_eq!(cfg.inputs_of("b"), vec!["source"]);
    assert_eq!(cfg.inputs_of("out"), vec!["a", "b"]);
}

#[test]
fn source_is_a_reserved_node_id() {
    let yaml = r#"
nodes:
  - id: source
    type: filter
    condition: 'severity_text == "ERROR"'
    action: keep
  - id: out
    type: sink.memory
"#;
    match load_str(yaml) {
        Err(ConfigError::ReservedId { node }) => assert_eq!(node, "source"),
        other => panic!("expected ReservedId, got {other:?}"),
    }
}

#[test]
fn rejects_from_target_that_does_not_exist() {
    let yaml = r#"
nodes:
  - id: a
    type: filter
    condition: 'severity_text == "ERROR"'
    action: keep
    from: nope
  - id: out
    type: sink.memory
"#;
    match load_str(yaml) {
        Err(ConfigError::UnknownFrom { node, target }) => {
            assert_eq!(node, "a");
            assert_eq!(target, "nope");
        }
        other => panic!("expected UnknownFrom, got {other:?}"),
    }
}

#[test]
fn rejects_cycle_naming_a_node_on_it() {
    let yaml = r#"
nodes:
  - id: a
    type: filter
    condition: 'severity_text == "ERROR"'
    action: keep
    from: [source, b]
  - id: b
    type: filter
    condition: 'severity_text == "WARN"'
    action: keep
    from: a
  - id: out
    type: sink.memory
    from: b
"#;
    match load_str(yaml) {
        Err(ConfigError::Cycle { node }) => assert!(node == "a" || node == "b", "got {node}"),
        other => panic!("expected Cycle, got {other:?}"),
    }
}

#[test]
fn rejects_unreachable_island() {
    let yaml = r#"
nodes:
  - id: a
    type: filter
    condition: 'severity_text == "ERROR"'
    action: keep
  - id: out
    type: sink.memory
  - id: island
    type: filter
    condition: 'severity_text == "WARN"'
    action: keep
    from: island2
  - id: island2
    type: filter
    condition: 'severity_text == "WARN"'
    action: keep
    from: island
"#;
    match load_str(yaml) {
        Err(ConfigError::Unreachable { node }) => assert!(node.starts_with("island"), "got {node}"),
        other => panic!("expected Unreachable, got {other:?}"),
    }
}

#[test]
fn rejects_node_no_path_from_source_reaches() {
    // A node whose only input is a sink: nothing flows out of a sink, so it
    // is unreachable even though its `from` target exists.
    let yaml = r#"
nodes:
  - id: a
    type: filter
    condition: 'severity_text == "ERROR"'
    action: keep
  - id: out
    type: sink.memory
  - id: after_sink
    type: filter
    condition: 'severity_text == "WARN"'
    action: keep
    from: out
"#;
    match load_str(yaml) {
        Err(ConfigError::Unreachable { node }) => assert_eq!(node, "after_sink"),
        other => panic!("expected Unreachable, got {other:?}"),
    }
}

#[test]
fn rejects_config_with_no_sink() {
    let yaml = r#"
nodes:
  - id: a
    type: filter
    condition: 'severity_text == "ERROR"'
    action: keep
"#;
    assert_eq!(load_str(yaml).unwrap_err(), ConfigError::NoSink);
}
