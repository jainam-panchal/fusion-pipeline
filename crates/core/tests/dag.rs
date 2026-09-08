//! Load-time graph validation: every rejection names the offending node.

use fusion_core::config::{Config, ConfigError};
use fusion_core::dag::Dag;

fn dag(yaml: &str) -> Result<Dag, ConfigError> {
    let config = Config::from_yaml(yaml)?;
    Dag::from_config(&config)
}

#[test]
fn linear_pipeline_is_accepted_in_file_order() {
    let yaml = r#"
nodes:
  - id: keep_errors
    type: filter
  - id: out
    type: sink.memory
"#;

    let dag = dag(yaml).expect("valid dag");

    let order: Vec<&str> = dag.order().iter().map(|n| n.id.as_str()).collect();
    assert_eq!(order, ["keep_errors", "out"]);
    assert_eq!(dag.successors("source"), ["keep_errors"]);
    assert_eq!(dag.successors("keep_errors"), ["out"]);
    assert!(dag.successors("out").is_empty());
}

#[test]
fn from_target_that_does_not_exist_is_rejected() {
    let yaml = r#"
nodes:
  - id: keep_errors
    type: filter
    from: nope
  - id: out
    type: sink.memory
"#;

    let err = dag(yaml).expect_err("unknown from");

    assert!(
        matches!(err, ConfigError::UnknownFrom { ref node, ref target } if node == "keep_errors" && target == "nope"),
        "{err}"
    );
}

#[test]
fn cycle_is_rejected_naming_a_node_on_it() {
    let yaml = r#"
nodes:
  - id: a
    type: filter
    from: [source, c]
  - id: b
    type: filter
    from: a
  - id: c
    type: filter
    from: b
  - id: out
    type: sink.memory
    from: c
"#;

    let err = dag(yaml).expect_err("cycle");

    assert!(
        matches!(err, ConfigError::Cycle { ref node } if ["a", "b", "c"].contains(&node.as_str())),
        "{err}"
    );
}

#[test]
fn island_cycle_is_reported_as_unreachable_naming_the_first_island_node() {
    let yaml = r#"
nodes:
  - id: keep_errors
    type: filter
  - id: out
    type: sink.memory
  - id: island
    type: filter
    from: island_peer
  - id: island_peer
    type: filter
    from: island
"#;

    let err = dag(yaml).expect_err("unreachable");

    assert!(matches!(err, ConfigError::Unreachable { ref node } if node == "island"), "{err}");
}

#[test]
fn node_reading_from_a_sink_is_unreachable_because_sinks_emit_nothing() {
    let yaml = r#"
nodes:
  - id: keep_errors
    type: filter
  - id: out
    type: sink.memory
  - id: after_sink
    type: filter
    from: out
  - id: out2
    type: sink.memory
"#;

    let err = dag(yaml).expect_err("unreachable");

    assert!(matches!(err, ConfigError::Unreachable { ref node } if node == "after_sink"), "{err}");
}

#[test]
fn pipeline_without_a_sink_is_rejected() {
    let yaml = r#"
nodes:
  - id: keep_errors
    type: filter
"#;

    let err = dag(yaml).expect_err("no sink");

    assert!(matches!(err, ConfigError::NoSink), "{err}");
}
