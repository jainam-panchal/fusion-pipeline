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

    assert!(
        matches!(err, ConfigError::Unreachable { ref node } if node == "island"),
        "{err}"
    );
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

    assert!(
        matches!(err, ConfigError::Unreachable { ref node } if node == "after_sink"),
        "{err}"
    );
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

const ROUTED: &str = r#"
nodes:
  - id: by_format
    type: route
    routes:
      linux: resource."log.format" == "Linux"
      apache: resource."log.format" == "Apache"
    default: other
  - id: linux_out
    type: sink.memory
    from: by_format.linux
  - id: apache_out
    type: sink.memory
    from: by_format.apache
  - id: other_out
    type: sink.memory
    from: by_format.other
"#;

#[test]
fn route_labels_are_edges_to_the_nodes_that_name_them() {
    let dag = dag(ROUTED).expect("valid dag");

    assert_eq!(dag.successors("source"), ["by_format"]);
    assert_eq!(
        dag.successors("by_format"),
        ["linux_out", "apache_out", "other_out"]
    );
    let index = dag.index_of("by_format").expect("route node present");
    let labels: Vec<Option<&str>> = dag
        .edges(index)
        .iter()
        .map(|e| e.label.as_deref())
        .collect();
    assert_eq!(labels, [Some("linux"), Some("apache"), Some("other")]);
}

#[test]
fn route_label_nobody_reads_is_rejected() {
    let yaml = r#"
nodes:
  - id: by_format
    type: route
    routes:
      linux: resource."log.format" == "Linux"
      apache: resource."log.format" == "Apache"
    default: linux
  - id: linux_out
    type: sink.memory
    from: by_format.linux
"#;

    let err = dag(yaml).expect_err("unconsumed label");

    assert!(
        matches!(err, ConfigError::UnconsumedRouteLabel { ref route, ref label } if route == "by_format" && label == "apache"),
        "{err}"
    );
}

#[test]
fn default_label_nobody_reads_is_rejected_too() {
    let yaml = ROUTED.replace("from: by_format.other", "from: by_format.linux");

    let err = dag(&yaml).expect_err("unconsumed default label");

    assert!(
        matches!(err, ConfigError::UnconsumedRouteLabel { ref route, ref label } if route == "by_format" && label == "other"),
        "{err}"
    );
}

#[test]
fn default_drop_needs_no_consumer() {
    let yaml = ROUTED
        .replace("default: other", "default: drop")
        .replace("from: by_format.other", "from: by_format.linux");

    let dag = dag(&yaml).expect("valid dag");

    assert_eq!(dag.successors("by_format").len(), 3);
}

#[test]
fn reading_a_label_the_route_does_not_declare_is_rejected() {
    let yaml = ROUTED.replace("from: by_format.other", "from: by_format.windows");

    let err = dag(&yaml).expect_err("unknown label");

    assert!(
        matches!(err, ConfigError::UnknownRouteLabel { ref node, ref route, ref label }
            if node == "other_out" && route == "by_format" && label == "windows"),
        "{err}"
    );
}

#[test]
fn reading_a_route_without_a_label_is_rejected() {
    let yaml = ROUTED.replace("from: by_format.other", "from: by_format");

    let err = dag(&yaml).expect_err("label required");

    assert!(
        matches!(err, ConfigError::RouteNeedsLabel { ref node, ref route } if node == "other_out" && route == "by_format"),
        "{err}"
    );
}

#[test]
fn reading_a_label_from_a_non_route_node_is_rejected() {
    let yaml = r#"
nodes:
  - id: keep_errors
    type: filter
  - id: out
    type: sink.memory
    from: keep_errors.yes
"#;

    let err = dag(yaml).expect_err("not a route");

    assert!(
        matches!(err, ConfigError::NotARoute { ref node, ref target, ref label }
            if node == "out" && target == "keep_errors" && label == "yes"),
        "{err}"
    );
}

#[test]
fn route_without_default_is_rejected_naming_the_node() {
    let yaml = ROUTED.replace("    default: other\n", "");

    let err = dag(&yaml).expect_err("default required");

    assert!(
        matches!(err, ConfigError::InvalidParams { ref node, ref message } if node == "by_format" && message.contains("default")),
        "{err}"
    );
}

#[test]
fn route_label_named_drop_is_rejected() {
    let yaml = ROUTED
        .replace("      apache:", "      drop:")
        .replace("from: by_format.apache", "from: by_format.drop");

    let err = dag(&yaml).expect_err("drop is reserved");

    assert!(
        matches!(err, ConfigError::InvalidParams { ref node, ref message } if node == "by_format" && message.contains("reserved")),
        "{err}"
    );
}

#[test]
fn reading_a_label_from_source_is_rejected() {
    let yaml = r#"
nodes:
  - id: keep_errors
    type: filter
    from: source.anything
  - id: out
    type: sink.memory
"#;

    let err = dag(yaml).expect_err("source is not a route");

    assert!(
        matches!(err, ConfigError::NotARoute { ref node, ref target, ref label }
            if node == "keep_errors" && target == "source" && label == "anything"),
        "{err}"
    );
}
