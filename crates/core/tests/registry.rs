//! Registry through its public surface: source factories keyed by the `source` block's type.

use fusion_core::config::{Config, ConfigError, NodeConfig};
use fusion_core::io::{Intake, Source, SourceError};
use fusion_core::memory::MemorySinks;
use fusion_core::registry::Registry;
use fusion_core::stage::Stage;

struct NoopSource;

impl Source for NoopSource {
    fn run(self: Box<Self>, _intake: Intake) -> Result<(), SourceError> {
        Ok(())
    }
}

fn load(yaml: &str) -> fusion_core::config::SourceConfig {
    Config::from_yaml(yaml)
        .expect("config loads")
        .source
        .expect("source block present")
}

#[test]
fn registered_source_type_builds_from_the_source_block() {
    let mut registry = Registry::new();
    registry.register_source("nats", |source: &fusion_core::config::SourceConfig| {
        let url: String =
            source.parse_params::<std::collections::BTreeMap<String, String>>()?["url"].clone();
        assert_eq!(url, "nats://localhost:4222");
        Ok(Box::new(NoopSource) as Box<dyn Source>)
    });
    let source = load("source:\n  type: nats\n  url: nats://localhost:4222\nnodes: []\n");

    assert!(registry.build_source(&source).is_ok());
}

#[test]
fn unregistered_source_type_is_an_unknown_type_error_naming_source() {
    let registry = Registry::new();
    let source = load("source:\n  type: kafka\nnodes: []\n");

    let Err(err) = registry.build_source(&source) else {
        panic!("unknown type must be rejected");
    };

    assert!(
        matches!(err, ConfigError::UnknownType { ref node, ref kind } if node == "source" && kind == "kafka"),
        "{err}"
    );
}

#[test]
fn stage_types_lists_every_registered_stage_type_in_name_order() {
    let mut registry = Registry::new();
    let build =
        |_: &NodeConfig| -> Result<Box<dyn Stage>, ConfigError> { unreachable!("never built") };
    registry.register_stage("route", build);
    registry.register_stage("filter", build);
    registry.register_sink("sink.memory", MemorySinks::new());

    assert_eq!(
        registry.stage_types().collect::<Vec<_>>(),
        ["filter", "route"]
    );
}
