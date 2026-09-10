//! Registry through its public surface: source factories keyed by the `source` block's type.

use fusion_core::config::{Config, ConfigError};
use fusion_core::io::{Intake, Source, SourceError};
use fusion_core::registry::Registry;

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
        let url: String = source.parse_params::<std::collections::BTreeMap<String, String>>()?
            ["url"]
            .clone();
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
