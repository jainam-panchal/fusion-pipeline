//! Endpoint resolution: the environment overrides the YAML, and both fall back to the
//! default local server.

use fusion_nats::config::{DEFAULT_URL, resolve_url};

#[test]
fn env_url_overrides_yaml_url() {
    assert_eq!(
        resolve_url(Some("nats://yaml:4222"), Some("nats://env:4222")),
        "nats://env:4222"
    );
}

#[test]
fn yaml_url_is_used_without_env() {
    assert_eq!(
        resolve_url(Some("nats://yaml:4222"), None),
        "nats://yaml:4222"
    );
}

#[test]
fn empty_env_does_not_override() {
    assert_eq!(
        resolve_url(Some("nats://yaml:4222"), Some("")),
        "nats://yaml:4222"
    );
}

#[test]
fn neither_falls_back_to_the_default() {
    assert_eq!(resolve_url(None, None), DEFAULT_URL);
    assert_eq!(DEFAULT_URL, "nats://127.0.0.1:4222");
}
