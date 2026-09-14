//! Where the store URL comes from: `DRAGONFLY_URL`, else the compose port. A malformed URL
//! is covered through the binary in the pipeline crate's `cli.rs`.

use fusion_state::{DEFAULT_URL, resolve_url};

#[test]
fn env_url_wins() {
    assert_eq!(
        resolve_url(Some("redis://dragonfly:6379")),
        "redis://dragonfly:6379"
    );
}

#[test]
fn no_env_means_the_compose_port() {
    assert_eq!(resolve_url(None), DEFAULT_URL);
    assert_eq!(DEFAULT_URL, "redis://127.0.0.1:6379");
}

#[test]
fn empty_env_does_not_count() {
    assert_eq!(resolve_url(Some("")), DEFAULT_URL);
}
