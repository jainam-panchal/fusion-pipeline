//! The `dedupe` stage's load-time contract: every bad parameter is rejected naming the node
//! and saying what to write instead. Behaviour over records is covered through the engine
//! in the pipeline crate.

use fusion_core::config::Config;
use fusion_stages::Dedupe;

fn build(params: &str) -> Result<Dedupe, String> {
    let yaml = format!(
        "nodes:\n  - id: dd\n    type: dedupe\n{params}  - id: out\n    type: sink.memory\n"
    );
    let config = Config::from_yaml(&yaml).expect("config loads");
    Dedupe::from_node(&config.nodes[0]).map_err(|e| e.to_string())
}

#[test]
fn a_well_formed_node_builds() {
    assert!(build("    key: [body]\n    window: 10s\n").is_ok());
    assert!(
        build("    key: [body, resource.host]\n    window: 500ms\n    on_state_error: nak\n")
            .is_ok()
    );
}

#[test]
fn an_empty_key_list_is_rejected_naming_the_node() {
    let err = build("    key: []\n    window: 10s\n").expect_err("rejected");

    assert!(err.contains("dd") && err.contains("key"), "{err}");
}

#[test]
fn a_key_path_that_does_not_parse_is_rejected_with_the_path_and_the_hint() {
    let err = build("    key: [attributes]\n    window: 10s\n").expect_err("rejected");

    assert!(err.contains("dd") && err.contains("attributes"), "{err}");
}

#[test]
fn a_window_without_a_unit_is_rejected_saying_which_units_exist() {
    let err = build("    key: [body]\n    window: 10\n").expect_err("rejected");

    assert!(
        err.contains("dd") && err.contains("ms") && err.contains("h"),
        "{err}"
    );
}

#[test]
fn a_window_under_one_millisecond_is_rejected() {
    let err = build("    key: [body]\n    window: 0s\n").expect_err("rejected");

    assert!(err.contains("dd") && err.contains("1ms"), "{err}");
}

#[test]
fn an_unknown_policy_is_rejected_naming_the_node() {
    let err = build("    key: [body]\n    window: 10s\n    on_state_error: drop\n")
        .expect_err("rejected");

    assert!(err.contains("dd") && err.contains("drop"), "{err}");
}

#[test]
fn a_missing_window_is_rejected_naming_the_node() {
    let err = build("    key: [body]\n").expect_err("rejected");

    assert!(err.contains("dd") && err.contains("window"), "{err}");
}
