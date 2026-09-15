//! The `sample` stage's load-time contract: every bad parameter is rejected naming the node
//! and saying what to write instead. Behaviour over records is covered through the engine
//! in the pipeline crate.

use fusion_core::config::Config;
use fusion_stages::Sample;

fn build(params: &str) -> Result<Sample, String> {
    let yaml = format!(
        "nodes:\n  - id: keep_some\n    type: sample\n{params}  - id: out\n    type: sink.memory\n"
    );
    let config = Config::from_yaml(&yaml).expect("config loads");
    Sample::from_node(&config.nodes[0]).map_err(|e| e.to_string())
}

#[test]
fn a_well_formed_node_builds_in_each_mode() {
    assert!(build("    mode: random\n    percent: 10\n").is_ok());
    assert!(build("    mode: random\n    percent: 0.5\n").is_ok());
    assert!(build("    mode: random\n    percent: 100\n").is_ok());
    assert!(build("    mode: every_nth\n    n: 10\n").is_ok());
    assert!(build("    mode: every_nth\n    n: 1\n    on_state_error: nak\n").is_ok());
    assert!(build("    mode: consistent\n    percent: 50\n    key: [resource.host]\n").is_ok());
    assert!(
        build("    mode: consistent\n    percent: 50\n    key: [resource.host, attributes.pod]\n")
            .is_ok()
    );
}

#[test]
fn a_missing_mode_is_rejected_listing_the_modes() {
    let err = build("    percent: 10\n").expect_err("rejected");

    assert!(
        err.contains("keep_some")
            && err.contains("mode")
            && err.contains("random")
            && err.contains("every_nth")
            && err.contains("consistent"),
        "{err}"
    );
}

#[test]
fn an_unknown_mode_is_rejected_listing_the_modes() {
    let err = build("    mode: weighted\n    percent: 10\n").expect_err("rejected");

    assert!(
        err.contains("keep_some") && err.contains("weighted") && err.contains("every_nth"),
        "{err}"
    );
}

#[test]
fn random_without_percent_is_rejected_naming_the_field() {
    let err = build("    mode: random\n").expect_err("rejected");

    assert!(
        err.contains("keep_some") && err.contains("percent"),
        "{err}"
    );
}

#[test]
fn a_percent_outside_zero_exclusive_to_one_hundred_is_rejected() {
    for percent in ["0", "-5", "100.5", "200"] {
        let err =
            build(&format!("    mode: random\n    percent: {percent}\n")).expect_err("rejected");

        assert!(
            err.contains("keep_some") && err.contains("percent") && err.contains("100"),
            "percent {percent}: {err}"
        );
    }
}

#[test]
fn every_nth_without_n_is_rejected_naming_the_field() {
    let err = build("    mode: every_nth\n").expect_err("rejected");

    assert!(err.contains("keep_some") && err.contains("`n`"), "{err}");
}

#[test]
fn an_n_of_zero_is_rejected() {
    let err = build("    mode: every_nth\n    n: 0\n").expect_err("rejected");

    assert!(err.contains("keep_some") && err.contains("`n`"), "{err}");
}

#[test]
fn an_n_above_i64_is_rejected_because_the_store_counts_in_i64() {
    let err = build("    mode: every_nth\n    n: 18446744073709551615\n").expect_err("rejected");

    assert!(
        err.contains("keep_some") && err.contains("`n`") && err.contains("large"),
        "{err}"
    );
}

#[test]
fn consistent_without_a_key_is_rejected_naming_the_field() {
    let err = build("    mode: consistent\n    percent: 50\n").expect_err("rejected");

    assert!(err.contains("keep_some") && err.contains("key"), "{err}");
}

#[test]
fn an_empty_key_list_is_rejected_naming_the_field() {
    let err = build("    mode: consistent\n    percent: 50\n    key: []\n").expect_err("rejected");

    assert!(err.contains("keep_some") && err.contains("key"), "{err}");
}

#[test]
fn a_key_path_that_does_not_parse_is_rejected_with_the_path() {
    let err = build("    mode: consistent\n    percent: 50\n    key: [attributes]\n")
        .expect_err("rejected");

    assert!(
        err.contains("keep_some") && err.contains("attributes"),
        "{err}"
    );
}

#[test]
fn a_field_of_another_mode_is_rejected_naming_the_mode_it_belongs_to() {
    let err = build("    mode: random\n    percent: 10\n    n: 3\n").expect_err("rejected");
    assert!(
        err.contains("keep_some") && err.contains("`n`") && err.contains("every_nth"),
        "{err}"
    );

    let err = build("    mode: random\n    percent: 10\n    key: [body]\n").expect_err("rejected");
    assert!(
        err.contains("keep_some") && err.contains("key") && err.contains("consistent"),
        "{err}"
    );

    let err = build("    mode: every_nth\n    n: 3\n    percent: 10\n").expect_err("rejected");
    assert!(
        err.contains("keep_some") && err.contains("percent"),
        "{err}"
    );

    let err =
        build("    mode: consistent\n    percent: 10\n    key: [body]\n    on_state_error: pass\n")
            .expect_err("rejected");
    assert!(
        err.contains("keep_some") && err.contains("on_state_error") && err.contains("every_nth"),
        "{err}"
    );
}

#[test]
fn an_unknown_policy_is_rejected_naming_the_node() {
    let err =
        build("    mode: every_nth\n    n: 3\n    on_state_error: drop\n").expect_err("rejected");

    assert!(err.contains("keep_some") && err.contains("drop"), "{err}");
}

#[test]
fn an_unknown_field_is_rejected_naming_the_node() {
    let err = build("    mode: random\n    percent: 10\n    seed: 4\n").expect_err("rejected");

    assert!(err.contains("keep_some") && err.contains("seed"), "{err}");
}
