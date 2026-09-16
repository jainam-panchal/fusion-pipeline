//! The `edit` stage's load-time contract: the issue's example builds, and every bad op is
//! rejected naming the node, the op's position and what to write instead. Behaviour over
//! records is covered through the engine in the pipeline crate.

use fusion_core::config::Config;
use fusion_stages::Edit;

fn build(params: &str) -> Result<Edit, String> {
    let yaml = format!(
        "nodes:\n  - id: normalise\n    type: edit\n{params}  - id: out\n    type: sink.memory\n"
    );
    let config = Config::from_yaml(&yaml).expect("config loads");
    Edit::from_node(&config.nodes[0]).map_err(|e| e.to_string())
}

const EXAMPLE: &str = "    ops:
      - set:    { field: resource.env, value: prod }
      - rename: { from: attributes.http.path, to: attributes.http.route }
      - copy:   { from: body, to: attributes.raw }
      - hash:   { field: attributes.user.email }
      - delete: { fields: [attributes.debug] }
";

#[test]
fn the_issue_example_builds() {
    build(EXAMPLE).expect("the example from issue #21 builds");
}

/// Every rejection names the node and the op's position, and says what to write instead.
fn rejects(params: &str, index: &str, hints: &[&str]) {
    let err = build(params).expect_err("rejected");
    assert!(err.contains("normalise"), "names the node: {err}");
    assert!(
        err.contains(index),
        "names the op position `{index}`: {err}"
    );
    for hint in hints {
        assert!(err.contains(hint), "says `{hint}`: {err}");
    }
}

#[test]
fn an_unknown_op_is_rejected_listing_the_ops() {
    rejects(
        "    ops:\n      - replace: { field: body, value: x }\n",
        "op 0",
        &["replace", "set", "rename", "copy", "hash", "delete"],
    );
}

#[test]
fn an_entry_with_two_ops_or_none_is_rejected() {
    rejects(
        "    ops:\n      - set: { field: body, value: x }\n        hash: { field: body }\n",
        "op 0",
        &["one op per entry"],
    );
    rejects("    ops:\n      - {}\n", "op 0", &["one op per entry"]);
}

#[test]
fn an_empty_ops_list_is_rejected() {
    let err = build("    ops: []\n").expect_err("rejected");
    assert!(err.contains("normalise") && err.contains("ops"), "{err}");
}

#[test]
fn a_missing_key_is_rejected_naming_it() {
    rejects(
        "    ops:\n      - set: { field: body, value: x }\n      - rename: { from: body }\n",
        "op 1",
        &["rename", "to"],
    );
    rejects("    ops:\n      - hash: {}\n", "op 0", &["hash", "field"]);
}

#[test]
fn an_unknown_key_is_rejected_naming_it() {
    rejects(
        "    ops:\n      - copy: { from: body, to: attributes.raw, overwrite: false }\n",
        "op 0",
        &["copy", "overwrite"],
    );
}

#[test]
fn a_malformed_path_is_rejected_with_the_parser_hint() {
    rejects(
        "    ops:\n      - rename: { from: 'attributes.http path', to: attributes.route }\n",
        "op 0",
        &["rename", "from", "instead use", "\"http path\""],
    );
    rejects(
        "    ops:\n      - delete: { fields: ['attributes[debug]'] }\n",
        "op 0",
        &["delete", "fields", "brackets"],
    );
}

#[test]
fn every_op_may_name_id_or_kind_since_they_are_payload() {
    for op in [
        "set: { field: id, value: 7 }",
        "set: { field: kind, value: span }",
        "rename: { from: attributes.n, to: id }",
        "copy: { from: attributes.k, to: kind }",
        "delete: { fields: [id, kind] }",
    ] {
        let ops = format!("    ops:\n      - {op}\n");
        assert!(build(&ops).is_ok(), "{op}");
    }
}

#[test]
fn a_literal_or_hash_id_and_kind_do_not_take_is_rejected_by_type() {
    rejects(
        "    ops:\n      - set: { field: id, value: seven }\n",
        "op 0",
        &["id", "non-negative integer", "\"seven\""],
    );
    rejects(
        "    ops:\n      - set: { field: kind, value: trace }\n",
        "op 0",
        &["kind", "`log`, `metric` or `span`"],
    );
    rejects(
        "    ops:\n      - hash: { field: id }\n",
        "op 0",
        &["hash", "id"],
    );
}

#[test]
fn copy_from_id_is_allowed_since_it_only_reads() {
    assert!(build("    ops:\n      - copy: { from: id, to: attributes.record_id }\n").is_ok());
}

#[test]
fn every_op_naming_the_tenant_is_accepted_since_it_is_payload() {
    for op in [
        "set: { field: resource.tenant.id, value: acme }",
        "rename: { from: resource.tenant.id, to: resource.owner }",
        "rename: { from: resource.owner, to: resource.tenant.id }",
        "copy: { from: resource.owner, to: resource.tenant.id }",
        "hash: { field: resource.tenant.id }",
        "delete: { fields: [resource.tenant.id] }",
    ] {
        let ops = format!("    ops:\n      - {op}\n");
        assert!(build(&ops).is_ok(), "{op}");
    }
}

#[test]
fn a_set_literal_that_is_a_map_or_list_is_rejected() {
    rejects(
        "    ops:\n      - set: { field: body, value: { a: 1 } }\n",
        "op 0",
        &["set", "value", "string, number, bool or null"],
    );
    rejects(
        "    ops:\n      - set: { field: attributes.x, value: [1, 2] }\n",
        "op 0",
        &["set", "value", "string, number, bool or null"],
    );
}

#[test]
fn a_set_literal_of_the_wrong_type_is_rejected_quoting_it() {
    rejects(
        "    ops:\n      - set: { field: severity_number, value: high }\n",
        "op 0",
        &["severity_number", "integer", "\"high\""],
    );
    rejects(
        "    ops:\n      - set: { field: severity_text, value: 3 }\n",
        "op 0",
        &["severity_text", "string", "3"],
    );
    rejects(
        "    ops:\n      - set: { field: trace_id, value: true }\n",
        "op 0",
        &["trace_id", "string", "true"],
    );
    rejects(
        "    ops:\n      - set: { field: span_id, value: 1.5 }\n",
        "op 0",
        &["span_id", "string", "1.5"],
    );
    rejects(
        "    ops:\n      - set: { field: time_unix_nano, value: -1 }\n",
        "op 0",
        &["time_unix_nano", "-1"],
    );
}

#[test]
fn a_set_literal_of_the_right_type_or_null_is_accepted() {
    for op in [
        "set: { field: severity_number, value: 9 }",
        "set: { field: severity_text, value: ERROR }",
        "set: { field: severity_text, value: null }",
        "set: { field: attributes.flag, value: true }",
        "set: { field: attributes.x, value: null }",
        "set: { field: body, value: 'replaced' }",
        "set: { field: time_unix_nano, value: 1700000000000000000 }",
    ] {
        assert!(build(&format!("    ops:\n      - {op}\n")).is_ok(), "{op}");
    }
}

#[test]
fn hash_of_a_field_that_cannot_hold_a_string_is_rejected() {
    rejects(
        "    ops:\n      - hash: { field: severity_number }\n",
        "op 0",
        &["hash", "severity_number", "integer", "hash writes a string"],
    );
    assert!(build("    ops:\n      - hash: { field: severity_text }\n").is_ok());
    assert!(build("    ops:\n      - hash: { field: body }\n").is_ok());
}

#[test]
fn from_equal_to_to_is_rejected_on_parsed_paths() {
    rejects(
        "    ops:\n      - rename: { from: attributes.http.path, to: 'attributes.\"http.path\"' }\n",
        "op 0",
        &["rename", "from", "to", "same field"],
    );
    rejects(
        "    ops:\n      - copy: { from: body, to: body }\n",
        "op 0",
        &["copy", "same field"],
    );
}

#[test]
fn an_empty_delete_list_is_rejected() {
    rejects(
        "    ops:\n      - delete: { fields: [] }\n",
        "op 0",
        &["delete", "fields"],
    );
}

#[test]
fn on_unapplied_outside_skip_and_drop_is_rejected_listing_both() {
    let err = build(&format!("    on_unapplied: nak\n{EXAMPLE}")).expect_err("rejected");
    assert!(
        err.contains("normalise")
            && err.contains("nak")
            && err.contains("skip")
            && err.contains("drop"),
        "{err}"
    );
    assert!(build(&format!("    on_unapplied: drop\n{EXAMPLE}")).is_ok());
    assert!(build(&format!("    on_unapplied: skip\n{EXAMPLE}")).is_ok());
}

#[test]
fn an_unknown_node_key_is_rejected() {
    let err = build(&format!("    on_missing: skip\n{EXAMPLE}")).expect_err("rejected");
    assert!(
        err.contains("normalise") && err.contains("on_missing"),
        "{err}"
    );
}
