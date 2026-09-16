//! The `lua` stage's load-time contract: the script is checked when the config loads, and
//! every rejection names the node and says what is wrong, with the line where the script
//! is at fault. Behaviour over records is covered through the engine in the pipeline crate.

use fusion_core::config::Config;
use fusion_lua::Lua;

fn build(params: &str) -> Result<Lua, String> {
    let yaml = format!(
        "nodes:\n  - id: script\n    type: lua\n{params}  - id: out\n    type: sink.memory\n"
    );
    let config = Config::from_yaml(&yaml).expect("config loads");
    Lua::from_node(&config.nodes[0]).map_err(|e| e.to_string())
}

fn inline(source: &str) -> String {
    let indented: String = source.lines().map(|l| format!("      {l}\n")).collect();
    format!("    source: |\n{indented}")
}

fn rejects(params: &str, hints: &[&str]) -> String {
    let err = build(params).expect_err("rejected");
    assert!(err.contains("`script`"), "names the node: {err}");
    for hint in hints {
        assert!(err.contains(hint), "says `{hint}`: {err}");
    }
    err
}

#[test]
fn a_script_defining_process_builds() {
    build(&inline("function process(record)\n  return record\nend")).expect("builds");
}

#[test]
fn a_script_from_a_file_builds_and_a_missing_file_is_rejected() {
    let dir = std::env::temp_dir().join(format!("fusion-lua-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join("ok.lua");
    std::fs::write(&path, "function process(record) return record end").expect("write");
    build(&format!("    script: {}\n", path.display())).expect("builds from a file");
    rejects(
        &format!("    script: {}\n", dir.join("missing.lua").display()),
        &["missing.lua", "cannot read"],
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn script_and_source_are_alternatives() {
    rejects("", &["`script`", "`source`"]);
    rejects(
        &format!(
            "    script: x.lua\n{}",
            inline("function process(r) return r end")
        ),
        &["alternatives"],
    );
}

#[test]
fn every_forbidden_global_is_rejected_at_load_with_its_line() {
    for name in [
        "os", "io", "package", "require", "load", "debug", "loadfile", "dofile",
    ] {
        let source = format!(
            "function process(record)\n  -- comment mentioning {name}\n  local s = \"{name}\"\n  local x = {name}\n  return record\nend"
        );
        let err = rejects(&inline(&source), &[&format!("`{name}`"), "sandbox"]);
        assert!(
            err.contains("script:4:"),
            "line of the use, not the comment or string: {err}"
        );
    }
}

#[test]
fn a_field_named_like_a_forbidden_global_is_not_a_global() {
    build(&inline("function process(record)\n  record.attributes.os = record.attributes[\"os.name\"]\n  local t = { io = 1 }\n  return record\nend"))
        .expect("`x.os` and a table key are not the global");
}

#[test]
fn a_syntax_error_is_rejected_with_its_line() {
    let err = rejects(
        &inline("function process(record)\n  return record\n\nend end"),
        &["script:4:"],
    );
    assert!(err.contains("syntax"), "{err}");
}

#[test]
fn a_script_without_process_is_rejected() {
    rejects(&inline("local x = 1"), &["process(record, meta)"]);
    rejects(&inline("process = 42"), &["process(record, meta)"]);
}

#[test]
fn a_top_level_that_loops_is_rejected_by_the_budget() {
    rejects(
        &format!(
            "    limits: {{ instructions: 1000 }}\n{}",
            inline("while true do end\nfunction process(r) return r end")
        ),
        &["instruction budget"],
    );
}

#[test]
fn limits_and_policies_outside_their_values_are_rejected() {
    let ok = inline("function process(r) return r end");
    rejects(
        &format!("    limits: {{ instructions: 0 }}\n{ok}"),
        &["`limits.instructions`"],
    );
    rejects(
        &format!("    limits: {{ memory_kib: 8 }}\n{ok}"),
        &["`limits.memory_kib`", "64"],
    );
    rejects(
        &format!("    limits: {{ output_kib: 0 }}\n{ok}"),
        &["`limits.output_kib`"],
    );
    rejects(
        &format!("    limits: {{ heap_kib: 1 }}\n{ok}"),
        &["heap_kib"],
    );
    rejects(&format!("    on_error: retry\n{ok}"), &["retry"]);
    rejects(&format!("    on_state_error: drop\n{ok}"), &["drop"]);
    let stateful = inline("function process(r)\n  state.get(\"k\")\n  return r\nend");
    build(&format!(
        "    on_error: nak\n    on_state_error: pass\n{stateful}"
    ))
    .expect("builds");
}

#[test]
fn print_and_loadstring_are_refused_at_load_and_print_names_log_info() {
    let err = rejects(
        &inline("function process(r)\n  print(r.body)\n  return r\nend"),
        &["`print`", "log.info"],
    );
    assert!(err.contains("script:2:"), "{err}");
    rejects(
        &inline("function process(r)\n  loadstring(\"x\")\n  return r\nend"),
        &["`loadstring`"],
    );
}

#[test]
fn on_state_error_on_a_script_that_never_uses_state_is_rejected() {
    rejects(
        &format!(
            "    on_state_error: pass\n{}",
            inline("function process(r) return r end")
        ),
        &["`on_state_error`", "`state`"],
    );
}
