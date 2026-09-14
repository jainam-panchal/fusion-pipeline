//! The `pipelined` binary through its command line: usage errors and fail-fast startup.

use std::path::PathBuf;
use std::process::{Command, Output};

fn pipelined() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pipelined"))
}

fn write_config(name: &str, yaml: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("pipelined-cli-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join(name);
    std::fs::write(&path, yaml).expect("config written");
    path
}

/// A one-node NATS config with the given source and sink URLs and source stream.
fn nats_config(source_url: &str, sink_url: &str, stream: &str) -> String {
    format!(
        "source:\n  type: nats\n  url: {source_url}\n  stream: {stream}\n  consumer: pipeline\n\
         nodes:\n  - id: out\n    type: sink.nats\n    url: {sink_url}\n    stream: PROCESSED\n    subject: processed.logs\n"
    )
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn missing_config_flag_is_a_usage_error() {
    let output = pipelined().output().expect("binary runs");

    assert!(!output.status.success());
    assert!(stderr(&output).contains("--config"), "{}", stderr(&output));
}

#[test]
fn unreadable_config_names_the_path() {
    let output = pipelined()
        .args(["--config", "/nonexistent/pipeline.yaml"])
        .output()
        .expect("binary runs");

    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("/nonexistent/pipeline.yaml"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn config_without_a_source_block_is_rejected() {
    let path = write_config(
        "no-source.yaml",
        "nodes:\n  - id: out\n    type: sink.memory\n",
    );

    let output = pipelined()
        .args(["--config", path.to_str().expect("utf-8 path")])
        .output()
        .expect("binary runs");

    assert!(!output.status.success());
    assert!(stderr(&output).contains("source"), "{}", stderr(&output));
}

#[test]
fn unreachable_server_fails_fast_naming_the_url() {
    let path = write_config(
        "unreachable.yaml",
        &nats_config("nats://127.0.0.1:1", "nats://127.0.0.1:1", "LOGS"),
    );

    let output = pipelined()
        .args(["--config", path.to_str().expect("utf-8 path")])
        .env_remove("NATS_URL")
        .output()
        .expect("binary runs");

    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("127.0.0.1:1"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn nats_url_env_overrides_the_yaml_url() {
    let path = write_config(
        "env-override.yaml",
        &nats_config("nats://127.0.0.1:4222", "nats://127.0.0.1:4222", "LOGS"),
    );

    let output = pipelined()
        .args(["--config", path.to_str().expect("utf-8 path")])
        .env("NATS_URL", "nats://127.0.0.1:2")
        .output()
        .expect("binary runs");

    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("127.0.0.1:2"),
        "{}",
        stderr(&output)
    );
}

/// Needs the compose stack: the stream in the config does not exist there.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn missing_stream_fails_fast_naming_the_stream() {
    let path = write_config(
        "missing-stream.yaml",
        &nats_config(
            "nats://127.0.0.1:4222",
            "nats://127.0.0.1:4222",
            "NO_SUCH_STREAM",
        ),
    );

    let output = pipelined()
        .args(["--config", path.to_str().expect("utf-8 path")])
        .output()
        .expect("binary runs");

    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("NO_SUCH_STREAM"),
        "{}",
        stderr(&output)
    );
}

/// Needs the compose stack. Both YAML URLs are bogus and `NATS_URL` points at the real
/// server: the sink (built first) connects fine, and the source then fails on its missing
/// stream at the env URL. Neither bogus URL appears in the error.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn nats_url_env_overrides_both_the_source_and_the_sink_url() {
    let real = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_owned());
    let path = write_config(
        "env-override-both.yaml",
        &nats_config("nats://127.0.0.1:1", "nats://127.0.0.1:2", "NO_SUCH_STREAM"),
    );

    let output = pipelined()
        .args(["--config", path.to_str().expect("utf-8 path")])
        .env("NATS_URL", &real)
        .output()
        .expect("binary runs");

    assert!(!output.status.success());
    let err = stderr(&output);
    assert!(
        err.contains("NO_SUCH_STREAM") && err.contains(&real),
        "{err}"
    );
    assert!(
        !err.contains("127.0.0.1:1") && !err.contains("127.0.0.1:2"),
        "{err}"
    );
}

/// A dedupe node makes the store required. Needs the compose NATS (the source and sink are
/// set up before the store is opened); the store URL points nowhere.
#[test]
#[ignore = "needs a JetStream server at NATS_URL"]
fn unreachable_state_store_fails_fast_naming_its_url_when_a_node_uses_state() {
    let path = write_config(
        "state-unreachable.yaml",
        &nats_config("nats://127.0.0.1:4222", "nats://127.0.0.1:4222", "LOGS").replace(
            "nodes:\n",
            "nodes:\n  - id: dd\n    type: dedupe\n    key: [body]\n    window: 10s\n",
        ),
    );

    let output = pipelined()
        .args(["--config", path.to_str().expect("utf-8 path")])
        .env("DRAGONFLY_URL", "redis://127.0.0.1:1")
        .output()
        .expect("binary runs");

    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("127.0.0.1:1"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn a_malformed_state_store_url_is_rejected_before_anything_connects() {
    let path = write_config(
        "state-bad-url.yaml",
        &nats_config("nats://127.0.0.1:1", "nats://127.0.0.1:1", "LOGS"),
    );

    let output = pipelined()
        .args(["--config", path.to_str().expect("utf-8 path")])
        .env("DRAGONFLY_URL", "not a url")
        .env_remove("NATS_URL")
        .output()
        .expect("binary runs");

    assert!(!output.status.success());
    let err = stderr(&output);
    assert!(err.contains("not a url"), "{err}");
}
