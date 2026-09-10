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
        "source:\n  type: nats\n  url: nats://127.0.0.1:1\n  stream: LOGS\n  consumer: pipeline\nnodes:\n  - id: out\n    type: sink.nats\n    stream: PROCESSED\n    subject: processed.logs\n",
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
        "source:\n  type: nats\n  url: nats://127.0.0.1:4222\n  stream: LOGS\n  consumer: pipeline\nnodes:\n  - id: out\n    type: sink.nats\n    stream: PROCESSED\n    subject: processed.logs\n",
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
        "source:\n  type: nats\n  stream: NO_SUCH_STREAM\n  consumer: pipeline\nnodes:\n  - id: out\n    type: sink.nats\n    stream: PROCESSED\n    subject: processed.logs\n",
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
