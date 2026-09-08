//! `fusion-pipeline check <config.yaml>`: load and validate a pipeline config.
//! Running a pipeline needs the NATS source and sink, which land in their
//! own ticket.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [cmd, path] if cmd == "check" => check(path),
        _ => {
            eprintln!("usage: fusion-pipeline check <config.yaml>");
            ExitCode::from(2)
        }
    }
}

fn check(path: &str) -> ExitCode {
    let yaml = match std::fs::read_to_string(path) {
        Ok(y) => y,
        Err(e) => {
            eprintln!("{path}: {e}");
            return ExitCode::FAILURE;
        }
    };
    match pipeline_core::config::load_str(&yaml) {
        Ok(cfg) => {
            println!("{path}: ok ({} nodes)", cfg.nodes.len());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{path}: {e}");
            ExitCode::FAILURE
        }
    }
}
