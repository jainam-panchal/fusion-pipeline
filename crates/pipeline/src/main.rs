//! `pipelined --config <path>`: run one pipeline from a YAML file until Ctrl-C.

use std::path::PathBuf;
use std::process::ExitCode;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const USAGE: &str = "usage: pipelined --config <path>";

fn config_path(args: impl Iterator<Item = String>) -> Result<PathBuf, String> {
    let mut args = args.peekable();
    let mut path = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => {
                path = Some(
                    args.next()
                        .ok_or_else(|| format!("--config needs a path\n{USAGE}"))?,
                );
            }
            other => match other.strip_prefix("--config=") {
                Some(value) => path = Some(value.to_owned()),
                None => return Err(format!("unknown argument `{other}`\n{USAGE}")),
            },
        }
    }
    path.map(PathBuf::from).ok_or_else(|| USAGE.to_owned())
}

fn main() -> ExitCode {
    let path = match config_path(std::env::args().skip(1)) {
        Ok(path) => path,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };
    match fusion_pipeline::run(&path) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("pipelined: {err}");
            let mut source = std::error::Error::source(&err);
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            ExitCode::FAILURE
        }
    }
}
