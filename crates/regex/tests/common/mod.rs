//! Fixtures and helpers shared by the integration tests.
#![allow(dead_code, clippy::unwrap_used)]

use fusion_regex::canary::CanaryConfig;
use fusion_regex::{CompileError, EngineChoice, Limits, Options, Regex};

pub const BOTH_ENGINES: [EngineChoice; 2] = [EngineChoice::Linear, EngineChoice::Backtracking];

/// The loghub Linux syslog pattern from the spec: lifts `Month, Date, Time, Level,
/// Component, PID, Content` and must pass every load-time check.
pub const LINUX_SYSLOG: &str = r"^(?<Month>[A-Z][a-z]{2}) +(?<Date>\d{1,2}) (?<Time>\d{2}:\d{2}:\d{2}) (?<Level>\S+) (?<Component>[^\[:]+)(?:\[(?<PID>\d+)\])?: (?<Content>.*)$";

pub const LINUX_LINE: &str =
    "Jun 14 15:16:01 combo sshd(pam_unix)[19939]: authentication failure; logname= uid=0";

/// Exponential on PCRE2; the lookahead keeps it off the linear engine.
pub const NESTED_BACKTRACKING: &str = r"^(?=a)(a+)+$";

/// Compiles with the given engine choice and limits, no lint, no canary.
pub fn try_compile(
    pattern: &str,
    engine: EngineChoice,
    limits: Limits,
) -> Result<Regex, CompileError> {
    Regex::with_options(
        pattern,
        &Options {
            limits,
            engine,
            ..Options::unchecked()
        },
    )
}

/// [`try_compile`], unwrapped.
pub fn compile(pattern: &str, engine: EngineChoice, limits: Limits) -> Regex {
    try_compile(pattern, engine, limits).unwrap()
}

/// Default options with the lint on and the canary off.
pub fn lint_only() -> Options {
    Options {
        lint: true,
        canary: None,
        ..Options::default()
    }
}

/// Default options with the canary on (as `config`) and the lint off.
pub fn canary_only(config: CanaryConfig) -> Options {
    Options {
        lint: false,
        canary: Some(config),
        ..Options::default()
    }
}

/// Compiles on the given engine with default limits, no lint, no canary.
pub fn compile_on(engine: EngineChoice, pattern: &str) -> Regex {
    compile(pattern, engine, Limits::default())
}
