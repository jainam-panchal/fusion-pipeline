//! Fixtures and helpers shared by the integration tests.
#![allow(dead_code, clippy::unwrap_used)]

use fusion_regex::{EngineChoice, Limits, Options, Regex};

/// The loghub Linux syslog pattern from the spec: lifts `Month, Date, Time, Level,
/// Component, PID, Content` and must pass every load-time check.
pub const LINUX_SYSLOG: &str = r"^(?<Month>[A-Z][a-z]{2}) +(?<Date>\d{1,2}) (?<Time>\d{2}:\d{2}:\d{2}) (?<Level>\S+) (?<Component>[^\[:]+)(?:\[(?<PID>\d+)\])?: (?<Content>.*)$";

pub const LINUX_LINE: &str =
    "Jun 14 15:16:01 combo sshd(pam_unix)[19939]: authentication failure; logname= uid=0";

/// Exponential on PCRE2; the lookahead keeps it off the linear engine.
pub const NESTED_BACKTRACKING: &str = r"^(?=a)(a+)+$";

/// Compiles with the given engine choice and limits, no lint, no canary.
pub fn compile(pattern: &str, engine: EngineChoice, limits: Limits) -> Regex {
    Regex::with_options(
        pattern,
        &Options {
            limits,
            engine,
            ..Options::unchecked()
        },
    )
    .unwrap()
}

/// Compiles on the given engine with default limits, no lint, no canary.
pub fn compile_on(engine: EngineChoice, pattern: &str) -> Regex {
    compile(pattern, engine, Limits::default())
}
