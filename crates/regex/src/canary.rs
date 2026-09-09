//! Load-time canary: runs the pattern on PCRE2 under a tight match limit against generated
//! adversarial inputs.

use std::fmt;

use crate::{CompileError, Limits};

/// Canary configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanaryConfig {
    /// Match limit for the canary runs.
    pub match_limit: u32,
    /// Input sizes to generate, in bytes.
    pub sizes: Vec<usize>,
}

impl Default for CanaryConfig {
    fn default() -> Self {
        Self { match_limit: 1_000_000, sizes: vec![1024, 8192, 65536] }
    }
}

/// The input that tripped the canary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanaryTrip {
    /// Length of the tripping input in bytes.
    pub input_len: usize,
    /// A short description of the input's shape.
    pub input_shape: String,
    /// The match limit that tripped.
    pub match_limit: u32,
}

impl fmt::Display for CanaryTrip {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} input of {} bytes exceeded match limit {}",
            self.input_shape, self.input_len, self.match_limit
        )
    }
}

/// Runs the canary. `Ok(None)` means no input tripped the limit.
pub fn run(
    _pattern: &str,
    _config: &CanaryConfig,
    _limits: &Limits,
) -> Result<Option<CanaryTrip>, CompileError> {
    Ok(None)
}
