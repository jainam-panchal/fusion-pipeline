//! Structural ReDoS lint over the `regex-syntax` HIR.

use std::fmt;

/// One ReDoS shape the lint found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedosRisk {
    /// An unbounded repetition contains another variable-length repetition, e.g. `(a+)+`.
    NestedQuantifiers {
        /// Byte offset of the outer repetition.
        offset: usize,
    },
    /// A repeated alternation whose branches can match the same text, e.g. `(a|aa)*`.
    OverlappingAlternation {
        /// Byte offset of the repetition.
        offset: usize,
    },
    /// An unbounded repetition followed by an unbounded repetition that can match the same
    /// characters, e.g. `\w*\d+` or `\s*.*`.
    OverlappingSuffix {
        /// Byte offset of the first repetition.
        offset: usize,
    },
}

impl fmt::Display for RedosRisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NestedQuantifiers { offset } => {
                write!(f, "nested unbounded quantifiers at offset {offset}")
            }
            Self::OverlappingAlternation { offset } => {
                write!(f, "overlapping alternation under repetition at offset {offset}")
            }
            Self::OverlappingSuffix { offset } => {
                write!(f, "unbounded quantifier followed by an overlapping quantifier at offset {offset}")
            }
        }
    }
}

/// What the lint concluded about a pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LintReport {
    /// Findings, empty when the pattern is clean.
    pub risks: Vec<RedosRisk>,
    /// False when the pattern could not be parsed even after desugaring PCRE2-only syntax,
    /// in which case `risks` is empty and says nothing.
    pub parsed: bool,
}

/// Lints `pattern` for the three textbook ReDoS shapes.
pub fn lint(_pattern: &str) -> LintReport {
    LintReport { risks: Vec::new(), parsed: false }
}
