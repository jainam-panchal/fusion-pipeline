//! Byte-level pattern scanner shared by the compile-time guards, the lint's PCRE2-to-`regex`
//! desugaring and the canary's alphabet extraction. It understands escapes and character
//! classes well enough to find group parentheses; it is not a parser.

use crate::{CompileError, Limits};

/// Rejects patterns longer than the length limit or nested deeper than the parens limit.
/// Both errors carry the byte offset at which the limit was crossed.
pub(crate) fn check_guards(pattern: &str, limits: &Limits) -> Result<(), CompileError> {
    if pattern.len() > limits.max_pattern_length {
        return Err(CompileError::PatternTooLong {
            len: pattern.len(),
            limit: limits.max_pattern_length,
            offset: limits.max_pattern_length,
        });
    }
    let mut depth: u32 = 0;
    let mut in_class = false;
    let mut class_start = false;
    let bytes = pattern.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\\' {
            i += 2;
            continue;
        }
        if in_class {
            // `]` as the first class member is a literal.
            if b == b']' && !class_start {
                in_class = false;
            }
            class_start = false;
        } else {
            match b {
                b'[' => {
                    in_class = true;
                    // `[^]...]` and `[]...]` both start with a literal `]`.
                    class_start = true;
                    if bytes.get(i + 1) == Some(&b'^') {
                        i += 1;
                    }
                }
                b'(' => {
                    depth += 1;
                    if depth > limits.parens_nest_limit {
                        return Err(CompileError::ParensTooDeep {
                            limit: limits.parens_nest_limit,
                            offset: i,
                        });
                    }
                }
                b')' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
        i += 1;
    }
    Ok(())
}
