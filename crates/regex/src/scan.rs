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

/// A pattern rewritten so `regex-syntax` can parse PCRE2-only constructs, plus a map from
/// each byte offset of the rewritten text to the offset in the original pattern.
pub(crate) struct Desugared {
    pub(crate) text: String,
    pub(crate) offsets: Vec<usize>,
}

impl Desugared {
    fn push_range(&mut self, pattern: &str, start: usize, end: usize) {
        let end = end.min(pattern.len());
        for i in start..end {
            self.offsets.push(i);
        }
        self.text.push_str(&pattern[start..end]);
    }

    fn push_replacement(&mut self, replacement: &str, at: usize) {
        for _ in 0..replacement.len() {
            self.offsets.push(at);
        }
        self.text.push_str(replacement);
    }
}

/// Rewrites PCRE2-only syntax into structurally equivalent `regex` syntax for the lint:
///
/// - lookaround `(?=`, `(?!`, `(?<=`, `(?<!` and atomic `(?>` become `(?:`;
/// - backreferences `\1`..`\9`, `\g{..}`, `\g<..>`, `\k<..>`, `\k'..'`, `\k{..}` and
///   `(?P=name)` become the empty group `(?:)`;
/// - a possessive `+` after a quantifier is dropped.
///
/// Anything else is copied through unchanged; constructs this does not know about are left
/// for the parser to reject.
pub(crate) fn desugar_for_lint(pattern: &str) -> Desugared {
    let bytes = pattern.as_bytes();
    let mut out = Desugared { text: String::with_capacity(pattern.len()), offsets: Vec::new() };
    let mut in_class = false;
    let mut class_start = false;
    let mut after_quantifier = false;
    let mut i = 0;
    'outer: while i < bytes.len() {
        let b = bytes[i];
        if in_class {
            if b == b'\\' {
                out.push_range(pattern, i, i + 2);
                i += 2;
                continue;
            }
            if b == b']' && !class_start {
                in_class = false;
            }
            class_start = false;
            out.push_range(pattern, i, i + 1);
            i += 1;
            continue;
        }
        if b == b'\\' {
            let consumed = match bytes.get(i + 1) {
                Some(b'1'..=b'9') => Some(2),
                Some(b'g') | Some(b'k') => bracketed_len(&bytes[i + 2..]).map(|n| n + 2),
                _ => None,
            };
            if let Some(n) = consumed {
                out.push_replacement("(?:)", i);
                i += n;
            } else {
                out.push_range(pattern, i, i + 2);
                i += 2;
            }
            after_quantifier = false;
            continue;
        }
        if after_quantifier && b == b'+' {
            // Possessive quantifier: the second `+` adds no backtracking, drop it.
            i += 1;
            after_quantifier = false;
            continue;
        }
        after_quantifier = matches!(b, b'*' | b'+' | b'?' | b'}');
        match b {
            b'[' => {
                in_class = true;
                class_start = true;
                out.push_range(pattern, i, i + 1);
                i += 1;
                if bytes.get(i) == Some(&b'^') {
                    out.push_range(pattern, i, i + 1);
                    i += 1;
                }
                continue;
            }
            b'(' => {
                let rest = &bytes[i..];
                for prefix in [&b"(?<="[..], b"(?<!", b"(?=", b"(?!", b"(?>"] {
                    if rest.starts_with(prefix) {
                        out.push_replacement("(?:", i);
                        i += prefix.len();
                        continue 'outer;
                    }
                }
                if rest.starts_with(b"(?P=") {
                    if let Some(end) = rest.iter().position(|&c| c == b')') {
                        out.push_replacement("(?:)", i);
                        i += end + 1;
                        continue;
                    }
                }
            }
            _ => {}
        }
        out.push_range(pattern, i, i + 1);
        i += 1;
    }
    out.offsets.push(pattern.len());
    out
}

/// Length of a `{..}`, `<..>` or `'..'` group at the start of `rest`, including delimiters.
fn bracketed_len(rest: &[u8]) -> Option<usize> {
    let close = match rest.first()? {
        b'{' => b'}',
        b'<' => b'>',
        b'\'' => b'\'',
        _ => return None,
    };
    rest.iter().skip(1).position(|&c| c == close).map(|n| n + 2)
}

/// Characters that appear literally in the pattern, in first-seen order. Used by the canary
/// when the pattern does not parse on `regex-syntax`. Escaped characters are skipped since
/// most escapes are classes, not literals.
pub(crate) fn raw_literal_alphabet(pattern: &str) -> Vec<char> {
    let mut out = Vec::new();
    let mut escaped = false;
    for c in pattern.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if c == '\\' {
            escaped = true;
            continue;
        }
        if c.is_alphanumeric() && !out.contains(&c) {
            out.push(c);
        }
    }
    out
}
