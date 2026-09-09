//! Byte-level pattern scanner shared by the compile-time guards, the PCRE2-to-`regex`
//! desugaring used by the lint and the canary, and the canary's raw alphabet extraction.
//! It understands escapes and character classes well enough to find group parentheses; it
//! is not a parser. It does not know `\Q…\E` or `(?x)` comments, so PCRE2's own
//! `parens_nest_limit` stays set on the compile context as the backstop for parentheses the
//! scanner miscounts. The length guard needs no backstop: it runs on the byte length before
//! either engine sees the pattern.

use crate::{CompileError, Limits};

/// Bracketed-class state shared by the scanners. Inside `[...]` only the closing `]` means
/// anything, and `]` as the first member (`[]a]`, `[^]a]`) is a literal.
#[derive(Default)]
struct ClassState {
    in_class: bool,
    class_start: bool,
}

impl ClassState {
    /// Feeds the unescaped byte at `i`. Returns how many bytes belong to class syntax (the
    /// opening `[` with an optional `^`, or one member byte), or `None` when `i` is outside
    /// a class.
    fn step(&mut self, bytes: &[u8], i: usize) -> Option<usize> {
        if self.in_class {
            if bytes[i] == b']' && !self.class_start {
                self.in_class = false;
            }
            self.class_start = false;
            return Some(1);
        }
        if bytes[i] == b'[' {
            self.in_class = true;
            self.class_start = true;
            let caret = usize::from(bytes.get(i + 1) == Some(&b'^'));
            return Some(1 + caret);
        }
        None
    }
}

/// Rejects patterns longer than the length limit or nested deeper than the parens limit.
/// Both errors carry the byte offset at which the limit was crossed.
pub(crate) fn check_guards(pattern: &str, limits: &Limits) -> Result<(), CompileError> {
    if pattern.len() > limits.max_pattern_length {
        let mut offset = limits.max_pattern_length;
        while !pattern.is_char_boundary(offset) {
            offset -= 1;
        }
        return Err(CompileError::PatternTooLong {
            len: pattern.len(),
            limit: limits.max_pattern_length,
            offset,
        });
    }
    let mut depth: u32 = 0;
    let mut class = ClassState::default();
    let bytes = pattern.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            i += 2;
            continue;
        }
        if let Some(n) = class.step(bytes, i) {
            i += n;
            continue;
        }
        match bytes[i] {
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
        self.offsets.extend(start..end);
        self.text.push_str(&pattern[start..end]);
    }

    fn push_replacement(&mut self, replacement: &str, at: usize) {
        self.offsets
            .extend(std::iter::repeat_n(at, replacement.len()));
        self.text.push_str(replacement);
    }
}

/// Rewrites PCRE2-only syntax into structurally equivalent `regex` syntax for the lint and
/// the canary:
///
/// - lookaround `(?=`, `(?!`, `(?<=`, `(?<!` and atomic `(?>` become `(?:`;
/// - backreferences `\1`..`\9`, `\g{..}`, `\g<..>`, `\k<..>`, `\k'..'`, `\k{..}` and
///   `(?P=name)` become the empty group `(?:)`;
/// - a possessive `+` after a quantifier is dropped.
///
/// Anything else is copied through unchanged; constructs this does not know about are left
/// for the parser to reject.
pub(crate) fn desugar_pcre2_syntax(pattern: &str) -> Desugared {
    let bytes = pattern.as_bytes();
    let mut out = Desugared {
        text: String::with_capacity(pattern.len()),
        offsets: Vec::with_capacity(pattern.len() + 1),
    };
    let mut class = ClassState::default();
    let mut after_quantifier = false;
    let mut i = 0;
    'outer: while i < bytes.len() {
        let b = bytes[i];
        if b == b'\\' && class.in_class {
            out.push_range(pattern, i, i + 2);
            i += 2;
            continue;
        }
        if let Some(n) = class.step(bytes, i) {
            out.push_range(pattern, i, i + n);
            i += n;
            after_quantifier = false;
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
        if b == b'(' {
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

/// Alphanumeric characters that appear literally in the pattern, in pattern order with
/// repeats kept. Used by the canary when the pattern does not parse on `regex-syntax`.
/// Escaped characters are skipped since most escapes are classes, not literals.
pub(crate) fn raw_literal_sequence(pattern: &str) -> Vec<char> {
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
        if c.is_alphanumeric() {
            out.push(c);
        }
    }
    out
}

/// Desugars PCRE2-only syntax and parses the result with `parse`; if that fails, parses the
/// pattern as written. Returns the parse output and the offset map of the text that parsed.
pub(crate) fn parse_with_fallback<T>(
    pattern: &str,
    mut parse: impl FnMut(&str) -> Option<T>,
) -> Option<(T, Desugared)> {
    let desugared = desugar_pcre2_syntax(pattern);
    if let Some(parsed) = parse(&desugared.text) {
        return Some((parsed, desugared));
    }
    let parsed = parse(pattern)?;
    let identity = Desugared {
        text: pattern.to_owned(),
        offsets: (0..=pattern.len()).collect(),
    };
    Some((parsed, identity))
}
