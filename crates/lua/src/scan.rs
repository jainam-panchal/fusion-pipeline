//! A scan of a script's source for the names it uses as free identifiers: an identifier not
//! preceded by `.` or `:`, outside strings and comments. Load uses it to refuse a script
//! that names a forbidden global anywhere, even inside a function that would only run per
//! record, and to see whether a script reaches for `state` at all.
//!
//! It is deliberately coarse: `local os = 1` is refused like `os.exit()`, and a name built
//! at run time (`_G["o".."s"]`) is not seen here and finds `nil` in the sandbox instead.

/// Every free identifier in `source` with the line it is on, in source order.
pub(crate) fn free_names(source: &str) -> Vec<(&str, usize)> {
    let bytes = source.as_bytes();
    let mut names = Vec::new();
    let mut i = 0;
    let mut line = 1;
    // The last byte that was not whitespace or a comment, to tell `a.os` from `os`.
    let mut previous: Option<u8> = None;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\n' {
            line += 1;
            i += 1;
            continue;
        }
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c == b'-' && bytes.get(i + 1) == Some(&b'-') {
            let after = i + 2;
            match long_bracket(bytes, after) {
                Some((end, newlines)) => {
                    line += newlines;
                    i = end;
                }
                None => {
                    while i < bytes.len() && bytes[i] != b'\n' {
                        i += 1;
                    }
                }
            }
            continue;
        }
        if c == b'"' || c == b'\'' {
            i += 1;
            while i < bytes.len() && bytes[i] != c {
                if bytes[i] == b'\\' {
                    i += 1;
                }
                if bytes.get(i) == Some(&b'\n') {
                    line += 1;
                }
                i += 1;
            }
            i += 1;
            previous = Some(c);
            continue;
        }
        if c == b'[' {
            if let Some((end, newlines)) = long_bracket(bytes, i) {
                line += newlines;
                i = end;
                previous = Some(b']');
                continue;
            }
        }
        if c.is_ascii_digit() {
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.') {
                i += 1;
            }
            previous = Some(b'0');
            continue;
        }
        if c.is_ascii_alphabetic() || c == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            if !matches!(previous, Some(b'.' | b':')) {
                names.push((&source[start..i], line));
            }
            previous = Some(b'a');
            continue;
        }
        previous = Some(c);
        i += 1;
    }
    names
}

/// A long bracket (`[[ ... ]]`, `[=[ ... ]=]`) starting at `at`: the index after its close
/// and the newlines inside it. `None` when `at` does not open one.
fn long_bracket(bytes: &[u8], at: usize) -> Option<(usize, usize)> {
    if bytes.get(at) != Some(&b'[') {
        return None;
    }
    let mut level = 0;
    while bytes.get(at + 1 + level) == Some(&b'=') {
        level += 1;
    }
    if bytes.get(at + 1 + level) != Some(&b'[') {
        return None;
    }
    let mut i = at + 2 + level;
    let mut newlines = 0;
    while i < bytes.len() {
        if bytes[i] == b'\n' {
            newlines += 1;
        }
        if bytes[i] == b']'
            && bytes[i + 1..].iter().take(level).all(|&b| b == b'=')
            && bytes.get(i + 1 + level) == Some(&b']')
        {
            return Some((i + 2 + level, newlines));
        }
        i += 1;
    }
    Some((bytes.len(), newlines))
}
