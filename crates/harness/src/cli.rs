//! Flag parsing for the harness binaries: `--name value` pairs, each at most once.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;

/// The flags a binary was given.
#[derive(Debug, Default)]
pub struct Flags(BTreeMap<String, String>);

/// The flags in `args`, each one of `known`.
///
/// # Errors
///
/// A message naming a flag that is unknown, given twice or given without a value.
pub fn parse(args: impl Iterator<Item = String>, known: &[&str]) -> Result<Flags, String> {
    let mut flags = BTreeMap::new();
    let mut args = args;
    while let Some(flag) = args.next() {
        if !known.contains(&flag.as_str()) {
            return Err(format!("unknown flag `{flag}`; known: {}", known.join(" ")));
        }
        let value = args
            .next()
            .ok_or_else(|| format!("`{flag}` needs a value"))?;
        if flags.insert(flag.clone(), value).is_some() {
            return Err(format!("`{flag}` given twice"));
        }
    }
    Ok(Flags(flags))
}

impl Flags {
    /// The value of `flag`, if given.
    #[must_use]
    pub fn get(&self, flag: &str) -> Option<&str> {
        self.0.get(flag).map(String::as_str)
    }

    /// The value of `flag` parsed as `T`, else `default`.
    ///
    /// # Errors
    ///
    /// A message naming the flag and the value that does not parse.
    pub fn value<T: FromStr>(&self, flag: &str, default: T) -> Result<T, String> {
        self.get(flag).map_or(Ok(default), |text| {
            text.parse()
                .map_err(|_| format!("`{flag} {text}` is not a valid value"))
        })
    }
}

/// A duration written as seconds (`2s`, `1.5s`) or milliseconds (`500ms`).
///
/// # Errors
///
/// A message when the text has no unit or is not a non-negative number.
pub fn duration(text: &str) -> Result<Duration, String> {
    let invalid = || format!("`{text}` is not a duration like `2s` or `500ms`");
    let (number, scale) = if let Some(ms) = text.strip_suffix("ms") {
        (ms, 1e-3)
    } else if let Some(s) = text.strip_suffix('s') {
        (s, 1.0)
    } else {
        return Err(invalid());
    };
    let value: f64 = number.parse().map_err(|_| invalid())?;
    Duration::try_from_secs_f64(value * scale).map_err(|_| invalid())
}
