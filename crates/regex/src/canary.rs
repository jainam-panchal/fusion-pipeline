//! Load-time canary: runs the pattern on PCRE2 under a tight match limit against generated
//! adversarial inputs.
//!
//! Inputs are built from the pattern's literal alphabet (literal characters plus one
//! representative of every character class) at each configured size: every alphabet
//! character repeated on its own, the alphabet cycled, and each of those with the pattern's
//! literal sequence as a prefix (so `ERROR: (a+)+$` sees `ERROR: aaaa…`) and with a "poison"
//! character the pattern never mentions appended, since catastrophic backtracking needs a
//! near-miss rather than a match.
//!
//! Matching is anchored. An unanchored run retries from every start position, which is
//! quadratic for any pattern that has no required literal and would make the 64 KiB input
//! take seconds for patterns as ordinary as `[a-z]+[0-9]+`. Anchoring loses nothing the
//! match limit can detect: PCRE2's counter is per call, and the blow-ups it catches happen
//! within one start position.
//!
//! The canary always runs on PCRE2, whichever engine the pattern would normally use, so the
//! verdict describes the pattern rather than the engine it happens to land on today.

use std::fmt;

use regex_syntax::hir::{self, Hir, HirKind};

use crate::{CompileError, Limits, MatchError, pcre2, scan};

/// Canary configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanaryConfig {
    /// Match limit for the canary runs. The default equals the default runtime limit: an
    /// exponential shape trips it inside the 1 KiB input, while a benign group loop such as
    /// `^(?:a|b)*$` needs about 130 000 steps at 64 KiB and must pass. Lower it together
    /// with [`crate::Limits::match_limit`] when records are known to be short.
    pub match_limit: u32,
    /// Input sizes to generate, in bytes.
    pub sizes: Vec<usize>,
}

impl Default for CanaryConfig {
    fn default() -> Self {
        Self {
            match_limit: 1_000_000,
            sizes: vec![1024, 8192, 65536],
        }
    }
}

/// The input that tripped the canary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanaryTrip {
    /// Length of the tripping input in bytes.
    pub input_len: usize,
    /// A short description of the input's shape.
    pub input_shape: String,
    /// The match limit the canary ran under.
    pub match_limit: u32,
    /// Which limit tripped.
    pub error: MatchError,
}

impl fmt::Display for CanaryTrip {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} on {} input of {} bytes (match limit {})",
            self.error, self.input_shape, self.input_len, self.match_limit
        )
    }
}

/// Most alphabet characters the generator uses; bounds the number of runs.
const MAX_ALPHABET: usize = 16;

/// Runs the canary. `Ok(None)` means no input tripped a limit.
///
/// # Errors
///
/// [`CompileError::Syntax`] when PCRE2 rejects the pattern, [`CompileError::Internal`] or
/// [`CompileError::OutOfMemory`] when PCRE2 fails for another reason. A tripped limit is
/// not an error; it is the `Some` result.
pub fn run(
    pattern: &str,
    config: &CanaryConfig,
    limits: &Limits,
) -> Result<Option<CanaryTrip>, CompileError> {
    let canary_limits = Limits {
        match_limit: config.match_limit,
        ..limits.clone()
    };
    let re = pcre2::Pcre2Regex::compile(pattern, &canary_limits)?;
    let Literals { alphabet, sequence } = literals(pattern);
    let poison = ['!', '~', '\u{1}']
        .into_iter()
        .find(|c| !alphabet.contains(c))
        .unwrap_or('\u{2}');
    let prefix: String = sequence.into_iter().collect();

    for &size in &config.sizes {
        for &c in &alphabet {
            let run_of = |n: usize| std::iter::repeat_n(c, n).collect::<String>();
            let inputs = [
                (run_of(size), format!("'{c}' repeated")),
                (
                    with_poison(run_of(size), poison),
                    format!("'{c}' repeated then poison"),
                ),
                (
                    with_poison(
                        format!("{prefix}{}", run_of(size.saturating_sub(prefix.len()))),
                        poison,
                    ),
                    format!("literals then '{c}' repeated then poison"),
                ),
            ];
            for (input, shape) in inputs {
                if let Some(trip) = probe(&re, &input, &shape, config.match_limit)? {
                    return Ok(Some(trip));
                }
            }
        }
        if alphabet.len() > 1 {
            let cycled: String = alphabet.iter().cycle().take(size).collect();
            if let Some(trip) = probe(&re, &cycled, "alphabet cycled", config.match_limit)? {
                return Ok(Some(trip));
            }
            let poisoned = with_poison(cycled, poison);
            if let Some(trip) = probe(
                &re,
                &poisoned,
                "alphabet cycled then poison",
                config.match_limit,
            )? {
                return Ok(Some(trip));
            }
        }
    }
    Ok(None)
}

fn with_poison(mut input: String, poison: char) -> String {
    input.pop();
    input.push(poison);
    input
}

fn probe(
    re: &pcre2::Pcre2Regex,
    input: &str,
    shape: &str,
    match_limit: u32,
) -> Result<Option<CanaryTrip>, CompileError> {
    match re.captures(input, true) {
        Ok(_) => Ok(None),
        Err(error @ (MatchError::MatchLimit | MatchError::DepthLimit | MatchError::HeapLimit)) => {
            Ok(Some(CanaryTrip {
                input_len: input.len(),
                input_shape: shape.to_owned(),
                match_limit,
                error,
            }))
        }
        Err(MatchError::OutOfMemory) => Err(CompileError::OutOfMemory),
        Err(MatchError::Engine { code, message }) => Err(CompileError::Internal { code, message }),
        Err(MatchError::InputTooLarge { .. }) => Ok(None),
    }
}

/// Longest literal sequence used as an input prefix.
const MAX_SEQUENCE: usize = 64;

/// What the generator knows about a pattern's literals.
struct Literals {
    /// Distinct literal characters plus one representative per class, in first-seen order.
    alphabet: Vec<char>,
    /// The same characters in pattern order with repeats kept, so required literals such
    /// as `ERROR: ` appear verbatim when used as a prefix.
    sequence: Vec<char>,
}

fn literals(pattern: &str) -> Literals {
    let mut sequence = Vec::new();
    let desugared = scan::desugar_for_lint(pattern);
    let parse = |text: &str| regex_syntax::ParserBuilder::new().build().parse(text).ok();
    match parse(&desugared.text).or_else(|| parse(pattern)) {
        Some(hir) => collect_sequence(&hir, &mut sequence),
        None => sequence = scan::raw_literal_alphabet(pattern),
    }
    if sequence.is_empty() {
        sequence.push('a');
    }
    let mut alphabet: Vec<char> = Vec::new();
    for &c in &sequence {
        if !alphabet.contains(&c) {
            alphabet.push(c);
        }
    }
    alphabet.truncate(MAX_ALPHABET);
    sequence.truncate(MAX_SEQUENCE);
    Literals { alphabet, sequence }
}

fn collect_sequence(hir: &Hir, out: &mut Vec<char>) {
    match hir.kind() {
        HirKind::Literal(lit) => out.extend(String::from_utf8_lossy(&lit.0).chars()),
        HirKind::Class(hir::Class::Unicode(class)) => out.extend(representative(class)),
        HirKind::Class(hir::Class::Bytes(class)) => {
            out.extend(class.ranges().first().map(|r| char::from(r.start())));
        }
        HirKind::Repetition(rep) => collect_sequence(&rep.sub, out),
        HirKind::Capture(cap) => collect_sequence(&cap.sub, out),
        HirKind::Concat(subs) | HirKind::Alternation(subs) => {
            subs.iter().for_each(|h| collect_sequence(h, out));
        }
        HirKind::Empty | HirKind::Look(_) => {}
    }
}

/// A printable ASCII member of the class when it has one, else its lowest character.
fn representative(class: &hir::ClassUnicode) -> Option<char> {
    let ranges = class.ranges();
    ranges
        .iter()
        .find_map(|r| {
            let lo = r.start().max(' ');
            (lo <= r.end() && (lo.is_ascii_graphic() || lo == ' ')).then_some(lo)
        })
        .or_else(|| ranges.first().map(|r| r.start()))
}
