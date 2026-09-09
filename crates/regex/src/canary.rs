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
//! match limit can detect: PCRE2 resets its match counter at every start position
//! (`pcre2_match.c`, bump-along loop), so a limit that never trips within one start
//! position never trips at all, and the exponential blow-ups it does catch happen within
//! one start position. The cost of the start loop itself is bounded only by
//! [`Limits::input_bytes`], which is why input sizes above it are not tried.
//!
//! The canary describes PCRE2 behaviour, so [`crate::Regex::with_options`] runs it only for
//! patterns that will execute on PCRE2. [`run`] itself is engine-agnostic.

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
    /// Input sizes to generate, in bytes. Sizes above [`Limits::input_bytes`] are skipped,
    /// since the runtime would reject such a record before matching it.
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

/// How a canary input was built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum InputShape {
    /// One alphabet character repeated to the input size.
    Repeated(char),
    /// One alphabet character repeated, with the poison character last.
    RepeatedThenPoison(char),
    /// The pattern's literal sequence, then one character repeated, then the poison.
    LiteralsThenRepeatedThenPoison(char),
    /// The alphabet cycled to the input size.
    Cycled,
    /// The alphabet cycled, with the poison character last.
    CycledThenPoison,
}

impl fmt::Display for InputShape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Repeated(c) => write!(f, "{c:?} repeated"),
            Self::RepeatedThenPoison(c) => write!(f, "{c:?} repeated then poison"),
            Self::LiteralsThenRepeatedThenPoison(c) => {
                write!(f, "literals then {c:?} repeated then poison")
            }
            Self::Cycled => f.write_str("alphabet cycled"),
            Self::CycledThenPoison => f.write_str("alphabet cycled then poison"),
        }
    }
}

/// The input that tripped the canary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanaryTrip {
    /// Length of the tripping input in bytes.
    pub input_len: usize,
    /// How the input was built.
    pub input_shape: InputShape,
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

    let sizes = config
        .sizes
        .iter()
        .copied()
        .filter(|&n| n <= limits.input_bytes);
    for size in sizes {
        for (input, shape) in inputs_of(size, &alphabet, &prefix, poison) {
            if let Some(trip) = probe(&re, &input, shape, config.match_limit)? {
                return Ok(Some(trip));
            }
        }
    }
    Ok(None)
}

/// Every adversarial input of one size, in the order they are tried.
fn inputs_of(
    size: usize,
    alphabet: &[char],
    prefix: &str,
    poison: char,
) -> Vec<(String, InputShape)> {
    let mut inputs = Vec::with_capacity(alphabet.len() * 3 + 2);
    for &c in alphabet {
        let run_of = |n: usize| std::iter::repeat_n(c, n).collect::<String>();
        inputs.push((run_of(size), InputShape::Repeated(c)));
        inputs.push((
            with_poison(run_of(size), poison),
            InputShape::RepeatedThenPoison(c),
        ));
        inputs.push((
            with_poison(
                format!("{prefix}{}", run_of(size.saturating_sub(prefix.len()))),
                poison,
            ),
            InputShape::LiteralsThenRepeatedThenPoison(c),
        ));
    }
    if alphabet.len() > 1 {
        let cycled: String = alphabet.iter().cycle().take(size).collect();
        inputs.push((cycled.clone(), InputShape::Cycled));
        inputs.push((with_poison(cycled, poison), InputShape::CycledThenPoison));
    }
    inputs
}

fn with_poison(mut input: String, poison: char) -> String {
    input.pop();
    input.push(poison);
    input
}

fn probe(
    re: &pcre2::Pcre2Regex,
    input: &str,
    shape: InputShape,
    match_limit: u32,
) -> Result<Option<CanaryTrip>, CompileError> {
    match re.captures(input, true) {
        Ok(_) => Ok(None),
        Err(error @ (MatchError::MatchLimit | MatchError::DepthLimit | MatchError::HeapLimit)) => {
            Ok(Some(CanaryTrip {
                input_len: input.len(),
                input_shape: shape,
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
    let parsed = scan::parse_with_fallback(pattern, |text| {
        regex_syntax::ParserBuilder::new().build().parse(text).ok()
    });
    match parsed {
        Some((hir, _)) => collect_sequence(&hir, &mut sequence),
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
