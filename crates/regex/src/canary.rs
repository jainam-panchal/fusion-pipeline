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
//! Every input is probed anchored, and the smallest size is probed unanchored as well.
//! Anchored probes find blow-ups inside one start position (the exponential shapes) at
//! every size without paying for the start loop. PCRE2 resets `match_limit` at every start
//! position (`pcre2_match.c`, bump-along loop), so the start loop needs a different bound:
//! the unanchored probe runs under a work budget of `work_per_byte × size` counted across
//! all start positions, which a group loop that restarts everywhere (`(?:a|b)*(?=c)`)
//! exceeds even at 1 KiB. Sizes are clamped to [`Limits::input_bytes`], since the runtime
//! rejects anything larger before matching it.
//!
//! The canary describes PCRE2 behaviour, so [`crate::Regex::with_options`] runs it only for
//! patterns that will execute on PCRE2. [`run`] itself is engine-agnostic.

use std::fmt;

use regex_syntax::hir::{self, Hir, HirKind};

use crate::{CompileError, Limits, MatchError, pcre2, scan};

/// Canary configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanaryConfig {
    /// Match limit for the canary runs, or `None` for the runtime `match_limit` in the
    /// [`Limits`] passed to [`run`].
    ///
    /// The canary asks "would this pattern trip at runtime on its worst input?", so the
    /// runtime limit is the honest answer: a lower value rejects patterns the runtime would
    /// accept (`^(?:a|b)*$` needs about 130 000 steps at 64 KiB). What makes the canary
    /// tight is `work_per_byte`, which bounds the whole call at a fraction of the runtime
    /// `work_limit`.
    pub match_limit: Option<u32>,
    /// Work budget for one probe, per byte of input, counted across start positions.
    /// The default 64 lets a linear scan of the input pass with margin (`^(?:a|b)*$` needs
    /// about 2 items per byte) and trips a group loop that restarts at every position
    /// (`(?:a|b)*(?=c)` needs about 2 500 per byte at 1 KiB).
    pub work_per_byte: u32,
    /// Input sizes to generate, in bytes. Each is clamped to [`Limits::input_bytes`] and
    /// duplicates are dropped, so a node with short records still gets its worst case
    /// probed at the largest size it will accept.
    pub sizes: Vec<usize>,
}

impl Default for CanaryConfig {
    fn default() -> Self {
        Self {
            match_limit: None,
            work_per_byte: 64,
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
    /// Whether the probe was anchored at the start of the input.
    pub anchored: bool,
    /// The match limit the canary ran under.
    pub match_limit: u32,
    /// Which limit tripped.
    pub error: MatchError,
}

impl fmt::Display for CanaryTrip {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} on {} {} input of {} bytes (match limit {})",
            self.error,
            if self.anchored {
                "anchored"
            } else {
                "unanchored"
            },
            self.input_shape,
            self.input_len,
            self.match_limit
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
    let match_limit = config.match_limit.unwrap_or(limits.match_limit);
    let mut sizes: Vec<usize> = config
        .sizes
        .iter()
        .map(|&n| n.min(limits.input_bytes))
        .collect();
    sizes.sort_unstable();
    sizes.dedup();
    let Literals { alphabet, sequence } = literals(pattern);
    let poison = ['!', '~', '\u{1}']
        .into_iter()
        .find(|c| !alphabet.contains(c))
        .unwrap_or('\u{2}');
    let prefix: String = sequence.into_iter().collect();

    // The work budget is per probe and depends on the input size, so each size gets its
    // own compile; PCRE2 compiles in microseconds and there are three sizes.
    for (i, &size) in sizes.iter().enumerate() {
        let budget =
            u32::try_from(u64::from(config.work_per_byte) * size as u64).unwrap_or(u32::MAX);
        let canary_limits = Limits {
            match_limit,
            work_limit: Some(budget),
            ..limits.clone()
        };
        let re = pcre2::Pcre2Regex::compile(pattern, &canary_limits)?;
        // Anchored at every size: catches blow-ups within one start position without paying
        // for the start loop. Unanchored at the smallest size only: catches a group loop
        // that restarts at every position, which is quadratic and therefore visible even
        // on the smallest input.
        let unanchored_too = i == 0;
        for (input, shape) in inputs_of(size, &alphabet, &prefix, poison) {
            if let Some(trip) = probe(&re, &input, shape, true, match_limit)? {
                return Ok(Some(trip));
            }
            if unanchored_too {
                if let Some(trip) = probe(&re, &input, shape, false, match_limit)? {
                    return Ok(Some(trip));
                }
            }
        }
    }
    Ok(None)
}

/// Every adversarial input of one size in bytes, in the order they are tried. Lazy: the
/// caller stops at the first trip, and a 64 KiB input per shape adds up.
fn inputs_of<'a>(
    size: usize,
    alphabet: &'a [char],
    prefix: &'a str,
    poison: char,
) -> impl Iterator<Item = (String, InputShape)> + 'a {
    let run_of = move |c: char, bytes: usize| -> String {
        std::iter::repeat_n(c, bytes / c.len_utf8().max(1)).collect()
    };
    let per_char = alphabet.iter().flat_map(move |&c| {
        [
            (run_of(c, size), InputShape::Repeated(c)),
            (
                with_poison(run_of(c, size), poison),
                InputShape::RepeatedThenPoison(c),
            ),
            (
                with_poison(
                    format!("{prefix}{}", run_of(c, size.saturating_sub(prefix.len()))),
                    poison,
                ),
                InputShape::LiteralsThenRepeatedThenPoison(c),
            ),
        ]
    });
    let cycled = (alphabet.len() > 1).then(move || {
        let mut text = String::with_capacity(size);
        for &c in alphabet.iter().cycle() {
            if text.len() + c.len_utf8() > size {
                break;
            }
            text.push(c);
        }
        [
            (text.clone(), InputShape::Cycled),
            (with_poison(text, poison), InputShape::CycledThenPoison),
        ]
    });
    per_char.chain(cycled.into_iter().flatten())
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
    anchored: bool,
    match_limit: u32,
) -> Result<Option<CanaryTrip>, CompileError> {
    match re.is_match(input, anchored) {
        Ok(_) => Ok(None),
        Err(
            error @ (MatchError::MatchLimit
            | MatchError::DepthLimit
            | MatchError::HeapLimit
            | MatchError::WorkLimit),
        ) => Ok(Some(CanaryTrip {
            input_len: input.len(),
            input_shape: shape,
            anchored,
            match_limit,
            error,
        })),
        Err(MatchError::OutOfMemory) => Err(CompileError::OutOfMemory),
        Err(MatchError::Engine { code, message }) => Err(CompileError::Internal { code, message }),
        // The wrapper never checks `input_bytes`; the facade does, before dispatch.
        Err(other) => Err(CompileError::Internal {
            code: 0,
            message: format!("unexpected canary error: {other}"),
        }),
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
        None => sequence = scan::raw_literal_sequence(pattern),
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
