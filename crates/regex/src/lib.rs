//! Two-engine regex facade.
//!
//! A pattern is compiled on the Rust `regex` crate when its syntax allows, which makes it
//! [`Engine::Linear`]: matching is linear in the haystack and cannot backtrack. Syntax the
//! `regex` crate rejects (lookaround, backreferences, atomic and possessive groups,
//! recursion) falls back to PCRE2 through `pcre2-sys`, which makes it
//! [`Engine::Backtracking`]: every PCRE2 limit is configurable through [`Limits`] and a
//! tripped limit comes back as its own [`MatchError`] variant. JIT is never used.
//!
//! Load-time ReDoS checks live in [`lint`] and [`canary`]; [`Regex::with_options`] runs both
//! under the caller's [`RedosPolicy`].
//!
//! All `unsafe` in the workspace lives in the private `pcre2` module.

pub mod canary;
pub mod lint;
mod pcre2;
mod scan;

use std::fmt;
use std::num::NonZeroU32;

use lint::RedosRisk;

/// Which engine a pattern compiled on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Engine {
    /// The Rust `regex` crate: linear time, cannot backtrack.
    Linear,
    /// PCRE2 interpreter: backtracking, bounded by [`Limits`].
    Backtracking,
}

impl Engine {
    /// The label used in metrics and logs.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Linear => "linear",
            Self::Backtracking => "backtracking",
        }
    }
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which engine the caller wants. [`EngineChoice::Auto`] is the facade's normal behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EngineChoice {
    /// Linear first, PCRE2 if the `regex` crate rejects the syntax.
    #[default]
    Auto,
    /// Linear only; syntax the `regex` crate rejects is a compile error.
    Linear,
    /// PCRE2 only, even for syntax the `regex` crate accepts.
    Backtracking,
}

/// Limits applied to a pattern.
///
/// `match_limit`, `depth_limit`, `heap_limit_kib` and `work_limit` apply on the PCRE2 path
/// only; the linear engine cannot backtrack and needs none of them. `input_bytes` applies
/// on both. `max_pattern_length` and `parens_nest_limit` are checked at compile time on
/// both.
///
/// Public structs in this crate are not `#[non_exhaustive]` on purpose: callers build them
/// with struct-literal syntax and `..Default::default()`, which that attribute forbids from
/// outside the crate. Adding a field is therefore a breaking change here, accepted for a
/// crate with one consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    /// Upper bound on PCRE2 backtracking steps within one start position. PCRE2 resets the
    /// counter each time its bump-along loop advances, so an unanchored call over n start
    /// positions may spend up to n times this; see `work_limit` for the bound on the whole
    /// call.
    pub match_limit: u32,
    /// Upper bound on nested backtracking frames per match call.
    pub depth_limit: u32,
    /// Upper bound on heap PCRE2 may use for backtracking frames, in KiB.
    pub heap_limit_kib: u32,
    /// Upper bound on pattern items PCRE2 may visit in one match call, counted across every
    /// start position, or `None` to skip the count.
    ///
    /// PCRE2 resets `match_limit` at each start position, so an unanchored non-match whose
    /// leading group loop restarts everywhere (`(?:a|b)*(?=c)`) is O(n²) and trips nothing
    /// else: about two minutes on a 64 KiB record. This limit is what bounds it, at
    /// 1.5–1.8× the matching cost on the PCRE2 path. Single-character repeats loop inside
    /// one item and are not counted; their worst case is one character scan per start
    /// position, seconds at 64 KiB, bounded by `input_bytes`.
    pub work_limit: Option<NonZeroU32>,
    /// Largest haystack accepted, in bytes. Longer inputs are [`MatchError::InputTooLarge`].
    ///
    /// On the backtracking engine this is the only bound on unanchored patterns made of
    /// single-character repeats (`\w+\s+\w+`, `[a-z]+[0-9]+`): they scan from every start
    /// position, `match_limit` resets per start and `work_limit` does not see inside a
    /// repeat, so a non-matching record costs O(n²) character steps. At the default 64 KiB
    /// that is a few seconds; it is also the largest input the canary tries.
    pub input_bytes: usize,
    /// Longest pattern accepted, in bytes.
    pub max_pattern_length: usize,
    /// Deepest parenthesis nesting accepted.
    pub parens_nest_limit: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            match_limit: 1_000_000,
            depth_limit: 1_000_000,
            heap_limit_kib: 20_000,
            work_limit: NonZeroU32::new(10_000_000),
            input_bytes: 64 * 1024,
            max_pattern_length: 8192,
            parens_nest_limit: 250,
        }
    }
}

/// What to do when the lint or the canary finds a ReDoS risk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RedosPolicy {
    /// Fail compilation with [`CompileError::RedosRisk`] or [`CompileError::CanaryTripped`].
    #[default]
    Reject,
    /// Compile anyway and expose the findings through [`Regex::redos_warnings`].
    Warn,
}

/// Everything [`Regex::with_options`] needs beyond the pattern.
///
/// `Default` is [`Options::checked`]: lint and canary on, rejecting on any finding, as the
/// spec requires of a stage at config load. [`Regex::new`] opts out with
/// [`Options::unchecked`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    /// Runtime and compile-time limits.
    pub limits: Limits,
    /// Engine selection.
    pub engine: EngineChoice,
    /// Reject or warn on lint and canary findings.
    pub on_redos_risk: RedosPolicy,
    /// Whether to run the structural lint.
    pub lint: bool,
    /// Canary configuration, or `None` to skip the canary.
    pub canary: Option<canary::CanaryConfig>,
}

impl Default for Options {
    fn default() -> Self {
        Self::checked()
    }
}

impl Options {
    /// Limits only: no lint, no canary. What [`Regex::new`] uses.
    #[must_use]
    pub fn unchecked() -> Self {
        Self {
            limits: Limits::default(),
            engine: EngineChoice::default(),
            on_redos_risk: RedosPolicy::default(),
            lint: false,
            canary: None,
        }
    }

    /// All three load-time checks on, rejecting on any finding. The `Default`.
    #[must_use]
    pub fn checked() -> Self {
        Self {
            lint: true,
            canary: Some(canary::CanaryConfig::default()),
            ..Self::unchecked()
        }
    }
}

/// Why a pattern failed to compile.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CompileError {
    /// The engine rejected the pattern's syntax. `offset` is a byte offset into the pattern.
    #[error("{engine} engine: {message} at offset {offset}")]
    Syntax {
        /// Engine that produced the error.
        engine: Engine,
        /// PCRE2's error code; `None` for the linear engine, which has no numeric codes.
        code: Option<i32>,
        /// The engine's message, verbatim.
        message: String,
        /// Byte offset into the pattern.
        offset: usize,
    },
    /// The pattern is longer than [`Limits::max_pattern_length`].
    #[error("pattern is {len} bytes, longer than the {limit} byte limit (offset {offset})")]
    PatternTooLong {
        /// Pattern length in bytes.
        len: usize,
        /// The configured limit.
        limit: usize,
        /// The last char boundary at or before the limit.
        offset: usize,
    },
    /// The linear engine's compiled program exceeded its size limit. Only the linear engine
    /// has this limit; PCRE2 bounds pattern size through `max_pattern_length`.
    #[error("{engine} engine: compiled program larger than {limit} bytes")]
    CompiledTooBig {
        /// Engine that produced the error.
        engine: Engine,
        /// The engine's size limit in bytes.
        limit: usize,
    },
    /// Parentheses nest deeper than [`Limits::parens_nest_limit`].
    #[error("parentheses nest deeper than {limit} at offset {offset}")]
    ParensTooDeep {
        /// The configured limit.
        limit: u32,
        /// Byte offset of the parenthesis that exceeded it.
        offset: usize,
    },
    /// The lint found a ReDoS shape and the policy is [`RedosPolicy::Reject`].
    #[error("ReDoS risk: {}", .0.iter().map(ToString::to_string).collect::<Vec<_>>().join("; "))]
    RedosRisk(Vec<RedosRisk>),
    /// The canary tripped its match limit and the policy is [`RedosPolicy::Reject`].
    #[error("canary tripped: {0}")]
    CanaryTripped(canary::CanaryTrip),
    /// PCRE2 could not allocate.
    #[error("PCRE2 allocation failed")]
    OutOfMemory,
    /// PCRE2 returned an error the wrapper does not expect.
    #[error("PCRE2 internal error {code}: {message}")]
    Internal {
        /// PCRE2 error code.
        code: i32,
        /// PCRE2's message.
        message: String,
    },
}

/// Why a match call failed. Each PCRE2 limit has its own variant.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MatchError {
    /// [`Limits::match_limit`] tripped.
    #[error("match limit exceeded")]
    MatchLimit,
    /// [`Limits::depth_limit`] tripped.
    #[error("backtracking depth limit exceeded")]
    DepthLimit,
    /// [`Limits::heap_limit_kib`] tripped.
    #[error("heap limit exceeded")]
    HeapLimit,
    /// [`Limits::work_limit`] tripped.
    #[error("work limit exceeded across start positions")]
    WorkLimit,
    /// The haystack is longer than [`Limits::input_bytes`].
    #[error("input is {len} bytes, longer than the {limit} byte limit")]
    InputTooLarge {
        /// Haystack length in bytes.
        len: usize,
        /// The configured limit.
        limit: usize,
    },
    /// PCRE2 could not allocate match data.
    #[error("PCRE2 allocation failed")]
    OutOfMemory,
    /// Any other engine error.
    #[error("engine error {code}: {message}")]
    Engine {
        /// PCRE2 error code.
        code: i32,
        /// PCRE2's message.
        message: String,
    },
}

/// Byte range of a capture group inside the haystack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    /// Inclusive start byte offset.
    pub start: usize,
    /// Exclusive end byte offset.
    pub end: usize,
}

enum Inner {
    Linear(regex::Regex),
    Backtracking(pcre2::Pcre2Regex),
}

/// A compiled pattern on one of the two engines.
pub struct Regex {
    pattern: String,
    inner: Inner,
    names: Vec<Option<String>>,
    input_bytes: usize,
    warnings: Vec<RedosRisk>,
    canary_warning: Option<canary::CanaryTrip>,
}

impl fmt::Debug for Regex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Regex")
            .field("pattern", &self.pattern)
            .field("engine", &self.engine())
            .finish_non_exhaustive()
    }
}

impl Regex {
    /// Compiles `pattern` with default [`Limits`] and no lint or canary.
    ///
    /// # Errors
    ///
    /// See [`Regex::with_options`].
    pub fn new(pattern: &str) -> Result<Self, CompileError> {
        Self::with_options(pattern, &Options::unchecked())
    }

    /// Compiles `pattern` under `options`: guards, engine selection, then lint and canary
    /// as configured.
    ///
    /// # Errors
    ///
    /// [`CompileError::PatternTooLong`] and [`CompileError::ParensTooDeep`] from the guards;
    /// [`CompileError::Syntax`] when the selected engine rejects the pattern;
    /// [`CompileError::RedosRisk`] and [`CompileError::CanaryTripped`] under
    /// [`RedosPolicy::Reject`]; [`CompileError::OutOfMemory`] and [`CompileError::Internal`]
    /// when PCRE2 fails outside its documented syntax errors.
    pub fn with_options(pattern: &str, options: &Options) -> Result<Self, CompileError> {
        let limits = &options.limits;
        scan::check_guards(pattern, limits)?;

        let inner = match options.engine {
            EngineChoice::Auto => match compile_linear(pattern) {
                Ok(re) => Inner::Linear(re),
                Err(_) => Inner::Backtracking(pcre2::Pcre2Regex::compile(pattern, limits)?),
            },
            EngineChoice::Linear => Inner::Linear(compile_linear(pattern)?),
            EngineChoice::Backtracking => {
                Inner::Backtracking(pcre2::Pcre2Regex::compile(pattern, limits)?)
            }
        };

        // The lint runs whichever engine was chosen: it describes the pattern's shape, which
        // stays the same if the engine choice changes, and the spec asks for it at load
        // regardless of classification. The canary below is different: it measures PCRE2.
        let mut warnings = Vec::new();
        if options.lint {
            let risks = lint::lint(pattern).risks;
            if !risks.is_empty() {
                match options.on_redos_risk {
                    RedosPolicy::Reject => return Err(CompileError::RedosRisk(risks)),
                    RedosPolicy::Warn => warnings = risks,
                }
            }
        }

        // The canary describes PCRE2 behaviour, so it only runs for patterns that will
        // execute on PCRE2. A linear pattern cannot backtrack, and syntax the `regex` crate
        // accepts but PCRE2 reads differently (class set operations) would make the verdict
        // about a different pattern.
        let mut canary_warning = None;
        if let (Some(config), Inner::Backtracking(_)) = (&options.canary, &inner) {
            if let Some(trip) = canary::run(pattern, config, limits)? {
                match options.on_redos_risk {
                    RedosPolicy::Reject => return Err(CompileError::CanaryTripped(trip)),
                    RedosPolicy::Warn => canary_warning = Some(trip),
                }
            }
        }

        let names = match &inner {
            Inner::Linear(re) => re.capture_names().map(|n| n.map(str::to_owned)).collect(),
            Inner::Backtracking(re) => re.capture_names().to_vec(),
        };

        Ok(Self {
            pattern: pattern.to_owned(),
            inner,
            names,
            input_bytes: limits.input_bytes,
            warnings,
            canary_warning,
        })
    }

    /// The pattern as given.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.pattern
    }

    /// Which engine this pattern runs on.
    #[must_use]
    pub fn engine(&self) -> Engine {
        match self.inner {
            Inner::Linear(_) => Engine::Linear,
            Inner::Backtracking(_) => Engine::Backtracking,
        }
    }

    /// Lint findings kept under [`RedosPolicy::Warn`]. Empty otherwise.
    #[must_use]
    pub fn redos_warnings(&self) -> &[RedosRisk] {
        &self.warnings
    }

    /// The canary trip kept under [`RedosPolicy::Warn`], if any. Only a backtracking-engine
    /// pattern can carry one; the canary is skipped for linear patterns.
    #[must_use]
    pub fn canary_warning(&self) -> Option<&canary::CanaryTrip> {
        self.canary_warning.as_ref()
    }

    /// Capture group names by group index. Index 0 is the whole match and is `None`.
    pub fn capture_names(&self) -> impl Iterator<Item = Option<&str>> + '_ {
        self.names.iter().map(Option::as_deref)
    }

    /// Whether the pattern matches anywhere in `haystack`.
    ///
    /// # Errors
    ///
    /// [`MatchError::InputTooLarge`] on either engine; a tripped PCRE2 limit as its own
    /// variant on the backtracking engine.
    pub fn is_match(&self, haystack: &str) -> Result<bool, MatchError> {
        self.check_input(haystack)?;
        match &self.inner {
            Inner::Linear(re) => Ok(re.is_match(haystack)),
            Inner::Backtracking(re) => re.is_match(haystack, false),
        }
    }

    /// The leftmost match with all capture groups, or `None` when there is no match.
    ///
    /// # Errors
    ///
    /// Same as [`Regex::is_match`].
    pub fn captures<'h>(&self, haystack: &'h str) -> Result<Option<Captures<'_, 'h>>, MatchError> {
        self.check_input(haystack)?;
        let spans = match &self.inner {
            Inner::Linear(re) => re.captures(haystack).map(|caps| {
                (0..caps.len())
                    .map(|i| {
                        caps.get(i).map(|m| Span {
                            start: m.start(),
                            end: m.end(),
                        })
                    })
                    .collect()
            }),
            Inner::Backtracking(re) => re.captures(haystack, false)?,
        };
        Ok(spans.map(|spans| Captures {
            haystack,
            spans,
            names: &self.names,
        }))
    }

    fn check_input(&self, haystack: &str) -> Result<(), MatchError> {
        if haystack.len() > self.input_bytes {
            return Err(MatchError::InputTooLarge {
                len: haystack.len(),
                limit: self.input_bytes,
            });
        }
        Ok(())
    }
}

fn compile_linear(pattern: &str) -> Result<regex::Regex, CompileError> {
    regex::RegexBuilder::new(pattern)
        .build()
        .map_err(|e| match e {
            regex::Error::Syntax(msg) => CompileError::Syntax {
                engine: Engine::Linear,
                code: None,
                offset: linear_error_offset(pattern),
                message: msg,
            },
            regex::Error::CompiledTooBig(limit) => CompileError::CompiledTooBig {
                engine: Engine::Linear,
                limit,
            },
            other => CompileError::Syntax {
                engine: Engine::Linear,
                code: None,
                offset: 0,
                message: other.to_string(),
            },
        })
}

/// The `regex` crate's error type carries its span only in the message; re-parse with
/// `regex-syntax` to recover the byte offset.
fn linear_error_offset(pattern: &str) -> usize {
    match regex_syntax::ast::parse::Parser::new().parse(pattern) {
        Err(e) => e.span().start.offset,
        Ok(_) => match regex_syntax::Parser::new().parse(pattern) {
            Err(regex_syntax::Error::Translate(e)) => e.span().start.offset,
            _ => 0,
        },
    }
}

/// Capture groups of one match. Borrows the haystack and the pattern's names.
#[derive(Debug)]
pub struct Captures<'r, 'h> {
    haystack: &'h str,
    spans: Vec<Option<Span>>,
    names: &'r [Option<String>],
}

impl<'r, 'h> Captures<'r, 'h> {
    /// Text of group `index`, or `None` if the group did not participate.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<&'h str> {
        let span = (*self.spans.get(index)?)?;
        self.haystack.get(span.start..span.end)
    }

    /// Byte span of group `index`, or `None` if the group did not participate.
    #[must_use]
    pub fn span(&self, index: usize) -> Option<Span> {
        *self.spans.get(index)?
    }

    /// Text of the group called `name`, or `None` if there is no such group or it did not
    /// participate.
    #[must_use]
    pub fn name(&self, name: &str) -> Option<&'h str> {
        let index = self.names.iter().position(|n| n.as_deref() == Some(name))?;
        self.get(index)
    }

    /// Every named group that participated, as `(name, text)` in group order.
    pub fn named(&self) -> impl Iterator<Item = (&'r str, &'h str)> + '_ {
        self.names.iter().enumerate().filter_map(|(i, n)| {
            let name = n.as_deref()?;
            Some((name, self.get(i)?))
        })
    }

    /// Number of groups, counting group 0.
    #[must_use]
    pub fn len(&self) -> usize {
        self.spans.len()
    }

    /// Whether there are no groups. Never true for a match, which always has group 0; the
    /// method exists to pair with [`Captures::len`].
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }
}
