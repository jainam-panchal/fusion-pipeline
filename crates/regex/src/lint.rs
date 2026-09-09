//! Structural ReDoS lint over the `regex-syntax` AST.
//!
//! Three textbook shapes are flagged:
//!
//! 1. **Nested quantifiers**: an unbounded repetition whose body contains another
//!    variable-length repetition, e.g. `(a+)+` or `(\w+\s?)*`. Exponential on a backtracking
//!    engine.
//! 2. **Overlapping alternation under repetition**: a repeated alternation where one branch's
//!    text can also be matched by another branch plus a restart of the loop, e.g. `(a|aa)*`
//!    or `(\d|\w)+`. Exponential.
//! 3. **Overlapping suffix**: an unbounded single-class repetition followed, possibly after
//!    nullable items, by an unbounded repetition that accepts some of the same characters,
//!    e.g. `\w*\d+` or `\s*.*`. Quadratic, and not bounded by PCRE2's match limit because
//!    single-character loops do not push backtracking frames.
//!
//! PCRE2-only syntax the `regex-syntax` parser rejects is desugared first (lookaround and
//! atomic groups become non-capturing groups, backreferences become empty groups, possessive
//! quantifiers lose their `+`). Patterns that still do not parse yield
//! [`LintReport::parsed`] `== false` and no findings; the canary is the remaining check for
//! those.
//!
//! The lint is a heuristic. It has false positives (`(ab|abc)*` is fine but a shape close to
//! it is not) and false negatives (it does not model lookaround); [`crate::RedosPolicy::Warn`]
//! exists for the former and [`crate::canary`] for the latter.

use std::fmt;

use regex_syntax::ast::{self, Ast, RepetitionKind, RepetitionRange};
use regex_syntax::hir::{self, Hir, HirKind};

use crate::scan;

/// One ReDoS shape the lint found. Offsets are byte offsets into the original pattern.
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

impl RedosRisk {
    /// Byte offset into the pattern.
    pub fn offset(&self) -> usize {
        match self {
            Self::NestedQuantifiers { offset }
            | Self::OverlappingAlternation { offset }
            | Self::OverlappingSuffix { offset } => *offset,
        }
    }
}

impl fmt::Display for RedosRisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NestedQuantifiers { offset } => {
                write!(f, "nested unbounded quantifiers at offset {offset}")
            }
            Self::OverlappingAlternation { offset } => {
                write!(
                    f,
                    "overlapping alternation under repetition at offset {offset}"
                )
            }
            Self::OverlappingSuffix { offset } => write!(
                f,
                "unbounded quantifier followed by an overlapping quantifier at offset {offset}"
            ),
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
pub fn lint(pattern: &str) -> LintReport {
    let Some((text, ast, offsets)) = parse(pattern) else {
        return LintReport {
            risks: Vec::new(),
            parsed: false,
        };
    };
    let mut walker = Walker {
        text: &text,
        offsets: &offsets,
        risks: Vec::new(),
    };
    walker.walk(&ast);
    LintReport {
        risks: walker.risks,
        parsed: true,
    }
}

/// Desugars PCRE2-only syntax and parses the result, falling back to the pattern as written.
/// Returns the text that parsed, its AST, and a map from byte offsets in that text back to
/// byte offsets in the original pattern.
fn parse(pattern: &str) -> Option<(String, Ast, Vec<usize>)> {
    let parser = || ast::parse::ParserBuilder::new().build();
    let desugared = scan::desugar_for_lint(pattern);
    if let Ok(ast) = parser().parse(&desugared.text) {
        return Some((desugared.text, ast, desugared.offsets));
    }
    let ast = parser().parse(pattern).ok()?;
    let identity = (0..=pattern.len()).collect();
    Some((pattern.to_owned(), ast, identity))
}

struct Walker<'a> {
    text: &'a str,
    offsets: &'a [usize],
    risks: Vec<RedosRisk>,
}

impl Walker<'_> {
    fn original_offset(&self, span_start: usize) -> usize {
        self.offsets.get(span_start).copied().unwrap_or(span_start)
    }

    fn walk(&mut self, ast: &Ast) {
        match ast {
            Ast::Repetition(rep) => {
                if is_unbounded(&rep.op.kind) {
                    let offset = self.original_offset(rep.span.start.offset);
                    if contains_variable_repetition(&rep.ast) {
                        self.risks.push(RedosRisk::NestedQuantifiers { offset });
                    }
                    if self.has_overlapping_alternation(&rep.ast) {
                        self.risks
                            .push(RedosRisk::OverlappingAlternation { offset });
                    }
                }
                self.walk(&rep.ast);
            }
            Ast::Group(group) => self.walk(&group.ast),
            Ast::Alternation(alt) => alt.asts.iter().for_each(|a| self.walk(a)),
            Ast::Concat(concat) => {
                self.check_overlapping_suffixes(&concat.asts);
                concat.asts.iter().for_each(|a| self.walk(a));
            }
            _ => {}
        }
    }

    /// Rule 2 over every alternation inside a repeated body.
    fn has_overlapping_alternation(&self, body: &Ast) -> bool {
        let mut alternations = Vec::new();
        collect_alternations(body, &mut alternations);
        alternations
            .iter()
            .any(|alt| self.alternation_overlaps(alt))
    }

    fn alternation_overlaps(&self, alt: &ast::Alternation) -> bool {
        let restart = self.first_set(&Ast::Alternation(Box::new(alt.clone())));
        let shapes: Vec<Shape> = alt.asts.iter().map(|a| self.shape(a)).collect();
        for (i, a) in shapes.iter().enumerate() {
            for b in shapes.iter().skip(i + 1) {
                if branches_overlap(a, b, &restart) || branches_overlap(b, a, &restart) {
                    return true;
                }
            }
        }
        false
    }

    /// Rule 3 over the items of one concatenation.
    fn check_overlapping_suffixes(&mut self, items: &[Ast]) {
        for (i, item) in items.iter().enumerate() {
            let Some(class) = self.single_class_unbounded(item) else {
                continue;
            };
            for next in &items[i + 1..] {
                if let Some(next_class) = self.single_class_unbounded(next) {
                    if !intersection_is_empty(&class, &next_class) {
                        let offset = self.original_offset(span_of(item).start.offset);
                        self.risks.push(RedosRisk::OverlappingSuffix { offset });
                    }
                    break;
                }
                if !self.nullable(next) {
                    break;
                }
            }
        }
    }

    /// If `ast` (after unwrapping groups) is an unbounded repetition of exactly one class,
    /// that class.
    fn single_class_unbounded(&self, ast: &Ast) -> Option<hir::ClassUnicode> {
        match unwrap_groups(ast) {
            Ast::Repetition(rep) if is_unbounded(&rep.op.kind) => {
                let shape = self.shape(&rep.ast);
                if shape.exact && shape.prefix.len() == 1 {
                    shape.prefix.into_iter().next()
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn nullable(&self, ast: &Ast) -> bool {
        match ast {
            Ast::Empty(_) | Ast::Flags(_) | Ast::Assertion(_) => true,
            Ast::Literal(_)
            | Ast::Dot(_)
            | Ast::ClassUnicode(_)
            | Ast::ClassPerl(_)
            | Ast::ClassBracketed(_) => false,
            Ast::Group(g) => self.nullable(&g.ast),
            Ast::Alternation(alt) => alt.asts.iter().any(|a| self.nullable(a)),
            Ast::Concat(c) => c.asts.iter().all(|a| self.nullable(a)),
            Ast::Repetition(rep) => min_of(&rep.op.kind) == 0 || self.nullable(&rep.ast),
        }
    }

    /// Characters that can start a match of `ast`.
    fn first_set(&self, ast: &Ast) -> hir::ClassUnicode {
        match ast {
            Ast::Empty(_) | Ast::Flags(_) | Ast::Assertion(_) => hir::ClassUnicode::empty(),
            Ast::Literal(_)
            | Ast::Dot(_)
            | Ast::ClassUnicode(_)
            | Ast::ClassPerl(_)
            | Ast::ClassBracketed(_) => self
                .leaf_classes(ast)
                .into_iter()
                .next()
                .unwrap_or_else(hir::ClassUnicode::empty),
            Ast::Group(g) => self.first_set(&g.ast),
            Ast::Repetition(rep) => self.first_set(&rep.ast),
            Ast::Alternation(alt) => {
                let mut acc = hir::ClassUnicode::empty();
                for a in &alt.asts {
                    acc.union(&self.first_set(a));
                }
                acc
            }
            Ast::Concat(c) => {
                let mut acc = hir::ClassUnicode::empty();
                for a in &c.asts {
                    acc.union(&self.first_set(a));
                    if !self.nullable(a) {
                        break;
                    }
                }
                acc
            }
        }
    }

    /// The sequence of character classes `ast` matches, as far as it is fixed.
    fn shape(&self, ast: &Ast) -> Shape {
        match ast {
            Ast::Empty(_) | Ast::Flags(_) | Ast::Assertion(_) => Shape::exact(Vec::new()),
            Ast::Literal(_)
            | Ast::Dot(_)
            | Ast::ClassUnicode(_)
            | Ast::ClassPerl(_)
            | Ast::ClassBracketed(_) => Shape::exact(self.leaf_classes(ast)),
            Ast::Group(g) => self.shape(&g.ast),
            Ast::Concat(c) => {
                let mut out = Shape::exact(Vec::new());
                for a in &c.asts {
                    let part = self.shape(a);
                    out.prefix.extend(part.prefix);
                    if !part.exact {
                        out.exact = false;
                        break;
                    }
                }
                out
            }
            Ast::Alternation(_) => Shape {
                prefix: Vec::new(),
                exact: false,
            },
            Ast::Repetition(rep) => {
                let inner = self.shape(&rep.ast);
                let (min, max) = (min_of(&rep.op.kind), max_of(&rep.op.kind));
                let mut out = Shape::exact(Vec::new());
                let reps = usize::try_from(min).unwrap_or(usize::MAX).min(MAX_UNROLL);
                for _ in 0..reps {
                    out.prefix.extend(inner.prefix.iter().cloned());
                    if !inner.exact {
                        out.exact = false;
                        return out;
                    }
                }
                if max != Some(min) || u64::from(min) > MAX_UNROLL as u64 {
                    out.exact = false;
                }
                out
            }
        }
    }

    /// Translates one leaf node to its character classes.
    fn leaf_classes(&self, leaf: &Ast) -> Vec<hir::ClassUnicode> {
        let mut translator = hir::translate::TranslatorBuilder::new().build();
        let Ok(hir) = translator.translate(self.text, leaf) else {
            return Vec::new();
        };
        hir_classes(&hir)
    }
}

/// Longest fixed repetition the shape model unrolls.
const MAX_UNROLL: usize = 32;

/// The fixed prefix of what a sub-pattern matches, one class per character.
#[derive(Debug, Clone)]
struct Shape {
    prefix: Vec<hir::ClassUnicode>,
    /// True when `prefix` is the whole language; false when variable-length content follows.
    exact: bool,
}

impl Shape {
    fn exact(prefix: Vec<hir::ClassUnicode>) -> Self {
        Self {
            prefix,
            exact: true,
        }
    }
}

/// True when text matched by branch `a` can also be matched by branch `b` followed by a
/// restart of the enclosing loop, which is the ambiguity that makes `(a|aa)*` exponential.
fn branches_overlap(a: &Shape, b: &Shape, restart: &hir::ClassUnicode) -> bool {
    let n = a.prefix.len().min(b.prefix.len());
    if a.prefix[..n]
        .iter()
        .zip(&b.prefix[..n])
        .any(|(x, y)| intersection_is_empty(x, y))
    {
        return false;
    }
    if !a.exact {
        // `a` continues with unknown content past the shared prefix: assume it overlaps.
        return true;
    }
    if a.prefix.len() > b.prefix.len() {
        // `b` is the shorter one here; the symmetric call handles this pair.
        return !b.exact;
    }
    match b.prefix.get(a.prefix.len()) {
        // `b` continues after `a` ends: ambiguous when that continuation could also start a
        // fresh iteration of the loop.
        Some(next) => !intersection_is_empty(next, restart),
        // Same prefix length and every position overlaps: the same text matches both.
        None => true,
    }
}

fn intersection_is_empty(a: &hir::ClassUnicode, b: &hir::ClassUnicode) -> bool {
    let mut x = a.clone();
    x.intersect(b);
    x.ranges().is_empty()
}

fn hir_classes(hir: &Hir) -> Vec<hir::ClassUnicode> {
    match hir.kind() {
        HirKind::Literal(lit) => String::from_utf8_lossy(&lit.0)
            .chars()
            .map(|c| hir::ClassUnicode::new([hir::ClassUnicodeRange::new(c, c)]))
            .collect(),
        HirKind::Class(hir::Class::Unicode(c)) => vec![c.clone()],
        HirKind::Class(hir::Class::Bytes(c)) => {
            let ranges = c
                .ranges()
                .iter()
                .map(|r| hir::ClassUnicodeRange::new(char::from(r.start()), char::from(r.end())));
            vec![hir::ClassUnicode::new(ranges)]
        }
        _ => Vec::new(),
    }
}

fn collect_alternations<'a>(ast: &'a Ast, out: &mut Vec<&'a ast::Alternation>) {
    match ast {
        Ast::Alternation(alt) => {
            out.push(alt);
            alt.asts.iter().for_each(|a| collect_alternations(a, out));
        }
        Ast::Group(g) => collect_alternations(&g.ast, out),
        Ast::Concat(c) => c.asts.iter().for_each(|a| collect_alternations(a, out)),
        Ast::Repetition(rep) => collect_alternations(&rep.ast, out),
        _ => {}
    }
}

/// True when `ast` contains a repetition that can match more than one length and more than
/// one iteration, which is what makes it ambiguous inside an outer loop.
fn contains_variable_repetition(ast: &Ast) -> bool {
    match ast {
        Ast::Repetition(rep) => is_variable(&rep.op.kind) || contains_variable_repetition(&rep.ast),
        Ast::Group(g) => contains_variable_repetition(&g.ast),
        Ast::Alternation(alt) => alt.asts.iter().any(contains_variable_repetition),
        Ast::Concat(c) => c.asts.iter().any(contains_variable_repetition),
        _ => false,
    }
}

fn unwrap_groups(ast: &Ast) -> &Ast {
    match ast {
        Ast::Group(g) => unwrap_groups(&g.ast),
        Ast::Concat(c) if c.asts.len() == 1 => unwrap_groups(&c.asts[0]),
        other => other,
    }
}

fn is_unbounded(kind: &RepetitionKind) -> bool {
    max_of(kind).is_none()
}

/// Variable length and able to repeat more than once: `+`, `*`, `{m,n}` with `n > m > 0`
/// or `n > 1`, `{m,}`. Excludes `?`, `{n}` and `{0,1}`.
fn is_variable(kind: &RepetitionKind) -> bool {
    let (min, max) = (min_of(kind), max_of(kind));
    max != Some(min) && max != Some(1)
}

fn min_of(kind: &RepetitionKind) -> u32 {
    match kind {
        RepetitionKind::ZeroOrOne | RepetitionKind::ZeroOrMore => 0,
        RepetitionKind::OneOrMore => 1,
        RepetitionKind::Range(RepetitionRange::Exactly(n))
        | RepetitionKind::Range(RepetitionRange::AtLeast(n))
        | RepetitionKind::Range(RepetitionRange::Bounded(n, _)) => *n,
    }
}

fn max_of(kind: &RepetitionKind) -> Option<u32> {
    match kind {
        RepetitionKind::ZeroOrOne => Some(1),
        RepetitionKind::ZeroOrMore | RepetitionKind::OneOrMore => None,
        RepetitionKind::Range(RepetitionRange::Exactly(n)) => Some(*n),
        RepetitionKind::Range(RepetitionRange::AtLeast(_)) => None,
        RepetitionKind::Range(RepetitionRange::Bounded(_, n)) => Some(*n),
    }
}

fn span_of(ast: &Ast) -> &ast::Span {
    ast.span()
}
