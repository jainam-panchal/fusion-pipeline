//! The condition grammar used by `filter` and `route`.
//!
//! ```text
//! condition := or
//! or        := and ("or" and)*
//! and       := not ("and" not)*
//! not       := "not" not | primary
//! primary   := "(" condition ")" | path op literal
//! path      := root ("." segment)*          (see `crate::path`)
//! op        := "==" | "!=" | "=~" | "!~" | "<" | ">" | "<=" | ">="
//! literal   := string | number | "true" | "false" | "null"
//! ```
//!
//! A path is resolved by [`FieldPath`]: the root is a top-level record field and, under
//! `attributes`, `resource` or `scope`, the segments joined with dots are the flat map key.
//! `=~` and `!~` take a string literal, the pattern. Core has no regex engine: a stage
//! compiles the patterns [`Condition::regex_patterns`] lists through the facade and
//! evaluates with [`Condition::matches_with`], handing in the match function; a `!~` is the
//! negation, and a field that is not a string matches neither (`=~` false, `!~` true, as
//! `!=` is on a type mismatch). [`Condition::matches`] is for conditions without regex
//! operators and treats them as false.

use std::cmp::Ordering;

use crate::path::{FieldPath, FieldValue, Num, PathError, quoted_end};
use crate::record::Record;

/// Errors from parsing a condition string.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ConditionError {
    /// A character that starts no token.
    #[error("unexpected character `{ch}` at offset {offset}")]
    UnexpectedChar {
        /// The character.
        ch: char,
        /// Byte offset in the expression.
        offset: usize,
    },
    /// The expression ended where more was required.
    #[error("unexpected end of expression")]
    UnexpectedEnd,
    /// A token that does not fit the grammar at that position.
    #[error("unexpected `{token}` at offset {offset}")]
    UnexpectedToken {
        /// The token text.
        token: String,
        /// Byte offset in the expression.
        offset: usize,
    },
    /// A string literal without a closing quote.
    #[error("unterminated string starting at offset {offset}")]
    UnterminatedString {
        /// Byte offset of the opening quote.
        offset: usize,
    },
    /// A number literal that does not parse.
    #[error("invalid number `{text}` at offset {offset}")]
    InvalidNumber {
        /// The literal text.
        text: String,
        /// Byte offset in the expression.
        offset: usize,
    },
    /// `=~` or `!~` with something other than a string literal on the right.
    #[error("`=~` and `!~` take a quoted pattern (at offset {offset})")]
    RegexNeedsString {
        /// Byte offset of the literal in the expression.
        offset: usize,
    },
    /// A field path that does not follow the path rule.
    #[error("{source} (path at offset {offset})")]
    Field {
        /// Byte offset of the path in the expression.
        offset: usize,
        /// What was wrong with the path.
        #[source]
        source: PathError,
    },
}

/// Comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CompareOp {
    /// `==`
    Eq,
    /// `!=`
    Ne,
    /// `<`
    Lt,
    /// `>`
    Gt,
    /// `<=`
    Le,
    /// `>=`
    Ge,
    /// `=~`: the field is a string and the pattern matches somewhere in it.
    Match,
    /// `!~`: the negation of `=~`.
    NotMatch,
}

/// Right-hand side of a comparison.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Literal {
    /// `null`
    Null,
    /// `true` or `false`
    Bool(bool),
    /// An integer, kept exact.
    Int(i128),
    /// A float.
    Float(f64),
    /// A quoted string.
    Str(String),
}

/// A parsed condition, ready to evaluate against records.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Condition {
    /// `field op literal`
    Compare {
        /// The record field.
        field: FieldPath,
        /// The operator.
        op: CompareOp,
        /// The literal to compare against.
        literal: Literal,
    },
    /// Both sides hold.
    And(Box<Condition>, Box<Condition>),
    /// Either side holds.
    Or(Box<Condition>, Box<Condition>),
    /// The inner condition does not hold.
    Not(Box<Condition>),
}

impl Condition {
    /// Parse an expression.
    ///
    /// # Errors
    ///
    /// Returns a [`ConditionError`] describing the first problem found, with its offset where
    /// one applies.
    pub fn parse(expr: &str) -> Result<Self, ConditionError> {
        let tokens = lex(expr)?;
        let mut parser = Parser { tokens, pos: 0 };
        let condition = parser.or()?;
        match parser.peek() {
            None => Ok(condition),
            Some(t) => Err(t.unexpected()),
        }
    }

    /// The pattern of every `=~` and `!~` leaf, in tree order, duplicates included.
    #[must_use]
    pub fn regex_patterns(&self) -> Vec<&str> {
        let mut out = Vec::new();
        self.collect_patterns(&mut out);
        out
    }

    fn collect_patterns<'a>(&'a self, out: &mut Vec<&'a str>) {
        match self {
            Self::Compare {
                op: CompareOp::Match | CompareOp::NotMatch,
                literal: Literal::Str(pattern),
                ..
            } => out.push(pattern),
            Self::Compare { .. } => {}
            Self::And(a, b) | Self::Or(a, b) => {
                a.collect_patterns(out);
                b.collect_patterns(out);
            }
            Self::Not(inner) => inner.collect_patterns(out),
        }
    }

    /// Evaluate against `record` with regex operators treated as false. For a condition
    /// that uses them, see [`Condition::matches_with`].
    ///
    /// A missing field equals `null` and nothing else; comparisons between mismatched types
    /// are false (so `!=` is true); ordering applies to numbers and to strings.
    #[must_use]
    pub fn matches(&self, record: &Record) -> bool {
        self.matches_with(record, &mut |_, _| Ok::<bool, ()>(false))
            .unwrap_or(false)
    }

    /// Evaluate against `record`, asking `regex(pattern, text)` whether the pattern of a
    /// `=~` or `!~` leaf matches the field's text. `and` and `or` short-circuit, so a leaf
    /// the left side decides is not asked. The first error `regex` returns ends the
    /// evaluation with it.
    ///
    /// # Errors
    ///
    /// Whatever `regex` returns.
    pub fn matches_with<E>(
        &self,
        record: &Record,
        regex: &mut impl FnMut(&str, &str) -> Result<bool, E>,
    ) -> Result<bool, E> {
        match self {
            Self::Compare {
                field,
                op: op @ (CompareOp::Match | CompareOp::NotMatch),
                literal: Literal::Str(pattern),
            } => {
                let matched = match field.read(record) {
                    FieldValue::Str(text) => regex(pattern, text)?,
                    _ => false,
                };
                Ok(if *op == CompareOp::Match {
                    matched
                } else {
                    !matched
                })
            }
            Self::Compare { field, op, literal } => Ok(compare(field.read(record), *op, literal)),
            Self::And(a, b) => Ok(a.matches_with(record, regex)? && b.matches_with(record, regex)?),
            Self::Or(a, b) => Ok(a.matches_with(record, regex)? || b.matches_with(record, regex)?),
            Self::Not(inner) => Ok(!inner.matches_with(record, regex)?),
        }
    }
}

fn compare(value: FieldValue<'_>, op: CompareOp, literal: &Literal) -> bool {
    match op {
        CompareOp::Eq => equals(value, literal),
        CompareOp::Ne => !equals(value, literal),
        CompareOp::Lt => order(value, literal).is_some_and(Ordering::is_lt),
        CompareOp::Gt => order(value, literal).is_some_and(Ordering::is_gt),
        CompareOp::Le => order(value, literal).is_some_and(Ordering::is_le),
        CompareOp::Ge => order(value, literal).is_some_and(Ordering::is_ge),
        CompareOp::Match | CompareOp::NotMatch => false,
    }
}

fn equals(value: FieldValue<'_>, literal: &Literal) -> bool {
    match (value, literal) {
        (FieldValue::Null, Literal::Null) => true,
        (FieldValue::Bool(a), Literal::Bool(b)) => a == *b,
        (FieldValue::Str(a), Literal::Str(b)) => a == b,
        (FieldValue::Num(_), Literal::Int(_) | Literal::Float(_)) => {
            order(value, literal) == Some(Ordering::Equal)
        }
        _ => false,
    }
}

fn order(value: FieldValue<'_>, literal: &Literal) -> Option<Ordering> {
    match (value, literal) {
        (FieldValue::Num(a), Literal::Int(b)) => a.partial_cmp(&Num::Int(*b)),
        (FieldValue::Num(a), Literal::Float(b)) => a.partial_cmp(&Num::Float(*b)),
        (FieldValue::Str(a), Literal::Str(b)) => Some(a.cmp(b.as_str())),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// A bare word: a keyword, or a path with only a root.
    Ident(String),
    /// A path with segments, kept as written for [`FieldPath::parse`].
    Path(String),
    Str(String),
    Int(i128),
    Float(f64),
    Op(CompareOp),
    LParen,
    RParen,
}

#[derive(Debug, Clone, PartialEq)]
struct Token {
    tok: Tok,
    offset: usize,
    text: String,
}

impl Token {
    fn unexpected(&self) -> ConditionError {
        ConditionError::UnexpectedToken {
            token: self.text.clone(),
            offset: self.offset,
        }
    }
}

fn lex(expr: &str) -> Result<Vec<Token>, ConditionError> {
    let bytes = expr.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        let c = bytes[i];
        let tok = match c {
            b' ' | b'\t' | b'\n' | b'\r' => {
                i += 1;
                continue;
            }
            b'(' => {
                i += 1;
                Tok::LParen
            }
            b')' => {
                i += 1;
                Tok::RParen
            }
            b'=' | b'!' | b'<' | b'>' => {
                let next = bytes.get(i + 1).copied();
                let (op, len) = match (c, next) {
                    (b'=', Some(b'=')) => (CompareOp::Eq, 2),
                    (b'=', Some(b'~')) => (CompareOp::Match, 2),
                    (b'!', Some(b'=')) => (CompareOp::Ne, 2),
                    (b'!', Some(b'~')) => (CompareOp::NotMatch, 2),
                    (b'<', Some(b'=')) => (CompareOp::Le, 2),
                    (b'>', Some(b'=')) => (CompareOp::Ge, 2),
                    (b'<', _) => (CompareOp::Lt, 1),
                    (b'>', _) => (CompareOp::Gt, 1),
                    _ => {
                        return Err(ConditionError::UnexpectedChar {
                            ch: char::from(c),
                            offset: i,
                        });
                    }
                };
                i += len;
                Tok::Op(op)
            }
            b'"' | b'\'' => {
                let (s, end) = lex_string(expr, i)?;
                i = end;
                Tok::Str(s)
            }
            b'-' | b'0'..=b'9' => {
                i += 1;
                while i < bytes.len()
                    && matches!(bytes[i], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
                {
                    i += 1;
                }
                let text = &expr[start..i];
                if let Ok(n) = text.parse::<i128>() {
                    Tok::Int(n)
                } else if let Ok(f) = text.parse::<f64>() {
                    Tok::Float(f)
                } else {
                    return Err(ConditionError::InvalidNumber {
                        text: text.to_owned(),
                        offset: start,
                    });
                }
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                i += 1;
                while i < bytes.len()
                    && matches!(bytes[i], b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_')
                {
                    i += 1;
                }
                if i < bytes.len() && matches!(bytes[i], b'.' | b'[') {
                    i = lex_path_rest(expr, start, i)?;
                    Tok::Path(expr[start..i].to_owned())
                } else {
                    Tok::Ident(expr[start..i].to_owned())
                }
            }
            _ => {
                let ch = expr[i..].chars().next().unwrap_or('?');
                return Err(ConditionError::UnexpectedChar { ch, offset: i });
            }
        };
        tokens.push(Token {
            tok,
            offset: start,
            text: expr[start..i].to_owned(),
        });
    }
    Ok(tokens)
}

/// Lex the segments of a path after its root, starting at `i` (a `.` or `[`), and return
/// the offset just past them. The text is kept as written; [`FieldPath::parse`] judges it,
/// so bracket syntax, bad characters, escapes and text glued to a closing quote get the
/// path error and its hint. `start` is the root's offset, reported with an unclosed quote.
fn lex_path_rest(expr: &str, start: usize, mut i: usize) -> Result<usize, ConditionError> {
    let bytes = expr.as_bytes();
    while i < bytes.len() {
        match bytes[i] {
            b'.' => {
                i += 1;
                if bytes.get(i) == Some(&b'"') {
                    i = quoted_end(expr, i).ok_or_else(|| ConditionError::Field {
                        offset: start,
                        source: PathError::UnterminatedQuote {
                            path: expr[start..].to_owned(),
                        },
                    })?;
                }
                while i < bytes.len() && continues_bare_segment(bytes[i]) {
                    i += 1;
                }
            }
            b'[' => {
                // Old bracket syntax: take the whole bracket, quotes honoured, so the path
                // parser reports it with a dotted hint. An unclosed quote runs to the end.
                i += 1;
                while i < bytes.len() && bytes[i] != b']' {
                    i = match bytes[i] {
                        b'"' | b'\'' => quoted_end(expr, i).unwrap_or(bytes.len()),
                        _ => i + 1,
                    };
                }
                i = (i + 1).min(bytes.len());
            }
            _ => break,
        }
    }
    Ok(i)
}

/// Bytes that keep a bare segment going in the lexer. Wider than the segment charset on
/// purpose, so a stray `:` or `/` is reported as a path error with a hint instead of an
/// unexpected token.
const fn continues_bare_segment(b: u8) -> bool {
    !matches!(
        b,
        b' ' | b'\t'
            | b'\n'
            | b'\r'
            | b'.'
            | b'('
            | b')'
            | b'['
            | b']'
            | b'='
            | b'!'
            | b'<'
            | b'>'
            | b'"'
            | b'\''
    )
}

/// Lex a quoted string starting at `start` (the quote). Returns the unescaped contents and
/// the offset just past the closing quote.
fn lex_string(expr: &str, start: usize) -> Result<(String, usize), ConditionError> {
    let mut chars = expr[start..].char_indices();
    let quote = chars
        .next()
        .map(|(_, c)| c)
        .ok_or(ConditionError::UnexpectedEnd)?;
    let mut out = String::new();
    while let Some((i, c)) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some((_, 'n')) => out.push('\n'),
                Some((_, 't')) => out.push('\t'),
                Some((_, 'r')) => out.push('\r'),
                Some((_, other)) => out.push(other),
                None => return Err(ConditionError::UnterminatedString { offset: start }),
            },
            c if c == quote => return Ok((out, start + i + c.len_utf8())),
            c => out.push(c),
        }
    }
    Err(ConditionError::UnterminatedString { offset: start })
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Result<&Token, ConditionError> {
        let t = self
            .tokens
            .get(self.pos)
            .ok_or(ConditionError::UnexpectedEnd)?;
        self.pos += 1;
        Ok(t)
    }

    fn peek_keyword(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(Token { tok: Tok::Ident(s), .. }) if s == kw)
    }

    fn or(&mut self) -> Result<Condition, ConditionError> {
        let mut left = self.and()?;
        while self.peek_keyword("or") {
            self.pos += 1;
            let right = self.and()?;
            left = Condition::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn and(&mut self) -> Result<Condition, ConditionError> {
        let mut left = self.not()?;
        while self.peek_keyword("and") {
            self.pos += 1;
            let right = self.not()?;
            left = Condition::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn not(&mut self) -> Result<Condition, ConditionError> {
        if self.peek_keyword("not") {
            self.pos += 1;
            return Ok(Condition::Not(Box::new(self.not()?)));
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<Condition, ConditionError> {
        let token = self.next()?.clone();
        match token.tok {
            Tok::LParen => {
                let inner = self.or()?;
                match self.next()?.clone() {
                    Token {
                        tok: Tok::RParen, ..
                    } => Ok(inner),
                    other => Err(other.unexpected()),
                }
            }
            Tok::Ident(ref name) | Tok::Path(ref name) => {
                let field = FieldPath::parse(name).map_err(|source| ConditionError::Field {
                    offset: token.offset,
                    source,
                })?;
                let op = match self.next()?.clone() {
                    Token {
                        tok: Tok::Op(op), ..
                    } => op,
                    other => return Err(other.unexpected()),
                };
                let literal_offset = self.peek().map_or(0, |t| t.offset);
                let literal = self.literal()?;
                if matches!(op, CompareOp::Match | CompareOp::NotMatch)
                    && !matches!(literal, Literal::Str(_))
                {
                    return Err(ConditionError::RegexNeedsString {
                        offset: literal_offset,
                    });
                }
                Ok(Condition::Compare { field, op, literal })
            }
            _ => Err(token.unexpected()),
        }
    }

    fn literal(&mut self) -> Result<Literal, ConditionError> {
        let token = self.next()?.clone();
        Ok(match token.tok {
            Tok::Str(s) => Literal::Str(s),
            Tok::Int(n) => Literal::Int(n),
            Tok::Float(f) => Literal::Float(f),
            Tok::Ident(ref kw) => match kw.as_str() {
                "true" => Literal::Bool(true),
                "false" => Literal::Bool(false),
                "null" => Literal::Null,
                _ => return Err(token.unexpected()),
            },
            _ => return Err(token.unexpected()),
        })
    }
}
