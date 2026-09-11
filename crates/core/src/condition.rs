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
//! `=~` and `!~` parse here; the regex ticket wires their evaluation, so
//! [`Condition::matches`] treats them as false and stages reject them at load through
//! [`Condition::has_regex_ops`].

use std::cmp::Ordering;

use serde_json::Value;

use crate::path::{FieldPath, PathError};
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

/// A number as seen by the grammar. Two integers compare exactly; when either side is a
/// float both are compared as `f64`, which is lossy above 2^53 and never equal for NaN.
#[derive(Debug, Clone, Copy)]
enum Num {
    Int(i128),
    Float(f64),
}

impl Num {
    fn from_value(v: &Value) -> Option<Self> {
        let n = v.as_number()?;
        if let Some(i) = n.as_i64() {
            Some(Self::Int(i128::from(i)))
        } else if let Some(u) = n.as_u64() {
            Some(Self::Int(i128::from(u)))
        } else {
            n.as_f64().map(Self::Float)
        }
    }

    fn as_f64(self) -> f64 {
        match self {
            Self::Int(i) => i as f64,
            Self::Float(f) => f,
        }
    }
}

impl PartialEq for Num {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl PartialOrd for Num {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (*self, *other) {
            (Self::Int(a), Self::Int(b)) => Some(a.cmp(&b)),
            (a, b) => a.as_f64().partial_cmp(&b.as_f64()),
        }
    }
}

/// A resolved field value, borrowed from the record where possible.
#[derive(Debug, Clone, Copy)]
enum FieldValue<'a> {
    Null,
    Bool(bool),
    Num(Num),
    Str(&'a str),
    /// Arrays and objects: comparable to nothing.
    Composite,
}

impl<'a> FieldValue<'a> {
    fn from_json(v: &'a Value) -> Self {
        match v {
            Value::Null => Self::Null,
            Value::Bool(b) => Self::Bool(*b),
            Value::Number(_) => Num::from_value(v).map_or(Self::Null, Self::Num),
            Value::String(s) => Self::Str(s),
            Value::Array(_) | Value::Object(_) => Self::Composite,
        }
    }
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
    /// `=~` (regex match; evaluation lands with the regex ticket)
    Match,
    /// `!~` (regex non-match; evaluation lands with the regex ticket)
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

    /// Whether the condition uses `=~` or `!~` anywhere.
    #[must_use]
    pub fn has_regex_ops(&self) -> bool {
        match self {
            Self::Compare { op, .. } => matches!(op, CompareOp::Match | CompareOp::NotMatch),
            Self::And(a, b) | Self::Or(a, b) => a.has_regex_ops() || b.has_regex_ops(),
            Self::Not(inner) => inner.has_regex_ops(),
        }
    }

    /// Evaluate against `record`.
    ///
    /// A missing field equals `null` and nothing else; comparisons between mismatched types
    /// are false (so `!=` is true); ordering applies to numbers and to strings.
    #[must_use]
    pub fn matches(&self, record: &Record) -> bool {
        match self {
            Self::Compare { field, op, literal } => {
                let resolved = field.read(record);
                let value = resolved
                    .as_deref()
                    .map_or(FieldValue::Null, FieldValue::from_json);
                compare(value, *op, literal)
            }
            Self::And(a, b) => a.matches(record) && b.matches(record),
            Self::Or(a, b) => a.matches(record) || b.matches(record),
            Self::Not(inner) => !inner.matches(record),
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
/// so bracket syntax, bad characters and escapes get the path error and its hint. `start`
/// is the root's offset, reported with an unclosed quote.
fn lex_path_rest(expr: &str, start: usize, mut i: usize) -> Result<usize, ConditionError> {
    let bytes = expr.as_bytes();
    while i < bytes.len() {
        match bytes[i] {
            b'.' => {
                i += 1;
                if bytes.get(i) == Some(&b'"') {
                    i = skip_quoted(bytes, i).ok_or_else(|| ConditionError::Field {
                        offset: start,
                        source: PathError::UnterminatedQuote {
                            path: expr[start..].to_owned(),
                        },
                    })?;
                } else {
                    while i < bytes.len() && continues_bare_segment(bytes[i]) {
                        i += 1;
                    }
                }
            }
            b'[' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b']' {
                    i = match bytes[i] {
                        b'"' | b'\'' => skip_quoted(bytes, i).unwrap_or(bytes.len()),
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

/// From the opening quote at `i`, the offset just past the closing quote of the same kind,
/// skipping backslash-escaped bytes. `None` when the quote is never closed.
fn skip_quoted(bytes: &[u8], i: usize) -> Option<usize> {
    let quote = bytes[i];
    let mut j = i + 1;
    while j < bytes.len() {
        match bytes[j] {
            b'\\' => j += 2,
            b if b == quote => return Some(j + 1),
            _ => j += 1,
        }
    }
    None
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
                let literal = self.literal()?;
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
