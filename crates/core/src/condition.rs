//! Condition grammar for `filter` and `route`:
//!
//! ```text
//! expr       := or
//! or         := and ("or" and)*
//! and        := unary ("and" unary)*
//! unary      := "not" unary | primary
//! primary    := "(" expr ")" | comparison
//! comparison := path op literal
//! path       := ident ("." ident | "[" string "]")*
//! op         := "==" | "!=" | "=~" | "!~" | "<" | ">" | "<=" | ">="
//! literal    := string | number | "true" | "false" | "null"
//! ```
//!
//! `=~` and `!~` parse; evaluation goes through the regex facade, which the
//! regex ticket wires in. Until then they evaluate to [`EvalError::RegexNotWired`].

use crate::record::Record;
use serde_json::Value;
use std::borrow::Cow;
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    Regex,
    NotRegex,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Compare {
        path: Vec<String>,
        op: Op,
        literal: Value,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}

/// A parsed condition, ready to evaluate against records.
#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    expr: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message} at offset {offset}")]
pub struct ParseError {
    pub message: String,
    pub offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EvalError {
    #[error("regex operators are not wired yet")]
    RegexNotWired,
}

impl Condition {
    pub fn parse(input: &str) -> Result<Condition, ParseError> {
        let tokens = lex(input)?;
        let mut parser = Parser { tokens, pos: 0 };
        let expr = parser.expr()?;
        if let Some(tok) = parser.peek() {
            return Err(ParseError {
                message: format!("unexpected `{}`", tok.text),
                offset: tok.offset,
            });
        }
        Ok(Condition { expr })
    }

    pub fn eval(&self, record: &Record) -> Result<bool, EvalError> {
        eval(&self.expr, record)
    }
}

// ---------------------------------------------------------------- evaluation

fn eval(expr: &Expr, record: &Record) -> Result<bool, EvalError> {
    Ok(match expr {
        Expr::And(a, b) => eval(a, record)? && eval(b, record)?,
        Expr::Or(a, b) => eval(a, record)? || eval(b, record)?,
        Expr::Not(a) => !eval(a, record)?,
        Expr::Compare { path, op, literal } => {
            let actual = record.get_path(path).unwrap_or(Cow::Owned(Value::Null));
            let actual: &Value = &actual;
            match op {
                Op::Eq => values_equal(actual, literal),
                Op::Ne => !values_equal(actual, literal),
                Op::Lt => compare(actual, literal) == Some(Ordering::Less),
                Op::Gt => compare(actual, literal) == Some(Ordering::Greater),
                Op::Le => matches!(
                    compare(actual, literal),
                    Some(Ordering::Less | Ordering::Equal)
                ),
                Op::Ge => matches!(
                    compare(actual, literal),
                    Some(Ordering::Greater | Ordering::Equal)
                ),
                Op::Regex | Op::NotRegex => return Err(EvalError::RegexNotWired),
            }
        }
    })
}

/// Equality with numeric normalisation (`503 == 503.0`). Mismatched types
/// are never equal.
fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        _ => a == b,
    }
}

/// Ordering for numbers and strings only. Anything else is incomparable.
fn compare(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64()?.partial_cmp(&y.as_f64()?),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

// ------------------------------------------------------------------- lexing

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Str(String),
    Num(Value),
    Op(Op),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Dot,
    And,
    Or,
    Not,
    True,
    False,
    Null,
}

#[derive(Debug, Clone)]
struct Token {
    tok: Tok,
    offset: usize,
    text: String,
}

fn lex(input: &str) -> Result<Vec<Token>, ParseError> {
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        let start = i;
        let tok = match c {
            ' ' | '\t' | '\n' | '\r' => {
                i += 1;
                continue;
            }
            '(' => {
                i += 1;
                Tok::LParen
            }
            ')' => {
                i += 1;
                Tok::RParen
            }
            '[' => {
                i += 1;
                Tok::LBracket
            }
            ']' => {
                i += 1;
                Tok::RBracket
            }
            '.' => {
                i += 1;
                Tok::Dot
            }
            '"' | '\'' => {
                let (s, end) = lex_string(input, i)?;
                i = end;
                Tok::Str(s)
            }
            '=' | '!' | '<' | '>' => {
                let two = input.get(i..i + 2).unwrap_or("");
                let (op, len) = match two {
                    "==" => (Op::Eq, 2),
                    "!=" => (Op::Ne, 2),
                    "=~" => (Op::Regex, 2),
                    "!~" => (Op::NotRegex, 2),
                    "<=" => (Op::Le, 2),
                    ">=" => (Op::Ge, 2),
                    _ if c == '<' => (Op::Lt, 1),
                    _ if c == '>' => (Op::Gt, 1),
                    _ => {
                        return Err(ParseError {
                            message: format!("unknown operator starting with `{c}`"),
                            offset: i,
                        })
                    }
                };
                i += len;
                Tok::Op(op)
            }
            c if c.is_ascii_digit() || c == '-' => {
                let end = number_end(bytes, i);
                let text = &input[i..end];
                let num: serde_json::Number = text.parse().map_err(|_| ParseError {
                    message: format!("invalid number `{text}`"),
                    offset: i,
                })?;
                i = end;
                Tok::Num(Value::Number(num))
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let mut end = i + 1;
                while end < bytes.len()
                    && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_')
                {
                    end += 1;
                }
                let word = &input[i..end];
                i = end;
                match word {
                    "and" => Tok::And,
                    "or" => Tok::Or,
                    "not" => Tok::Not,
                    "true" => Tok::True,
                    "false" => Tok::False,
                    "null" => Tok::Null,
                    _ => Tok::Ident(word.to_string()),
                }
            }
            other => {
                return Err(ParseError {
                    message: format!("unexpected character `{other}`"),
                    offset: i,
                })
            }
        };
        tokens.push(Token {
            tok,
            offset: start,
            text: input[start..i].to_string(),
        });
    }
    Ok(tokens)
}

/// End of a number literal starting at `start`: an optional `-`, digits, an
/// optional fraction, an optional exponent with its own sign. Nothing else.
fn number_end(bytes: &[u8], start: usize) -> usize {
    let mut i = start;
    if bytes.get(i) == Some(&b'-') {
        i += 1;
    }
    let digits = |mut i: usize| {
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        i
    };
    i = digits(i);
    if bytes.get(i) == Some(&b'.') && bytes.get(i + 1).is_some_and(u8::is_ascii_digit) {
        i = digits(i + 1);
    }
    if matches!(bytes.get(i), Some(b'e' | b'E')) {
        let mut j = i + 1;
        if matches!(bytes.get(j), Some(b'+' | b'-')) {
            j += 1;
        }
        if bytes.get(j).is_some_and(u8::is_ascii_digit) {
            i = digits(j);
        }
    }
    i
}

/// Lex a quoted string starting at `start` (the quote). Supports `\"`, `\'`,
/// `\\`, `\n`, `\t`. Returns the value and the index after the closing quote.
fn lex_string(input: &str, start: usize) -> Result<(String, usize), ParseError> {
    let quote = input.as_bytes()[start];
    let mut out = String::new();
    let mut chars = input[start + 1..].char_indices();
    while let Some((rel, c)) = chars.next() {
        match c {
            '\\' => {
                let Some((_, e)) = chars.next() else { break };
                out.push(match e {
                    'n' => '\n',
                    't' => '\t',
                    other => other,
                });
            }
            c if c as u32 == quote as u32 => return Ok((out, start + 1 + rel + 1)),
            c => out.push(c),
        }
    }
    Err(ParseError {
        message: "unterminated string".into(),
        offset: start,
    })
}

// ------------------------------------------------------------------ parsing

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.pos).cloned();
        self.pos += 1;
        t
    }

    fn end_offset(&self) -> usize {
        self.tokens
            .last()
            .map(|t| t.offset + t.text.len())
            .unwrap_or(0)
    }

    fn expect(&mut self, want: Tok, what: &str) -> Result<(), ParseError> {
        match self.next() {
            Some(t) if t.tok == want => Ok(()),
            Some(t) => Err(ParseError {
                message: format!("expected {what}, found `{}`", t.text),
                offset: t.offset,
            }),
            None => Err(ParseError {
                message: format!("expected {what}, found end of input"),
                offset: self.end_offset(),
            }),
        }
    }

    fn expr(&mut self) -> Result<Expr, ParseError> {
        self.or()
    }

    fn or(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.and()?;
        while matches!(self.peek(), Some(Token { tok: Tok::Or, .. })) {
            self.next();
            let right = self.and()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn and(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.unary()?;
        while matches!(self.peek(), Some(Token { tok: Tok::And, .. })) {
            self.next();
            let right = self.unary()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn unary(&mut self) -> Result<Expr, ParseError> {
        if matches!(self.peek(), Some(Token { tok: Tok::Not, .. })) {
            self.next();
            return Ok(Expr::Not(Box::new(self.unary()?)));
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<Expr, ParseError> {
        if matches!(
            self.peek(),
            Some(Token {
                tok: Tok::LParen,
                ..
            })
        ) {
            self.next();
            let inner = self.expr()?;
            self.expect(Tok::RParen, "`)`")?;
            return Ok(inner);
        }
        self.comparison()
    }

    fn comparison(&mut self) -> Result<Expr, ParseError> {
        let path = self.path()?;
        let op = match self.next() {
            Some(Token {
                tok: Tok::Op(op), ..
            }) => op,
            Some(t) => {
                return Err(ParseError {
                    message: format!("expected operator, found `{}`", t.text),
                    offset: t.offset,
                })
            }
            None => {
                return Err(ParseError {
                    message: "expected operator, found end of input".into(),
                    offset: self.end_offset(),
                })
            }
        };
        let literal = match self.next() {
            Some(Token {
                tok: Tok::Str(s), ..
            }) => Value::String(s),
            Some(Token {
                tok: Tok::Num(n), ..
            }) => n,
            Some(Token { tok: Tok::True, .. }) => Value::Bool(true),
            Some(Token {
                tok: Tok::False, ..
            }) => Value::Bool(false),
            Some(Token { tok: Tok::Null, .. }) => Value::Null,
            Some(t) => {
                return Err(ParseError {
                    message: format!("expected literal, found `{}`", t.text),
                    offset: t.offset,
                })
            }
            None => {
                return Err(ParseError {
                    message: "expected literal, found end of input".into(),
                    offset: self.end_offset(),
                })
            }
        };
        Ok(Expr::Compare { path, op, literal })
    }

    fn path(&mut self) -> Result<Vec<String>, ParseError> {
        let mut path = match self.next() {
            Some(Token {
                tok: Tok::Ident(s), ..
            }) => vec![s],
            Some(t) => {
                return Err(ParseError {
                    message: format!("expected field path, found `{}`", t.text),
                    offset: t.offset,
                })
            }
            None => {
                return Err(ParseError {
                    message: "expected field path, found end of input".into(),
                    offset: self.end_offset(),
                })
            }
        };
        loop {
            match self.peek().map(|t| &t.tok) {
                Some(Tok::Dot) => {
                    self.next();
                    match self.next() {
                        Some(Token {
                            tok: Tok::Ident(s), ..
                        }) => path.push(s),
                        Some(Token {
                            tok: Tok::Num(n), ..
                        }) => path.push(n.to_string()),
                        Some(t) => {
                            return Err(ParseError {
                                message: format!(
                                    "expected field name after `.`, found `{}`",
                                    t.text
                                ),
                                offset: t.offset,
                            })
                        }
                        None => {
                            return Err(ParseError {
                                message: "expected field name after `.`".into(),
                                offset: self.end_offset(),
                            })
                        }
                    }
                }
                Some(Tok::LBracket) => {
                    self.next();
                    match self.next() {
                        Some(Token {
                            tok: Tok::Str(s), ..
                        }) => path.push(s),
                        Some(Token {
                            tok: Tok::Num(n), ..
                        }) => path.push(n.to_string()),
                        Some(t) => {
                            return Err(ParseError {
                                message: format!("expected key inside `[]`, found `{}`", t.text),
                                offset: t.offset,
                            })
                        }
                        None => {
                            return Err(ParseError {
                                message: "expected key inside `[]`".into(),
                                offset: self.end_offset(),
                            })
                        }
                    }
                    self.expect(Tok::RBracket, "`]`")?;
                }
                _ => return Ok(path),
            }
        }
    }
}
