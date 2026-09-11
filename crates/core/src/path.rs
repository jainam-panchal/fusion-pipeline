//! Field paths: the one way every stage names a record field.
//!
//! A path is `root ("." segment)*`. The root is a top-level record field. When it is
//! `attributes`, `resource` or `scope`, the segments joined with dots are the map key, so
//! `attributes.http.status` names the `http.status` key of `attributes` and
//! `resource.k8s.pod-name` the `k8s.pod-name` key of `resource`. A segment is one or more of
//! `[A-Za-z0-9_-]`, or a double-quoted string for keys with other characters:
//! `attributes."Event ID".code` names the `Event ID.code` key. The three maps are flat: values
//! are scalars and nothing below a key is addressable. `body` is addressed only as a whole,
//! and the scalar fields take no segments.

use std::borrow::Cow;
use std::fmt;

use serde_json::{Map, Value};

use crate::record::Record;

/// Errors from parsing a path or writing through one. Each message says what is wrong and
/// what to write instead.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PathError {
    /// The path is empty or has an empty segment (`a..b`, a leading or trailing dot).
    #[error(
        "`{path}` has an empty segment; instead use `<field>.<key>` with one dot between names"
    )]
    EmptySegment {
        /// The path text.
        path: String,
    },
    /// A bare segment has a character outside `[A-Za-z0-9_-]`.
    #[error("`{segment}` has `{ch}`; instead use `{instead}`")]
    InvalidSegment {
        /// The offending segment.
        segment: String,
        /// The first character that is not allowed.
        ch: char,
        /// The same path with that segment quoted.
        instead: String,
    },
    /// A quoted segment has no closing quote.
    #[error("`{path}` has an unclosed quote; instead close it: `attributes.\"some key\"`")]
    UnterminatedQuote {
        /// The path text.
        path: String,
    },
    /// The path uses `[...]`, which is no longer part of the grammar.
    #[error("brackets are not allowed; instead use `{instead}`")]
    BracketSyntax {
        /// The path text.
        path: String,
        /// The same path in dotted form.
        instead: String,
    },
    /// The first segment is not a record field.
    #[error("`{name}` is not a record field; instead use one of {FIELDS}")]
    UnknownField {
        /// The segment text.
        name: String,
    },
    /// A field that is addressed only as a whole was given further segments.
    #[error("`{field}` is one value and has no fields; instead use `{field}`{hint}")]
    NotAMap {
        /// The field name.
        field: String,
        /// Extra advice for `body`; empty otherwise.
        hint: &'static str,
    },
    /// A map field was named without a key.
    #[error("`{field}` needs a key; instead use `{field}.<key>`")]
    MapNeedsKey {
        /// The map field name.
        field: String,
    },
    /// `id` and `kind` cannot be written or removed.
    #[error("`{field}` is read-only and cannot be written or removed")]
    ReadOnly {
        /// The field name.
        field: String,
    },
    /// The value has the wrong JSON type for the field.
    #[error("`{field}` takes {expected}, not {actual}")]
    WrongType {
        /// The field or map key.
        field: String,
        /// What the field accepts.
        expected: &'static str,
        /// The JSON type of the offered value.
        actual: &'static str,
    },
}

/// The record fields a path may start at, for error messages.
const FIELDS: &str = "id, kind, body, severity_text, severity_number, time_unix_nano, \
observed_time_unix_nano, trace_id, span_id, attributes.<key>, resource.<key>, scope.<key>";

fn json_type(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a bool",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// Top-level record field a path starts at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Root {
    Id,
    Kind,
    TimeUnixNano,
    ObservedTimeUnixNano,
    SeverityText,
    SeverityNumber,
    Body,
    Attributes,
    Resource,
    Scope,
    TraceId,
    SpanId,
}

impl Root {
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "id" => Self::Id,
            "kind" => Self::Kind,
            "time_unix_nano" => Self::TimeUnixNano,
            "observed_time_unix_nano" => Self::ObservedTimeUnixNano,
            "severity_text" => Self::SeverityText,
            "severity_number" => Self::SeverityNumber,
            "body" => Self::Body,
            "attributes" => Self::Attributes,
            "resource" => Self::Resource,
            "scope" => Self::Scope,
            "trace_id" => Self::TraceId,
            "span_id" => Self::SpanId,
            _ => return None,
        })
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Kind => "kind",
            Self::TimeUnixNano => "time_unix_nano",
            Self::ObservedTimeUnixNano => "observed_time_unix_nano",
            Self::SeverityText => "severity_text",
            Self::SeverityNumber => "severity_number",
            Self::Body => "body",
            Self::Attributes => "attributes",
            Self::Resource => "resource",
            Self::Scope => "scope",
            Self::TraceId => "trace_id",
            Self::SpanId => "span_id",
        }
    }

    const fn is_map(self) -> bool {
        matches!(self, Self::Attributes | Self::Resource | Self::Scope)
    }
}

/// A parsed dotted path into a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldPath {
    root: Root,
    /// The flat map key; `Some` exactly when the root is a map.
    key: Option<String>,
}

/// `attributes["http.status"]["x"]` as `attributes.http.status.x`, for the bracket error.
fn unbracket(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for part in path.split(['[', ']']) {
        let part = part.trim_matches(['"', '\'']);
        if part.is_empty() {
            continue;
        }
        if !out.is_empty() && !out.ends_with('.') && !part.starts_with('.') {
            out.push('.');
        }
        out.push_str(part);
    }
    out
}

/// Whether `c` may appear in a bare path segment.
#[must_use]
pub const fn is_segment_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

/// Split `path` on dots into its segments, honouring double-quoted segments. The root is
/// never quoted. Every bare segment is checked against [`is_segment_char`].
fn split_segments(path: &str) -> Result<Vec<String>, PathError> {
    let empty = || PathError::EmptySegment {
        path: path.to_owned(),
    };
    let mut segments = Vec::new();
    let mut chars = path.char_indices().peekable();
    loop {
        match chars.peek().copied() {
            Some((_, '"')) if !segments.is_empty() => {
                chars.next();
                let mut out = String::new();
                let closed = loop {
                    match chars.next() {
                        None => break false,
                        Some((_, '"')) => break true,
                        Some((_, '\\')) => match chars.next() {
                            Some((_, c)) => out.push(c),
                            None => break false,
                        },
                        Some((_, c)) => out.push(c),
                    }
                };
                if !closed {
                    return Err(PathError::UnterminatedQuote {
                        path: path.to_owned(),
                    });
                }
                segments.push(out);
            }
            _ => {
                let mut out = String::new();
                while let Some(&(_, c)) = chars.peek() {
                    if c == '.' {
                        break;
                    }
                    chars.next();
                    out.push(c);
                }
                if out.is_empty() {
                    return Err(empty());
                }
                // The root is checked by name; only key segments have a charset.
                let is_key = !segments.is_empty();
                if let Some(ch) = out.chars().find(|&c| is_key && !is_segment_char(c)) {
                    let instead = quote_segment_in(path, &out);
                    return Err(PathError::InvalidSegment {
                        segment: out,
                        ch,
                        instead,
                    });
                }
                segments.push(out);
            }
        }
        match chars.next() {
            None => return Ok(segments),
            Some((_, '.')) => {
                if chars.peek().is_none() {
                    return Err(empty());
                }
            }
            Some(_) => return Err(empty()),
        }
    }
}

/// `path` with the first occurrence of `segment` wrapped in quotes, for the error hint.
fn quote_segment_in(path: &str, segment: &str) -> String {
    path.replacen(segment, &format!("\"{}\"", quote_escape(segment)), 1)
}

fn quote_escape(segment: &str) -> String {
    segment.replace('\\', "\\\\").replace('"', "\\\"")
}

/// A segment as written in a path: bare when it can be, quoted otherwise.
fn display_segment(segment: &str) -> String {
    if !segment.is_empty() && segment.chars().all(is_segment_char) {
        segment.to_owned()
    } else {
        format!("\"{}\"", quote_escape(segment))
    }
}

impl FieldPath {
    /// Parse a dotted path.
    ///
    /// # Errors
    ///
    /// A [`PathError`] when the text is not `root ("." segment)*`, the root is not a record
    /// field, a scalar or `body` is given segments, or a map is named without a key.
    pub fn parse(path: &str) -> Result<Self, PathError> {
        if path.contains('[') || path.contains(']') {
            return Err(PathError::BracketSyntax {
                path: path.to_owned(),
                instead: unbracket(path),
            });
        }
        let segments = split_segments(path)?;
        let (root_name, rest) = segments
            .split_first()
            .ok_or_else(|| PathError::EmptySegment {
                path: path.to_owned(),
            })?;
        Self::from_segments(root_name, rest)
    }

    /// Build from an already-split path: the root name and the key segments after it.
    ///
    /// # Errors
    ///
    /// As [`FieldPath::parse`], for the errors that concern the root and the segment count.
    pub fn from_segments(root_name: &str, segments: &[String]) -> Result<Self, PathError> {
        let root = Root::parse(root_name).ok_or_else(|| PathError::UnknownField {
            name: root_name.to_owned(),
        })?;
        match (root.is_map(), segments.is_empty()) {
            (true, true) => Err(PathError::MapNeedsKey {
                field: root_name.to_owned(),
            }),
            (true, false) => Ok(Self {
                root,
                key: Some(segments.join(".")),
            }),
            (false, true) => Ok(Self { root, key: None }),
            (false, false) => Err(PathError::NotAMap {
                field: root_name.to_owned(),
                hint: if root == Root::Body {
                    ", or parse it into attributes first"
                } else {
                    ""
                },
            }),
        }
    }

    /// Read the field. `None` when it is absent.
    ///
    /// Map keys and `body` are borrowed; the typed top-level fields are converted to an
    /// owned [`Value`].
    #[must_use]
    pub fn read<'a>(&self, record: &'a Record) -> Option<Cow<'a, Value>> {
        fn owned<'a>(v: Value) -> Option<Cow<'a, Value>> {
            Some(Cow::Owned(v))
        }
        match self.root {
            Root::Id => record.id.map(|id| Cow::Owned(Value::from(id.0))),
            Root::Kind => owned(Value::from(record.kind.as_str())),
            Root::TimeUnixNano => record.time_unix_nano.map(Value::from).map(Cow::Owned),
            Root::ObservedTimeUnixNano => record
                .observed_time_unix_nano
                .map(Value::from)
                .map(Cow::Owned),
            Root::SeverityText => record
                .severity_text
                .as_deref()
                .map(Value::from)
                .map(Cow::Owned),
            Root::SeverityNumber => record.severity_number.map(Value::from).map(Cow::Owned),
            Root::Body => record.body.as_ref().map(Cow::Borrowed),
            Root::Attributes => self.map_get(&record.attributes),
            Root::Resource => self.map_get(&record.resource),
            Root::Scope => self.map_get(&record.scope),
            Root::TraceId => record.trace_id.as_deref().map(Value::from).map(Cow::Owned),
            Root::SpanId => record.span_id.as_deref().map(Value::from).map(Cow::Owned),
        }
    }

    fn map_get<'a>(&self, map: &'a Map<String, Value>) -> Option<Cow<'a, Value>> {
        map.get(self.key.as_deref()?).map(Cow::Borrowed)
    }

    /// Write `value` to the field, creating or replacing it.
    ///
    /// `null` clears an optional top-level field and is stored as-is under a map key.
    ///
    /// # Errors
    ///
    /// [`PathError::ReadOnly`] for `id` and `kind`; [`PathError::WrongType`] when a typed
    /// field is offered the wrong JSON type (`severity_number` takes an integer in `i32`,
    /// `severity_text`, `trace_id` and `span_id` a string, the time fields an integer in
    /// `u64`) or a map key is offered an array or object. The record is unchanged on error.
    pub fn write(&self, record: &mut Record, value: Value) -> Result<(), PathError> {
        match self.root {
            Root::Id | Root::Kind => Err(self.read_only()),
            Root::TimeUnixNano => {
                record.time_unix_nano = self.expect_u64(value)?;
                Ok(())
            }
            Root::ObservedTimeUnixNano => {
                record.observed_time_unix_nano = self.expect_u64(value)?;
                Ok(())
            }
            Root::SeverityText => {
                record.severity_text = self.expect_string(value)?;
                Ok(())
            }
            Root::SeverityNumber => {
                record.severity_number = self.expect_i32(value)?;
                Ok(())
            }
            Root::Body => {
                record.body = Some(value);
                Ok(())
            }
            Root::Attributes => self.map_insert(&mut record.attributes, value),
            Root::Resource => self.map_insert(&mut record.resource, value),
            Root::Scope => self.map_insert(&mut record.scope, value),
            Root::TraceId => {
                record.trace_id = self.expect_string(value)?;
                Ok(())
            }
            Root::SpanId => {
                record.span_id = self.expect_string(value)?;
                Ok(())
            }
        }
    }

    /// Remove the field, returning the old value. Absent is `Ok(None)`.
    ///
    /// # Errors
    ///
    /// [`PathError::ReadOnly`] for `id` and `kind`.
    pub fn remove(&self, record: &mut Record) -> Result<Option<Value>, PathError> {
        Ok(match self.root {
            Root::Id | Root::Kind => return Err(self.read_only()),
            Root::TimeUnixNano => record.time_unix_nano.take().map(Value::from),
            Root::ObservedTimeUnixNano => record.observed_time_unix_nano.take().map(Value::from),
            Root::SeverityText => record.severity_text.take().map(Value::from),
            Root::SeverityNumber => record.severity_number.take().map(Value::from),
            Root::Body => record.body.take(),
            Root::Attributes => self.map_remove(&mut record.attributes),
            Root::Resource => self.map_remove(&mut record.resource),
            Root::Scope => self.map_remove(&mut record.scope),
            Root::TraceId => record.trace_id.take().map(Value::from),
            Root::SpanId => record.span_id.take().map(Value::from),
        })
    }

    fn map_remove(&self, map: &mut Map<String, Value>) -> Option<Value> {
        map.remove(self.key.as_deref()?)
    }

    fn map_insert(&self, map: &mut Map<String, Value>, value: Value) -> Result<(), PathError> {
        let Some(key) = &self.key else {
            return Err(PathError::MapNeedsKey {
                field: self.root.name().to_owned(),
            });
        };
        if matches!(value, Value::Array(_) | Value::Object(_)) {
            return Err(self.wrong_type("a scalar", &value));
        }
        map.insert(key.clone(), value);
        Ok(())
    }

    fn read_only(&self) -> PathError {
        PathError::ReadOnly {
            field: self.root.name().to_owned(),
        }
    }

    fn wrong_type(&self, expected: &'static str, actual: &Value) -> PathError {
        PathError::WrongType {
            field: self.to_string(),
            expected,
            actual: json_type(actual),
        }
    }

    fn expect_string(&self, value: Value) -> Result<Option<String>, PathError> {
        match value {
            Value::Null => Ok(None),
            Value::String(s) => Ok(Some(s)),
            other => Err(self.wrong_type("a string", &other)),
        }
    }

    fn expect_u64(&self, value: Value) -> Result<Option<u64>, PathError> {
        match value {
            Value::Null => Ok(None),
            Value::Number(ref n) if n.as_u64().is_some() => Ok(n.as_u64()),
            other => Err(self.wrong_type("a non-negative integer", &other)),
        }
    }

    fn expect_i32(&self, value: Value) -> Result<Option<i32>, PathError> {
        match value {
            Value::Null => Ok(None),
            Value::Number(ref n) if n.as_i64().and_then(|i| i32::try_from(i).ok()).is_some() => {
                Ok(n.as_i64().and_then(|i| i32::try_from(i).ok()))
            }
            other => Err(self.wrong_type("an integer", &other)),
        }
    }

    /// The flat map key when the path names a key of `attributes`, `resource` or `scope`.
    #[must_use]
    pub fn map_key(&self) -> Option<&str> {
        self.key.as_deref()
    }
}

impl fmt::Display for FieldPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.root.name())?;
        if let Some(key) = &self.key {
            for part in key.split('.') {
                write!(f, ".{}", display_segment(part))?;
            }
        }
        Ok(())
    }
}
