//! Field paths: the one way every stage names a record field.
//!
//! A path is `root ("." segment)*`. The root is a top-level record field. When it is
//! `attributes`, `resource` or `scope`, the segments joined with dots are the map key, so
//! `attributes.http.status` names the `http.status` key of `attributes` and
//! `resource.k8s.pod-name` the `k8s.pod-name` key of `resource`. A bare segment is one or
//! more of `[A-Za-z0-9_-]`; a segment with any other character is double-quoted, with `\"`
//! and `\\` as the only escapes: `attributes."Event ID".code` names the `Event ID.code` key.
//! The three maps are flat: values are scalars and nothing below a key is addressable.
//! `body` is addressed only as a whole, and the scalar fields take no segments.

use std::borrow::Cow;
use std::fmt;

use serde_json::{Map, Value};

use crate::record::Record;

/// Errors from parsing a path or writing through one. Each message says what is wrong and
/// what to write instead.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PathError {
    /// The path is empty or has an empty segment (`a..b`, `a.""`, a leading or trailing dot).
    #[error(
        "`{path}` has an empty segment; instead use `<field>.<key>` with one dot between names"
    )]
    EmptySegment {
        /// The path text.
        path: String,
    },
    /// A bare segment has a character outside `[A-Za-z0-9_-]`, or a quoted segment is
    /// followed by something other than a dot.
    #[error("`{segment}` has `{ch}`; instead use `{instead}`")]
    InvalidSegment {
        /// The offending segment as written.
        segment: String,
        /// The first character that is not allowed there.
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

/// A top-level field that is addressed as a whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Field {
    Id,
    Kind,
    TimeUnixNano,
    ObservedTimeUnixNano,
    SeverityText,
    SeverityNumber,
    Body,
    TraceId,
    SpanId,
}

/// One of the three flat maps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MapField {
    Attributes,
    Resource,
    Scope,
}

/// What a path names once parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Target {
    Field(Field),
    /// A key of one of the maps.
    Key(MapField, String),
}

const FIELD_NAMES: [(&str, Field); 9] = [
    ("id", Field::Id),
    ("kind", Field::Kind),
    ("time_unix_nano", Field::TimeUnixNano),
    ("observed_time_unix_nano", Field::ObservedTimeUnixNano),
    ("severity_text", Field::SeverityText),
    ("severity_number", Field::SeverityNumber),
    ("body", Field::Body),
    ("trace_id", Field::TraceId),
    ("span_id", Field::SpanId),
];

const MAP_NAMES: [(&str, MapField); 3] = [
    ("attributes", MapField::Attributes),
    ("resource", MapField::Resource),
    ("scope", MapField::Scope),
];

impl Field {
    fn parse(name: &str) -> Option<Self> {
        FIELD_NAMES
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, f)| *f)
    }

    fn name(self) -> &'static str {
        FIELD_NAMES
            .iter()
            .find(|(_, f)| *f == self)
            .map_or("", |(n, _)| n)
    }
}

impl MapField {
    fn parse(name: &str) -> Option<Self> {
        MAP_NAMES.iter().find(|(n, _)| *n == name).map(|(_, m)| *m)
    }

    fn name(self) -> &'static str {
        MAP_NAMES
            .iter()
            .find(|(_, m)| *m == self)
            .map_or("", |(n, _)| n)
    }

    fn get(self, record: &Record) -> &Map<String, Value> {
        match self {
            Self::Attributes => &record.attributes,
            Self::Resource => &record.resource,
            Self::Scope => &record.scope,
        }
    }

    fn get_mut(self, record: &mut Record) -> &mut Map<String, Value> {
        match self {
            Self::Attributes => &mut record.attributes,
            Self::Resource => &mut record.resource,
            Self::Scope => &mut record.scope,
        }
    }
}

/// A parsed dotted path into a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldPath {
    target: Target,
}

/// Whether `c` may appear in a bare path segment.
const fn is_segment_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

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

/// A segment as written in a path: bare when it can be, quoted otherwise.
fn display_segment(segment: &str) -> String {
    if !segment.is_empty() && segment.chars().all(is_segment_char) {
        segment.to_owned()
    } else {
        format!("\"{}\"", segment.replace('\\', "\\\\").replace('"', "\\\""))
    }
}

/// `attributes["http.status"]["a b"]` as `attributes.http.status."a b"`, for the bracket
/// error's hint.
fn unbracket(path: &str) -> String {
    let mut segments: Vec<String> = Vec::new();
    for part in path.split(['[', ']']) {
        let part = part.trim_matches(['"', '\'']);
        for segment in part.split('.') {
            if !segment.is_empty() {
                segments.push(display_segment(segment));
            }
        }
    }
    segments.join(".")
}

/// The segments of `path`, in order, with quotes and escapes resolved. Only key segments
/// have a charset; the root is judged by name afterwards.
fn split_segments(path: &str) -> Result<Vec<String>, PathError> {
    let empty = || PathError::EmptySegment {
        path: path.to_owned(),
    };
    let mut segments: Vec<String> = Vec::new();
    let mut i = 0;
    loop {
        let start = i;
        let rest = &path[i..];
        let segment = if rest.starts_with('"') && !segments.is_empty() {
            let (segment, end) = unquote(path, i)?;
            i = end;
            if segment.is_empty() {
                return Err(empty());
            }
            segment
        } else {
            let end = rest
                .find(['.', '[', ']'])
                .map_or(path.len(), |offset| i + offset);
            let segment = &path[i..end];
            i = end;
            if segment.is_empty() && !path[i..].starts_with(['[', ']']) {
                return Err(empty());
            }
            if let Some(ch) = segment
                .chars()
                .find(|&c| !segments.is_empty() && !is_segment_char(c))
            {
                return Err(PathError::InvalidSegment {
                    segment: segment.to_owned(),
                    ch,
                    instead: quoted_hint(&segments, segment, &path[i..]),
                });
            }
            segment.to_owned()
        };
        match path[i..].chars().next() {
            None => {
                segments.push(segment);
                return Ok(segments);
            }
            Some('.') => {
                segments.push(segment);
                i += 1;
                if i == path.len() {
                    return Err(empty());
                }
            }
            Some('[' | ']') => {
                return Err(PathError::BracketSyntax {
                    path: path.to_owned(),
                    instead: unbracket(path),
                });
            }
            Some(ch) => {
                // Text glued to a closing quote: `attributes."a"b`.
                let end = path[i..].find('.').map_or(path.len(), |offset| i + offset);
                return Err(PathError::InvalidSegment {
                    segment: path[start..end].to_owned(),
                    ch,
                    instead: quoted_hint(
                        &segments,
                        &format!("{segment}{}", &path[i..end]),
                        &path[end..],
                    ),
                });
            }
        }
    }
}

/// Read a quoted segment starting at `start` (the quote). Returns the contents and the
/// offset just past the closing quote. `\"` and `\\` are the only escapes.
fn unquote(path: &str, start: usize) -> Result<(String, usize), PathError> {
    let mut out = String::new();
    let mut chars = path[start + 1..].char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '"' => return Ok((out, start + 1 + i + 1)),
            '\\' => match chars.next() {
                Some((_, c @ ('"' | '\\'))) => out.push(c),
                Some((_, other)) => {
                    return Err(PathError::InvalidSegment {
                        segment: path[start..].to_owned(),
                        ch: other,
                        instead: format!(
                            "{}\\\\{}",
                            &path[..start + 1 + i],
                            &path[start + 1 + i + 1..]
                        ),
                    });
                }
                None => break,
            },
            c => out.push(c),
        }
    }
    Err(PathError::UnterminatedQuote {
        path: path.to_owned(),
    })
}

/// `before.` + the bad segment quoted + the rest of the path, for the segment error's hint.
fn quoted_hint(before: &[String], bad: &str, rest: &str) -> String {
    let mut out: Vec<String> = before.iter().map(|s| display_segment(s)).collect();
    out.push(display_segment(bad));
    format!("{}{rest}", out.join("."))
}

impl FieldPath {
    /// Parse a dotted path.
    ///
    /// # Errors
    ///
    /// A [`PathError`] when the text is not `root ("." segment)*`, the root is not a record
    /// field, a scalar or `body` is given segments, or a map is named without a key.
    pub fn parse(path: &str) -> Result<Self, PathError> {
        let segments = split_segments(path)?;
        let (root, keys) = segments
            .split_first()
            .ok_or_else(|| PathError::EmptySegment {
                path: path.to_owned(),
            })?;
        if let Some(map) = MapField::parse(root) {
            return if keys.is_empty() {
                Err(PathError::MapNeedsKey {
                    field: root.clone(),
                })
            } else {
                Ok(Self {
                    target: Target::Key(map, keys.join(".")),
                })
            };
        }
        let field =
            Field::parse(root).ok_or_else(|| PathError::UnknownField { name: root.clone() })?;
        if keys.is_empty() {
            Ok(Self {
                target: Target::Field(field),
            })
        } else {
            Err(PathError::NotAMap {
                field: root.clone(),
                hint: if field == Field::Body {
                    ", or parse it into attributes first"
                } else {
                    ""
                },
            })
        }
    }

    /// The flat map key when the path names a key of `attributes`, `resource` or `scope`.
    #[must_use]
    pub fn map_key(&self) -> Option<&str> {
        match &self.target {
            Target::Key(_, key) => Some(key),
            Target::Field(_) => None,
        }
    }

    /// Read the field. `None` when it is absent.
    ///
    /// Map keys and `body` are borrowed; the typed top-level fields are converted to an
    /// owned [`Value`].
    #[must_use]
    pub fn read<'a>(&self, record: &'a Record) -> Option<Cow<'a, Value>> {
        let field = match &self.target {
            Target::Key(map, key) => return map.get(record).get(key).map(Cow::Borrowed),
            Target::Field(field) => *field,
        };
        let owned = match field {
            Field::Id => record.id.map(|id| Value::from(id.0)),
            Field::Kind => Some(Value::from(record.kind.as_str())),
            Field::TimeUnixNano => record.time_unix_nano.map(Value::from),
            Field::ObservedTimeUnixNano => record.observed_time_unix_nano.map(Value::from),
            Field::SeverityText => record.severity_text.as_deref().map(Value::from),
            Field::SeverityNumber => record.severity_number.map(Value::from),
            Field::Body => return record.body.as_ref().map(Cow::Borrowed),
            Field::TraceId => record.trace_id.as_deref().map(Value::from),
            Field::SpanId => record.span_id.as_deref().map(Value::from),
        };
        owned.map(Cow::Owned)
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
        let field = match &self.target {
            Target::Key(map, key) => {
                if matches!(value, Value::Array(_) | Value::Object(_)) {
                    return Err(self.wrong_type("a scalar", &value));
                }
                map.get_mut(record).insert(key.clone(), value);
                return Ok(());
            }
            Target::Field(field) => *field,
        };
        match field {
            Field::Id | Field::Kind => return Err(self.read_only(field)),
            Field::TimeUnixNano => record.time_unix_nano = self.expect_u64(value)?,
            Field::ObservedTimeUnixNano => {
                record.observed_time_unix_nano = self.expect_u64(value)?;
            }
            Field::SeverityText => record.severity_text = self.expect_string(value)?,
            Field::SeverityNumber => record.severity_number = self.expect_i32(value)?,
            Field::Body => record.body = Some(value),
            Field::TraceId => record.trace_id = self.expect_string(value)?,
            Field::SpanId => record.span_id = self.expect_string(value)?,
        }
        Ok(())
    }

    /// Remove the field, returning the old value. Absent is `Ok(None)`.
    ///
    /// # Errors
    ///
    /// [`PathError::ReadOnly`] for `id` and `kind`.
    pub fn remove(&self, record: &mut Record) -> Result<Option<Value>, PathError> {
        let field = match &self.target {
            Target::Key(map, key) => return Ok(map.get_mut(record).remove(key)),
            Target::Field(field) => *field,
        };
        Ok(match field {
            Field::Id | Field::Kind => return Err(self.read_only(field)),
            Field::TimeUnixNano => record.time_unix_nano.take().map(Value::from),
            Field::ObservedTimeUnixNano => record.observed_time_unix_nano.take().map(Value::from),
            Field::SeverityText => record.severity_text.take().map(Value::from),
            Field::SeverityNumber => record.severity_number.take().map(Value::from),
            Field::Body => record.body.take(),
            Field::TraceId => record.trace_id.take().map(Value::from),
            Field::SpanId => record.span_id.take().map(Value::from),
        })
    }

    fn read_only(&self, field: Field) -> PathError {
        PathError::ReadOnly {
            field: field.name().to_owned(),
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
        match &value {
            Value::Null => Ok(None),
            Value::Number(n) => n
                .as_u64()
                .map(Some)
                .ok_or_else(|| self.wrong_type("a non-negative integer", &value)),
            _ => Err(self.wrong_type("a non-negative integer", &value)),
        }
    }

    fn expect_i32(&self, value: Value) -> Result<Option<i32>, PathError> {
        match &value {
            Value::Null => Ok(None),
            Value::Number(n) => n
                .as_i64()
                .and_then(|i| i32::try_from(i).ok())
                .map(Some)
                .ok_or_else(|| self.wrong_type("an integer", &value)),
            _ => Err(self.wrong_type("an integer", &value)),
        }
    }
}

impl fmt::Display for FieldPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.target {
            Target::Field(field) => f.write_str(field.name()),
            Target::Key(map, key) => {
                f.write_str(map.name())?;
                for part in key.split('.') {
                    write!(f, ".{}", display_segment(part))?;
                }
                Ok(())
            }
        }
    }
}
