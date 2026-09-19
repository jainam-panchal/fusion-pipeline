//! Field paths: the one way every stage names part of a record.
//!
//! A record is any JSON value (issue #79, ADR 0008), so a path is a list of segments walked
//! over it: `level` names a top-level key, `test2.key2` a key inside an object,
//! `attributes.0.value` a list position and then keys below it. A segment that is one or
//! more of `[A-Za-z0-9_-]` is written bare; any other segment is double-quoted, with `\"`
//! and `\\` as the only escapes, so `resource."log.format"` names the key `log.format` of
//! `resource` and `attributes."Event ID".code` the `code` key under `Event ID`.
//!
//! `.` alone names the whole record. A path may also be written with a leading dot, which
//! names the record and nothing else: `."log.format"` and `.0.value` are the way to start a
//! path at a key that is not a bare word, and `.meta` is the payload's own `meta` key.
//!
//! Without that leading dot, `meta` is reserved: `meta.id`, `meta.tenant`,
//! `meta.ingestion_time` and `meta.delivery_count` name the pipeline's view of the record
//! rather than the record (ADR 0005). A meta path reads the record's [`Meta`] and refuses
//! every write and removal, which is the one refusal left in this module.
//!
//! Reading a path that is not there is [`FieldValue::Null`], never an error: with no field
//! list there is no such thing as a wrong path, only one that matches nothing. Writing
//! through a path makes the path exist: a missing key is created, and a value in the way of
//! the walk (a scalar, or a list with no such position) is replaced by an object. So a write
//! through a record path cannot fail, and [`FieldPath::writable`] proves that once, at load,
//! by handing back a [`WritePath`].

use std::cmp::Ordering;
use std::fmt;
use std::sync::LazyLock;

use serde_json::{Map, Value};

use crate::meta::{Meta, MetaField, MetaValue};
use crate::record::Record;

/// Errors from parsing a path or writing through one. Each message says what is wrong and
/// what to write instead.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PathError {
    /// The path is empty or has an empty segment (`a..b`, `a.""`, a trailing dot).
    #[error(
        "`{path}` has an empty segment; instead use `<name>.<name>` with one dot between names"
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
    /// The path uses `[...]`, which is not part of the grammar.
    #[error("brackets are not allowed; instead use `{instead}`")]
    BracketSyntax {
        /// The same path in dotted form.
        instead: String,
    },
    /// `meta` was named without a field, or with one that is not a meta field.
    #[error(
        "`{path}` is not a meta field; instead use one of {}, or `.{META_ROOT}...` for a \
         payload key spelled `{META_ROOT}`",
        META_FIELDS.as_str()
    )]
    UnknownMetaField {
        /// The path text.
        path: String,
    },
    /// A write or removal named a meta path.
    #[error(
        "`{path}` is the pipeline's and cannot be written or removed; instead copy it into a \
         record field: `copy {{from: {path}, to: <field>}}`"
    )]
    ReadOnly {
        /// The meta path.
        path: String,
    },
}

/// Every meta path, as the unknown-meta-field error lists them.
static META_FIELDS: LazyLock<String> = LazyLock::new(|| {
    MetaField::ALL
        .into_iter()
        .map(|field| format!("`{META_ROOT}.{}`", field.as_str()))
        .collect::<Vec<_>>()
        .join(", ")
});

/// The root of the meta paths.
const META_ROOT: &str = "meta";

/// What a path names once parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Target {
    /// Segments walked over the record. Empty is the whole record.
    Record(Vec<String>),
    /// A value of the record's `Meta`.
    Meta(MetaField),
}

/// A number as read from a record. Two integers compare exactly; when either side is a
/// float both are compared as `f64`, which is lossy above 2^53 and never equal for NaN.
#[derive(Debug, Clone, Copy)]
pub enum Num {
    /// An integer, wide enough for both `i64` and `u64`.
    Int(i128),
    /// A float.
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

/// A value as read from a record: a borrowed view, never a copy. A path that matches nothing
/// reads as [`FieldValue::Null`], the same as a JSON `null`.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum FieldValue<'a> {
    /// Absent, or JSON `null`.
    Null,
    /// A JSON bool.
    Bool(bool),
    /// A JSON number.
    Num(Num),
    /// A JSON string.
    Str(&'a str),
    /// An array or object. Comparable to nothing.
    Json(&'a Value),
}

impl<'a> FieldValue<'a> {
    /// View a JSON value.
    #[must_use]
    pub fn from_json(v: &'a Value) -> Self {
        match v {
            Value::Null => Self::Null,
            Value::Bool(b) => Self::Bool(*b),
            Value::Number(_) => Num::from_value(v).map_or(Self::Null, Self::Num),
            Value::String(s) => Self::Str(s),
            Value::Array(_) | Value::Object(_) => Self::Json(v),
        }
    }
}

/// A parsed path into a record or its `Meta`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldPath {
    target: Target,
}

/// A path proved to name the record rather than its `Meta`, so writing and removing through
/// it cannot fail. [`FieldPath::writable`] is the only way to get one, which puts the one
/// refusal in this module at config load, where the node can be named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritePath {
    segments: Vec<String>,
}

/// Whether `c` may appear in a bare path segment.
const fn is_segment_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

/// The list position `segment` names, if it names one: digits with no leading zero, so one
/// segment cannot mean two things by the record's shape. `007` would otherwise be position 7
/// in a list and the key `007` in an object; it is only ever the key.
fn position(segment: &str) -> Option<usize> {
    if segment.is_empty() || !segment.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if segment.len() > 1 && segment.starts_with('0') {
        return None;
    }
    segment.parse().ok()
}

/// A segment as written in a path: bare when it can be, quoted otherwise.
fn display_segment(segment: &str) -> String {
    if !segment.is_empty() && segment.chars().all(is_segment_char) {
        segment.to_owned()
    } else {
        format!("\"{}\"", segment.replace('\\', "\\\\").replace('"', "\\\""))
    }
}

/// `attributes["http.status"]["a b"]` as `attributes."http.status"."a b"`, for the bracket
/// error's hint. The scan honours quotes, so `["a[0]"]` and `['a]b']` name the key the user
/// wrote; an unclosed quote runs to the end of the path.
fn unbracket(path: &str) -> String {
    let mut segments: Vec<String> = Vec::new();
    let mut i = 0;
    while i < path.len() {
        let rest = &path[i..];
        if let Some(inner) = rest.strip_prefix('[') {
            let (inner, consumed) = bracket_contents(inner);
            i += 1 + consumed;
            segments.extend(bracket_pieces(inner));
        } else if rest.starts_with(']') {
            i += 1;
        } else {
            let end = rest.find(['[', ']']).unwrap_or(rest.len());
            segments.extend(dotted_pieces(&rest[..end]));
            i += end;
        }
    }
    let mut out: Vec<String> = segments.iter().map(|s| display_segment(s)).collect();
    if out.len() == 1 && out[0] == META_ROOT {
        out.push("<field>".to_owned());
    }
    out.join(".")
}

/// The text inside a bracket that opens just before `text`, and how many bytes of `text` it
/// and the closing `]` take. Quotes are honoured; an unclosed quote or bracket runs to the
/// end.
fn bracket_contents(text: &str) -> (&str, usize) {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' | b'\'' => match quoted_end(text, i) {
                Some(end) => i = end,
                None => return (text, text.len()),
            },
            b']' => return (&text[..i], i + 1),
            _ => i += 1,
        }
    }
    (text, text.len())
}

/// What one bracket's contents name: a quoted part is unescaped and kept whole, since
/// `["http.status"]` named one flat key and `."http.status"` is how that key is written now.
/// An unquoted part is split on dots. Empty pieces are dropped.
fn bracket_pieces(inner: &str) -> Vec<String> {
    let inner = inner.trim();
    match inner.chars().next() {
        Some(quote @ ('"' | '\'')) => {
            let body = &inner[1..];
            let key = unescape(body.strip_suffix(quote).unwrap_or(body));
            if key.is_empty() {
                Vec::new()
            } else {
                vec![key]
            }
        }
        _ => inner
            .split('.')
            .filter(|piece| !piece.is_empty())
            .map(str::to_owned)
            .collect(),
    }
}

/// The pieces of dotted text outside brackets, each bare or `"`-quoted. Empty pieces are
/// dropped.
fn dotted_pieces(part: &str) -> Vec<String> {
    let mut pieces = Vec::new();
    let mut rest = part;
    while !rest.is_empty() {
        if rest.starts_with('"') {
            match quoted_end(rest, 0) {
                Some(end) => {
                    pieces.push(unescape(&rest[1..end - 1]));
                    rest = rest[end..].trim_start_matches('.');
                }
                None => {
                    pieces.push(unescape(&rest[1..]));
                    rest = "";
                }
            }
        } else {
            let end = rest.find('.').unwrap_or(rest.len());
            if end > 0 {
                pieces.push(rest[..end].to_owned());
            }
            rest = rest[end..].trim_start_matches('.');
        }
    }
    pieces
}

/// Drop the backslash from every `\x`, for hints built from old bracket text.
fn unescape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                if let Some(next) = chars.next() {
                    out.push(next);
                }
            }
            c => out.push(c),
        }
    }
    out
}

/// From the opening quote at `start` (`"` or `'`), the offset just past the matching closing
/// quote, skipping backslash-escaped characters. `None` when the quote is never closed. The
/// condition lexer uses this too, so both sides agree on where a quoted segment ends.
#[must_use]
pub fn quoted_end(text: &str, start: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let quote = *bytes.get(start)?;
    let mut i = start + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b if b == quote => return Some(i + 1),
            _ => i += 1,
        }
    }
    None
}

/// The segments of `path`, in order, with quotes and escapes resolved. Every segment has the
/// same rules, the first one included: bare within the charset, quoted otherwise.
fn split_segments(path: &str) -> Result<Vec<String>, PathError> {
    let empty = || PathError::EmptySegment {
        path: path.to_owned(),
    };
    let mut segments: Vec<String> = Vec::new();
    let mut i = 0;
    loop {
        let start = i;
        let rest = &path[i..];
        let segment = if rest.starts_with('"') {
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
            if let Some(ch) = segment.chars().find(|&c| !is_segment_char(c)) {
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
    let end = quoted_end(path, start).ok_or_else(|| PathError::UnterminatedQuote {
        path: path.to_owned(),
    })?;
    let inner = &path[start + 1..end - 1];
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some((_, c @ ('"' | '\\'))) => out.push(c),
                Some((_, other)) => {
                    let at = start + 1 + i;
                    return Err(PathError::InvalidSegment {
                        segment: path[start..end].to_owned(),
                        ch: other,
                        instead: format!("{}\\\\{}", &path[..at], &path[at + 1..]),
                    });
                }
                None => break,
            },
            c => out.push(c),
        }
    }
    Ok((out, end))
}

/// `before.` + the bad segment quoted + the rest of the path, for the segment error's hint.
fn quoted_hint(before: &[String], bad: &str, rest: &str) -> String {
    let mut out: Vec<String> = before.iter().map(|s| display_segment(s)).collect();
    out.push(display_segment(bad));
    format!("{}{rest}", out.join("."))
}

/// Walk `value` by `segments`, or `None` when the walk leaves the record.
fn read_at<'a>(value: &'a Value, segments: &[String]) -> Option<&'a Value> {
    let mut current = value;
    for segment in segments {
        current = match current {
            Value::Object(map) => map.get(segment)?,
            Value::Array(items) => items.get(position(segment)?)?,
            _ => return None,
        };
    }
    Some(current)
}

/// The slot `segment` names under `current`, making it exist: an existing list position is
/// itself, and anything else becomes a key of an object, replacing whatever was in the way.
fn slot<'a>(current: &'a mut Value, segment: &str) -> &'a mut Value {
    let at = match &*current {
        Value::Array(items) => position(segment).filter(|i| *i < items.len()),
        _ => None,
    };
    match (current, at) {
        (Value::Array(items), Some(i)) => &mut items[i],
        (current, _) => {
            if !current.is_object() {
                *current = Value::Object(Map::new());
            }
            current
                .as_object_mut()
                .expect("just replaced by an object")
                .entry(segment.to_owned())
                .or_insert(Value::Null)
        }
    }
}

/// Store `value` at `segments`, creating what the path needs. Empty segments replace the
/// whole record.
fn write_at(record: &mut Value, segments: &[String], value: Value) {
    let Some((last, prefix)) = segments.split_last() else {
        *record = value;
        return;
    };
    let mut current = record;
    for segment in prefix {
        current = slot(current, segment);
    }
    *slot(current, last) = value;
}

/// Remove what `segments` names, returning it, or `None` when it was not there. Removing a
/// list position closes the gap; removing the whole record leaves `null`.
fn remove_at(record: &mut Value, segments: &[String]) -> Option<Value> {
    let Some((last, prefix)) = segments.split_last() else {
        return Some(std::mem::replace(record, Value::Null));
    };
    let mut current = record;
    for segment in prefix {
        current = match current {
            Value::Object(map) => map.get_mut(segment)?,
            Value::Array(items) => {
                let at = position(segment)?;
                items.get_mut(at)?
            }
            _ => return None,
        };
    }
    match current {
        Value::Object(map) => map.remove(last),
        Value::Array(items) => {
            let at = position(last).filter(|i| *i < items.len())?;
            Some(items.remove(at))
        }
        _ => None,
    }
}

impl FieldPath {
    /// Parse a path.
    ///
    /// # Errors
    ///
    /// A [`PathError`] when the text is not `segment ("." segment)*`, a segment holds a
    /// character that needs quoting, a quote is unclosed, brackets are used, or `meta` is
    /// not followed by exactly one meta field.
    pub fn parse(path: &str) -> Result<Self, PathError> {
        // A leading dot names the record and nothing else, so a root that is not a bare word
        // (`."log.format"`, `.0`) can be written, and `meta` stops being reserved.
        let (text, rooted) = match path.strip_prefix('.') {
            Some(rest) => (rest, true),
            None => (path, false),
        };
        if rooted && text.is_empty() {
            return Ok(Self::whole());
        }
        // The error names the path the author wrote, not the text left after the dot.
        let segments = split_segments(text).map_err(|e| match e {
            PathError::EmptySegment { .. } => PathError::EmptySegment {
                path: path.to_owned(),
            },
            PathError::UnterminatedQuote { .. } => PathError::UnterminatedQuote {
                path: path.to_owned(),
            },
            other => other,
        })?;
        if !rooted && segments.first().is_some_and(|first| first == META_ROOT) {
            let unknown = || PathError::UnknownMetaField {
                path: path.to_owned(),
            };
            let [field] = &segments[1..] else {
                return Err(unknown());
            };
            return MetaField::parse(field)
                .map(|field| Self {
                    target: Target::Meta(field),
                })
                .ok_or_else(unknown);
        }
        Ok(Self {
            target: Target::Record(segments),
        })
    }

    /// The path of the whole record, `.`.
    #[must_use]
    pub const fn whole() -> Self {
        Self {
            target: Target::Record(Vec::new()),
        }
    }

    /// Read what the path names: from `record`, or from `meta` for a meta path. A path that
    /// matches nothing reads as [`FieldValue::Null`].
    #[must_use]
    pub fn read<'a>(&self, record: &'a Record, meta: &'a Meta) -> FieldValue<'a> {
        match &self.target {
            Target::Meta(field) => match meta.get(*field) {
                MetaValue::Str(text) => FieldValue::Str(text),
                MetaValue::U64(n) => FieldValue::Num(Num::Int(i128::from(n))),
            },
            Target::Record(segments) => {
                read_at(record.value(), segments).map_or(FieldValue::Null, FieldValue::from_json)
            }
        }
    }

    /// The same path, proved to name the record, so writes and removals through it cannot
    /// fail. Asked once at config load, where the node can be named.
    ///
    /// # Errors
    ///
    /// [`PathError::ReadOnly`] for a meta path.
    pub fn writable(&self) -> Result<WritePath, PathError> {
        match &self.target {
            Target::Meta(_) => Err(PathError::ReadOnly {
                path: self.to_string(),
            }),
            Target::Record(segments) => Ok(WritePath {
                segments: segments.clone(),
            }),
        }
    }
}

impl WritePath {
    /// The path this writes through, for a message or a metric label.
    #[must_use]
    pub fn path(&self) -> FieldPath {
        FieldPath {
            target: Target::Record(self.segments.clone()),
        }
    }

    /// The path of `segment` under this one.
    #[must_use]
    pub fn child(&self, segment: &str) -> Self {
        let mut segments = self.segments.clone();
        segments.push(segment.to_owned());
        Self { segments }
    }

    /// Read what the path names. A [`WritePath`] is never the pipeline's, so this needs no
    /// `Meta`; a path that matches nothing reads as [`FieldValue::Null`].
    #[must_use]
    pub fn read<'a>(&self, record: &'a Record) -> FieldValue<'a> {
        read_at(record.value(), &self.segments).map_or(FieldValue::Null, FieldValue::from_json)
    }

    /// Write `value`, creating what the path needs: a missing key is added, and a scalar or
    /// a list with no such position in the way is replaced by an object. A write through the
    /// whole-record path, `.`, replaces the record.
    pub fn write(&self, record: &mut Record, value: Value) {
        write_at(record.value_mut(), &self.segments, value);
    }

    /// Remove what the path names, returning it, or `None` when it was not there. Removing a
    /// list position closes the gap; removing `.` leaves a `null` record.
    pub fn remove(&self, record: &mut Record) -> Option<Value> {
        remove_at(record.value_mut(), &self.segments)
    }
}

impl fmt::Display for FieldPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.target {
            Target::Meta(field) => write!(f, "{META_ROOT}.{}", field.as_str()),
            Target::Record(segments) => {
                let Some((first, rest)) = segments.split_first() else {
                    return f.write_str(".");
                };
                // A record path whose first segment is `meta` needs the leading dot back, or
                // it would parse as the pipeline's `meta`.
                if first == META_ROOT {
                    f.write_str(".")?;
                }
                f.write_str(&display_segment(first))?;
                for segment in rest {
                    write!(f, ".{}", display_segment(segment))?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for WritePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.path(), f)
    }
}
