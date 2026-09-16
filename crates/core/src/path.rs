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
//!
//! A fourth root, `meta`, names the pipeline's view of the record rather than the record:
//! `meta.id`, `meta.tenant`, `meta.ingestion_time`, `meta.delivery_count` (ADR 0005). A meta
//! path reads the record's [`Meta`] and refuses every write and removal.

use std::cmp::Ordering;
use std::fmt;
use std::sync::LazyLock;

use serde_json::{Map, Value};

use crate::closed_set::closed_set;
use crate::meta::{Meta, MetaField, MetaValue};
use crate::record::{Kind, Record, RecordId};

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
        /// The same path in dotted form.
        instead: String,
    },
    /// The first segment is not a record field.
    #[error("`{name}` is not a record field; instead use one of {}", FIELDS.as_str())]
    UnknownField {
        /// The segment text.
        name: String,
    },
    /// `meta` was named without a field, or with one that is not a meta field.
    #[error("`{path}` is not a meta field; instead use one of {}", META_FIELDS.as_str())]
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

/// The roots a path may start at, as the unknown-field error lists them: every [`Field`],
/// then every [`MapField`] with `.<key>`, then `meta.<field>`.
static FIELDS: LazyLock<String> = LazyLock::new(|| {
    let fields = Field::ALL
        .into_iter()
        .map(|field| field.as_str().to_owned());
    let maps = MapField::ALL
        .into_iter()
        .map(|map| format!("{}.<key>", map.as_str()));
    let meta = std::iter::once(format!("{META_ROOT}.<field>"));
    fields
        .chain(maps)
        .chain(meta)
        .collect::<Vec<_>>()
        .join(", ")
});

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

closed_set! {
    /// A top-level field that is addressed as a whole, by its path spelling, in the order the
    /// unknown-field error lists them.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Field {
        Id = "id",
        Kind = "kind",
        Body = "body",
        SeverityText = "severity_text",
        SeverityNumber = "severity_number",
        TimeUnixNano = "time_unix_nano",
        ObservedTimeUnixNano = "observed_time_unix_nano",
        TraceId = "trace_id",
        SpanId = "span_id",
    }
}

closed_set! {
    /// One of the three flat maps, by its path spelling.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum MapField {
        Attributes = "attributes",
        Resource = "resource",
        Scope = "scope",
    }
}

/// What a path may start at: a record field or a map, each a closed set, or `meta`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Root {
    Field(Field),
    Map(MapField),
    Meta,
}

impl Root {
    /// The root spelled `name`. Every variant of `Root` needs its arm here; `name` below is
    /// exhaustive, this is not.
    fn parse(name: &str) -> Option<Self> {
        Field::parse(name)
            .map(Self::Field)
            .or_else(|| MapField::parse(name).map(Self::Map))
            .or_else(|| (name == META_ROOT).then_some(Self::Meta))
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Field(field) => field.as_str(),
            Self::Map(map) => map.as_str(),
            Self::Meta => META_ROOT,
        }
    }
}

impl MapField {
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

/// What a path names once parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Target {
    Field(Field),
    /// A key of one of the maps.
    Key(MapField, String),
    /// A value of the record's `Meta`.
    Meta(MetaField),
}

/// A value the write rules accepted, in the type its field stores. Storing it cannot fail.
enum Typed<'p> {
    Key(MapField, &'p str, Value),
    Id(Option<RecordId>),
    Kind(Kind),
    TimeUnixNano(Option<u64>),
    ObservedTimeUnixNano(Option<u64>),
    SeverityText(Option<String>),
    SeverityNumber(Option<i32>),
    Body(Value),
    TraceId(Option<String>),
    SpanId(Option<String>),
}

impl Typed<'_> {
    fn store(self, record: &mut Record) {
        match self {
            Self::Key(map, key, value) => {
                map.get_mut(record).insert(key.to_owned(), value);
            }
            Self::Id(id) => record.id = id,
            Self::Kind(kind) => record.kind = kind,
            Self::TimeUnixNano(n) => record.time_unix_nano = n,
            Self::ObservedTimeUnixNano(n) => record.observed_time_unix_nano = n,
            Self::SeverityText(s) => record.severity_text = s,
            Self::SeverityNumber(n) => record.severity_number = n,
            Self::Body(value) => record.body = Some(value),
            Self::TraceId(s) => record.trace_id = s,
            Self::SpanId(s) => record.span_id = s,
        }
    }
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

/// A field as read from a record: a borrowed view, never a copy. An absent field is
/// [`FieldValue::Null`].
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum FieldValue<'a> {
    /// Absent, or JSON `null`.
    Null,
    /// A JSON bool.
    Bool(bool),
    /// A JSON number.
    Num(Num),
    /// A string field or a JSON string.
    Str(&'a str),
    /// An array or object: `body` when structured. Comparable to nothing.
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
/// error's hint. The scan honours quotes, so `["a[0]"]` and `['a]b']` name the key the user
/// wrote; an unclosed quote runs to the end of the path. The hint is a valid path whenever
/// the input named a key; an empty bracket on a map hints `<map>.<key>`.
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
    if out.len() == 1 {
        match Root::parse(&out[0]) {
            Some(Root::Map(_)) => out.push("<key>".to_owned()),
            Some(Root::Meta) => out.push("<field>".to_owned()),
            Some(Root::Field(_)) | None => {}
        }
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

/// The pieces one bracket's contents name: a quoted part is unescaped, then split on dots
/// since `["http.status"]` and `.http.status` name the same flat key. Empty pieces are
/// dropped.
fn bracket_pieces(inner: &str) -> Vec<String> {
    let inner = inner.trim();
    let unquoted = match inner.chars().next() {
        Some(quote @ ('"' | '\'')) => {
            let body = &inner[1..];
            unescape(body.strip_suffix(quote).unwrap_or(body))
        }
        _ => inner.to_owned(),
    };
    unquoted
        .split('.')
        .filter(|piece| !piece.is_empty())
        .map(str::to_owned)
        .collect()
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

impl FieldPath {
    /// Parse a dotted path.
    ///
    /// # Errors
    ///
    /// A [`PathError`] when the text is not `root ("." segment)*`, the root is not a record
    /// field or `meta`, a scalar or `body` is given segments, a map is named without a key,
    /// or `meta` is not followed by exactly one meta field.
    pub fn parse(path: &str) -> Result<Self, PathError> {
        let segments = split_segments(path)?;
        let (name, keys) = segments
            .split_first()
            .ok_or_else(|| PathError::EmptySegment {
                path: path.to_owned(),
            })?;
        let root =
            Root::parse(name).ok_or_else(|| PathError::UnknownField { name: name.clone() })?;
        match (root, keys.is_empty()) {
            (Root::Map(_), true) => Err(PathError::MapNeedsKey {
                field: name.clone(),
            }),
            (Root::Map(map), false) => Ok(Self {
                target: Target::Key(map, keys.join(".")),
            }),
            (Root::Field(field), true) => Ok(Self {
                target: Target::Field(field),
            }),
            (Root::Meta, _) => match keys {
                [field] => MetaField::parse(field)
                    .map(|field| Self {
                        target: Target::Meta(field),
                    })
                    .ok_or_else(|| PathError::UnknownMetaField {
                        path: path.to_owned(),
                    }),
                _ => Err(PathError::UnknownMetaField {
                    path: path.to_owned(),
                }),
            },
            (Root::Field(field), false) => Err(PathError::NotAMap {
                field: name.clone(),
                hint: if field == Field::Body {
                    ", or parse it into attributes first"
                } else {
                    ""
                },
            }),
        }
    }

    /// The path of the top-level record field spelled `name`, as `parse(name)` gives it;
    /// `None` for a map, for `meta`, and for anything that is not a record field. For a
    /// caller that holds a record as named values rather than as path text.
    #[must_use]
    pub fn top_level(name: &str) -> Option<Self> {
        Field::parse(name).map(|field| Self {
            target: Target::Field(field),
        })
    }

    /// The path of `key` under the map spelled `map` (`attributes`, `resource` or `scope`),
    /// whatever characters `key` holds; `None` when `map` is not one of the three.
    #[must_use]
    pub fn under(map: &str, key: &str) -> Option<Self> {
        MapField::parse(map).map(|map| Self {
            target: Target::Key(map, key.to_owned()),
        })
    }

    /// Whether `name` is one of the three maps.
    #[must_use]
    pub fn is_map(name: &str) -> bool {
        MapField::parse(name).is_some()
    }

    /// The flat map key when the path names a key of `attributes`, `resource` or `scope`.
    #[must_use]
    pub fn map_key(&self) -> Option<&str> {
        match &self.target {
            Target::Key(_, key) => Some(key),
            Target::Field(_) | Target::Meta(_) => None,
        }
    }

    /// Read the field as a borrowed view: from `record`, or from `meta` for a meta path. An
    /// absent field is [`FieldValue::Null`].
    #[must_use]
    pub fn read<'a>(&self, record: &'a Record, meta: &'a Meta) -> FieldValue<'a> {
        fn num(n: impl Into<i128>) -> FieldValue<'static> {
            FieldValue::Num(Num::Int(n.into()))
        }
        fn opt_str(s: Option<&str>) -> FieldValue<'_> {
            s.map_or(FieldValue::Null, FieldValue::Str)
        }
        let field = match &self.target {
            Target::Key(map, key) => {
                return map
                    .get(record)
                    .get(key)
                    .map_or(FieldValue::Null, FieldValue::from_json);
            }
            Target::Meta(field) => {
                return match meta.get(*field) {
                    MetaValue::Str(text) => FieldValue::Str(text),
                    MetaValue::U64(n) => num(n),
                };
            }
            Target::Field(field) => *field,
        };
        match field {
            Field::Id => record.id.map_or(FieldValue::Null, |id| num(id.0)),
            Field::Kind => FieldValue::Str(record.kind.as_str()),
            Field::TimeUnixNano => record.time_unix_nano.map_or(FieldValue::Null, num),
            Field::ObservedTimeUnixNano => {
                record.observed_time_unix_nano.map_or(FieldValue::Null, num)
            }
            Field::SeverityText => opt_str(record.severity_text.as_deref()),
            Field::SeverityNumber => record.severity_number.map_or(FieldValue::Null, num),
            Field::Body => record
                .body
                .as_ref()
                .map_or(FieldValue::Null, FieldValue::from_json),
            Field::TraceId => opt_str(record.trace_id.as_deref()),
            Field::SpanId => opt_str(record.span_id.as_deref()),
        }
    }

    /// Whether a write through this path can happen at all: every record field is payload
    /// and writable within its type (ADR 0005); a meta path is the pipeline's.
    ///
    /// # Errors
    ///
    /// [`PathError::ReadOnly`] for a meta path.
    pub fn writable(&self) -> Result<(), PathError> {
        match self.target {
            Target::Meta(_) => Err(self.read_only()),
            Target::Field(_) | Target::Key(..) => Ok(()),
        }
    }

    /// Core's write rules for `value` through this path, without a record: `Ok` exactly when
    /// [`FieldPath::write`] would store it, and otherwise the error `write` would give. A
    /// stage that needs to know a type at load asks this instead of writing onto an empty
    /// record. It runs the same typing step as `write` on a copy of `value`: the copy is paid
    /// once per op at load, and one typing step keeps a field's type spelled in one place.
    ///
    /// # Errors
    ///
    /// As [`FieldPath::write`].
    pub fn accepts(&self, value: &Value) -> Result<(), PathError> {
        self.typed(value.clone()).map(drop)
    }

    /// Write `value` to the field, creating or replacing it.
    ///
    /// `null` clears an optional top-level field and is stored as-is under a map key. A map
    /// key takes any JSON value: the flat-map rule is the source's contract, unenforced at
    /// ingest, and a record must be able to leave the way it came. Every record field is
    /// payload (ADR 0005), `id` and `kind` included; they only have types.
    ///
    /// # Errors
    ///
    /// [`PathError::WrongType`] when a typed field is offered the wrong JSON type (`id` and
    /// the time fields take an integer in `u64`, `kind` one of `log`, `metric` or `span`,
    /// `severity_number` an integer in `i32`, `severity_text`, `trace_id` and `span_id` a
    /// string); [`PathError::ReadOnly`] for a meta path. The record is unchanged on error.
    pub fn write(&self, record: &mut Record, value: Value) -> Result<(), PathError> {
        self.typed(value)?.store(record);
        Ok(())
    }

    /// `value` as the field stores it, or why the field refuses it. The one place a field's
    /// type is spelled.
    fn typed(&self, value: Value) -> Result<Typed<'_>, PathError> {
        let field = match &self.target {
            Target::Key(map, key) => return Ok(Typed::Key(*map, key, value)),
            Target::Meta(_) => return Err(self.read_only()),
            Target::Field(field) => *field,
        };
        Ok(match field {
            Field::Id => Typed::Id(self.expect_u64(value)?.map(RecordId)),
            Field::Kind => Typed::Kind(self.expect_kind(&value)?),
            Field::TimeUnixNano => Typed::TimeUnixNano(self.expect_u64(value)?),
            Field::ObservedTimeUnixNano => Typed::ObservedTimeUnixNano(self.expect_u64(value)?),
            Field::SeverityText => Typed::SeverityText(self.expect_string(value)?),
            Field::SeverityNumber => Typed::SeverityNumber(self.expect_i32(value)?),
            Field::Body => Typed::Body(value),
            Field::TraceId => Typed::TraceId(self.expect_string(value)?),
            Field::SpanId => Typed::SpanId(self.expect_string(value)?),
        })
    }

    /// Remove the field, returning the old value, or `None` when it was absent. Every record
    /// field can be removed. `kind` is never absent: removing it leaves the wire default,
    /// `log`.
    ///
    /// # Errors
    ///
    /// [`PathError::ReadOnly`] for a meta path. The record is unchanged on error.
    pub fn remove(&self, record: &mut Record) -> Result<Option<Value>, PathError> {
        let field = match &self.target {
            Target::Key(map, key) => return Ok(map.get_mut(record).remove(key)),
            Target::Meta(_) => return Err(self.read_only()),
            Target::Field(field) => *field,
        };
        Ok(match field {
            Field::Id => record.id.take().map(|id| Value::from(id.0)),
            Field::Kind => Some(Value::from(std::mem::take(&mut record.kind).as_str())),
            Field::TimeUnixNano => record.time_unix_nano.take().map(Value::from),
            Field::ObservedTimeUnixNano => record.observed_time_unix_nano.take().map(Value::from),
            Field::SeverityText => record.severity_text.take().map(Value::from),
            Field::SeverityNumber => record.severity_number.take().map(Value::from),
            Field::Body => record.body.take(),
            Field::TraceId => record.trace_id.take().map(Value::from),
            Field::SpanId => record.span_id.take().map(Value::from),
        })
    }

    fn read_only(&self) -> PathError {
        PathError::ReadOnly {
            path: self.to_string(),
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

    fn expect_kind(&self, value: &Value) -> Result<Kind, PathError> {
        value
            .as_str()
            .and_then(Kind::parse)
            .ok_or_else(|| self.wrong_type(Kind::ONE_OF, value))
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
            Target::Field(field) => f.write_str(Root::Field(*field).name()),
            Target::Meta(field) => write!(f, "{}.{}", Root::Meta.name(), field.as_str()),
            Target::Key(map, key) => {
                f.write_str(Root::Map(*map).name())?;
                if key.split('.').any(str::is_empty) {
                    return write!(f, ".{}", display_segment(key));
                }
                for part in key.split('.') {
                    write!(f, ".{}", display_segment(part))?;
                }
                Ok(())
            }
        }
    }
}
