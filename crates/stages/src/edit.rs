//! `edit`: plain field edits, an ordered list of ops run on one record.
//!
//! ```yaml
//! - id: normalise
//!   type: edit
//!   on_unapplied: skip      # skip (default) | drop: what to do when an op cannot apply
//!   ops:
//!     - set:    { field: resource.env, value: prod }                        # write a literal
//!     - rename: { from: attributes.http.path, to: attributes.http.route }   # move a value
//!     - copy:   { from: body, to: attributes.raw }                          # duplicate a value
//!     - hash:   { field: attributes.user.email }                            # SHA-256, lowercase hex
//!     - delete: { fields: [attributes.debug] }                              # remove fields
//! ```
//!
//! Ops run top to bottom on the same record. `rename` and `copy` overwrite `to`. An op is
//! unapplied when its source reads as null (absent, or JSON `null`) or the target refuses
//! the value (`copy body -> severity_number` with a string body; `hash` on an array): the
//! record is left as it was by that op, the op counts on `edit_unapplied_total` under its
//! kind, source path and cause, and `on_unapplied` says whether the record goes on to the
//! next op or drops with reason `edit_unapplied`. Only `rename`, `copy` and `hash` can be
//! unapplied: a `set` literal is checked at load, and `delete` of an absent field is
//! nothing to do. Nothing here naks: the outcome is fixed by the record's shape, so a
//! redelivery would only repeat it.
//!
//! `hash` takes a string as its bytes, a number or bool as its canonical JSON text (the
//! text `sample consistent` hashes). It is a stable join key, not anonymisation: an
//! unsalted digest of a low-entropy field is dictionary-reversible.
//!
//! Every record field is payload (ADR 0005), `id`, `kind` and `resource.tenant.id` included:
//! the pipeline decides from the record's `Meta`, so an op may write or remove any of them.
//! A `meta.*` path is the pipeline's: an op may read it (`copy` from it is how a pipeline
//! value enters a record) and never write or remove it. What load can check, it refuses:
//! every path parses, a `set` literal is a scalar the field takes, a `hash` target takes a
//! string, `from` and `to` differ, no op writes or removes a `meta.*` path. Every refusal
//! names the node and the op's position.

use std::collections::BTreeMap;

use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::meta::Meta;
use fusion_core::metrics::{EditCause, EditOp};
use fusion_core::path::{FieldPath, FieldValue, Num, WritePath};
use fusion_core::record::Record;
use fusion_core::stage::{Context, DropReason, Stage, StageOutput};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::key_hash::write_canonical;
use label::{Labelled, Unapplied};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    ops: Vec<BTreeMap<String, Value>>,
    #[serde(default)]
    on_unapplied: OnUnapplied,
}

/// What the node does with a record on which an op could not apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnUnapplied {
    /// The record goes on to the next op, unchanged by this one.
    #[default]
    Skip,
    /// The record drops with reason `edit_unapplied`.
    Drop,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetParams {
    field: String,
    value: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MoveParams {
    from: String,
    to: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HashParams {
    field: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteParams {
    fields: Vec<String>,
}

/// The `edit` stage.
#[derive(Debug)]
pub struct Edit {
    ops: Vec<Op>,
    on_unapplied: OnUnapplied,
}

/// One op as parsed. The path an op can be unapplied on is a [`Labelled`]; `delete` has
/// none, since it is never unapplied.
#[derive(Debug)]
enum Op {
    Set {
        field: WritePath,
        value: Value,
    },
    Rename {
        from: Labelled<WritePath>,
        to: WritePath,
    },
    Copy {
        from: Labelled<FieldPath>,
        to: WritePath,
    },
    Hash {
        field: Labelled<WritePath>,
    },
    Delete {
        fields: Vec<WritePath>,
    },
}

/// The `field` label and what can build it. A module of its own so the fields are private
/// to it: outside, an [`Unapplied`] comes only from [`Labelled::unapplied`], and so its label
/// is always a source path rendered at load, never a literal.
mod label {
    use std::fmt::Display;

    use fusion_core::metrics::EditCause;

    /// A path an op reads, with its `field` label for `edit_unapplied_total` rendered once
    /// at load, so a record on the unapplied path pays no display: under
    /// `on_unapplied: skip` that path is every record of a tenant lacking the field, the
    /// steady state the metric exists to show. It is generic over the path type because an
    /// op that also writes where it read (`rename`, `hash`) holds a
    /// [`fusion_core::path::WritePath`], and `copy` may read `meta.*`.
    #[derive(Debug)]
    pub(super) struct Labelled<P> {
        pub(super) path: P,
        label: Box<str>,
    }

    impl<P: Display> Labelled<P> {
        pub(super) fn new(path: P) -> Self {
            Self {
                label: path.to_string().into_boxed_str(),
                path,
            }
        }

        /// This source's op could not apply because of `cause`.
        pub(super) fn unapplied(&self, cause: EditCause) -> Unapplied<'_> {
            Unapplied {
                field: &self.label,
                cause,
            }
        }
    }

    /// An op that could not apply to a record: the `field` and `cause` labels of
    /// `edit_unapplied_total`.
    pub(super) struct Unapplied<'a> {
        field: &'a str,
        cause: EditCause,
    }

    impl<'a> Unapplied<'a> {
        /// The `field` label: the source path the op stopped on.
        pub(super) const fn field(&self) -> &'a str {
            self.field
        }

        /// The `cause` label.
        pub(super) const fn cause(&self) -> EditCause {
            self.cause
        }
    }
}

/// Where in the config an error is: the node, the op's position, and the op's kind once
/// that is known.
struct At<'a> {
    node: &'a NodeConfig,
    index: usize,
    kind: Option<EditOp>,
}

impl At<'_> {
    fn error(&self, message: impl std::fmt::Display) -> ConfigError {
        match self.kind {
            Some(kind) => self
                .node
                .invalid_params(format!("op {} (`{kind}`): {message}", self.index)),
            None => self
                .node
                .invalid_params(format!("op {}: {message}", self.index)),
        }
    }

    /// `text` parsed as a path an op reads. Every path may be read, `meta.*` included.
    fn path(&self, key: &str, text: &str) -> Result<FieldPath, ConfigError> {
        FieldPath::parse(text).map_err(|e| self.error(format!("`{key}`: {e}")))
    }

    /// `text` parsed as a path an op writes or removes: the record's, never the pipeline's.
    /// A write through what this returns cannot fail.
    fn target(&self, key: &str, text: &str) -> Result<WritePath, ConfigError> {
        self.path(key, text)?
            .writable()
            .map_err(|e| self.error(format!("`{key}`: {e}")))
    }

    fn params<T: serde::de::DeserializeOwned>(&self, body: Value) -> Result<T, ConfigError> {
        serde_json::from_value(body).map_err(|e| self.error(e))
    }
}

impl Edit {
    /// Build from a node's `ops` and `on_unapplied`.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidParams`] naming the node and the op's position when `ops` is
    /// empty, an entry does not hold exactly one op, the op is unknown, a key is missing
    /// or unknown, a path does not parse, a `set` literal is not a scalar or not of the
    /// field's type, a `hash` target does not
    /// take a string, `from` equals `to`, or `on_unapplied` is not `skip` or `drop`.
    pub fn from_node(node: &NodeConfig) -> Result<Self, ConfigError> {
        let params: Params = node.parse_params()?;
        if params.ops.is_empty() {
            return Err(node.invalid_params("`ops` needs at least one op"));
        }
        let ops = params
            .ops
            .into_iter()
            .enumerate()
            .map(|(index, entry)| parse_op(node, index, entry))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            ops,
            on_unapplied: params.on_unapplied,
        })
    }

    /// Factory for a [`fusion_core::registry::Registry`].
    ///
    /// # Errors
    ///
    /// See [`Edit::from_node`].
    pub fn build(node: &NodeConfig) -> Result<Box<dyn Stage>, ConfigError> {
        Ok(Box::new(Self::from_node(node)?))
    }
}

fn parse_op(
    node: &NodeConfig,
    index: usize,
    mut entry: BTreeMap<String, Value>,
) -> Result<Op, ConfigError> {
    let mut at = At {
        node,
        index,
        kind: None,
    };
    let Some((name, body)) = entry.pop_first().filter(|_| entry.is_empty()) else {
        return Err(at.error(format!("one op per entry, one of {}", EditOp::ONE_OF)));
    };
    let Some(kind) = EditOp::parse(&name) else {
        return Err(at.error(format!("unknown op `{name}`; use {}", EditOp::ONE_OF)));
    };
    at.kind = Some(kind);
    Ok(match kind {
        EditOp::Set => {
            let p: SetParams = at.params(body)?;
            if matches!(p.value, Value::Array(_) | Value::Object(_)) {
                return Err(at.error("`value` must be a string, number, bool or null"));
            }
            Op::Set {
                field: at.target("field", &p.field)?,
                value: p.value,
            }
        }
        EditOp::Rename | EditOp::Copy => {
            let p: MoveParams = at.params(body)?;
            let to = at.target("to", &p.to)?;
            let same = |from: &FieldPath| {
                (*from == to.path()).then(|| at.error("`from` and `to` are the same field"))
            };
            if kind == EditOp::Rename {
                // `rename` removes its source, so the source is a target too.
                let from = at.target("from", &p.from)?;
                if let Some(error) = same(&from.path()) {
                    return Err(error);
                }
                Op::Rename {
                    from: Labelled::new(from),
                    to,
                }
            } else {
                let from = at.path("from", &p.from)?;
                if let Some(error) = same(&from) {
                    return Err(error);
                }
                Op::Copy {
                    from: Labelled::new(from),
                    to,
                }
            }
        }
        EditOp::Hash => {
            let p: HashParams = at.params(body)?;
            Op::Hash {
                field: Labelled::new(at.target("field", &p.field)?),
            }
        }
        EditOp::Delete => {
            let p: DeleteParams = at.params(body)?;
            if p.fields.is_empty() {
                return Err(at.error("`fields` needs at least one field path"));
            }
            let fields = p
                .fields
                .iter()
                .map(|f| at.target("fields", f))
                .collect::<Result<Vec<_>, _>>()?;
            Op::Delete { fields }
        }
    })
}

impl Op {
    /// The `op` label.
    const fn kind(&self) -> EditOp {
        match self {
            Self::Set { .. } => EditOp::Set,
            Self::Rename { .. } => EditOp::Rename,
            Self::Copy { .. } => EditOp::Copy,
            Self::Hash { .. } => EditOp::Hash,
            Self::Delete { .. } => EditOp::Delete,
        }
    }

    /// Apply to `record`, whose `Meta` is `meta`, or say why it could not; the record is
    /// unchanged on `Err`.
    fn apply(&self, record: &mut Record, meta: &Meta) -> Result<(), Unapplied<'_>> {
        match self {
            // A write through a `WritePath` makes its path exist (issue #79), so `set`
            // cannot be unapplied and carries no label.
            Self::Set { field, value } => {
                field.write(record, value.clone());
                Ok(())
            }
            Self::Rename { from, to } => {
                let value = owned(from.path.read(record))
                    .ok_or_else(|| from.unapplied(EditCause::Absent))?;
                to.write(record, value);
                let _removed = from.path.remove(record);
                Ok(())
            }
            Self::Copy { from, to } => {
                let value = owned(from.path.read(record, meta))
                    .ok_or_else(|| from.unapplied(EditCause::Absent))?;
                to.write(record, value);
                Ok(())
            }
            Self::Hash { field } => {
                let digest = match field.path.read(record) {
                    FieldValue::Null => return Err(field.unapplied(EditCause::Absent)),
                    FieldValue::Str(s) => sha256_hex(s.as_bytes()),
                    value @ (FieldValue::Bool(_) | FieldValue::Num(_)) => {
                        let mut text = String::new();
                        write_canonical(&mut text, value);
                        sha256_hex(text.as_bytes())
                    }
                    FieldValue::Json(_) => return Err(field.unapplied(EditCause::Type)),
                    // `FieldValue` is `#[non_exhaustive]`: a variant core adds later is a
                    // value this op does not know how to hash.
                    _ => return Err(field.unapplied(EditCause::Type)),
                };
                field.path.write(record, Value::String(digest));
                Ok(())
            }
            Self::Delete { fields } => {
                // An absent field is nothing to do, not an unapplied op.
                for field in fields {
                    let _removed = field.remove(record);
                }
                Ok(())
            }
        }
    }
}

/// A read value as an owned JSON value, `None` when it read as null or cannot be a JSON
/// value at all (an integer outside `i64` and `u64`, a non-finite float: neither can come
/// from a JSON record, so neither is written as `null` in its place).
fn owned(value: FieldValue<'_>) -> Option<Value> {
    match value {
        FieldValue::Null => None,
        FieldValue::Bool(b) => Some(Value::Bool(b)),
        FieldValue::Str(s) => Some(Value::String(s.to_owned())),
        FieldValue::Num(Num::Int(i)) => i64::try_from(i)
            .map(Value::from)
            .or_else(|_| u64::try_from(i).map(Value::from))
            .ok(),
        FieldValue::Num(Num::Float(f)) => serde_json::Number::from_f64(f).map(Value::Number),
        FieldValue::Json(v) => Some(v.clone()),
        // `FieldValue` is `#[non_exhaustive]`: a variant core adds later reads as null here
        // until this op learns it.
        _ => None,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

impl Stage for Edit {
    fn process(&self, mut record: Record, ctx: &Context<'_>) -> StageOutput {
        for op in &self.ops {
            if let Err(unapplied) = op.apply(&mut record, ctx.meta) {
                ctx.metrics
                    .edit_unapplied(op.kind(), unapplied.field(), unapplied.cause());
                if self.on_unapplied == OnUnapplied::Drop {
                    return StageOutput::Drop(DropReason::EditUnapplied);
                }
            }
        }
        StageOutput::Pass(record)
    }
}
