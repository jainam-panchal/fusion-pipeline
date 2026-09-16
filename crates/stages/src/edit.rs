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
//! What load can check, it refuses: every path parses, no op writes or removes `id`, `kind`
//! or `resource.tenant.id` (the engine fixes the tenant once per record for metrics and
//! state keys; `copy` may read them), a `set` literal is a scalar the field takes, a `hash`
//! target takes a string, `from` and `to` differ. Every refusal names the node and the op's
//! position.

use std::collections::BTreeMap;

use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::metrics::{EditCause, EditOp};
use fusion_core::path::{FieldPath, FieldValue, Num};
use fusion_core::record::Record;
use fusion_core::stage::{Context, DropReason, Stage, StageOutput};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::key_hash::write_canonical;

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

/// One op as parsed. `source` is the path the op reads, in canonical form: the `field`
/// label of `edit_unapplied_total`, fixed at load so no record pays for the display. For
/// `set` that is the field it writes, the only path it has. `delete` carries none: it is
/// never unapplied.
#[derive(Debug)]
enum Op {
    Set {
        field: FieldPath,
        value: Value,
        source: String,
    },
    Rename {
        from: FieldPath,
        to: FieldPath,
        source: String,
    },
    Copy {
        from: FieldPath,
        to: FieldPath,
        source: String,
    },
    Hash {
        field: FieldPath,
        source: String,
    },
    Delete {
        fields: Vec<FieldPath>,
    },
}

/// The op kinds as an error message lists them.
const OP_NAMES: &str = "set, rename, copy, hash or delete";

/// The tenant's path, which no op may write or remove: the engine reads the tenant once
/// per record for every label and state key, so an edit changing it would leave them
/// disagreeing.
const TENANT: &str = "resource.tenant.id";

/// An op that could not apply to a record: the `field` and `cause` labels of
/// `edit_unapplied_total`, built where the op failed so the label is always the op's path.
struct Unapplied<'a> {
    field: &'a str,
    cause: EditCause,
}

const fn unapplied(field: &str, cause: EditCause) -> Unapplied<'_> {
    Unapplied { field, cause }
}

/// Where in the config an error is: the node, the op's position, and the op's kind once
/// that is known. `tenant` is [`TENANT`] parsed once, at load.
struct At<'a> {
    node: &'a NodeConfig,
    index: usize,
    kind: Option<EditOp>,
    tenant: &'a FieldPath,
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

    /// `text` parsed as a path the op may write or remove: it parses, is not `id` or
    /// `kind`, and is not the tenant.
    fn editable(&self, key: &str, text: &str) -> Result<FieldPath, ConfigError> {
        let path = self.readable(key, text)?;
        if !path.is_writable() {
            return Err(self.error(format!("`{key}`: `{path}` is read-only")));
        }
        if path == *self.tenant {
            return Err(self.error(format!(
                "`{key}`: `{path}` is the tenant and cannot be edited"
            )));
        }
        Ok(path)
    }

    /// `text` parsed as a path the op only reads.
    fn readable(&self, key: &str, text: &str) -> Result<FieldPath, ConfigError> {
        FieldPath::parse(text).map_err(|e| self.error(format!("`{key}`: {e}")))
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
    /// or unknown, a path does not parse, an op names `id`, `kind` or `resource.tenant.id`,
    /// a `set` literal is not a scalar or not of the field's type, a `hash` target does not
    /// take a string, `from` equals `to`, or `on_unapplied` is not `skip` or `drop`.
    pub fn from_node(node: &NodeConfig) -> Result<Self, ConfigError> {
        let params: Params = node.parse_params()?;
        if params.ops.is_empty() {
            return Err(node.invalid_params("`ops` needs at least one op"));
        }
        // Fails closed: a tenant path that did not parse would be a bug in core, and is
        // reported as a config error rather than silently dropping the guard.
        let tenant = FieldPath::parse(TENANT)
            .map_err(|e| node.invalid_params(format!("tenant path `{TENANT}`: {e}")))?;
        let ops = params
            .ops
            .into_iter()
            .enumerate()
            .map(|(index, entry)| parse_op(node, index, entry, &tenant))
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
    tenant: &FieldPath,
) -> Result<Op, ConfigError> {
    let mut at = At {
        node,
        index,
        kind: None,
        tenant,
    };
    let Some((name, body)) = entry.pop_first().filter(|_| entry.is_empty()) else {
        return Err(at.error(format!("one op per entry, one of {OP_NAMES}")));
    };
    let kind = match name.as_str() {
        "set" => EditOp::Set,
        "rename" => EditOp::Rename,
        "copy" => EditOp::Copy,
        "hash" => EditOp::Hash,
        "delete" => EditOp::Delete,
        other => return Err(at.error(format!("unknown op `{other}`; use {OP_NAMES}"))),
    };
    at.kind = Some(kind);
    Ok(match kind {
        EditOp::Set => {
            let p: SetParams = at.params(body)?;
            if matches!(p.value, Value::Array(_) | Value::Object(_)) {
                return Err(at.error("`value` must be a string, number, bool or null"));
            }
            let field = at.editable("field", &p.field)?;
            // Core's own write rules, on an empty record: the field's type, refused here
            // rather than on every record.
            field
                .write(&mut Record::default(), p.value.clone())
                .map_err(|e| at.error(format!("value {}: {e}", p.value)))?;
            Op::Set {
                source: field.to_string(),
                field,
                value: p.value,
            }
        }
        EditOp::Rename | EditOp::Copy => {
            let p: MoveParams = at.params(body)?;
            let from = if kind == EditOp::Rename {
                at.editable("from", &p.from)?
            } else {
                at.readable("from", &p.from)?
            };
            let to = at.editable("to", &p.to)?;
            if from == to {
                return Err(at.error("`from` and `to` are the same field"));
            }
            let source = from.to_string();
            if kind == EditOp::Rename {
                Op::Rename { from, to, source }
            } else {
                Op::Copy { from, to, source }
            }
        }
        EditOp::Hash => {
            let p: HashParams = at.params(body)?;
            let field = at.editable("field", &p.field)?;
            field
                .write(&mut Record::default(), Value::String(String::new()))
                .map_err(|e| at.error(format!("{e}; hash writes a string")))?;
            Op::Hash {
                source: field.to_string(),
                field,
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
                .map(|f| at.editable("fields", f))
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

    /// Apply to `record`, or say why it could not; the record is unchanged on `Err`.
    fn apply(&self, record: &mut Record) -> Result<(), Unapplied<'_>> {
        match self {
            // The literal was written to an empty record at load and `write` never reads
            // the record, so this cannot refuse; the arm is here so the type says so.
            Self::Set {
                field,
                value,
                source,
            } => field
                .write(record, value.clone())
                .map_err(|_| unapplied(source, EditCause::Type)),
            Self::Rename { from, to, source } => {
                let value =
                    owned(from.read(record)).ok_or_else(|| unapplied(source, EditCause::Absent))?;
                to.write(record, value)
                    .map_err(|_| unapplied(source, EditCause::Type))?;
                // `from` held a value a moment ago and is not `to`, so this cannot refuse.
                from.remove(record)
                    .map(|_| ())
                    .map_err(|_| unapplied(source, EditCause::Type))
            }
            Self::Copy { from, to, source } => {
                let value =
                    owned(from.read(record)).ok_or_else(|| unapplied(source, EditCause::Absent))?;
                to.write(record, value)
                    .map_err(|_| unapplied(source, EditCause::Type))
            }
            Self::Hash { field, source } => {
                let digest = match field.read(record) {
                    FieldValue::Null => return Err(unapplied(source, EditCause::Absent)),
                    FieldValue::Str(s) => sha256_hex(s.as_bytes()),
                    value @ (FieldValue::Bool(_) | FieldValue::Num(_)) => {
                        let mut text = String::new();
                        write_canonical(&mut text, value);
                        sha256_hex(text.as_bytes())
                    }
                    FieldValue::Json(_) => return Err(unapplied(source, EditCause::Type)),
                    // `FieldValue` is `#[non_exhaustive]`: a variant core adds later is a
                    // value this op does not know how to hash.
                    _ => return Err(unapplied(source, EditCause::Type)),
                };
                field
                    .write(record, Value::String(digest))
                    .map_err(|_| unapplied(source, EditCause::Type))
            }
            Self::Delete { fields } => {
                // Every field was checked writable at load, so `remove` cannot refuse; an
                // absent field is nothing to do, not an unapplied op.
                for field in fields {
                    let _ = field.remove(record);
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
            if let Err(Unapplied { field, cause }) = op.apply(&mut record) {
                ctx.metrics.edit_unapplied(op.kind(), field, cause);
                if self.on_unapplied == OnUnapplied::Drop {
                    return StageOutput::Drop(DropReason::EditUnapplied);
                }
            }
        }
        StageOutput::Pass(record)
    }
}
