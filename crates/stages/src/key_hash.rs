//! Key fields: the `key` field paths a stage groups records by, and one hash of their
//! values. `dedupe` names a state key with the hash, `sample`'s `consistent` mode decides
//! with it. One parser and one hash, so the two stages agree on what "the same key" means.

use std::fmt::Write as _;

use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::path::{FieldPath, FieldValue, Num};
use fusion_core::record::Record;

/// The `key` field paths of `node`, parsed.
///
/// # Errors
///
/// [`ConfigError::InvalidParams`] naming the node when `key` is empty or a path in it does
/// not parse; the message quotes the path and the parser's hint.
pub(crate) fn parse_key_fields(
    node: &NodeConfig,
    key: &[String],
) -> Result<Vec<FieldPath>, ConfigError> {
    if key.is_empty() {
        return Err(node.invalid_params("`key` needs at least one field path"));
    }
    key.iter()
        .map(|path| {
            FieldPath::parse(path).map_err(|e| node.invalid_params(format!("key `{path}`: {e}")))
        })
        .collect()
}

/// FNV-1a 64 over the canonical JSON array of the key `fields` read from `record`. A
/// missing field is `null`, so records lacking it hash together.
#[must_use]
pub(crate) fn hash_key_fields(fields: &[FieldPath], record: &Record) -> u64 {
    let mut canonical = String::from("[");
    for (i, path) in fields.iter().enumerate() {
        if i > 0 {
            canonical.push(',');
        }
        write_canonical(&mut canonical, path.read(record));
    }
    canonical.push(']');
    fnv1a64(canonical.as_bytes())
}

/// One key field as canonical JSON, so equal values hash equal whatever their source.
/// `edit`'s `hash` uses it for numbers and bools, so the two agree on a number's text.
pub(crate) fn write_canonical(out: &mut String, value: FieldValue<'_>) {
    match value {
        FieldValue::Null => out.push_str("null"),
        FieldValue::Bool(b) => out.push_str(if b { "true" } else { "false" }),
        FieldValue::Num(Num::Int(i)) => {
            let _ = write!(out, "{i}");
        }
        FieldValue::Num(Num::Float(f)) => {
            let _ = write!(out, "{f:?}");
        }
        FieldValue::Str(s) => out.push_str(&serde_json::to_string(s).unwrap_or_default()),
        FieldValue::Json(v) => out.push_str(&serde_json::to_string(v).unwrap_or_default()),
        _ => out.push_str("null"),
    }
}

/// FNV-1a, 64-bit: stable across builds and platforms, and cheap next to a store round trip.
#[must_use]
pub(crate) fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    bytes
        .iter()
        .fold(OFFSET, |hash, &b| (hash ^ u64::from(b)).wrapping_mul(PRIME))
}
