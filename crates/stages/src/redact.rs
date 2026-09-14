//! `redact`: replace every match of a pattern in the listed fields, in place.
//!
//! ```yaml
//! - id: mask_phones
//!   type: redact
//!   fields: [body, attributes.msg]   # string fields; others are left alone
//!   pattern: '\d{3}-\d{4}'
//!   replace: '[phone]'               # literal text, no group expansion
//!   limits: { input_bytes: 65536 }   # see the regex module for every key
//!   on_redos_risk: reject            # reject (default) | warn
//! ```
//!
//! A record where no listed field matched passes unchanged and counts one non-match. A
//! tripped limit on any field drops the record with reason `regex_limit`, before any
//! field is written; any other engine failure is a stage error.

use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::path::{FieldPath, FieldValue};
use fusion_core::record::Record;
use fusion_core::stage::{Context, Stage, StageError, StageOutput};
use fusion_regex::Regex;
use serde::Deserialize;
use serde_json::Value;

use crate::regex::{RegexParams, match_failure};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    fields: Vec<String>,
    pattern: String,
    replace: String,
    #[serde(flatten)]
    regex: RegexParams,
}

/// The `redact` stage.
#[derive(Debug)]
pub struct Redact {
    fields: Vec<FieldPath>,
    regex: Regex,
    replace: String,
}

impl Redact {
    /// Build from a node's `fields`, `pattern`, `replace`, `limits` and `on_redos_risk`.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidParams`] naming the node when a parameter is missing or
    /// unknown, `fields` is empty, a field is not a path or is read-only (`id`, `kind`),
    /// or the pattern does not compile under the node's limits and ReDoS policy.
    pub fn from_node(node: &NodeConfig) -> Result<Self, ConfigError> {
        let params: Params = node.parse_params()?;
        if params.fields.is_empty() {
            return Err(node.invalid_params("`fields` needs at least one field path"));
        }
        let fields = params
            .fields
            .iter()
            .map(|field| {
                let path = FieldPath::parse(field)
                    .map_err(|e| node.invalid_params(format!("field `{field}`: {e}")))?;
                if !path.is_writable() {
                    return Err(node.invalid_params(format!(
                        "field `{field}` is read-only and cannot be redacted"
                    )));
                }
                Ok(path)
            })
            .collect::<Result<Vec<_>, ConfigError>>()?;
        let regex = params.regex.compile(node, "pattern", &params.pattern)?;
        Ok(Self {
            fields,
            regex,
            replace: params.replace,
        })
    }

    /// Factory for a [`fusion_core::registry::Registry`].
    ///
    /// # Errors
    ///
    /// See [`Redact::from_node`].
    pub fn build(node: &NodeConfig) -> Result<Box<dyn Stage>, ConfigError> {
        Ok(Box::new(Self::from_node(node)?))
    }
}

impl Stage for Redact {
    fn process(&self, mut record: Record, ctx: &Context<'_>) -> StageOutput {
        // Every field is scanned before any is written, so a limit tripped on the second
        // field cannot leave the first half-redacted on a record that is then dropped.
        let mut rewrites: Vec<(usize, String)> = Vec::new();
        for (i, path) in self.fields.iter().enumerate() {
            let FieldValue::Str(text) = path.read(&record) else {
                continue;
            };
            match self.regex.replace_all(text, &self.replace) {
                Ok(Some(masked)) => rewrites.push((i, masked)),
                Ok(None) => {}
                Err(error) => return match_failure(ctx.node_id, error),
            }
        }
        if rewrites.is_empty() {
            ctx.metrics.regex_nonmatch();
            return StageOutput::Pass(record);
        }
        for (i, masked) in rewrites {
            if let Err(e) = self.fields[i].write(&mut record, Value::String(masked)) {
                return StageOutput::Error(StageError::new(format!(
                    "node `{}`: cannot write `{}`: {e}",
                    ctx.node_id, self.fields[i]
                )));
            }
        }
        StageOutput::Pass(record)
    }

    fn engine_label(&self) -> Option<&'static str> {
        Some(self.regex.engine().as_str())
    }
}
