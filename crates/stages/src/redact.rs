//! `redact`: replace every match of a pattern in the listed fields, in place.
//!
//! ```yaml
//! - id: mask_phones
//!   type: redact
//!   fields: [body, attributes.msg]   # string fields; others are left alone
//!   pattern: '\d{3}-\d{4}'
//!   replace: '[phone]'               # literal text, no group expansion
//!   limits: { input_bytes: 65536 }   # see the regex_stage module for every key
//!   on_redos_risk: reject            # reject (default) | warn
//! ```
//!
//! A record where no listed field matched passes unchanged and counts one non-match. A
//! tripped limit on any field drops the record with reason `regex_limit`, before any
//! field is written; any other engine failure is a stage error.

use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::metrics::EngineLabel;
use fusion_core::path::{FieldPath, FieldValue};
use fusion_core::record::Record;
use fusion_core::stage::{Context, Stage, StageOutput};
use fusion_regex::Regex;
use serde::Deserialize;

use crate::regex_stage::{
    RegexParams, engine_label, log_node_engine, match_failure, write_strings,
};

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
        log_node_engine(node, Some(engine_label(regex.engine())));
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
        let mut rewrites: Vec<(&FieldPath, String)> = Vec::new();
        for path in &self.fields {
            let FieldValue::Str(text) = path.read(&record) else {
                continue;
            };
            match self.regex.replace_all(text, &self.replace) {
                Ok(Some(masked)) => rewrites.push((path, masked)),
                Ok(None) => {}
                Err(error) => return match_failure(ctx.node_id, error),
            }
        }
        if rewrites.is_empty() {
            ctx.metrics.regex_nonmatch();
            return StageOutput::Pass(record);
        }
        match write_strings(ctx.node_id, &mut record, rewrites) {
            Ok(()) => StageOutput::Pass(record),
            Err(error) => StageOutput::Error(error),
        }
    }

    fn engine_label(&self) -> Option<EngineLabel> {
        Some(engine_label(self.regex.engine()))
    }
}
