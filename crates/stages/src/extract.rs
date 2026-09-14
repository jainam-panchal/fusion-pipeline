//! `pcre2_extract`: lift a pattern's named groups out of one field into attributes.
//!
//! ```yaml
//! - id: parse_linux
//!   type: pcre2_extract
//!   field: body                       # any string field
//!   pattern: '^(?<Month>\w{3}) ...'   # named groups become attributes.<name>
//!   limits: { input_bytes: 65536 }    # see the regex module for every key
//!   on_redos_risk: reject             # reject (default) | warn
//! ```
//!
//! A non-match, or a field that is not a string, passes the record unchanged. A tripped
//! limit drops it with reason `regex_limit`; any other engine failure is a stage error.

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
    field: String,
    pattern: String,
    #[serde(flatten)]
    regex: RegexParams,
}

/// The `pcre2_extract` stage.
#[derive(Debug)]
pub struct Extract {
    field: FieldPath,
    regex: Regex,
    /// Where each named group is written, in group order.
    targets: Vec<(String, FieldPath)>,
}

impl Extract {
    /// Build from a node's `field`, `pattern`, `limits` and `on_redos_risk`.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidParams`] naming the node when a parameter is missing or
    /// unknown, `field` is not a field path, or the pattern does not compile under the
    /// node's limits and ReDoS policy.
    pub fn from_node(node: &NodeConfig) -> Result<Self, ConfigError> {
        let params: Params = node.parse_params()?;
        let field = FieldPath::parse(&params.field)
            .map_err(|e| node.invalid_params(format!("field `{}`: {e}", params.field)))?;
        let regex = params.regex.compile(node, "pattern", &params.pattern)?;
        let targets = regex
            .capture_names()
            .flatten()
            .map(|name| {
                let path = FieldPath::parse(&format!("attributes.{name}")).map_err(|e| {
                    node.invalid_params(format!("group `{name}` cannot name an attribute: {e}"))
                })?;
                Ok((name.to_owned(), path))
            })
            .collect::<Result<Vec<_>, ConfigError>>()?;
        Ok(Self {
            field,
            regex,
            targets,
        })
    }

    /// Factory for a [`fusion_core::registry::Registry`].
    ///
    /// # Errors
    ///
    /// See [`Extract::from_node`].
    pub fn build(node: &NodeConfig) -> Result<Box<dyn Stage>, ConfigError> {
        Ok(Box::new(Self::from_node(node)?))
    }
}

impl Stage for Extract {
    fn process(&self, mut record: Record, ctx: &Context<'_>) -> StageOutput {
        let FieldValue::Str(haystack) = self.field.read(&record) else {
            ctx.metrics.regex_nonmatch();
            return StageOutput::Pass(record);
        };
        let extracted: Vec<(usize, String)> = match self.regex.captures(haystack) {
            Ok(Some(caps)) => self
                .targets
                .iter()
                .enumerate()
                .filter_map(|(i, (name, _))| Some((i, caps.name(name)?.to_owned())))
                .collect(),
            Ok(None) => {
                ctx.metrics.regex_nonmatch();
                return StageOutput::Pass(record);
            }
            Err(error) => return match_failure(ctx.node_id, error),
        };
        for (i, text) in extracted {
            let (name, path) = &self.targets[i];
            if let Err(e) = path.write(&mut record, Value::String(text)) {
                return StageOutput::Error(StageError::new(format!(
                    "node `{}`: cannot write group `{name}`: {e}",
                    ctx.node_id
                )));
            }
        }
        StageOutput::Pass(record)
    }

    fn engine_label(&self) -> Option<&'static str> {
        Some(self.regex.engine().as_str())
    }
}
