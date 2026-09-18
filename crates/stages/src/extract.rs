//! `extract`: lift a pattern's named groups out of one field into another.
//!
//! ```yaml
//! - id: parse_linux
//!   type: extract
//!   field: body                       # any string field
//!   pattern: '^(?<Month>\w{3}) ...'   # named groups become <into>.<name>
//!   into: attributes                  # where the groups go; `attributes` by default
//!   limits: { input_bytes: 65536 }    # see the regex_stage module for every key
//!   on_redos_risk: reject             # reject (default) | warn
//! ```
//!
//! A non-match, or a field that is not a string, passes the record unchanged. A tripped
//! limit drops it with reason `regex_limit`; any other engine failure is a stage error.
//!
//! A write makes its path exist (issue #79), so `into` on a record that is not an object
//! replaces what is in the way. To keep a raw line from a `codec: text` source, copy it into
//! a field first: `edit copy {from: ., to: body}`.

use fusion_core::config::{ConfigError, NodeConfig};
use fusion_core::metrics::EngineLabel;
use fusion_core::path::{FieldPath, FieldValue, WritePath};
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
    field: String,
    pattern: String,
    #[serde(default = "default_into")]
    into: String,
    #[serde(flatten)]
    regex: RegexParams,
}

/// Where a node's groups go when it names no `into`.
fn default_into() -> String {
    DEFAULT_INTO.to_owned()
}

/// The `into` of a node that names none.
pub const DEFAULT_INTO: &str = "attributes";

/// The `extract` stage.
#[derive(Debug)]
pub struct Extract {
    field: FieldPath,
    regex: Regex,
    /// Where each named group is written, in group order.
    targets: Vec<(String, WritePath)>,
}

impl Extract {
    /// Build from a node's `field`, `pattern`, `limits` and `on_redos_risk`.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidParams`] naming the node when a parameter is missing or
    /// unknown, `field` or `into` is not a field path, `into` names the read-only `meta`,
    /// or the pattern does not compile under the node's limits and ReDoS policy.
    pub fn from_node(node: &NodeConfig) -> Result<Self, ConfigError> {
        let params: Params = node.parse_params()?;
        let field = FieldPath::parse(&params.field)
            .map_err(|e| node.invalid_params(format!("field `{}`: {e}", params.field)))?;
        let into = FieldPath::parse(&params.into)
            .map_err(|e| node.invalid_params(format!("`into` `{}`: {e}", params.into)))?
            .writable()
            .map_err(|e| node.invalid_params(format!("`into` `{}`: {e}", params.into)))?;
        let regex = params.regex.compile(node, "pattern", &params.pattern)?;
        log_node_engine(node, Some(engine_label(regex.engine())));
        // A group name is one bare segment, so appending it needs no path text.
        let targets = regex
            .capture_names()
            .flatten()
            .map(|name| (name.to_owned(), into.child(name)))
            .collect();
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
        let FieldValue::Str(haystack) = self.field.read(&record, ctx.meta) else {
            ctx.metrics.regex_nonmatch();
            return StageOutput::Pass(record);
        };
        let extracted: Vec<(&WritePath, String)> = match self.regex.captures(haystack) {
            Ok(Some(caps)) => self
                .targets
                .iter()
                .filter_map(|(name, path)| Some((path, caps.name(name)?.to_owned())))
                .collect(),
            Ok(None) => {
                ctx.metrics.regex_nonmatch();
                return StageOutput::Pass(record);
            }
            Err(error) => return match_failure(ctx.node_id, error),
        };
        write_strings(&mut record, extracted);
        StageOutput::Pass(record)
    }

    fn engine_label(&self) -> Option<EngineLabel> {
        Some(engine_label(self.regex.engine()))
    }
}
