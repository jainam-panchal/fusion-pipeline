//! The vendored loghub sets (`testdata/loghub/`): the lines the producer replays and the
//! attributes the structured CSV says extraction should lift from each.
//!
//! A set is loaded as one line per distinct body, under the first `LineId` that has it.
//! Apache and Mac repeat some lines word for word, and the pipeline's `dedupe` node rightly
//! drops a repeat inside its window; replaying only distinct bodies leaves the producer's
//! planned duplicates as the only repeats, so every drop the verifier sees is one it planned.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

/// The subject every set's records reach, after extraction and `edit`.
pub const MAIN: &str = "processed.loghub.main";

/// The subject Linux records also reach, straight from the route, before extraction.
pub const AUDIT: &str = "processed.loghub.audit";

/// A vendored set.
#[derive(Debug)]
pub struct Set {
    /// The set's name: its directory, its file prefix and its `resource.log.format`.
    pub name: &'static str,
    /// The tenant its messages are published under, one per set, so the state store keeps
    /// each set's dedupe keys apart.
    pub tenant: &'static str,
    /// The structured-CSV columns extraction must lift, in CSV order.
    pub columns: &'static [&'static str],
    /// The subjects `deploy/pipeline-poc.yaml` writes the set's records to, the main one
    /// first. Written down by hand, not read from the config: `deploy_configs.rs` checks the
    /// two agree.
    pub subjects: &'static [&'static str],
}

/// Every vendored set, in the order the producer interleaves them.
pub const SETS: [Set; 4] = [
    Set {
        name: "Linux",
        tenant: "linux",
        columns: &[
            "Month",
            "Date",
            "Time",
            "Level",
            "Component",
            "PID",
            "Content",
        ],
        subjects: &[MAIN, AUDIT],
    },
    Set {
        name: "OpenSSH",
        tenant: "openssh",
        columns: &["Date", "Day", "Time", "Component", "Pid", "Content"],
        subjects: &[MAIN],
    },
    Set {
        name: "Apache",
        tenant: "apache",
        columns: &["Time", "Level", "Content"],
        subjects: &[MAIN],
    },
    Set {
        name: "Mac",
        tenant: "mac",
        columns: &[
            "Month",
            "Date",
            "Time",
            "User",
            "Component",
            "PID",
            "Address",
            "Content",
        ],
        subjects: &[MAIN],
    },
];

/// One line to replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Line {
    /// The line's `LineId`, its 1-based position in the raw log.
    pub line_id: usize,
    /// The raw line, as the producer sends it in `body`.
    pub body: String,
    /// The set's columns from the line's CSV row, normalised; an empty cell is absent.
    pub attributes: BTreeMap<String, String>,
}

/// A set that could not be loaded.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// A file could not be read.
    #[error("could not read `{path}`: {source}")]
    Read {
        /// The file.
        path: PathBuf,
        /// The I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The structured CSV did not parse.
    #[error("`{path}`: {source}")]
    Csv {
        /// The file.
        path: PathBuf,
        /// The CSV error.
        #[source]
        source: csv::Error,
    },
    /// The structured CSV does not match the raw log.
    #[error("`{path}`: {message}")]
    Shape {
        /// The file.
        path: PathBuf,
        /// What is wrong.
        message: String,
    },
}

/// The payload the producer sends for `line` of `set`: the raw line, the set as the log
/// format, the `LineId`, and when the producer observed it. No id, no kind and no tenant:
/// those travel in the headers and the subject (ADRs 0005 and 0007).
#[must_use]
pub fn payload(set: &Set, line: &Line, observed_unix_nanos: u64) -> Value {
    json!({
        "observed_time_unix_nano": observed_unix_nanos,
        "body": line.body,
        "resource": {"log.format": set.name},
        "attributes": {"loghub.line_id": line.line_id},
    })
}

/// The set named `name`, if it is vendored.
#[must_use]
pub fn set(name: &str) -> Option<&'static Set> {
    SETS.iter().find(|set| set.name == name)
}

/// The workspace's `testdata/loghub` directory.
#[must_use]
pub fn testdata() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/loghub")
}

/// The lines of `set` under `dir`, one per distinct body, in `LineId` order.
///
/// # Errors
///
/// [`LoadError`] when a file is unreadable, the CSV does not parse, or a CSV row names a
/// line the raw log does not have or lacks one of the set's columns.
pub fn load(dir: &Path, set: &Set) -> Result<Vec<Line>, LoadError> {
    let raw_path = dir.join(set.name).join(format!("{}_2k.log", set.name));
    let raw = std::fs::read_to_string(&raw_path).map_err(|source| LoadError::Read {
        path: raw_path.clone(),
        source,
    })?;
    let raw: Vec<&str> = raw.lines().collect();

    let csv_path = dir
        .join(set.name)
        .join(format!("{}_2k.log_structured_corrected.csv", set.name));
    let csv_error = |source| LoadError::Csv {
        path: csv_path.clone(),
        source,
    };
    let shape = |message: String| LoadError::Shape {
        path: csv_path.clone(),
        message,
    };
    let mut reader = csv::Reader::from_path(&csv_path).map_err(csv_error)?;
    let headers = reader.headers().map_err(csv_error)?.clone();
    let index = |name: &str| {
        headers
            .iter()
            .position(|h| h == name)
            .ok_or_else(|| shape(format!("no `{name}` column")))
    };
    let line_id_at = index("LineId")?;
    let columns = set
        .columns
        .iter()
        .map(|column| index(column).map(|at| (*column, at)))
        .collect::<Result<Vec<_>, _>>()?;

    let mut seen = BTreeSet::new();
    let mut lines = Vec::new();
    for row in reader.records() {
        let row = row.map_err(csv_error)?;
        let cell = |at: usize| row.get(at).unwrap_or("");
        let line_id: usize = cell(line_id_at)
            .parse()
            .map_err(|_| shape(format!("LineId `{}` is not a number", cell(line_id_at))))?;
        let body = line_id
            .checked_sub(1)
            .and_then(|at| raw.get(at))
            .ok_or_else(|| shape(format!("LineId {line_id} is past the raw log")))?;
        if !seen.insert(*body) {
            continue;
        }
        let attributes = columns
            .iter()
            .filter_map(|&(column, at)| {
                let value = normalise(column, cell(at));
                (!value.is_empty()).then(|| (column.to_owned(), value))
            })
            .collect();
        lines.push(Line {
            line_id,
            body: (*body).to_owned(),
            attributes,
        });
    }
    lines.sort_by_key(|line| line.line_id);
    Ok(lines)
}

/// A CSV cell or an extracted value as the comparison sees it (README rules 1 and 3): a
/// pandas float `N.0` is the integer `N`, and `Content` loses its trailing whitespace.
/// Every other column compares exactly.
#[must_use]
pub fn normalise(column: &str, cell: &str) -> String {
    let trimmed = if column == "Content" {
        cell.trim_end()
    } else {
        cell
    };
    match trimmed.strip_suffix(".0") {
        Some(digits) if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) => {
            digits.to_owned()
        }
        _ => trimmed.to_owned(),
    }
}
