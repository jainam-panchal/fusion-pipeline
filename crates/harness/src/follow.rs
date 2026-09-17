//! Following a run while it happens: the verifier reads the expectations file as the
//! producer writes it and the sink and dead-letter streams as the pipeline writes them, and
//! judges what it has every few seconds, so the coverage panel shows published and received
//! converging.
//!
//! The run is over, and the last judgement final, once three things hold in this order: the
//! producer has written its done marker, the pipeline's consumer has settled, and every stream
//! read has been read to the end it had once the consumer settled. A dead letter and a sink's
//! `PubAck` both land before the source message's ack, so after settling nothing more
//! arrives.

use std::io::Write;
use std::path::{Path, PathBuf};

/// Complete lines out of bytes that arrive in arbitrary pieces.
#[derive(Debug, Default)]
pub struct LineBuffer {
    partial: Vec<u8>,
}

impl LineBuffer {
    /// The lines `bytes` completes, without their newline; empty lines are skipped. A line
    /// that is not UTF-8 is handed over lossily, for the JSON parser to refuse.
    pub fn push(&mut self, bytes: &[u8]) -> Vec<String> {
        self.partial.extend_from_slice(bytes);
        let Some(end) = self.partial.iter().rposition(|b| *b == b'\n') else {
            return Vec::new();
        };
        let rest = self.partial.split_off(end + 1);
        let complete = std::mem::replace(&mut self.partial, rest);
        complete
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| String::from_utf8_lossy(line).into_owned())
            .collect()
    }

    /// Whether bytes without their newline are held back.
    #[must_use]
    pub fn has_partial(&self) -> bool {
        !self.partial.is_empty()
    }
}

/// How the producer's run ended, as its done marker says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerOutcome {
    /// Every planned message was published and the timing held.
    Published,
    /// It did not: the expectations do not describe the plan.
    Failed,
}

impl ProducerOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Published => "published",
            Self::Failed => "failed",
        }
    }
}

/// The done marker of `expectations`: the same path with `.done` appended. The producer
/// removes it before writing the first expectation and writes it after the last.
#[must_use]
pub fn done_marker(expectations: &Path) -> PathBuf {
    let mut path = expectations.as_os_str().to_owned();
    path.push(".done");
    PathBuf::from(path)
}

/// Write `outcome` to the done marker at `path`, through a temporary file renamed into
/// place, so a reader never sees half of it.
///
/// # Errors
///
/// The I/O error of the write or the rename.
pub fn write_done(path: &Path, outcome: ProducerOutcome) -> std::io::Result<()> {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    let mut file = std::fs::File::create(&temporary)?;
    writeln!(file, "{}", outcome.as_str())?;
    file.sync_all()?;
    std::fs::rename(&temporary, path)
}

/// The outcome a done marker's text names, if it names one.
#[must_use]
pub fn parse_done(text: &str) -> Option<ProducerOutcome> {
    match text.trim() {
        "published" => Some(ProducerOutcome::Published),
        "failed" => Some(ProducerOutcome::Failed),
        _ => None,
    }
}

/// Where a stream ended once the pipeline had settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamEnd {
    /// The messages it held.
    pub messages: u64,
    /// The sequence of its last message, which a purge leaves in place.
    pub last_sequence: u64,
}

impl StreamEnd {
    /// Whether a read that has seen up to stream sequence `seen` has read everything: an
    /// empty stream always, else once its last sequence was seen.
    #[must_use]
    pub fn reached(self, seen: Option<u64>) -> bool {
        self.messages == 0 || seen.is_some_and(|seen| seen >= self.last_sequence)
    }
}
