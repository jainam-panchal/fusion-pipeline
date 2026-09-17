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

use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::expect::Expectation;

/// The expectations file, read as it grows.
#[derive(Debug)]
pub struct Tail {
    path: PathBuf,
    file: Option<File>,
    read: u64,
    lines: usize,
    buffer: LineBuffer,
    expectations: Vec<Expectation>,
}

impl Tail {
    /// A tail of the file at `path`, which need not exist yet.
    #[must_use]
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_owned(),
            file: None,
            read: 0,
            lines: 0,
            buffer: LineBuffer::default(),
            expectations: Vec::new(),
        }
    }

    /// Take in the lines written since the last call. A file not created yet has none.
    ///
    /// # Errors
    ///
    /// A message naming the file when it cannot be read or is shorter than what was already
    /// read (rewritten under the tail), and naming the line when a line is not an expectation.
    pub fn read(&mut self) -> Result<(), String> {
        let fail = |err: &dyn std::fmt::Display| format!("{}: {err}", self.path.display());
        if self.file.is_none() {
            match File::open(&self.path) {
                Ok(file) => self.file = Some(file),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(err) => return Err(fail(&err)),
            }
        }
        let Some(file) = self.file.as_mut() else {
            return Ok(());
        };
        let len = file.metadata().map_err(|err| fail(&err))?.len();
        if len < self.read {
            return Err(fail(&"rewritten while it was read; remove it before a run"));
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|err| fail(&err))?;
        self.read += bytes.len() as u64;
        for line in self.buffer.push(&bytes) {
            self.lines += 1;
            let expectation = serde_json::from_str(&line)
                .map_err(|err| format!("{}:{}: {err}", self.path.display(), self.lines))?;
            self.expectations.push(expectation);
        }
        Ok(())
    }

    /// The expectations read so far.
    #[must_use]
    pub fn expectations(&self) -> &[Expectation] {
        &self.expectations
    }

    /// Whether what was read ends with a whole line.
    #[must_use]
    pub fn ends_whole(&self) -> bool {
        !self.buffer.has_partial()
    }
}

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
    const ALL: [Self; 2] = [Self::Published, Self::Failed];

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
    with_suffix(expectations, ".done")
}

/// `path` with `suffix` appended to its last component.
fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut path = path.as_os_str().to_owned();
    path.push(suffix);
    PathBuf::from(path)
}

/// Write `outcome` to the done marker at `path`, through a temporary file renamed into
/// place, so a reader never sees half of it.
///
/// # Errors
///
/// The I/O error of the write or the rename.
pub fn write_done(path: &Path, outcome: ProducerOutcome) -> std::io::Result<()> {
    let temporary = with_suffix(path, ".tmp");
    let mut file = std::fs::File::create(&temporary)?;
    writeln!(file, "{}", outcome.as_str())?;
    file.sync_all()?;
    std::fs::rename(&temporary, path)
}

/// The outcome a done marker's text names, if it names one.
#[must_use]
pub fn parse_done(text: &str) -> Option<ProducerOutcome> {
    let text = text.trim();
    ProducerOutcome::ALL
        .into_iter()
        .find(|outcome| outcome.as_str() == text)
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
