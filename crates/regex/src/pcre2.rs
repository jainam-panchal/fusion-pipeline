//! Safe wrapper over `pcre2-sys`. This module is the only place in the workspace that
//! contains `unsafe`.
//!
//! Invariants the wrapper maintains:
//!
//! - Every pattern is compiled with `PCRE2_UTF | PCRE2_UCP` and matched with
//!   `PCRE2_NO_UTF_CHECK`. Haystacks are `&str`, so they are valid UTF-8 by construction and
//!   the check is redundant.
//! - `PCRE2_NEVER_BACKSLASH_C` is set so a match can never end inside a multi-byte
//!   character; every offset PCRE2 returns is a `char` boundary of the haystack.
//! - `PCRE2_DOLLAR_ENDONLY` is set so `$` means end of text, as it does on the `regex` crate.
//! - JIT is never compiled; matching always runs on the interpreter, where
//!   `match_limit`, `depth_limit` and `heap_limit` are deterministic.
//! - Compiled code and the match context are immutable after construction and are shared
//!   across threads. Match data is created per call and never shared.
//!
//! Every PCRE2 allocation is owned by an RAII newtype (`Code`, `CompileContext`,
//! `MatchContext`, `MatchData`) so no error path can leak.

#![allow(unsafe_code)]

use std::ffi::c_int;
use std::ptr::{self, NonNull};

use pcre2_sys::{
    PCRE2_ANCHORED, PCRE2_DOLLAR_ENDONLY, PCRE2_ERROR_DEPTHLIMIT, PCRE2_ERROR_HEAPLIMIT,
    PCRE2_ERROR_MATCHLIMIT, PCRE2_ERROR_NOMATCH, PCRE2_INFO_CAPTURECOUNT, PCRE2_INFO_NAMECOUNT,
    PCRE2_INFO_NAMEENTRYSIZE, PCRE2_INFO_NAMETABLE, PCRE2_NEVER_BACKSLASH_C, PCRE2_NO_UTF_CHECK,
    PCRE2_UCP, PCRE2_UNSET, PCRE2_UTF, pcre2_code_8, pcre2_code_free_8, pcre2_compile_8,
    pcre2_compile_context_8, pcre2_compile_context_create_8, pcre2_compile_context_free_8,
    pcre2_get_error_message_8, pcre2_get_ovector_count_8, pcre2_get_ovector_pointer_8,
    pcre2_match_8, pcre2_match_context_8, pcre2_match_context_create_8, pcre2_match_context_free_8,
    pcre2_match_data_8, pcre2_match_data_create_8, pcre2_match_data_free_8, pcre2_pattern_info_8,
    pcre2_set_depth_limit_8, pcre2_set_heap_limit_8, pcre2_set_match_limit_8,
    pcre2_set_max_pattern_length_8, pcre2_set_parens_nest_limit_8,
};

use crate::{CompileError, Limits, MatchError, Span};

/// A compiled PCRE2 pattern plus the match context carrying its runtime limits.
pub(crate) struct Pcre2Regex {
    code: Code,
    match_context: MatchContext,
    /// Number of capture groups, not counting group 0.
    capture_count: usize,
    /// Group name by group index (index 0 is the whole match and has no name).
    names: Vec<Option<String>>,
}

// SAFETY: PCRE2 documents that a compiled pattern is never modified by matching and can be
// used by several threads at once, and that a match context is only read by
// `pcre2_match`. Neither pointer is mutated after `compile` returns, and match data (the
// only mutable per-match state) is created and freed inside each `captures` or `is_match`
// call.
unsafe impl Send for Pcre2Regex {}
// SAFETY: see the `Send` impl above; shared references only ever read immutable state.
unsafe impl Sync for Pcre2Regex {}

/// Owned compiled pattern.
struct Code(NonNull<pcre2_code_8>);

impl Drop for Code {
    fn drop(&mut self) {
        // SAFETY: returned non-null by `pcre2_compile_8`, owned solely here, freed once.
        unsafe { pcre2_code_free_8(self.0.as_ptr()) }
    }
}

/// Owned compile context carrying the compile-time guards.
struct CompileContext(NonNull<pcre2_compile_context_8>);

impl CompileContext {
    fn new(limits: &Limits) -> Result<Self, CompileError> {
        // SAFETY: a null general context selects PCRE2's default allocator.
        let raw = unsafe { pcre2_compile_context_create_8(ptr::null_mut()) };
        let ctx = NonNull::new(raw).ok_or(CompileError::OutOfMemory)?;
        // SAFETY: `ctx` is a live compile context owned by this function.
        unsafe {
            pcre2_set_max_pattern_length_8(ctx.as_ptr(), limits.max_pattern_length);
            pcre2_set_parens_nest_limit_8(ctx.as_ptr(), limits.parens_nest_limit);
        }
        Ok(Self(ctx))
    }
}

impl Drop for CompileContext {
    fn drop(&mut self) {
        // SAFETY: created by `pcre2_compile_context_create_8`, owned solely here, freed once.
        unsafe { pcre2_compile_context_free_8(self.0.as_ptr()) }
    }
}

/// Owned match context carrying the runtime limits. Read-only after construction.
struct MatchContext(NonNull<pcre2_match_context_8>);

impl MatchContext {
    fn new(limits: &Limits) -> Result<Self, CompileError> {
        // SAFETY: a null general context selects the default allocator.
        let raw = unsafe { pcre2_match_context_create_8(ptr::null_mut()) };
        let ctx = NonNull::new(raw).ok_or(CompileError::OutOfMemory)?;
        // SAFETY: `ctx` is live and owned by this function.
        unsafe {
            pcre2_set_match_limit_8(ctx.as_ptr(), limits.match_limit);
            pcre2_set_depth_limit_8(ctx.as_ptr(), limits.depth_limit);
            pcre2_set_heap_limit_8(ctx.as_ptr(), limits.heap_limit_kib);
        }
        Ok(Self(ctx))
    }
}

impl Drop for MatchContext {
    fn drop(&mut self) {
        // SAFETY: created by `pcre2_match_context_create_8`, owned solely here, freed once.
        unsafe { pcre2_match_context_free_8(self.0.as_ptr()) }
    }
}

/// Owned match data block, one per match call.
struct MatchData(NonNull<pcre2_match_data_8>);

impl MatchData {
    /// A block with room for `pairs` offset pairs. PCRE2 needs at least one.
    fn new(pairs: usize) -> Result<Self, MatchError> {
        let pairs = u32::try_from(pairs.max(1)).map_err(|_| MatchError::OutOfMemory)?;
        // SAFETY: a null general context selects the default allocator.
        let raw = unsafe { pcre2_match_data_create_8(pairs, ptr::null_mut()) };
        NonNull::new(raw).map(Self).ok_or(MatchError::OutOfMemory)
    }
}

impl Drop for MatchData {
    fn drop(&mut self) {
        // SAFETY: created by `pcre2_match_data_create_8`, owned solely here, freed once.
        unsafe { pcre2_match_data_free_8(self.0.as_ptr()) }
    }
}

impl Pcre2Regex {
    /// Compiles `pattern` under `limits`. Never calls the JIT compiler.
    pub(crate) fn compile(pattern: &str, limits: &Limits) -> Result<Self, CompileError> {
        let compile_context = CompileContext::new(limits)?;
        let options = PCRE2_UTF | PCRE2_UCP | PCRE2_NEVER_BACKSLASH_C | PCRE2_DOLLAR_ENDONLY;
        let mut error_code: c_int = 0;
        let mut error_offset: usize = 0;
        // SAFETY: `pattern` is valid for `pattern.len()` bytes for the duration of the call;
        // the out-pointers point at live locals; the compile context is live.
        let raw = unsafe {
            pcre2_compile_8(
                pattern.as_ptr(),
                pattern.len(),
                options,
                &mut error_code,
                &mut error_offset,
                compile_context.0.as_ptr(),
            )
        };
        let Some(code) = NonNull::new(raw).map(Code) else {
            return Err(CompileError::Syntax {
                engine: crate::Engine::Backtracking,
                code: error_code,
                message: error_message(error_code),
                offset: error_offset,
            });
        };
        let match_context = MatchContext::new(limits)?;
        let capture_count = pattern_info_u32(&code, PCRE2_INFO_CAPTURECOUNT)? as usize;
        let names = read_name_table(&code, capture_count)?;
        Ok(Self {
            code,
            match_context,
            capture_count,
            names,
        })
    }

    pub(crate) fn capture_names(&self) -> &[Option<String>] {
        &self.names
    }

    /// Whether the pattern matches, without recording group spans.
    pub(crate) fn is_match(&self, haystack: &str, anchored: bool) -> Result<bool, MatchError> {
        let match_data = MatchData::new(1)?;
        let rc = self.run(&match_data, haystack, anchored);
        if rc == PCRE2_ERROR_NOMATCH {
            return Ok(false);
        }
        if rc < 0 {
            return Err(match_error(rc));
        }
        Ok(true)
    }

    /// Runs the interpreter over `haystack`. Returns the group spans on a match, `None` on
    /// no match, and a typed error when a limit trips.
    pub(crate) fn captures(
        &self,
        haystack: &str,
        anchored: bool,
    ) -> Result<Option<Vec<Option<Span>>>, MatchError> {
        let match_data = MatchData::new(self.capture_count + 1)?;
        let rc = self.run(&match_data, haystack, anchored);
        if rc == PCRE2_ERROR_NOMATCH {
            return Ok(None);
        }
        if rc < 0 {
            return Err(match_error(rc));
        }
        // SAFETY: the match data block is live; the ovector pointer PCRE2 returns is valid
        // for `2 * ovector_count` `usize`s for as long as the block lives, and the slice is
        // dropped before `match_data` is.
        let ovector = unsafe {
            let count = pcre2_get_ovector_count_8(match_data.0.as_ptr()) as usize;
            let ptr = pcre2_get_ovector_pointer_8(match_data.0.as_ptr());
            std::slice::from_raw_parts(ptr, count * 2)
        };
        // `rc` is the highest group number that matched plus one; groups at or beyond it are
        // unset. PCRE2 also marks unset groups inside that range with `PCRE2_UNSET`.
        let set_groups = usize::try_from(rc).unwrap_or(0);
        let spans = ovector
            .chunks_exact(2)
            .enumerate()
            .map(|(i, pair)| {
                let (start, end) = (pair[0], pair[1]);
                if i >= set_groups || start == PCRE2_UNSET || end == PCRE2_UNSET {
                    None
                } else {
                    Some(Span { start, end })
                }
            })
            .collect();
        Ok(Some(spans))
    }

    fn run(&self, match_data: &MatchData, haystack: &str, anchored: bool) -> c_int {
        let mut options = PCRE2_NO_UTF_CHECK;
        if anchored {
            options |= PCRE2_ANCHORED;
        }
        // SAFETY: `haystack` is valid UTF-8 (it is a `&str`), which is what
        // `PCRE2_NO_UTF_CHECK` requires; it is valid for `haystack.len()` bytes for the
        // duration of the call; `code` and `match_context` are live and immutable; the match
        // data block is live and exclusively owned by the caller for this call.
        unsafe {
            pcre2_match_8(
                self.code.0.as_ptr(),
                haystack.as_ptr(),
                haystack.len(),
                0,
                options,
                match_data.0.as_ptr(),
                self.match_context.0.as_ptr(),
            )
        }
    }
}

fn pattern_info_u32(code: &Code, what: u32) -> Result<u32, CompileError> {
    let mut out: u32 = 0;
    // SAFETY: `code` is a live compiled pattern; `what` is one of the `PCRE2_INFO_*` items
    // whose result type is `uint32_t`, so `out` is the right size.
    let rc = unsafe { pcre2_pattern_info_8(code.0.as_ptr(), what, (&mut out as *mut u32).cast()) };
    if rc != 0 {
        return Err(CompileError::Internal {
            code: rc,
            message: error_message(rc),
        });
    }
    Ok(out)
}

/// Reads PCRE2's name table into a `Vec` indexed by group number.
///
/// The table is `name_count` entries of `entry_size` bytes each: a big-endian `u16` group
/// number followed by the NUL-terminated name, padded to `entry_size`.
fn read_name_table(code: &Code, capture_count: usize) -> Result<Vec<Option<String>>, CompileError> {
    let mut names = vec![None; capture_count + 1];
    let name_count = pattern_info_u32(code, PCRE2_INFO_NAMECOUNT)? as usize;
    if name_count == 0 {
        return Ok(names);
    }
    let entry_size = pattern_info_u32(code, PCRE2_INFO_NAMEENTRYSIZE)? as usize;
    if entry_size < 3 {
        return Err(CompileError::Internal {
            code: 0,
            message: format!("name table entry size {entry_size} is too small"),
        });
    }
    let mut table: *const u8 = ptr::null();
    // SAFETY: `code` is live; `PCRE2_INFO_NAMETABLE` writes a `PCRE2_SPTR` into the
    // out-pointer, which is exactly `*const u8` for the 8-bit library.
    let rc = unsafe {
        pcre2_pattern_info_8(
            code.0.as_ptr(),
            PCRE2_INFO_NAMETABLE,
            (&mut table as *mut *const u8).cast(),
        )
    };
    if rc != 0 || table.is_null() {
        return Err(CompileError::Internal {
            code: rc,
            message: error_message(rc),
        });
    }
    // SAFETY: PCRE2 guarantees the table is `name_count * entry_size` bytes and lives as
    // long as the compiled pattern, which outlives this function.
    let bytes = unsafe { std::slice::from_raw_parts(table, name_count * entry_size) };
    for entry in bytes.chunks_exact(entry_size) {
        let group = usize::from(u16::from_be_bytes([entry[0], entry[1]]));
        let name_bytes = &entry[2..];
        let end = name_bytes
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(name_bytes.len());
        let name = String::from_utf8_lossy(&name_bytes[..end]).into_owned();
        if let Some(slot) = names.get_mut(group) {
            *slot = Some(name);
        }
    }
    Ok(names)
}

fn match_error(rc: c_int) -> MatchError {
    match rc {
        PCRE2_ERROR_MATCHLIMIT => MatchError::MatchLimit,
        PCRE2_ERROR_DEPTHLIMIT => MatchError::DepthLimit,
        PCRE2_ERROR_HEAPLIMIT => MatchError::HeapLimit,
        other => MatchError::Engine {
            code: other,
            message: error_message(other),
        },
    }
}

/// Renders a PCRE2 error code through `pcre2_get_error_message`.
pub(crate) fn error_message(code: c_int) -> String {
    let mut buf = [0u8; 256];
    // SAFETY: `buf` is valid for writes of `buf.len()` bytes, which is the length passed.
    let n = unsafe { pcre2_get_error_message_8(code, buf.as_mut_ptr(), buf.len()) };
    let Ok(len) = usize::try_from(n) else {
        return format!("PCRE2 error {code}");
    };
    String::from_utf8_lossy(&buf[..len.min(buf.len())]).into_owned()
}
