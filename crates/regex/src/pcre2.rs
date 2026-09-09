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

#![allow(unsafe_code)]

use std::ptr::{self, NonNull};

use pcre2_sys::{
    PCRE2_ANCHORED, PCRE2_DOLLAR_ENDONLY, PCRE2_ERROR_DEPTHLIMIT, PCRE2_ERROR_HEAPLIMIT,
    PCRE2_ERROR_MATCHLIMIT, PCRE2_ERROR_NOMATCH, PCRE2_INFO_CAPTURECOUNT, PCRE2_INFO_NAMECOUNT,
    PCRE2_INFO_NAMEENTRYSIZE, PCRE2_INFO_NAMETABLE, PCRE2_NEVER_BACKSLASH_C, PCRE2_NO_UTF_CHECK,
    PCRE2_UCP, PCRE2_UTF, pcre2_code_8, pcre2_code_free_8, pcre2_compile_8,
    pcre2_compile_context_create_8, pcre2_compile_context_free_8, pcre2_get_error_message_8,
    pcre2_get_ovector_count_8, pcre2_get_ovector_pointer_8, pcre2_match_8, pcre2_match_context_8,
    pcre2_match_context_create_8, pcre2_match_context_free_8, pcre2_match_data_create_8,
    pcre2_match_data_free_8, pcre2_pattern_info_8, pcre2_set_depth_limit_8, pcre2_set_heap_limit_8,
    pcre2_set_match_limit_8, pcre2_set_max_pattern_length_8, pcre2_set_parens_nest_limit_8,
};

use crate::{CompileError, Limits, MatchError, Span};

/// A compiled PCRE2 pattern plus the match context carrying its runtime limits.
pub(crate) struct Pcre2Regex {
    code: NonNull<pcre2_code_8>,
    match_context: NonNull<pcre2_match_context_8>,
    /// Number of capture groups, not counting group 0.
    capture_count: usize,
    /// Group name by group index (index 0 is the whole match and has no name).
    names: Vec<Option<String>>,
}

// SAFETY: PCRE2 documents that a compiled pattern is never modified by matching and can be
// used by several threads at once, and that a match context is only read by
// `pcre2_match`. Neither pointer is mutated after `compile` returns, and match data (the
// only mutable per-match state) is created and freed inside each `matches` call.
unsafe impl Send for Pcre2Regex {}
// SAFETY: see the `Send` impl above; shared references only ever read immutable state.
unsafe impl Sync for Pcre2Regex {}

impl Drop for Pcre2Regex {
    fn drop(&mut self) {
        // SAFETY: both pointers were returned non-null by PCRE2's create/compile functions,
        // are owned solely by this struct, and are freed exactly once here.
        unsafe {
            pcre2_match_context_free_8(self.match_context.as_ptr());
            pcre2_code_free_8(self.code.as_ptr());
        }
    }
}

/// Owned compile context, freed on drop so early returns cannot leak it.
struct CompileContext(NonNull<pcre2_sys::pcre2_compile_context_8>);

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
        // SAFETY: the context was created by `pcre2_compile_context_create_8` and is freed once.
        unsafe { pcre2_compile_context_free_8(self.0.as_ptr()) }
    }
}

/// Owned match data block, freed on drop.
struct MatchData(NonNull<pcre2_sys::pcre2_match_data_8>);

impl MatchData {
    fn new(capture_count: usize) -> Result<Self, MatchError> {
        let pairs = u32::try_from(capture_count + 1).map_err(|_| MatchError::OutOfMemory)?;
        // SAFETY: a null general context selects the default allocator; `pairs` is the
        // number of ovector pairs to allocate.
        let raw = unsafe { pcre2_match_data_create_8(pairs, ptr::null_mut()) };
        NonNull::new(raw).map(Self).ok_or(MatchError::OutOfMemory)
    }
}

impl Drop for MatchData {
    fn drop(&mut self) {
        // SAFETY: created by `pcre2_match_data_create_8`, owned here, freed once.
        unsafe { pcre2_match_data_free_8(self.0.as_ptr()) }
    }
}

impl Pcre2Regex {
    /// Compiles `pattern` under `limits`. Never calls the JIT compiler.
    pub(crate) fn compile(pattern: &str, limits: &Limits) -> Result<Self, CompileError> {
        let compile_context = CompileContext::new(limits)?;
        let options = PCRE2_UTF | PCRE2_UCP | PCRE2_NEVER_BACKSLASH_C | PCRE2_DOLLAR_ENDONLY;
        let mut error_code: CInt = 0;
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
        let Some(code) = NonNull::new(raw) else {
            return Err(CompileError::Syntax {
                engine: crate::Engine::Backtracking,
                code: error_code,
                message: error_message(error_code),
                offset: error_offset,
            });
        };
        // From here on `code` must be freed on every exit path.
        let built = Self::finish(code, limits);
        if built.is_err() {
            // SAFETY: `code` was returned by `pcre2_compile_8` and is not yet owned by a
            // `Pcre2Regex`, so it is freed exactly once here.
            unsafe { pcre2_code_free_8(code.as_ptr()) };
        }
        built
    }

    fn finish(code: NonNull<pcre2_code_8>, limits: &Limits) -> Result<Self, CompileError> {
        // SAFETY: a null general context selects the default allocator.
        let raw_ctx = unsafe { pcre2_match_context_create_8(ptr::null_mut()) };
        let match_context = NonNull::new(raw_ctx).ok_or(CompileError::OutOfMemory)?;
        // SAFETY: `match_context` is live and owned by this function.
        unsafe {
            pcre2_set_match_limit_8(match_context.as_ptr(), limits.match_limit);
            pcre2_set_depth_limit_8(match_context.as_ptr(), limits.depth_limit);
            pcre2_set_heap_limit_8(match_context.as_ptr(), limits.heap_limit_kib);
        }
        let capture_count = match pattern_info_u32(code, PCRE2_INFO_CAPTURECOUNT) {
            Ok(n) => n as usize,
            Err(e) => {
                // SAFETY: created above, owned here, freed once on this error path.
                unsafe { pcre2_match_context_free_8(match_context.as_ptr()) };
                return Err(e);
            }
        };
        let names = match read_name_table(code, capture_count) {
            Ok(n) => n,
            Err(e) => {
                // SAFETY: as above.
                unsafe { pcre2_match_context_free_8(match_context.as_ptr()) };
                return Err(e);
            }
        };
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

    /// Runs the interpreter over `haystack`. Returns the group spans on a match, `None` on
    /// no match, and a typed error when a limit trips.
    pub(crate) fn captures(
        &self,
        haystack: &str,
        anchored: bool,
    ) -> Result<Option<Vec<Option<Span>>>, MatchError> {
        let match_data = MatchData::new(self.capture_count)?;
        let mut options = PCRE2_NO_UTF_CHECK;
        if anchored {
            options |= PCRE2_ANCHORED;
        }
        // SAFETY: `haystack` is valid UTF-8 (it is a `&str`), which is what
        // `PCRE2_NO_UTF_CHECK` requires; it is valid for `haystack.len()` bytes for the
        // duration of the call; `code` and `match_context` are live and immutable; the match
        // data block is live and exclusively owned by this call.
        let rc = unsafe {
            pcre2_match_8(
                self.code.as_ptr(),
                haystack.as_ptr(),
                haystack.len(),
                0,
                options,
                match_data.0.as_ptr(),
                self.match_context.as_ptr(),
            )
        };
        if rc == PCRE2_ERROR_NOMATCH {
            return Ok(None);
        }
        if rc < 0 {
            return Err(match_error(rc));
        }
        // SAFETY: the match data block is live; the ovector pointer PCRE2 returns is valid
        // for `2 * ovector_count` `usize`s for as long as the block lives.
        let (ovector, pairs) = unsafe {
            let count = pcre2_get_ovector_count_8(match_data.0.as_ptr()) as usize;
            let ptr = pcre2_get_ovector_pointer_8(match_data.0.as_ptr());
            (std::slice::from_raw_parts(ptr, count * 2), count)
        };
        // `rc` is the highest group number that matched plus one; groups at or beyond it are
        // unset. PCRE2 also marks unset groups inside that range with `PCRE2_UNSET` (`!0`).
        let set_groups = rc as usize;
        let spans = (0..pairs)
            .map(|i| {
                let (start, end) = (ovector[2 * i], ovector[2 * i + 1]);
                if i >= set_groups || start == usize::MAX || end == usize::MAX {
                    None
                } else {
                    Some(Span { start, end })
                }
            })
            .collect();
        Ok(Some(spans))
    }
}

type CInt = ::std::os::raw::c_int;

fn pattern_info_u32(code: NonNull<pcre2_code_8>, what: u32) -> Result<u32, CompileError> {
    let mut out: u32 = 0;
    // SAFETY: `code` is a live compiled pattern; `what` is one of the `PCRE2_INFO_*` items
    // whose result type is `uint32_t`, so `out` is the right size.
    let rc = unsafe { pcre2_pattern_info_8(code.as_ptr(), what, (&mut out as *mut u32).cast()) };
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
fn read_name_table(
    code: NonNull<pcre2_code_8>,
    capture_count: usize,
) -> Result<Vec<Option<String>>, CompileError> {
    let mut names = vec![None; capture_count + 1];
    let name_count = pattern_info_u32(code, PCRE2_INFO_NAMECOUNT)? as usize;
    if name_count == 0 {
        return Ok(names);
    }
    let entry_size = pattern_info_u32(code, PCRE2_INFO_NAMEENTRYSIZE)? as usize;
    let mut table: *const u8 = ptr::null();
    // SAFETY: `code` is live; `PCRE2_INFO_NAMETABLE` writes a `PCRE2_SPTR` into the
    // out-pointer, which is exactly `*const u8` for the 8-bit library.
    let rc = unsafe {
        pcre2_pattern_info_8(
            code.as_ptr(),
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

fn match_error(rc: CInt) -> MatchError {
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
pub(crate) fn error_message(code: CInt) -> String {
    let mut buf = [0u8; 256];
    // SAFETY: `buf` is valid for writes of `buf.len()` bytes, which is the length passed.
    let n = unsafe { pcre2_get_error_message_8(code, buf.as_mut_ptr(), buf.len()) };
    if n < 0 {
        return format!("PCRE2 error {code}");
    }
    String::from_utf8_lossy(&buf[..n as usize]).into_owned()
}
