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
//!   `match_limit`, `depth_limit` and `heap_limit` are deterministic. All three are
//!   per start position: `pcre2_match` resets its counters each time its bump-along loop
//!   advances, so an unanchored non-match over n start positions may cost up to n times
//!   the limit. `Limits::input_bytes` is the bound on that.
//! - Compiled code and the match context are immutable after construction and are shared
//!   across threads. Match data is created per call and never shared.
//! - When `Limits::work_limit` is set the pattern is compiled with `PCRE2_AUTO_CALLOUT` and
//!   a callout counts every pattern item PCRE2 visits, across all start positions, aborting
//!   the call with `MatchError::WorkLimit` when the budget runs out. What that does and
//!   does not bound is documented on [`Limits::work_limit`].
//!
//! Every PCRE2 allocation is owned by an RAII newtype (`Code`, `CompileContext`,
//! `MatchContext`, `MatchData`) so no error path can leak.

#![allow(unsafe_code)]

use std::cell::Cell;
use std::ffi::{c_int, c_void};
use std::num::NonZeroU32;
use std::ptr::{self, NonNull};

use pcre2_sys::{
    PCRE2_ANCHORED, PCRE2_AUTO_CALLOUT, PCRE2_DOLLAR_ENDONLY, PCRE2_ERROR_CALLOUT,
    PCRE2_ERROR_DEPTHLIMIT, PCRE2_ERROR_HEAPLIMIT, PCRE2_ERROR_MATCHLIMIT, PCRE2_ERROR_NOMATCH,
    PCRE2_ERROR_PARENTHESES_NEST_TOO_DEEP, PCRE2_INFO_CAPTURECOUNT, PCRE2_INFO_NAMECOUNT,
    PCRE2_INFO_NAMEENTRYSIZE, PCRE2_INFO_NAMETABLE, PCRE2_NEVER_BACKSLASH_C, PCRE2_NO_UTF_CHECK,
    PCRE2_UCP, PCRE2_UNSET, PCRE2_UTF, pcre2_code_8, pcre2_code_free_8, pcre2_compile_8,
    pcre2_compile_context_8, pcre2_compile_context_create_8, pcre2_compile_context_free_8,
    pcre2_get_error_message_8, pcre2_get_ovector_count_8, pcre2_get_ovector_pointer_8,
    pcre2_match_8, pcre2_match_context_8, pcre2_match_context_create_8, pcre2_match_context_free_8,
    pcre2_match_data_8, pcre2_match_data_create_8, pcre2_match_data_free_8, pcre2_pattern_info_8,
    pcre2_set_depth_limit_8, pcre2_set_heap_limit_8, pcre2_set_match_limit_8,
    pcre2_set_parens_nest_limit_8,
};

use crate::{CompileError, Limits, MatchError, Span};

// `pcre2-sys` does not bind the callout API; the symbol is in the bundled static library.
// The first parameter is really `pcre2_callout_block_8 *`; the wrapper never reads it, so an
// opaque pointer has the same ABI and avoids redeclaring the struct.
unsafe extern "C" {
    unsafe fn pcre2_set_callout_8(
        ctx: *mut pcre2_match_context_8,
        callout: Option<extern "C" fn(*mut c_void, *mut c_void) -> c_int>,
        data: *mut c_void,
    ) -> c_int;
}

thread_local! {
    /// Work budget remaining for the match call running on this thread. The callout is
    /// registered on the shared match context with no data pointer, so the per-call counter
    /// has to live somewhere the callback can reach without an allocation; PCRE2 invokes
    /// callouts on the calling thread, so a thread-local is exactly per call.
    static WORK_REMAINING: Cell<u64> = const { Cell::new(u64::MAX) };
}

/// Auto-callout hook: one call per pattern item PCRE2 visits. Returning a negative value
/// aborts the match with that value; `PCRE2_ERROR_CALLOUT` is the one reserved for callers.
///
/// Nothing in here can unwind into C: the thread-local is const-initialised and has no
/// destructor, so `try_with` only fails during thread teardown, and that case aborts the
/// match rather than panicking.
extern "C" fn count_work(_block: *mut c_void, _data: *mut c_void) -> c_int {
    WORK_REMAINING
        .try_with(|remaining| {
            let left = remaining.get();
            if left == 0 {
                return PCRE2_ERROR_CALLOUT;
            }
            remaining.set(left - 1);
            0
        })
        .unwrap_or(PCRE2_ERROR_CALLOUT)
}

/// A compiled PCRE2 pattern plus the match context carrying its runtime limits.
pub(crate) struct Pcre2Regex {
    code: Code,
    match_context: MatchContext,
    /// Number of capture groups, not counting group 0.
    capture_count: usize,
    /// Total pattern items one match call may visit, when callouts are compiled in.
    work_limit: Option<NonZeroU32>,
}

// SAFETY: PCRE2 documents that a compiled pattern is never modified by matching and can be
// used by several threads at once, and that a match context is only read by
// `pcre2_match`. Neither pointer is mutated after `compile` returns; the callout registered
// on the context is a plain function pointer with no data pointer, and the counter it
// touches is thread-local. Match data (the only mutable per-match state) is created and
// freed inside each `captures` or `is_match` call.
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
        // `max_pattern_length` is not set here: `scan::check_guards` rejects long patterns
        // before PCRE2 sees them, so PCRE2's own check could never fire. The nest limit is
        // set because the scanner can undercount parentheses inside `\Q…\E` and `(?x)`
        // comments, and PCRE2's count is exact.
        // SAFETY: `ctx` is a live compile context owned by this function.
        unsafe {
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
        // SAFETY: `ctx` is live and owned by this function. `count_work` has the ABI PCRE2
        // expects and never dereferences its arguments.
        unsafe {
            pcre2_set_match_limit_8(ctx.as_ptr(), limits.match_limit);
            pcre2_set_depth_limit_8(ctx.as_ptr(), limits.depth_limit);
            pcre2_set_heap_limit_8(ctx.as_ptr(), limits.heap_limit_kib);
            if limits.work_limit.is_some() {
                pcre2_set_callout_8(ctx.as_ptr(), Some(count_work), ptr::null_mut());
            }
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
    /// A block with room for `pairs` offset pairs. PCRE2 needs at least one and allows at
    /// most 65 535 groups, so the conversion cannot fail in practice; saturating keeps the
    /// signature honest without inventing an error.
    fn new(pairs: usize) -> Result<Self, MatchError> {
        let pairs = u32::try_from(pairs.max(1)).unwrap_or(u32::MAX);
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
        let mut options = PCRE2_UTF | PCRE2_UCP | PCRE2_NEVER_BACKSLASH_C | PCRE2_DOLLAR_ENDONLY;
        if limits.work_limit.is_some() {
            options |= PCRE2_AUTO_CALLOUT;
        }
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
            // The scanner in `scan::check_guards` undercounts parentheses inside `\Q…\E` and
            // `(?x)` comments; when PCRE2's exact count trips instead, report the same
            // variant so callers see one shape for one limit.
            if error_code == PCRE2_ERROR_PARENTHESES_NEST_TOO_DEEP as c_int {
                return Err(CompileError::ParensTooDeep {
                    limit: limits.parens_nest_limit,
                    offset: error_offset,
                });
            }
            return Err(CompileError::Syntax {
                engine: crate::Engine::Backtracking,
                code: Some(error_code),
                message: error_message(error_code),
                offset: error_offset,
            });
        };
        let match_context = MatchContext::new(limits)?;
        let capture_count = pattern_info_u32(&code, U32Info::CaptureCount)? as usize;
        Ok(Self {
            code,
            match_context,
            capture_count,
            work_limit: limits.work_limit,
        })
    }

    /// Bytes of JIT code attached to the pattern. Always zero: the wrapper never compiles
    /// it. Exposed for the unit test that pins that.
    #[cfg(test)]
    fn jit_size(&self) -> Result<usize, CompileError> {
        let mut out: usize = 0;
        // SAFETY: `code` is live; `PCRE2_INFO_JITSIZE` writes a `size_t`, which `out` is.
        let rc = unsafe {
            pcre2_pattern_info_8(
                self.code.0.as_ptr(),
                pcre2_sys::PCRE2_INFO_JITSIZE,
                (&mut out as *mut usize).cast(),
            )
        };
        if rc != 0 {
            return Err(internal(rc));
        }
        Ok(out)
    }

    /// Group name by group index, read from PCRE2's name table. Index 0 is the whole match
    /// and has no name.
    pub(crate) fn capture_names(&self) -> Result<Vec<Option<String>>, CompileError> {
        read_name_table(&self.code, self.capture_count)
    }

    /// Whether the pattern matches, without recording group spans.
    pub(crate) fn is_match(&self, haystack: &str) -> Result<bool, MatchError> {
        self.probe(haystack, false, self.work_limit)
    }

    /// [`Pcre2Regex::is_match`] for the canary: optionally anchored at the start of the
    /// input, under its own work budget. The budget only counts when the pattern was
    /// compiled with `work_limit` set, which is what compiles the callouts in.
    pub(crate) fn probe(
        &self,
        haystack: &str,
        anchored: bool,
        work_limit: Option<NonZeroU32>,
    ) -> Result<bool, MatchError> {
        Ok(self.exec(haystack, anchored, work_limit, 1)?.is_some())
    }

    /// Runs the interpreter over `haystack`. Returns the group spans on a match, `None` on
    /// no match, and a typed error when a limit trips.
    pub(crate) fn captures(&self, haystack: &str) -> Result<Option<Vec<Option<Span>>>, MatchError> {
        let Some((match_data, rc)) =
            self.exec(haystack, false, self.work_limit, self.capture_count + 1)?
        else {
            return Ok(None);
        };
        // SAFETY: the match data block is live; the ovector pointer PCRE2 returns is valid
        // for `2 * ovector_count` `usize`s for as long as the block lives, and the slice is
        // dropped before `match_data` is.
        let ovector = unsafe {
            let count = pcre2_get_ovector_count_8(match_data.0.as_ptr()) as usize;
            let ptr = pcre2_get_ovector_pointer_8(match_data.0.as_ptr());
            std::slice::from_raw_parts(ptr, count * 2)
        };
        // `rc` is the highest group number that matched plus one; groups at or beyond it are
        // unset. PCRE2 also marks unset groups inside that range with `PCRE2_UNSET`. Zero
        // means the ovector was too small, which `MatchData::new(capture_count + 1)` rules
        // out; treat it as the wrapper bug it would be rather than as a match.
        if rc == 0 {
            return Err(MatchError::Engine {
                code: 0,
                message: "match data block too small for the pattern's groups".to_owned(),
            });
        }
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

    /// One `pcre2_match` call with a match data block of `pairs` offset pairs. `None` on no
    /// match; on a match, the block and PCRE2's return code (the highest group number that
    /// matched plus one).
    fn exec(
        &self,
        haystack: &str,
        anchored: bool,
        work_limit: Option<NonZeroU32>,
        pairs: usize,
    ) -> Result<Option<(MatchData, c_int)>, MatchError> {
        let match_data = MatchData::new(pairs)?;
        let mut options = PCRE2_NO_UTF_CHECK;
        if anchored {
            options |= PCRE2_ANCHORED;
        }
        if let Some(budget) = work_limit {
            WORK_REMAINING.with(|remaining| remaining.set(u64::from(budget.get())));
        }
        // SAFETY: `haystack` is valid UTF-8 (it is a `&str`), which is what
        // `PCRE2_NO_UTF_CHECK` requires; it is valid for `haystack.len()` bytes for the
        // duration of the call; `code` and `match_context` are live and immutable; the match
        // data block is live and exclusively owned by this call.
        let rc = unsafe {
            pcre2_match_8(
                self.code.0.as_ptr(),
                haystack.as_ptr(),
                haystack.len(),
                0,
                options,
                match_data.0.as_ptr(),
                self.match_context.0.as_ptr(),
            )
        };
        if rc == PCRE2_ERROR_NOMATCH {
            return Ok(None);
        }
        if rc < 0 {
            return Err(match_error(rc));
        }
        Ok(Some((match_data, rc)))
    }
}

/// The `PCRE2_INFO_*` items whose result is a `uint32_t`. Restricting the argument to this
/// enum is what makes `pattern_info_u32` safe: a pointer-valued item would overrun `out`.
#[derive(Clone, Copy)]
enum U32Info {
    CaptureCount,
    NameCount,
    NameEntrySize,
}

impl U32Info {
    fn code(self) -> u32 {
        match self {
            Self::CaptureCount => PCRE2_INFO_CAPTURECOUNT,
            Self::NameCount => PCRE2_INFO_NAMECOUNT,
            Self::NameEntrySize => PCRE2_INFO_NAMEENTRYSIZE,
        }
    }
}

fn pattern_info_u32(code: &Code, what: U32Info) -> Result<u32, CompileError> {
    let mut out: u32 = 0;
    // SAFETY: `code` is a live compiled pattern; every `U32Info` item has a `uint32_t`
    // result, so `out` is the right size for the write.
    let rc = unsafe {
        pcre2_pattern_info_8(code.0.as_ptr(), what.code(), (&mut out as *mut u32).cast())
    };
    if rc != 0 {
        return Err(internal(rc));
    }
    Ok(out)
}

/// Reads PCRE2's name table into a `Vec` indexed by group number.
///
/// The table is `name_count` entries of `entry_size` bytes each: a big-endian `u16` group
/// number followed by the NUL-terminated name, padded to `entry_size`.
fn read_name_table(code: &Code, capture_count: usize) -> Result<Vec<Option<String>>, CompileError> {
    let mut names = vec![None; capture_count + 1];
    let name_count = pattern_info_u32(code, U32Info::NameCount)? as usize;
    if name_count == 0 {
        return Ok(names);
    }
    let entry_size = pattern_info_u32(code, U32Info::NameEntrySize)? as usize;
    if entry_size < 3 {
        // Code 0 marks a check the wrapper made itself; PCRE2 codes are never zero.
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
        return Err(internal(rc));
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
        PCRE2_ERROR_CALLOUT => MatchError::WorkLimit,
        other => MatchError::Engine {
            code: other,
            message: error_message(other),
        },
    }
}

/// A PCRE2 error outside its documented syntax and limit errors.
fn internal(code: c_int) -> CompileError {
    CompileError::Internal {
        code,
        message: error_message(code),
    }
}

/// Renders a PCRE2 error code through `pcre2_get_error_message`.
fn error_message(code: c_int) -> String {
    let mut buf = [0u8; 256];
    // SAFETY: `buf` is valid for writes of `buf.len()` bytes, which is the length passed.
    let n = unsafe { pcre2_get_error_message_8(code, buf.as_mut_ptr(), buf.len()) };
    let Ok(len) = usize::try_from(n) else {
        return format!("PCRE2 error {code}");
    };
    String::from_utf8_lossy(&buf[..len.min(buf.len())]).into_owned()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn compiled_pattern_carries_no_jit_code() {
        let re = Pcre2Regex::compile(r"(?<=x)(?<n>\d+)", &Limits::default()).unwrap();
        assert_eq!(re.jit_size().unwrap(), 0);
    }
}
