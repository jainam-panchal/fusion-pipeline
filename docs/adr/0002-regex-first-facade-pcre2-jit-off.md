---
status: accepted
date: 2026-09-09
---

# Linear-first regex facade, PCRE2 without JIT, three load-time ReDoS checks

User-supplied patterns run against untrusted log lines on shared workers, so one catastrophic pattern must not pin a worker. We chose a two-engine facade in our own crate: every pattern compiles first on the Rust `regex` crate, which is linear-time by construction and cannot backtrack; only syntax it rejects (lookaround, backreferences, atomic and possessive groups, recursion) falls back to PCRE2 10.46 through `pcre2-sys`, with all `unsafe` confined to that half. Alternatives were `regex` alone (rejects the syslog and OpenSSH patterns operators actually write), the safe `pcre2` crate (does not expose `match_limit` or `depth_limit`; a fork and an open upstream PR exist, and we chose to own the wrapper rather than wait), and `irgx` (same linear-first shape but an opaque prebuilt Zig archive with no static analysis). PCRE2 JIT is never compiled. On the interpreter path `match_limit`, `depth_limit` and `heap_limit` are deterministic; under JIT the depth limit does not apply and the heap limit behaves differently, so a limit that trips reliably in tests could fail to trip in production. A unit test asserts `PCRE2_INFO_JITSIZE` is zero on every compiled pattern.

Because no sound static ReDoS analyser exists for Rust, the substitute is three layers at config load: classification (a pattern is `linear` or `backtracking`, and the engine is a metric label), a structural lint over the `regex-syntax` AST for nested unbounded quantifiers, overlapping alternation under repetition and overlapping unbounded quantifier pairs, and a canary that runs PCRE2-bound patterns against generated adversarial inputs at 1, 8 and 64 KiB under the runtime limits and a per-byte work budget. Runtime limits still apply per record regardless.

## Consequences

- Measured before the wrapper was written: PCRE2 resets `match_limit` at every start position, so an unanchored non-match whose leading group loop restarts everywhere (`(?:a|b)*(?=c)`) is O(n²) and trips no standard limit (1.9 s at 8 KiB). A fifth limit, `work`, compiles with `PCRE2_AUTO_CALLOUT` and counts pattern items across the whole call. It costs 1.5–1.8× on the PCRE2 path and is on by default (10 000 000).
- Single-character repeats loop inside one pattern item and are not counted by `work`, so an unanchored PCRE2-only pattern built from them is bounded only by `input_bytes` (default 64 KiB): seconds, not minutes. This is a known gap, recorded in the spec against story 13, and is the reason the default input cap is small.
- The lint runs on every pattern regardless of engine because it describes the pattern's shape, which survives an engine change. The canary runs only for patterns that land on PCRE2, because it measures PCRE2.
- `pcre2-sys` does not bind `pcre2_set_callout_8`; the wrapper declares it by hand against the bundled static library. An upgrade of `pcre2-sys` must re-check that declaration.
