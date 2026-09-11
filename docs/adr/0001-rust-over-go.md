---
status: accepted
date: 2026-09-08
---

# Rust over Go, decided by Lua embedding

The pipeline needs a sandboxed Lua stage: a per-node `process(record)` function under an instruction budget and a memory cap, with the record exposed as a plain table. That requirement, not raw throughput, decided the language. Rust with `mlua` (`lua54`, `vendored`) embeds real Lua 5.4 in-process and exposes `set_memory_limit` and `HookTriggers::every_nth_instruction`, which are exactly the two guardrails the operator stories ask for. Go had two options and neither met the bar: `gopher-lua` is a pure-Go reimplementation of Lua 5.1, so scripts get a dialect that is a decade behind and an interpreter measurably slower than the reference one; a cgo binding to real Lua pays a cgo crossing on every field access a script makes on the record table, which is the hot path of the stage. Rust was chosen on that evidence and is recorded here so the choice is documented rather than believed.

## Consequences

- Stages are synchronous and each worker OS thread owns one Lua VM. Tokio is used only for NATS I/O.
- Sandboxing is manual global stripping (`os`, `io`, `package`, `require`, `load`, `debug`); `mlua::Lua::sandbox` is Luau-only and does not apply.
- No LuaJIT: the instruction hook and memory limit are only reliable on the reference interpreter.
- The workspace carries C through `pcre2-sys` and `mlua`'s vendored Lua, which is why Miri cannot run the regex tests and AddressSanitizer on nightly is used instead.
