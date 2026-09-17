# Regex limits

Patterns appear in `extract`, `redact`, and the `=~` and `!~` operators of `filter` and `route`. All four take the same two keys to keep a pattern from running away:

```yaml
limits:
  input_bytes: 65536     # longest text a pattern runs on
  match: 1000000
  depth: 1000000
  heap_kib: 20000
  work: 10000000
on_redos_risk: reject    # or warn
```

Every key is optional. The values above are the defaults.

## Which engine runs a pattern

The pipeline tries each pattern on a fast engine first, the Rust `regex` crate. It runs in time that grows with the length of the text and cannot get stuck. Most patterns run there, and the metrics call it `linear`.

Some patterns need PCRE2: those with syntax the fast engine lacks, and the rare pattern too large for it. The syntax is lookahead and lookbehind (`(?=`, `(?!`, `(?<=`, `(?<!`), backreferences such as `\1`, atomic groups, possessive quantifiers and recursion. Those patterns run on PCRE2, called `backtracking` in the metrics. PCRE2 can be slow on some inputs, which is what most of the limits are for.

When it starts, the pipeline prints each pattern to standard error, and one line per node with its engine. A node with several patterns gets the slowest engine among them.

## The limits

| Key | Default | Engine | What it limits |
|---|---|---|---|
| `input_bytes` | 65536 | both | the longest text the pattern runs on |
| `match` | 1000000 | PCRE2 | backtracking steps from each start position |
| `depth` | 1000000 | PCRE2 | how deep backtracking can nest |
| `heap_kib` | 20000 | PCRE2 | memory for backtracking, in KiB |
| `work` | 10000000 | PCRE2 | total work in one call, over all start positions. `0` turns it off. |

When a record goes over a limit, the pipeline drops it with the reason `regex_limit` and acks the message. The next record is served as usual. A `filter` or `route` drops the record whatever its `action` or labels say.

```yaml
# messages in
{{#include ../examples/regex/input-bytes/input.yaml}}
```

```yaml
# config
{{#include ../examples/regex/input-bytes/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/regex/input-bytes/expected.yaml}}
```

The second body is 17 bytes, over the limit of 9, so it is dropped. For `redact`, the limit applies to each field.

Two more limits are fixed: a pattern may be at most 8192 bytes long, and nest parentheses at most 250 deep.

## Checks at start

The pipeline checks every pattern when it starts:

- It looks for shapes that are known to be slow, such as `(a+)+` (a repeat inside a repeat), `(a|aa)*` (overlapping choices under a repeat) and `\w*\d+` (two overlapping repeats in a row). This runs for every pattern.
- For PCRE2 patterns, it also runs the pattern on generated hostile inputs of up to 64 KiB (or `input_bytes`, if smaller), within the node's `match` limit and a work budget of its own. This is called the canary.

`on_redos_risk` decides what a finding does:

- `reject` (the default): the pipeline does not start.
- `warn`: the pattern loads, and the finding is printed to standard error.

The shape check can flag a pattern that is fine in practice. Rewrite the pattern if you can, and use `warn` when you have checked it.

```yaml
# messages in
{{#include ../examples/regex/canary-warn/input.yaml}}
```

```yaml
# config
{{#include ../examples/regex/canary-warn/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/regex/canary-warn/expected.yaml}}
```

## What the pipeline refuses

A risky shape:

```yaml
# config
{{#include ../examples/regex/rejected-lint/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/regex/rejected-lint/expected.yaml}}
```

Fix: remove the repeat inside the repeat.

A canary trip:

```yaml
# config
{{#include ../examples/regex/rejected-canary/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/regex/rejected-canary/expected.yaml}}
```

Fix: rewrite the pattern, or set `on_redos_risk: warn` as above.

A pattern neither engine can read:

```yaml
# config
{{#include ../examples/regex/rejected-syntax/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/regex/rejected-syntax/expected.yaml}}
```

A pattern that neither engine can read is always reported by PCRE2, so the message says `backtracking engine`. The offset is inside the pattern.

A key `limits` does not know:

```yaml
# config
{{#include ../examples/regex/rejected-limit-key/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/regex/rejected-limit-key/expected.yaml}}
```

Every pattern message, after the node id and the setting:

| Message | Fix |
|---|---|
| `backtracking engine: <reason> at offset <n>` | Fix the pattern syntax. |
| `ReDoS risk: <shape> at offset <n>` | Rewrite the pattern, or use `on_redos_risk: warn`. |
| `canary tripped: <reason>` | Rewrite the pattern, or use `on_redos_risk: warn`. |
| `pattern is <n> bytes, longer than the 8192 byte limit (offset <n>)` | Shorten the pattern. |
| `parentheses nest deeper than 250 at offset <n>` | Nest less. |
| `PCRE2 allocation failed` or `PCRE2 internal error <code>: <reason>` | Shorten the pattern. |
| `on_redos_risk` other than `reject` or `warn` | ``unknown variant `<value>`, expected `reject` or `warn` `` |
