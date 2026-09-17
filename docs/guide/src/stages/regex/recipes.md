# extract and redact recipes

## Parse, then mask

`redact` sees what `extract` wrote, so both the body and the new attribute can be masked:

```yaml
# messages in
{{#include ../../../examples/stages/regex/parse-then-mask/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/regex/parse-then-mask/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/parse-then-mask/expected.yaml}}
```

Put `redact` after `extract`. The other way round, the pattern for `extract` would have to expect `[ip]`.

## Mask only the secret

`replace` cannot keep part of the match. Use a lookbehind so the match is only the secret:

```yaml
# messages in
{{#include ../../../examples/stages/regex/redact-keep-prefix/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/regex/redact-keep-prefix/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/regex/redact-keep-prefix/expected.yaml}}
```

The lookbehind puts the pattern on PCRE2. The pattern passes the start-up checks, so it loads with the default `on_redos_risk`.
