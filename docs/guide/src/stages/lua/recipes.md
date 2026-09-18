# lua recipes

Small scripts to start from. Each one is a tested example.

## Parse key=value pairs

```yaml
# messages in
{{#include ../../../examples/stages/lua/kv-pairs/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/kv-pairs/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/kv-pairs/expected.yaml}}
```

Values come out as text. `string.gmatch` uses Lua patterns, not regular expressions. For a regular expression, use [`extract`](../regex/extract.md).

## Shorten long bodies

```yaml
# messages in
{{#include ../../../examples/stages/lua/truncate/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/truncate/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/truncate/expected.yaml}}
```

`#` counts bytes, and `string.sub` cuts by bytes, so a cut can split a multi-byte character. Use the `utf8` library to cut by characters.

## Status class from a status code

```yaml
# messages in
{{#include ../../../examples/stages/lua/on-error-pass/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/on-error-pass/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/on-error-pass/expected.yaml}}
```

`on_error: pass` lets records with a bad status go on unchanged.

## Pass only the first of each body

```yaml
# messages in
{{#include ../../../examples/stages/lua/first-seen/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/first-seen/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/first-seen/expected.yaml}}
```

The key expires after 60 seconds, so the same body passes again after that. The [`dedupe`](../dedupe.md) stage does this without a script.

More scripts are on the other pages: [adding a field from Meta](README.md#the-script), [one record per line](script-api.md#return-values) and [a counter](state-api.md).
