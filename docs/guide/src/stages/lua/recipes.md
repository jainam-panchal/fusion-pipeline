# lua recipes

Small scripts to start from. Each one is a tested example.

## Add a field from Meta

```yaml
# messages in
{{#include ../../../examples/stages/lua/tag-with-meta/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/tag-with-meta/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/tag-with-meta/expected.yaml}}
```

## One record per line

```yaml
# messages in
{{#include ../../../examples/stages/lua/split-lines/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/split-lines/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/split-lines/expected.yaml}}
```

A body with no text lines would give an empty list, which is an error. Check for it and return `record` in that case, as `deploy/pipeline.yaml` does.

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

## Count records per body

```yaml
# messages in
{{#include ../../../examples/stages/lua/state-counter/input.yaml}}
```

```yaml
# config
{{#include ../../../examples/stages/lua/state-counter/pipeline.yaml}}
```

```yaml
# result
{{#include ../../../examples/stages/lua/state-counter/expected.yaml}}
```
