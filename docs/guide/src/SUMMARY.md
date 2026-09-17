# Summary

[How to read this guide](introduction.md)

# Start here

- [Getting started](getting-started.md)
- [Concepts](concepts.md)
- [Writing a config](writing-a-config.md)

# Shared rules

- [Field paths](field-paths.md)
- [Conditions](conditions.md)
- [Regex limits](regex-limits.md)
- [State and failure policy](state-and-failure.md)

# Stages

- [filter](stages/filter.md)
- [route](stages/route.md)
- [dedupe](stages/dedupe.md)
- [edit](stages/edit/README.md)
  - [Ops](stages/edit/ops.md)
  - [Write rules](stages/edit/write-rules.md)
  - [Recipes](stages/edit/recipes.md)
  - [Errors](stages/edit/errors.md)
- [sample](stages/sample/README.md)
  - [random](stages/sample/random.md)
  - [every_nth](stages/sample/every-nth.md)
  - [consistent](stages/sample/consistent.md)
  - [Errors](stages/sample/errors.md)
- [extract and redact](stages/regex/README.md)
  - [extract](stages/regex/extract.md)
  - [redact](stages/regex/redact.md)
  - [Recipes](stages/regex/recipes.md)
  - [Errors](stages/regex/errors.md)
- [lua](stages/lua/README.md)
  - [Script API](stages/lua/script-api.md)
  - [State API](stages/lua/state-api.md)
  - [Budgets and errors](stages/lua/budgets-and-errors.md)
  - [Recipes](stages/lua/recipes.md)

# Running it

- [NATS](nats.md)
- [End to end](end-to-end.md)
- [Limits and guarantees](limits.md)
- [Troubleshooting](troubleshooting.md)
