# lua

`lua` runs a Lua 5.4 script on each record. Use it when no other stage does what you need.

```yaml
- id: tag
  type: lua
  source: |
    function process(record, meta)
      record.attributes["tenant"] = meta.tenant
      return record
    end
```

| Key | Default | What it does |
|---|---|---|
| `source` | | The script, written in the config. |
| `script` | | A path to a script file. Give `source` or `script`, not both. |
| `limits` | see below | `instructions`, `memory_kib` and `output_kib`. See [Budgets and errors](budgets-and-errors.md). |
| `on_error` | `pass` | What happens to a record when the script fails: `pass`, `drop` or `nak`. |
| `on_state_error` | `nak` | What happens when a `state` call gets no answer from Dragonfly: `nak` or `pass`. Only allowed when the script uses `state`. |

A `script` path is read from the directory the pipeline runs in, not the directory of the config file ([issue #41](https://github.com/jainam-panchal/fusion-pipeline/issues/41)). Use `source` or an absolute path to be safe.

## The script

The script defines `process(record, meta)`. The pipeline calls it once per record:

- `record` is the record as a Lua table. Change it and return it to pass it on.
- `meta` holds the record's Meta: `id`, `tenant`, `ingestion_time` and `delivery_count`. It is read-only.

What `process` returns decides what happens:

| Return | Result |
|---|---|
| a record table | the record goes on |
| `nil` | the record is dropped, reason `lua_drop` |
| a list of record tables | each one goes on as its own record |
| anything else | an error, handled by `on_error` |

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

The code outside `process` runs once in each worker, before its first record, so it is the place for setup. The pipeline also runs it once at start to check the script.

## Pages

- [Script API](script-api.md): the record table, `meta`, return values and everything a script can call.
- [State API](state-api.md): storing values in Dragonfly.
- [Budgets and errors](budgets-and-errors.md): limits, `on_error` and what the pipeline refuses.
- [Recipes](recipes.md): small scripts to start from.
