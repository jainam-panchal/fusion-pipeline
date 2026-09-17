# extract and redact

Both stages run a regular expression on text fields.

- [`extract`](extract.md) reads one field and writes the parts it finds into `attributes`.
- [`redact`](redact.md) replaces every match in one or more fields with fixed text.

```yaml
- id: parse
  type: extract
  field: body
  pattern: '^(?<level>[A-Z]+): (?<message>.+)$'

- id: mask_ips
  type: redact
  fields: [body, attributes.message]
  pattern: '\d+\.\d+\.\d+\.\d+'
  replace: '[ip]'
```

Both take `limits` and `on_redos_risk`. The pipeline runs each pattern on a fast engine when it can, and on PCRE2 for lookaround and the like. It checks every pattern when it starts, and drops a record that goes over a limit with the reason `regex_limit`. [Regex limits](../../regex-limits.md) has the details.

## Writing patterns in YAML

Put patterns in single quotes. YAML leaves a single-quoted string alone, so `\d` stays `\d`. To write a `'` inside, double it: `''`.

In double quotes, YAML reads backslashes itself, so `"\d"` is an error and `"\\d"` is needed.

## Pages

- [extract](extract.md)
- [redact](redact.md)
- [Recipes](recipes.md): parsing and masking together, and masking only the secret after a label.
- [Errors](errors.md): what the pipeline refuses.
