# Conditions

`filter` and `route` decide with a condition:

```yaml
condition: severity_text == "ERROR" and attributes.http.status >= 500
```

A condition compares a [field path](field-paths.md) on the left with a value on the right. Combine comparisons with `and`, `or`, `not` and parentheses.

## Values

- Text in double or single quotes: `"ERROR"`, `'ERROR'`. Unquoted text is an error.
- Numbers: `500`, `-1`, `0.5`, `1e3`.
- `true`, `false` and `null`.

The left side is always a path and the right side always a value. Two fields cannot be compared with each other.

## Operators

| Operator | True when |
|---|---|
| `==` | the field equals the value, with the same type |
| `!=` | `==` is false |
| `<`, `>`, `<=`, `>=` | both are numbers, or both are text (compared byte by byte), and the order holds |
| `=~` | the field is text and the pattern matches somewhere in it |
| `!~` | `=~` is false |

`and`, `or`, `not`, `true`, `false` and `null` are lowercase.

## And, or, not

`not` binds tightest, then `and`, then `or`. So `a or b and c` means `a or (b and c)`. Add parentheses when in doubt.

```yaml
# messages in
{{#include ../examples/conditions/precedence/input.yaml}}
```

```yaml
# config
{{#include ../examples/conditions/precedence/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/conditions/precedence/expected.yaml}}
```

Record 2 is `WARN` without `retry`, so neither side is true. `and` and `or` stop as soon as the answer is known.

## Types must match

`503` and `"503"` are different values. A comparison between different types is false, so `!=` is true:

```yaml
# messages in
{{#include ../examples/conditions/type-mismatch/input.yaml}}
```

```yaml
# config
{{#include ../examples/conditions/type-mismatch/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/conditions/type-mismatch/expected.yaml}}
```

The same goes for `<` and the other order operators: comparing a number field with `"18"` is always false. Check how your producer sends numbers.

## Missing fields

A missing field and a JSON `null` both equal `null`. Any other comparison with a missing field is false, so `!=` against a value is true.

```yaml
# messages in
{{#include ../examples/conditions/missing-field/input.yaml}}
```

```yaml
# config
{{#include ../examples/conditions/missing-field/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/conditions/missing-field/expected.yaml}}
```

A field holding an object or a list, such as a structured `body`, equals nothing.

## Patterns

`=~` searches the text. It matches if the pattern fits anywhere, so `"disk"` matches `no disk here`. A field that is not text never matches, so `!~` is true for it:

```yaml
# messages in
{{#include ../examples/conditions/regex-search/input.yaml}}
```

```yaml
# config
{{#include ../examples/conditions/regex-search/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/conditions/regex-search/expected.yaml}}
```

Add `^` or `$` to match at the start or end:

```yaml
# messages in
{{#include ../examples/conditions/regex-anchor/input.yaml}}
```

```yaml
# config
{{#include ../examples/conditions/regex-anchor/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/conditions/regex-anchor/expected.yaml}}
```

Inside the quotes, a backslash is taken off every character except `n`, `t` and `r`. So `"\d+"` reaches the pattern as `d+`. Write `"\\d+"`:

```yaml
# messages in
{{#include ../examples/conditions/backslash/input.yaml}}
```

```yaml
# config
{{#include ../examples/conditions/backslash/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/conditions/backslash/expected.yaml}}
```

This works in YAML without quotes around the condition. Patterns follow the rules on [Regex limits](regex-limits.md), and `filter` and `route` take its `limits` and `on_redos_risk` keys.

## YAML quoting

Most conditions need no YAML quotes, even with `"` inside. Wrap a condition in single quotes when it holds `: ` or ` #`, which YAML reads as its own syntax.

## What the pipeline refuses

```yaml
# config
{{#include ../examples/conditions/rejected-bare-word/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/conditions/rejected-bare-word/expected.yaml}}
```

Put text in quotes: `severity_text == "ERROR"`.

```yaml
# config
{{#include ../examples/conditions/rejected-single-equals/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/conditions/rejected-single-equals/expected.yaml}}
```

Use `==`.

```yaml
# config
{{#include ../examples/conditions/rejected-regex-number/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/conditions/rejected-regex-number/expected.yaml}}
```

Quote the pattern: `body =~ "42"`.

```yaml
# config
{{#include ../examples/conditions/rejected-open-string/pipeline.yaml}}
```

```yaml
# result
{{#include ../examples/conditions/rejected-open-string/expected.yaml}}
```

Close the quote.

Every message, after ``node `<id>`: condition `<text>`: `` (in a route, ``node `<id>`: route `<label>` `<text>`: ``). For a pattern error, the text in backticks is the pattern alone:

| Message | Fix |
|---|---|
| `` unexpected character `<c>` at offset <n> `` | Remove the character, or use a listed operator. |
| `unexpected end of expression` | Finish the comparison, or close the parenthesis. A path alone is not a condition. |
| `` unexpected `<token>` at offset <n> `` | Quote text values. Write `and`/`or` in lowercase. Quote path segments with spaces. |
| `unterminated string starting at offset <n>` | Close the quote. |
| `` invalid number `<text>` at offset <n> `` | Fix the number. |
| `` `=~` and `!~` take a quoted pattern (at offset <n>) `` | Quote the pattern. |
| a path message, then `(path at offset <n>)` | See [Field paths](field-paths.md#what-the-pipeline-refuses). |
| a pattern message | See [Regex limits](regex-limits.md#what-the-pipeline-refuses). |

Offsets count bytes from the start of the condition, starting at 0.
