# BUG: CLI `-in` rejects the `#id` tagged literal that query text accepts

**Status**: Open (2026-07-22). Found while probing minted-identity rendering during a downstream validation pass at `70474ea`: the same seed-string identity is expressible inline in query text but not as a CLI input, so any parameterized query over a seed-constructed entity has to fall back to `#identity` with a hand-computed hash.

## Reproduction

Tree-built CLI, any database:

```
datalog -db <any> \
  -query '[:find ?v :in $ ?x :where [(identity ?x) ?v]]' \
  -in '#id "r0c0"'
```

Result:

```
Input error: input 1 ("#id \"r0c0\""): unsupported tagged literal: #id
```

The same literal in query text works — and shows the parse-time rewrite doing its job (observed at `70474ea`; the CLI echoes the parsed query):

```
-query '[:find ?v :where [(identity #id "r0c0") ?v]]'
```

```
Query:
[:find ?v
 :where [(identity #identity "NBLBAL:-$;t>Q1;ScuSvltgwt") ?v]]
```

Observed rejecting on both `331ba214` and `70474ea` CLIs; on pins where `#id` exists at all (introduced `ed2ebc5`), query text accepts it and `-in` does not.

## Expected

`-in` values are documented as EDN-parsed with tagged literals supported (`#identity`, `#inst`, `#bytes` all work through `-in` today). `#id` is part of the same tagged-literal vocabulary — `NewIdentity` in literal form, hashed at parse time — and should parse identically through both entry points. The gap is one missing case in the CLI's `-in` tagged-literal handling, not an engine issue.

## Notes

Sibling report in the same conversion layer: `BUG_CLI_RELATION_INPUT_EDN_REJECTED.md` (relation-shaped `-in` values rejected). Both are the CLI's EDN-to-input conversion lagging the query parser's vocabulary; a shared fix pass over `cmd/datalog`'s input binding could close both, with red-first CLI subprocess tests per literal kind.
