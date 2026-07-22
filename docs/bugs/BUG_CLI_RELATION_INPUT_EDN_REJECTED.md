# BUG: CLI `-in` rejects relation-shaped EDN inputs

**Status**: Open (2026-07-22). Found while empirically verifying the plan-IR fragment shapes for `docs/wip/CORRECT_BY_CONSTRUCTION_PLANS.md`. The engine executes relation inputs correctly (the executor test suites construct them directly); the defect is in the CLI's EDN-to-input conversion.

## Reproduction

Tree-built CLI, any database (an empty `.edn` temp database suffices):

```
datalog -db empty.edn \
  -query '[:find ?e ?outer :in $ [[?e] ...] [[?outer] ...] :where (not-join [?e ?outer] [?x :entity/name ?e] [?x :entity/flag ?outer])]' \
  -in '[["a"] ["b"]]' -in '[["hot"]]'
```

Result:

```
Execution error: expected slice for relation tuple 0, got []interface {}
```

The same query shape with collection inputs (`[?e ...]` / `-in '["a" "b"]'`) executes correctly, so the failure is specific to the relation-input (`[[?e] ...]`) conversion path.

## Mechanism (suspected, unverified)

The EDN parser produces `[]interface{}` for vectors at every nesting level; the error message names the received type as `[]interface{}` while claiming to expect a slice — so the conversion layer is type-switching on a different concrete slice type (or rejecting before unwrapping the outer vector) rather than accepting the parser's uniform representation. The suspect is the CLI's `-in` value conversion for `RelationInput` bindings; the executor's own `BindQueryInputs` handles relation inputs correctly when handed relations directly.

## Expected

`-in` values are documented as EDN-parsed, one per `:in` binding. A relation input should accept a vector of tuple vectors, converting each inner vector to one tuple — mirroring how collection inputs already convert.

## Notes

Valid input, wrong layer refusing it — extend the conversion, not the caller. Fix belongs in `cmd/datalog`'s input binding; a red-first reproducer should pin the conversion (CLI subprocess test alongside the existing `-in` tests, both `-optimize` modes per the matrix).
