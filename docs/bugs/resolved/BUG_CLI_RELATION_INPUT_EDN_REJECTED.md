# BUG: CLI `-in` rejects relation-shaped EDN inputs

**Status**: ✅ RESOLVED (2026-07-22). The suspected mechanism below was wrong on both counts: the CLI's conversion is correct (uniform `[]interface{}` at every nesting level, exactly what EDN should produce), and the refusal was the **engine's** — the RelationInput arm of the input admission in `storage/database.go` is the only arm doing nested reflection, and indexing a `[]interface{}` yields a `reflect.Value` of Kind `Interface`, not `Slice`, so the inner kind check rejected interface-wrapped rows while concrete `[][]interface{}` passed. The fix unwraps `reflect.Interface` elements (guarded against nil, which must stay wrapped so the error path never calls `Interface()` on the zero Value) before the kind check — which also admits any Go caller passing `[]any{[]any{...}}`, not just the CLI. Pinned by `TestRelationInputAcceptsInterfaceWrappedRows` (engine, interface-wrapped rows with the concrete shape staying green) and `TestCLI_QueryWithRelationInput` (CLI subprocess), both mode-matrixed, red-first.

**Original report follows** (its "Mechanism (suspected, unverified)" and fix-location note are superseded by the resolution above).

**Status at filing**: Open (2026-07-22). Found while empirically verifying the plan-IR fragment shapes for `docs/wip/CORRECT_BY_CONSTRUCTION_PLANS.md`. The engine executes relation inputs correctly (the executor test suites construct them directly); the defect is in the CLI's EDN-to-input conversion.

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
