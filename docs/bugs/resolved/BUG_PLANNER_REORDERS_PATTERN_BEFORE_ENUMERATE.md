# Bug: Planner Reorders Data Pattern Before Enumerate Expression

**Status:** Fixed **Fix:** `datalog/planner/clause_utils.go` — `patternDependsOnPendingExpression`, `datalog/planner/clause_phasing.go` — deferral check in `selectPhaseClauses` **Repro:** `TestPlannerReordersDataPatternBeforeEnumerate` in `datalog/storage/crdt_vector_test.go` **Diagnostic test:** `TestVectorEnumerateRefWithJoinsAndFilter` (same file, multi-step narrowing)

## Summary

The greedy clause planner reorders data patterns before enumerate expressions that provide variables those patterns need. This causes incorrect query results: a cross-product join on the wrong symbol instead of a filtered join on the enumerate-provided variable.

## Reproduction

Two containers in the same room. Container A holds a red item, container B holds a blue item. Query asks: "which containers in this room have a red item?"

```clojure
[:find ?name ?label
 :in $ ?room ?color
 :where
 [?c :container/room ?room]
 [?c :container/name ?name]
 [?c :container/items ?vec]
 [(enumerate ?vec) [?idx ?item]]   ; provides ?item
 [?item :item/color ?color]        ; needs ?item from enumerate
 [?item :item/label ?label]]
```

**Expected:** `[["A" "Apple"]]` -- only container A has a red item. **Actual:** `[["A" "Apple"] ["B" "Apple"]]` -- both containers appear with the red item's label.

## Root Cause

The planner's greedy clause selection (`selectPhaseClauses` in `planner/clause_phasing.go`) picks clauses by score. The scoring function (`scoreClause` in `planner/clause_utils.go`) gives data patterns a massive advantage over expressions:

| Clause | Score | Breakdown |
|--------|-------|-----------|
| `[?item :item/color ?color]` | **210** | 100 (data pattern base) + 100 (`:item/color` constant) + 10 (`?color` available from `:in`) |
| `[(enumerate ?vec) [?idx ?item]]` | **20** | 10 per provided symbol x 2 (`?idx`, `?item`) |

The color pattern scores 10x higher than enumerate, so it always gets selected first.

### Why `Requires: nil` is the core issue

`extractPatternSymbols` (clause_utils.go:49-67) returns `Requires: nil` for all data patterns. This is technically correct -- a data pattern *can* execute without any bound variables by doing a full scan. But it means `canExecuteClauseWithContext` always returns true for data patterns, letting the scoring function decide ordering unconstrained.

The result: `[?item :item/color ?color]` is considered executable even though `?item` hasn't been provided by enumerate yet. When it runs, it scans ALL items matching the color, producing a relation `{?item, ?color}` disconnected from any specific container.

### The join failure

After the planner reorders, execution proceeds:

1. `:in` params create relation `{?room, ?color}` (both in one group)
2. Room/name/items patterns build up `{?room, ?color, ?c, ?name, ?vec}`
3. **Color pattern runs next** (before enumerate): scans all items with color=red, gets `{?item=redItem, ?color=:color/red}`
4. Joins with accumulated relation on `?color` -- the only shared symbol
5. Cross-product: every container tuple gets `?item=redItem`
6. Enumerate finally runs, but `?item` is already bound -- damage done

Annotation trace confirming the join:
```
ANNOTATION [join/hash]:
  left.attrs=[?room ?color ?inst ?folder ?folderName ?vec]   # no ?item!
  right.attrs=[?item ?color]
  result.attrs=[?room ?color ?inst ?folder ?folderName ?vec ?item]
  left.size=2  right.size=-1  result.size=2
```

The left side has no `?item` because enumerate hasn't run. The join is on `?color` only, producing the cross-product.

## Trigger Conditions

The bug requires **two `:in` parameters** where:
1. One (e.g. `?room`) is consumed by early data patterns
2. The other (e.g. `?color`) is consumed by a data pattern that should run AFTER enumerate

With a single `:in ?color` and no room-anchoring patterns, the bug still causes reordering but the final result may be accidentally correct (enumerate filters the cross-product when checking consistency of already-bound `?item`). With two `:in` params, `?color` enters the accumulated relation early via the shared input group, so the color-pattern join has a shared symbol (`?color`) and produces a real cross-product that enumerate can't undo.

### Verified non-triggers

These query shapes work correctly despite the same planner reordering:

- Single `:in ?color`, no instance/room joins (step 1 in diagnostic test)
- Instance join + inline constant color, no `:in` (step 2)
- Instance join + `:in ?color` only (step 2b)
- Instance join + `:in ?room` + inline constant color (step 2c)

## Affected Code

| File | Function | Role |
|------|----------|------|
| `planner/clause_utils.go:49-67` | `extractPatternSymbols` | Returns `Requires: nil` for data patterns |
| `planner/clause_utils.go:447-496` | `scoreClause` | Scores data patterns 100+ base, expressions only 10*N |
| `planner/clause_phasing.go:57-134` | `selectPhaseClauses` | Greedy selection by score, no ordering constraints |

## Fix Direction

The planner needs to respect that when an expression provides a symbol used by a subsequent data pattern, the expression must run first. Options:

1. **Expression-provided symbols as soft dependencies on patterns.** If a data pattern references a variable that an unselected expression provides, either (a) don't select the pattern until the expression has been selected, or (b) penalize the pattern's score below the expression's.

2. **Ordering constraints.** Before greedy selection, build a partial order: if expression E provides symbol S and pattern P uses S, then E must precede P. The greedy algorithm respects this partial order while otherwise selecting by score.

3. **Boost expression scores when they provide symbols needed by pending patterns.** If an expression provides `?item` and there are unselected patterns referencing `?item`, boost the expression's score above those patterns.

The fix must not break the general principle that data patterns with constants should run early (that's correct for selectivity). It only needs to ensure that an expression providing a variable runs before patterns consuming that variable.
