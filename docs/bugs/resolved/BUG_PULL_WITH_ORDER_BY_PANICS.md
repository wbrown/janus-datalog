# Bug: `(pull ...)` Combined With `:order-by` Panics

**Status**: FIXED (2026-07-04) via the **pull relocation**: pull rendering moved from the QueryExecutor's last phase to the result boundary in `ExecuteRealized`, after sort → strip → limit. Entity bindings stay `Identity` — a first-class, natively hashable value — through every relational operation, so no crash site needed any change to its deduplication. Companion changes landed with it: `(pull ...)` is rejected at parse time inside subquery finds (subquery results feed outer relational flow); stripping retained sort attributes simplified to a plain deduplicating projection (its pull exemption became dead code); and the value domain is now **loudly enforced** — `hashValue`'s default and `ValuesEqual`'s fallback panic naming any non-value type instead of silently mis-hashing or crashing cryptically. On first full-suite contact the loud default immediately caught a further domain gap — the storage vector-matching path produces typed slices (`[]string`) in tuples, which the equality layer already treats as vectors (polymorphic `reflect.Slice` comparison), so the hash layer now hashes every slice kind through the same generic element iteration, keeping the `ValuesEqual(a,b) ⇒ hash(a)==hash(b)` invariant across representations. All reproduction tests for the class are green, as is `go test -count=1 ./...`.

Historical note: one approach was **considered and rejected** (2026-07-04) before the relocation: making `SortRelation`'s construction blanket non-deduplicating. Rejected because it works around the panic instead of fixing the layering underneath it, and the risk asymmetry is wrong — it trades a loud panic for a latent silent-duplicates risk on a shared materialization primitive, and proving that safe requires whole-codebase rigor far beyond this bug's scope. **Discovered**: 2026-07-04, by the coverage audit for `BUG_ORDER_BY_NON_PROJECTED_VARIABLE_SILENTLY_IGNORED.md` (matrix cases N/P) **Affected releases**: every tag since v0.2.0 (the Pull API's first release, commit `bc8f96a`) crashes on pull + `:order-by` and pull + relation-input; pull + `:limit ≥2` crashes since `:limit` shipped (v0.13.x).

## The Crash Class

The order-by panic is one instance of a general class: **any two or more post-pull tuples flowing through any full-tuple deduplication site panic, near-deterministically**. Two compounding root causes:

1. **`datalog.ValuesEqual` is partial.** Its fallback (`datalog/compare.go:410-412`) reads "Safe: neither a nor b is a slice here, so == cannot panic" — but slices are not the only uncomparable kind. `map[string]interface{}` (every pulled value) reaches `a == b` and panics.
2. **`hashValue`'s default case guarantees the collision.** For types it doesn't recognize (`datalog/executor/tuple_key.go:161-163`) it hashes the *stack address of its local interface variable* — the same slot on every call from a given site — so all pulled maps get the same hash, every pair collides in `TupleKeyMap`, and the equality check that panics is always reached. (For other unknown types this same default silently produces never-equal hashes instead — the exact failure mode the `[]byte` and `time.Time` cases in that switch were added to fix.)

`executePulls` knows about the problem — it deliberately bypasses the deduplicating constructor ("pulled maps ... would panic during deduplication") — but nothing prevents its output flowing into dedup sites downstream. Site-by-site status, confirmed by reproduction tests:

| Site | Query shape | Status |
|------|------------|--------|
| `SortRelation` (`executor_utils.go`) | pull + `:order-by` (tied sort keys, or relations containing only pull results) | **FIXED by relocation** — maps never reach the sort. `TestOrderByPullWithTiedSortKeys` green; `TestSortRelationRejectsNonValueTuples` pins that a hand-built map-bearing relation now fails loudly as a value-domain violation |
| Post-sort strip (`executor.go`) | pull + non-projected `:order-by` | **FIXED by relocation** — the strip is a plain deduplicating projection over value attributes (`TestOrderByNonProjectedWithPull` green); the former pull-exempt branch was deleted as dead code |
| `LimitRelation.ensure` (`limit_relation.go:58`) | pull + `:limit ≥2` | **FIXED by relocation** — the capped result deduplicates `Identity` rows, then the boundary pulls only the surviving N (`TestPullWithLimitTwoRows` green) |
| Relation-input iteration combine (`executor.go`) | pull + `:in $ [[?x] ...]` | **FIXED by relocation** — the union deduplicates `Identity` rows (correct by-entity set semantics), then the boundary pulls each surviving row (`TestPullWithRelationInputUnion` green) |
| Multi-group product path (`query_executor.go`) | pull + disjoint find groups | never crashed, previously uncovered — `TestPullWithDisjointFindGroups` pins pull rendering through it end-to-end |

Reproductions: `datalog/executor/pull_dedup_test.go`.

Sites *not* reachable: all other `NewTupleKeyFull` users (joins, subquery dedup, iterator composition, OR-clause unions) run during clause execution, before `executePulls` — pulls exist only in the last phase's find clause.

## Fix Direction for the Class (decision pending)

The full reasoning genealogy — how two workaround-shaped fixes were proposed and rejected before the layering analysis below, and why — is documented in `docs/TALE_AND_LESSONS_OF_WORKAROUNDS_AND_INVARIANTS.md`.

The leading direction (from that analysis): **pull is result presentation, not a relational operator** — `map[string]interface{}` is not a janus value type, and its presence in relation tuples is the violation. Relocate `executePulls` from the QueryExecutor's last phase to the result boundary in `ExecuteRealized`, after sort → strip → limit: entity bindings stay `Identity` through all relational operations, every crash site heals untouched, deduplication gains correct by-entity set semantics, and pull + `:limit N` pulls only N entities. Companion enforcement: `hashValue`/`ValuesEqual` defaults become loud failures naming the type; the vector (`[]interface{}`) hashing gap is fixed as a legitimate in-domain case; and `(pull ...)` in subquery finds (currently accepted by the parser) needs a ruling, since subquery results feed outer joins. Earlier candidate directions, retained for the record:

- **(a) Make the comparison/hash layer total.** Extend `ValuesEqual` with deep map equality (recursing through nested maps/slices, which pull results contain — cardinality-many attributes produce `[]interface{}` values) and give `hashValue` an order-independent content hash for maps. Dedup then *works* on pulled rows uniformly (set semantics everywhere; `executePulls`' exemption becomes an optimization), and the class is dead at the root. Also lets `hashValue`'s default case become a loud failure instead of a silent wrong-answer hash.
- **(b) Exempt post-pull relations from dedup per site.** `LimitRelation.ensure` → no-dedup constructor (independently arguable: truncation must not re-deduplicate, and today's dedup can under-fill the limit); relation-input combine → skip dedup when the find spec has pulls. Smaller surface, but the class stays alive for any future consumer of post-pull tuples, and pull results keep bag semantics.
- **Hybrid**: (b)'s limit fix (correct regardless of pulls) plus (a) for the root.

## Symptoms

Any query that combines a `(pull ...)` find spec with `:order-by` panics:

```
panic: runtime error: comparing uncomparable type map[string]interface {}
```

```clojure
;; Panics — even though the sort key ?age is fully projected:
[:find (pull ?e [:user/name]) ?age
 :where [?e :user/age ?age]
 :order-by [[?age :asc]]]
```

This is independent of the non-projected-sort-key defect: the panic fires with projected keys too. The combination has evidently never been executed by any test until the coverage audit.

Reproduced by `TestOrderByNonProjectedWithPull` and `TestOrderByProjectedKeyWithPull` in `datalog/executor/executor_sorting_test.go` (currently red).

## Root Cause

Materialized relations deduplicate at creation: `NewMaterializedRelation`/`NewMaterializedRelationWithOptions` run `deduplicateTuples` (`datalog/executor/relation.go:367-388`, `:421-439`), which compares tuple values via `datalog.ValuesEqual`. Pulled values are `map[string]interface{}`, which Go cannot compare — so `executePulls` deliberately bypasses the deduplicating constructor, returning a `&MaterializedRelation{...}` struct literal with the comment "Return relation without deduplication - pulled maps (map[string]interface{}) are not comparable and would panic during deduplication" (`datalog/executor/query_executor.go` end of `executePulls`).

`SortRelation` (`datalog/executor/executor_utils.go:112-161`) undoes that exemption: it materializes the tuples, sorts them, and rebuilds the result via `NewMaterializedRelationWithOptions` (`:155`) — re-running the deduplication the pull path was built to avoid. `ExecuteRealized` calls `Sort` whenever the query has `:order-by` (`executor.go:231-233`), so pull + order-by always hits it:

```
ExecuteRealized → Sort → SortRelation
  → NewMaterializedRelationWithOptions → deduplicateTuples
  → TupleKeyMap.Exists → tupleValuesEqual → ValuesEqual → panic
```

## Fix Direction

Sorting is a permutation — it must never change relation membership, so it has no business deduplicating at all. `SortRelation` should construct its result via the existing `NewMaterializedRelationNoDedupeWithOptions` (`relation.go:401`). Its input relation's membership is already established by whatever built it (set semantics is enforced at materialization creation); re-deduplication is redundant for comparable tuples and fatal for pulled ones.

The companion strip step being added by the order-by fix (project retained sort attributes away after sorting) must likewise skip deduplication when the find spec contains pulls, mirroring `executePulls`' own exemption.

See the Design Decision section of `BUG_ORDER_BY_NON_PROJECTED_VARIABLE_SILENTLY_IGNORED.md` (set-vs-bag wrinkle) for the full reasoning; both fixes land together.

## Impact

Every pull + order-by query crashes the process today. No workaround other than dropping one of the two features from the query (e.g. sort client-side after pulling).
