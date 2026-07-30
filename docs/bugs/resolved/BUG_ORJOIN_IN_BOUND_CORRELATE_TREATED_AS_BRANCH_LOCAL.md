# BUG: or-join treats a branch external bound only by `:in` as a branch-local (silent over-match)

**Status**: Fixed (2026-07-22, the environment-relation commit). `:in` scalar and tuple parameters are now the query scope's **environment**: one single-tuple Relation carried on the executor Context (`Environment()`/`WithEnvironment`), bound at every query-scope boundary (top-level plan binding, per-tuple iteration Runs, subquery entry — an inner scope never captures an outer scope's parameters) and joined into or-branch evaluation at the or-fallback boundary. Branch visibility now derives from the canonical clause interface (`query.ScopeOf` Provides ∪ Correlates for explicit-header forms; every shared branch free variable for inference forms) instead of executor-local header sets; branch-consumed environment symbols join in via `envRel` regardless. Pinned by the mode-matrixed reproducers in `datalog/storage/orjoin_in_bound_correlate_test.go` (both-branches, mixed-correlates, aggregate-over-residual-group, tuple-input, branch-predicate, or-default-join cacheable and non-cacheable arms, wide-header and subquery controls, RelationInput iteration) and verified against this doc's CLI reproduction on a real fixture database in both planner modes. One adjacent defect surfaced and was ruled into its own arc: a bare `(not …)` whose variables span disjoint groups anti-joined each group separately — see `BUG_NOT_SPANNING_DISJOINT_GROUPS_APPLIED_PER_GROUP` (fixed in the follow-on anti-join bridging commit; its or-branch reproducer is green).

**Original report** (2026-07-22): Found by a downstream integration's dependency bump `331ba214` → `ba6ee242`: its test suite broke on every or-join whose correlate reaches the query through `:in`, while clause-bound correlates kept working. Bisected to `4a43b5d`; still reproduces at this tree's head (`92fb497`). The documented or-join contract (this repo's `.claude/skills/datalog/SKILL.md`, OR clauses section) is upheld for clause-bound externals and violated for `:in`-bound ones.

## Symptom

An or-join whose branches consume a variable bound **only by `:in`**, with the header kept narrow per the documented contract ("Every or-join branch must bind every header variable. Keep branch-specific filter inputs out of the header"):

```clojure
[:find ?e
 :in $ ?region ?ref
 :where
 [?e :entity/region ?region]
 (or-join [?e]
   [?e :entity/name ?ref]
   [?e :entity/code ?ref])]
```

treats `?ref` as a fresh per-branch local instead of a correlate: each branch degenerates to "has any name" / "has any code", and the query returns every named/coded entity in the region instead of the one equal to the input. Silent — no validation error, no planner rejection. The same shape with `?ref` bound by a where clause instead of `:in` returns the correct single row.

## Reproduction

Against a real-world fixture database (any db with two string attributes works; this one has 289 entities in the region, 251 of them named/coded). `?region` is a grouping entity, `?ref` is a name exactly one entity carries as `:entity/name`. Expected count: 1.

```bash
datalog -db <fixture>.jdzl \
  -query '[:find (count ?e) :in $ ?region ?ref :where [?e :entity/region ?region] (or-join [?e] [?e :entity/name ?ref] [?e :entity/code ?ref])]' \
  -in '#identity "<region-entity>"' -in '"<unique name>"'
```

Variants, each `datalog` built at the stated commit with stock CLI defaults (blank = not measured):

| Variant | 331ba214 | 1b4348c | 8887b9b | 4a43b5d | a58c0f8 | ba6ee242 | 92fb497 (this tree) |
|---|---|---|---|---|---|---|---|
| narrow header, `?ref` `:in`-bound | 1 ✓ | 1 ✓ | 1 ✓ | **482 ✗ (flip)** | 251 ✗ | 251 ✗ default / 482 ✗ `-optimize` | 482 ✗ |
| narrow header, `?ref` clause-bound (`[?probe :entity/name "<unique name>"] [?probe :entity/name ?ref]` replacing the `:in` binding) | | | | | | 1 ✓ | 1 ✓ |
| wide header `(or-join [?e ?ref] …)` | 1 ✓ | | | | | 1 ✓ | 1 ✓ |

251 vs 482 is row dedup only (distinct entities vs. entities counted once per matching branch); both are the same over-match.

## What the discriminators say

- **The classification miss is specific to `:in`-only bindings.** A clause-bound external correlates into the branches correctly at ba6ee242 and at this tree; the identical query with the binding moved to `:in` over-matches. Whatever computes branch-visible outer bindings / correlates for or-join's correlated route does not count a symbol whose only binding is `:in` as outer-bound, so it falls into the `4a43b5d` branch-locals rule.
- **Both planner modes are affected** (default 251, `-optimize` 482 at ba6ee242; 482 at this tree's default), so the miss sits in shared scope classification, not one lowering.
- **The bisect lands on `4a43b5d`** ("or-default-join declares its interface: required-vars header syntax") — the commit that restricted or-join branch-visible outer bindings to the header, with branch variables outside the header as locals (pinned by `TestOrJoinBranchLocalAlphaEquivalence`). That rule is correct for genuine locals; the defect is that an `:in`-bound external classifies as one. `a58c0f8`'s correlated round-trip fix (`compileOrUnionCorrelated`, Required from the clause's canonical scope — see `BUG_CORRELATED_ORJOIN_GLOBAL_FALLBACK_DROPS_TUPLES`) repaired clause-bound correlates but never receives the `:in`-bound case: an or-join whose only externals are `:in`-bound classifies as uncorrelated. Sibling precedent for `:in`-bound symbols missing from a classification: `BUG_ALGEBRA_ORDERING_IN_BOUND_CORRELATE_FOLDS_FIRST`.
- **The contract requires these to correlate.** The skill's or-join section keeps branch-specific inputs out of the header, and its worked example is exactly the mixed-correlate admission shape — each branch binds only one of two `:in`-bound correlates:

  ```clojure
  (or-join [?e]
    [?e :entity/site ?site]
    [?e :entity/region ?region])
  ```

  That shape has no wide-header rewrite — every branch must bind every header variable, and each branch binds only one of the two correlates — and or-join parse-rejects the or-default-join required-vars header (`error parsing or-join vars: join variable 0 must be a symbol`). Under branch-locals-treatment of `:in`-bound externals the shape is inexpressible, so the documented example only has a meaning if `:in`-bound externals correlate.

## Impact

Downstream at ba6ee242: 9 tests fail directly on `:in`-bound or-join call sites — entity-ref resolution (a unique name ambiguous, matching 251), read/edit surfaces riding it, entity-admission filtering (a foreign scope's instances admitted — an isolation hole, since an admitted foreign copy can claim a kind and receive this scope's writes), and a source-or-target union query (3 rows where 2 are correct). Every production or-join correlate downstream is `:in`-bound — Go callers pass identities and strings as query inputs — so this is the dominant integration pattern, not an edge case.

The both-branches-bind case (`?ref`) can work around via the wide header (verified correct at 331ba214, ba6ee242, and this tree); the mixed-correlate admission sites have no workaround.
