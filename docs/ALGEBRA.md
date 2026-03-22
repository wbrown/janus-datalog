# Relational Algebra for Datalog Query Optimization

## Operators

Ten operators. No internal-only types. Every node in the algebra tree is one of these.

### Leaves (0 children)

**Scan(source, pattern) → R**
Read datoms from storage matching a pattern. Output symbols come from the pattern's variables.

**Constant(symbols, values) → R**
A single-tuple relation with literal values. Compiled from `(ground ...)`.

### Unary (1 child)

**Select(predicate)(R) → R'**
σ_p(R). Filter tuples where predicate holds. Output symbols = input symbols.

**Project(symbols)(R) → R'**
π_S(R). Keep only the listed symbols. Output symbols = S.

**Map(expression, binding)(R) → R'**
Extend each tuple with a computed value. Output symbols = input symbols + binding symbols.

**Aggregate(groupBy, functions)(R) → R'**
γ_{G,F}(R). Group by G, compute aggregate functions F per group. Output symbols = G + F result symbols.

### Binary (2 children)

**Join(kind, joinSymbols)(R, S) → R'**
R ⋈ S. Combine tuples matching on shared symbols.
- `InnerJoin`: only matching tuples
- `LeftOuterJoin(defaults)`: all left tuples; fill defaults for non-matches

**AntiJoin(joinSymbols)(R, S) → R'**
R ▷ S. Tuples from R with NO match in S.

### N-ary (N children)

**Union()(R₁, R₂, ..., Rₙ) → R'**
R₁ ∪ R₂ ∪ ... ∪ Rₙ. All tuples from all branches. All branches must produce the same symbols.

### Correlated (1+ children)

**LateralJoin(correlationVars, innerQuery, binding, defaults)(R) → R'**
R ⋈_L S(r.x). For each tuple in R, execute innerQuery with correlation variable values bound, apply binding form. If inner returns empty and defaults are present, use defaults. **This is the optimization target — it exists to be rewritten.**

---

## Round-Trip Specifications

Each operator has a compile mapping (Datalog → algebra) and a decompile mapping
(algebra → Datalog). For the system to be correct, the round-trip must preserve
semantics: `execute(decompile(compile(clauses))) ≡ execute(clauses)`.

### Scan

**Compile:** `[?e :attr ?v]` → `Scan(source=$, pattern=[?e :attr ?v], output=[?e, ?v])`

**Decompile:** `Scan(pattern=P)` → `P` (the DataPattern itself)

**Round-trip invariant:** The DataPattern is preserved exactly. The Scan node
carries the original DataPattern object; decompilation returns it unchanged.

**Test:** Compile `[?e :attr ?v]`, decompile, assert the clause equals the input.

### Constant

**Compile:** `[(ground 42) ?x]` → `Constant(symbols=[?x], values=[42])`

For tuple ground: `[(ground [1 2]) [[?a ?b]]]` → `Constant(symbols=[?a, ?b], values=[1, 2])`

**Decompile:** `Constant(symbols=[?x], values=[42])` → `[(ground 42) ?x]`

For multiple symbols: `Constant(symbols=[?a, ?b], values=[1, 2])` → `[(ground [1 2]) [[?a ?b]]]`

**Round-trip invariant:** Symbol names and values are preserved exactly.

**Test:** Compile ground expression, decompile, assert clause equality.

### Select

**Compile:** `[(> ?x 10)]` → `Select(predicate=(> ?x 10))(child)`

The child is whatever relation provides `?x`. The Select wraps it.

**Decompile:** `Select(predicate=P)(child)` → `decompile(child) ++ [P]`

Child clauses followed by the predicate clause.

**Round-trip invariant:** Predicate object preserved. Child clauses preserved.
Order: child clauses come before the predicate (dependency ordering).

**Test:** Compile `[?e :attr ?x] [(> ?x 10)]`, decompile, assert both clauses
present in correct order.

### Project

**Compile:** Not compiled from clauses directly — created by the `:find` clause
or by optimization passes.

**Decompile:** `Project(symbols)(child)` → `decompile(child)` (pass-through)

Project doesn't produce clauses; it's handled by the executor's `:find` projection.

**Round-trip invariant:** Child clauses preserved. Project is transparent to
decompilation.

**Test:** Compile clauses, wrap in Project, decompile, assert child clauses unchanged.

### Map

**Compile:** `[(+ ?x ?y) ?z]` → `Map(expression=(+ ?x ?y), binding=?z)(child)`

Also: `[[(> ?x 0)] ?result]` → `Map(expression=(> ?x 0), binding=?result)(child)`

Also: `[(get-else $ ?e :attr default) ?v]` → `Map(expression=get-else(...), binding=?v)(child)`

**Decompile:** `Map(expression=E)(child)` → `decompile(child) ++ [E]`

Child clauses followed by the expression clause.

**Round-trip invariant:** Expression object (function + binding) preserved exactly.

**Test:** Compile `[?e :attr ?x] [(+ ?x 1) ?y]`, decompile, assert both clauses.

### Aggregate

**Compile:** Not compiled from clauses directly — created by the decorrelation
transform from a LateralJoin with aggregates.

**Decompile:** `Aggregate(groupBy=[?s], fns=[(count ?t)])(child)` →

```
[(q [:find ?s (count ?t) :in $ :where <decompiled child clauses>]
    $) [[?s ?count] ...]]
```

The Aggregate becomes an uncorrelated SubqueryPattern:
- `:find` = GROUP BY variables + aggregate functions
- `:in` = `$` only (uncorrelated)
- `:where` = decompiled child clauses
- Binding = RelationBinding with GROUP BY + aggregate output symbols
- Input = `$` (Constant source marker)

**Round-trip invariant:** GROUP BY symbols, aggregate functions, and inner WHERE
clauses are all preserved. The SubqueryPattern's RelationBinding maps the `:find`
columns to output symbols positionally.

**Test:** Build an Aggregate node over compiled inner clauses, decompile, verify
the SubqueryPattern has correct `:find`, `:in`, `:where`, and binding.

### Join(Inner)

**Compile:** Two clause groups that share symbols are compiled independently
and combined with `Join(Inner, sharedSymbols)`.

**Decompile:** `Join(Inner)(left, right)` → `decompile(left) ++ decompile(right)`

Concatenation. The executor joins on shared symbols automatically during Collapse.

**Round-trip invariant:** All clauses from both sides preserved. Order: left
clauses before right clauses.

**Test:** Compile `[?e :attr1 ?x] [?e :attr2 ?y]`, decompile, assert both
DataPatterns present.

### Join(LeftOuter, defaults)

**Compile:** `(or [subq-branch] [ground-defaults])` where the subquery branch
has been decorrelated (no longer correlated). This is the OUTPUT of the
decorrelation transform, not a direct compilation from source.

**Decompile:** `Join(LeftOuter, joinSyms=[?x], defaults=[0])(left, right)` →

```
decompile(left) ++
(or-join [?x] <decompile(right)>
               <default branch>)
```

Left side clauses followed by an or-default-join:
- Branch 1: decompiled right side (the Aggregate → SubqueryPattern)
- Branch 2: default expressions for non-join symbols only (join keys subtracted)

The default branch depends on `DefaultAttr`:
- When `DefaultAttr` is set (from get-else rewrite): emits `get-else` expression
  so `TypedDefaulter` produces schema-typed defaults (e.g., `[]string` not `[]interface{}`)
- Otherwise: emits `ground` expressions

The or-join evaluates per outer tuple. Branch 1 returns all subquery results;
`filterBranchToOuterTuple` matches to the current outer tuple on shared symbols.
Non-matching tuples get branch 2 (defaults).

**Round-trip invariant:** Left clauses preserved. Right side becomes an
or-default-join SubqueryPattern. Default values map to ground or get-else
expressions. Join symbols are explicit on the or-default-join clause.

**Test:** Build LeftOuterJoin(defaults=[0]) over an outer Scan and an inner
Aggregate. Decompile. Verify: outer DataPattern + OrDefaultJoinClause with
SubqueryPattern branch and default branch.

### AntiJoin

**Compile:** `(not [?e :deleted true])` → `AntiJoin(joinSymbols=[?e])(outer, inner)`

For `not-join`: `(not-join [?e] [...])` → `AntiJoin(joinSymbols=[?e], explicitJoin=true)`

**Decompile:**
- `AntiJoin(explicitJoin=false)` → `decompile(left) ++ (not <decompile(right)>)`
- `AntiJoin(explicitJoin=true)` → `decompile(left) ++ (not-join [vars] <decompile(right)>)`

**Round-trip invariant:** Join symbols preserved. Inner clauses wrapped in
NotClause or NotJoinClause. Left clauses come first.

**Test:** Compile `[?e :type :foo] (not [?e :deleted true])`, decompile, assert
DataPattern + NotClause.

### Union

**Compile:** `(or [?e :a ?x] [?e :b ?x])` → `Union(output=[?e, ?x])(branch1, branch2)`

For `or-join`: `(or-join [?e] ...)` → `Union(output=[?e, ?x], joinVars=[?e])(branch1, branch2)`

When an `(or ...)` or `(or-join ...)` branch contains correlated predicates
(NOT, missing?) that require outer context, the compiler detects this via
`branchesRequireOuterContext()` and routes to the LateralUnion path instead.

**Decompile:**
- `Union(joinVars=nil)(children...)` → `(or <decompile(child1)> <decompile(child2)> ...)`
- `Union(joinVars=[?e])(children...)` → `(or-join [?e] <decompile(child1)> <decompile(child2)> ...)`

**Round-trip invariant:** Each branch's clauses preserved. `or` ↔ OrClause,
`or-join` ↔ OrJoinClause with join vars preserved.

**Test:** Compile pattern-only OR and or-join, decompile, assert correct clause
type with branches and join vars.

### LateralUnion

**Compile:** `(or-default [?e :a ?x] [(ground 0) ?x])` (general fallback) →
`LateralUnion(output=[?e, ?x])(branch1, branch2)`

For `or-default-join`: `(or-default-join [?x] ...)` →
`LateralUnion(output=[?e, ?x], joinVars=[?x])(branch1, branch2)`

Also produced when `(or ...)` / `(or-join ...)` branches contain correlated
predicates (NOT, missing?) that require per-tuple evaluation against the outer
relation. Independent Union compilation cannot express anti-joins without context.

**Decompile:**
- `LateralUnion(joinVars=nil)(children...)` → `(or-default <decompile(child1)> <decompile(child2)> ...)`
- `LateralUnion(joinVars=[?x])(children...)` → `(or-default-join [?x] <decompile(child1)> <decompile(child2)> ...)`

**Round-trip invariant:** Each branch's clauses preserved. `or-default` ↔
OrDefaultClause, `or-default-join` ↔ OrDefaultJoinClause with join vars preserved.

**Test:** Compile OR-default with ground fallback, decompile, assert
OrDefaultClause/OrDefaultJoinClause with correct branches.

### LateralJoin

**Compile:** `(or-default [(q [...] $ ?x) [[?count]]] [(ground 0) ?count])` →

```
LateralJoin(
  correlationVars=[?x],
  innerQuery=[:find (count ?t) :in $ ?s :where ...],
  binding=[[?count]],
  defaults=[0]
)(outer)
```

**Decompile:** `LateralJoin(correlationVars=[?x], defaults=[0])(outer)` →

```
decompile(outer) ++
(or-default-join [?x] [(q <innerQuery> $ ?x) <binding>]
                       [(ground 0) ?count])
```

Join vars come from correlation vars (correlated) or shared symbols between
binding and outer relation (uncorrelated). Default branch binds only non-join
symbols — join keys come from the outer relation via or-default-join.

Without defaults: `decompile(outer) ++ [(q <innerQuery> $ ?x) <binding>]`

**Round-trip invariant:** Inner query, binding form, correlation variables, and
default values all preserved. Correlation variables appear as SubqueryPattern
inputs. Defaults produce or-default-join with ground branch. Join vars ensure
the phaser doesn't reorder the OR before the patterns that provide the join keys.

**Test:** Compile an OR-fallback with correlated subquery + ground, decompile,
assert OrDefaultJoinClause (not OrJoinClause) with correct join vars.

---

## Equivalence Rules

### Rule 1: Lateral Join Decorrelation

```
R ⋈_L(x, defaults) [S(x) GROUP BY F]
  →
R ⋈_LeftOuter(x, defaults) Aggregate(x, F)(S)
```

**Preconditions:**
- Inner query has aggregate functions in `:find`
- Correlation variable `x` maps to an inner parameter
- **The LateralJoin is NOT a child of a Union node.** Decorrelation inside a
  Union moves the correlation variable from input to output, creating a schema
  mismatch with other Union branches (e.g., ground defaults) that lack the
  correlation variable. This causes cross-products when the Union result is
  joined with the outer relation.

**Proof of equivalence:**

The left side executes S(x) once per tuple in R, filtering S to rows where the
correlation variable matches, then aggregating. The result is one aggregate row
per R tuple (or defaults if S has no matches for that x value).

The right side executes S once for ALL values of x, groups by x, aggregates per
group. This produces one row per distinct x value. The LeftOuterJoin matches each
R tuple to its group by x. Tuples in R with no matching group get defaults.

Both produce the same output: for each tuple in R, the aggregate of S rows
matching on x, or defaults if none match.

**Key insight:** LeftOuterJoin, not InnerJoin. InnerJoin drops R tuples without
matches. The original LateralJoin with defaults preserves them.

**Decompilation of the result:**

The rewritten tree `LeftOuterJoin(defaults) [R, Aggregate(S)]` decompiles to:

```
<R clauses>
(or [(q [:find ?x (agg ?v) :in $ :where <S clauses>] $) [[?x ?result] ...]]
    [(ground default) ?result])
```

This is valid Datalog. The SubqueryPattern runs once (uncorrelated). The OR-fallback
provides defaults for non-matching outer tuples.

### OR-Fallback Branch Caching

The decorrelated SubqueryPattern inside an or-join runs once and returns ALL
groups. The or-join evaluates per outer tuple: for each outer tuple, it
executes branch 1 (SubqueryPattern), filters to matching rows, and uses
the result (or defaults from branch 2 if no match).

Without caching, `filterBranchToOuterTuple` evaluates the SubqueryPattern
AND scans the full result O(M) for EVERY outer tuple — total O(N*M).
This is worse than the correlated path which uses storage indices O(N*log M).

**The cache contract:**

An uncorrelated branch produces the same result for every outer tuple.
Therefore:
1. On first evaluation of a branch, execute it and build a hash index
   keyed on the shared symbols between the branch result and the outer
   relation.
2. On subsequent evaluations of the SAME branch, skip execution entirely
   and probe the cached hash index.

**Correctness conditions:**
- The branch must be uncorrelated (its result is independent of the outer
  tuple). A branch is uncorrelated when its SubqueryPattern has `:in $`
  only (no correlation parameters from the outer context).
- Correlated branches MUST NOT be cached — each outer tuple produces
  different results.

**Detection:**
A branch is uncorrelated if and only if ALL of these hold:
1. The branch contains exactly one SubqueryPattern clause
2. The SubqueryPattern's Inputs contains only `$` (no variables)
3. The SubqueryPattern's `:in` clause contains only DatabaseInput

If any condition fails, the branch is correlated and must be re-evaluated
per outer tuple without caching.

**Complexity:**
- Without cache: O(N * M) where N = outer tuples, M = branch result size
- With cache: O(M) build + O(N) probe = O(N + M)

**Implementation:**
The `OrFallbackIterator` holds a `branchCache map[int]cachedBranch` where:
```go
type cachedBranch struct {
    index      *TupleKeyMap  // hash index keyed on shared symbols
    branchSyms []query.Symbol
    outerIdx   []int         // shared symbol positions in outer tuple
    branchIdx  []int         // shared symbol positions in branch tuple
}
```

On first evaluation of branch `i`:
1. Check `isUncorrelatedBranch(branch)` — inspect the clause structure
2. If uncorrelated: execute, build index, store in `branchCache[i]`
3. If correlated: execute, filter with `filterBranchToOuterTuple`, no cache

On subsequent evaluations of branch `i`:
1. If `branchCache[i]` exists: probe the index, return matching tuples
2. If not cached: re-execute (correlated branch)

### Rule 4: General Correlated Subquery Decorrelation

```
R ⋈_L(x, defaults) [S(x)]
  →
R ⋈_LeftOuter(x, defaults) S'
```

Where S' is S with:
- Correlation variable `x` removed from `:in`
- Correlation variable `x` added to `:find`
- Binding changed from TupleBinding to RelationBinding (includes `x`)
- WHERE clauses unchanged

**Preconditions:**
- Correlation variable `x` maps to an inner parameter
- **The LateralJoin is NOT a child of a Union node.** Decorrelation inside a
  Union moves the correlation variable from input to output, creating a schema
  mismatch with other Union branches (e.g., ground defaults) that lack the
  correlation variable. This causes cross-products when the Union result is
  joined with the outer relation.
- Inner query satisfies at least one of:
  1. Has aggregate functions in `:find` (Rule 1 — already handled)
  2. Contains a nested correlated subquery sharing the same correlation variable
  3. Contains filtering clauses (predicates, expressions, NOT) beyond bare data patterns

Condition 2 is the argmax pattern. Condition 3 covers most real queries.

**Condition that PREVENTS decorrelation:**
- Inner query consists of only bare DataPatterns with no predicates, expressions,
  or nested subqueries. In this case, the correlated path uses indexed storage
  lookups O(log M) per tuple, which is faster than scanning all data O(M).

**Proof of equivalence:**

The left side executes S(x) once per tuple in R, binding x to the current tuple's
value, producing results filtered to that x value.

The right side executes S once for ALL values of x. Since x was a parameter that
filtered the WHERE clauses (e.g., `[?t :task/root ?s]` with ?s bound), removing
it from `:in` makes ?s a free variable. The WHERE clauses still reference ?s, so
the results include ALL ?s values. Adding ?s to `:find` makes it an output column.
The LeftOuterJoin matches each R tuple to its S' rows by x. Non-matching R tuples
get defaults.

Both produce the same output: for each tuple in R, the S rows where x matches, or
defaults if none match.

**Key insight for nested correlation (argmax pattern):**

When the inner query contains a nested subquery `(q [...(max ?ca)...] $ ?s)` that
shares the same correlation variable, decorrelating the outer makes `?s` a free
variable in the inner WHERE. The nested subquery's LateralJoin also has `?s` as
a correlation variable. The decorrelation transform processes bottom-up: the
nested LateralJoin is decorrelated first (Rule 1, since it has aggregates), then
the outer LateralJoin is decorrelated (Rule 4). Both levels of correlation are
eliminated in a single optimization pass.

Before:
```
R ⋈_L(?scenario) [
    Scan(tasks for ?s)
    ⋈_L(?s) [max(?ca)]         ← runs 75 times per scenario = 75² = 5625 times
    ⋈ Select(?ca = ?maxCa)
]
```

After:
```
R ⋈ [
    Scan(ALL tasks)
    ⋈ Aggregate(groupBy=[?s], max(?ca))   ← runs ONCE
    ⋈ Select(?ca = ?maxCa)
]
```

From O(N²) nested correlated executions to O(1) single pass.

**Decompilation of the result:**

Same as Rule 1: the decorrelated subquery becomes an uncorrelated SubqueryPattern
with RelationBinding inside an or-join (if defaults exist) or bare (if no defaults).

```
<R clauses>
(or-join [?scenario]
    [(q [:find ?s ?key ?ca
         :in $
         :where [?t :task/root ?s]
                ...
                [(q [:find ?s (max ?ca) :in $ :where ...] $) [[?s ?maxCa] ...]]
                [(= ?ca ?maxCa)]]
        $) [[?scenario ?lastKey ?lastUpdatedAt] ...]]
    [(ground [:none :none]) [[?lastKey ?lastUpdatedAt]]])
```

Note: the nested subquery inside the WHERE is ALSO decorrelated — its `:in` changes
from `$ ?s` to `$`, and `?s` moves to its `:find`. This happens automatically
because the algebra compiler compiles it to a LateralJoin, and Rule 1 decorrelates
it in the same bottom-up pass.

**Decision function:**

```go
func shouldDecorrelate(lj *LateralJoin) bool {
    if hasAggregates(lj.InnerQuery)                { return true }  // Rule 1
    if hasNestedCorrelatedSubquery(lj.InnerQuery)  { return true }  // Argmax
    if hasFilteringClauses(lj.InnerQuery)           { return true }  // Selective
    return false  // Pure DataPatterns only — indexed lookup is faster
}
```

All conditions are structural — no cardinality estimates needed.

### Rule 2: Predicate Pushdown (future)

```
σ_p(R ⋈ S) → σ_p(R) ⋈ S     when p references only R's symbols
```

### Rule 5: Get-Else Scan Rewrite

```
Map(get-else(E, A, default))(R)
  →
R ⋈_LeftOuter(E, default) Scan([E A ?result])
```

**Preconditions:**
- The get-else expression references an entity variable `E` that exists in R
- The attribute `A` is a keyword constant
- The binding produces a single output symbol

**Proof of equivalence:**

The left side evaluates `get-else` per tuple: for each tuple in R, look up
entity E's attribute A in storage. If found, bind the value to `?result`.
If not found, bind the default value. This is a point lookup per tuple.

The right side scans ALL (E, A) datoms via the AEVT index, producing a
relation S with [E, ?result]. The LeftOuterJoin matches each R tuple to
its S row by E. For R tuples without a match in S (entity lacks the
attribute), the default fills in.

Both produce the same output: for each tuple in R, the attribute value
if it exists, or the default if not.

**Performance analysis:**

The current Map(get-else) does N point lookups via `LookupAttribute`,
each performing a BadgerDB seek (~0.6ms). Total: O(N × 0.6ms).

The rewritten LeftOuterJoin does 1 AEVT index scan (sequential I/O,
~microseconds for the whole scan) + 1 hash join (O(N + M) in-memory).
Total: O(scan) + O(N + M) ≈ milliseconds.

For the production query: N = 7,889 tasks, M = 4 attributes.
- Before: 7,889 × 4 × 0.6ms = 18.9s
- After: 4 scans + 4 hash joins ≈ <100ms

**Why this is always beneficial:**

BadgerDB seek cost (~0.6ms) is ~1000x the hash join per-operation cost
(~nanoseconds). Even for N=1, one seek + one scan is comparable to one
seek alone. For N>1, the scan+join dominates.

**Decompilation of the result:**

The rewritten `LeftOuterJoin(E, default) [R, Scan(E A ?result)]` decompiles
to the same OR-fallback pattern used by decorrelation:

```
<R clauses>
(or-join [E]
    [E A ?result]
    [(ground default) ?result])
```

This is valid Datalog. The DataPattern scans via index. The or-join
provides the default for non-matching entities.

**Round-trip specification:**

**Compile:** `[(get-else $ ?e :attr default) ?v]` → `Map(get-else(...))(child)`

**Optimize:** `Map(get-else(?e, :attr, default))` →
`LeftOuterJoin(on=[?e], default) [child, Scan([?e :attr ?v])]`

**Decompile:** → `(or-join [?e] [?e :attr ?v] [(ground default) ?v])`

**Test:** Compile get-else, optimize, decompile. Verify: DataPattern +
or-join with default. Execute against real data and verify same results
as unoptimized get-else.

### Rule 3: Join Reordering (future, cost-based)

```
R ⋈ S → S ⋈ R     when |S| < |R|
```

---

## Compilation: Datalog Clauses → Algebra

| Clause | Algebra Node |
|--------|-------------|
| `[?e :attr ?v]` | Scan |
| `[(> ?x 10)]` | Select wrapping child that provides ?x |
| `[(+ ?x ?y) ?z]` | Map wrapping child that provides ?x, ?y |
| `(not [...])` | AntiJoin(outer, inner) |
| `(not-join [?x] [...])` | AntiJoin(outer, inner) with explicit join vars |
| `(or b1 b2)` pattern-only | Union(b1, b2) |
| `(or-join [vars] b1 b2)` pattern-only | Union(b1, b2) with JoinVars |
| `(or [subq] [ground])` | LateralJoin(outer, inner, defaults) |
| `(or [DataPattern+NOT] [ground])` | Union via compileOrFallbackExclusive |
| `(or [subq+NOT] [ground])` | LateralJoin+defaults (NOT guards skipped) |
| `[(q [...] $ ?x) binding]` correlated | LateralJoin(correlationVars=[?x]) |
| `[(q [...] $) binding]` uncorrelated | LateralJoin(correlationVars=[]) |
| `[(missing? $ ?e :attr)]` | Select (predicate preserved as-is) |
| `[(ground val) ?x]` | Constant |
| `[[(> ?x 0)] ?result]` | Map (comparison binding) |

---

## Implementation Plan

### Phase 0: Round-Trip Foundation

Before any transforms, prove that every operator round-trips correctly.

**For each of the 10 operators, write a test that:**
1. Constructs a minimal Datalog clause (or set of clauses)
2. Compiles to an algebra tree
3. Verifies the algebra node type and data
4. Decompiles back to clauses
5. Asserts the decompiled clauses are semantically equivalent to the input

**Tests needed:**
- `TestRoundTrip_Scan` — `[?e :attr ?v]`
- `TestRoundTrip_Constant` — `[(ground 42) ?x]` and `[(ground [1 2]) [[?a ?b]]]`
- `TestRoundTrip_Select` — `[?e :attr ?x] [(> ?x 10)]`
- `TestRoundTrip_Map` — `[?e :attr ?x] [(+ ?x 1) ?y]`
- `TestRoundTrip_Map_GetElse` — `[?e :type :foo] [(get-else $ ?e :attr default) ?v]`
- `TestRoundTrip_JoinInner` — `[?e :a ?x] [?e :b ?y]`
- `TestRoundTrip_AntiJoin` — `[?e :type :foo] (not [?e :deleted true])`
- `TestRoundTrip_AntiJoinExplicit` — `[?e :type :foo] (not-join [?e] [?e :deleted true])`
- `TestRoundTrip_Union` — `(or [?e :a ?x] [?e :b ?x])`
- `TestRoundTrip_LateralJoin` — `(or [(q [...] $ ?e) [[?count]]] [(ground 0) ?count])`
- `TestRoundTrip_LateralJoinNoDefaults` — `[(q [...] $ ?e) [[?count]]]`
- `TestRoundTrip_Aggregate` — constructed manually (output of decorrelation)
- `TestRoundTrip_JoinLeftOuter` — constructed manually (output of decorrelation)

**Each test runs against a real database** (not mocks) via `db.Query()` to verify
that the decompiled clauses execute correctly.

### Phase 1: Clean Up Existing Code

After round-trip tests pass:

1. **Remove `decorrelatedScan`** — replace all uses with standard Aggregate + Join
2. **Remove `collapseLeftOuterJoinTransform`** — no longer needed
3. **Update `decompileAggregate`** — emit uncorrelated SubqueryPattern with
   RelationBinding
4. **Update `decompileLeftOuterJoin`** — emit OR-fallback with SubqueryPattern +
   ground defaults, using `filterBranchToOuterTuple` for per-tuple matching
5. **Verify**: all existing tests pass, round-trip tests pass

### Phase 2: Decorrelation Transform ✅

With proven round-trip mappings:

1. ✅ **Implement the transform** using only standard operators
2. ✅ **Verify**: `TestCorrelatedSubqueryAlgebraOptimizer` passes (194x speedup)
3. ✅ **Verify**: production profiler returns 75 scenarios (correct)
4. ✅ **Fix propagation**: `rebuildWithChildren` for non-decorrelatable parents
5. ✅ **Fix phaser**: `OrJoinClause.Provides` includes branch output symbols

**Status**: Decorrelation is algebraically correct. Production query returns
75 scenarios. Performance is worse (29.9s) because `filterBranchToOuterTuple`
scans O(M) per tuple without caching.

### Phase 3: OR-Fallback Branch Cache ✅

Implement the cache specified in "OR-Fallback Branch Caching" above.

1. ✅ Add `isCacheableBranch(branch, isOrJoin)` — structural detection
   supporting both SubqueryPattern inputs and DataPattern branches in or-join
2. ✅ Add `cachedBranch` struct with `TupleKeyMap` index
3. ✅ Add `branchCache map[int]*cachedBranch` to `OrFallbackIterator`
4. ✅ On first cacheable branch evaluation: execute, build index, cache
5. ✅ On subsequent evaluations: probe cached index in O(1)
6. ✅ Correlated branches: unchanged (re-execute per tuple)
7. ✅ DataPattern branches in or-join: evaluated with join vars free, cached
8. ✅ **Verify**: all tests pass

**Status**: Cache implemented. Extended from SubqueryPattern-only to
also cache DataPattern branches in or-join context (needed for Rule 5).
Production: 28.4s → 13.7s (2.1× speedup).

### Phase 4: General Correlated Subquery Decorrelation (Rule 4)

Extend decorrelation to non-aggregate correlated subqueries per Rule 4.

1. Replace `hasAggregates(lj.InnerQuery)` gate with `shouldDecorrelate(lj)`
2. Implement `shouldDecorrelate`: true for aggregates, nested correlation,
   or filtering clauses; false for pure DataPattern-only queries
3. Implement `hasNestedCorrelatedSubquery(q)`: walk WHERE clauses for
   SubqueryPatterns with variable (non-$) inputs
4. Implement `hasFilteringClauses(q)`: check for predicates, expressions,
   NOT clauses, or nested subqueries in WHERE
5. For non-aggregate decorrelation: same rewrite as Rule 1 but without
   the Aggregate node — just move correlation var to :find, change binding
   to RelationBinding, emit as bare SubqueryPattern or or-join
6. **Test**: `TestDecorrelation_ArgmaxPattern` — nested correlated subquery
   with max aggregate inside non-aggregate outer
7. **Test**: `TestDecorrelation_WithFilteringPredicates` — non-aggregate
   inner query with predicates
8. **Test**: `TestDecorrelation_PureDataPatternSkipped` — bare DataPattern
   inner query should NOT be decorrelated
9. **Verify**: `go test ./datalog/...` passes

**Status**: Rule 4 implemented with recursive inner optimization.
Production: 75 scenarios correct, events 9.3x reduction (450K → 49K).

### Phase 5: Get-Else Scan Rewrite (Rule 5) ✅

Rewrite `get-else` expressions from per-tuple point lookups to bulk
index scans with left-outer-join defaults.

1. ✅ Add `GetElseScanRewritePass()` optimization pass
2. ✅ Detect `Map(get-else(E, A, default))` nodes in the algebra tree
3. ✅ Rewrite to `LeftOuterJoin(on=[E], default) [child, Scan([E A ?result])]`
4. ✅ The LeftOuterJoin decompiles to `(or-join [E] [E A ?result] [(ground default) ?result])`
5. ✅ **Test**: `TestGetElseScanRewrite_Simple` — single get-else round-trip
6. ✅ **Test**: `TestGetElseScanRewrite_Multiple` — 4 chained get-else rewrites
7. ✅ **Test**: `TestGetElseScanRewrite_NonGetElsePreserved` — arithmetic preserved
8. ✅ **Test**: `TestGetElseScanRewrite_VectorDefaultSkipped` — vector defaults not rewritten
9. ✅ **Test**: `TestGetElseScanRewrite_InputParamEntitySkipped` — :in entities not rewritten
10. ✅ **Verify**: `go test ./datalog/...` passes, downstream tests pass

**Guard conditions** (skip rewrite when semantics would change):
- Vector/slice defaults: `ground []` produces `[]interface{}`, not schema-typed `[]string`
- Entity from `:in` parameter: Scan would have unbound variable
- Entity not in child symbols: same issue as input params

**Status**: Rule 5 implemented and enabled by default. Production: 28.4s → 13.7s.
Remaining time dominated by decorrelated subquery full-table scans (8K tasks).

### Phase 6: Production Hardening ✅

Enable algebra optimizer by default and fix all regressions.

1. ✅ Flip `EnableAlgebraOptimizer` default to `true`
2. ✅ **Bypass mechanism**: `optimizeViaAlgebra` errors fall back to
   original clauses silently (planner doesn't propagate algebra errors)
3. ✅ **Fix `compileOrFallbackGeneric`**: return error when branch compiles
   to empty-symbol node (e.g., standalone `missing?` predicate), triggering bypass
4. ✅ **Fix `GroundFunction.String()`**: handle empty slices without panic
5. ✅ **Fix `extractOrJoinClauseSymbols`**: join vars not produced by all
   branches are `requires` (not `provides`), preserving clause ordering
6. ✅ **Verify**: `go test ./datalog/...` — all 13 packages pass
7. ✅ **Verify**: downstream application tests pass

**Key insight**: or-join's join vars are NOT unconditionally provided.
When branch 2 (ground default) doesn't produce the join var, it must be
required from the outer relation. Otherwise the phaser reorders clauses,
breaking NOT clause dependencies.

---

## Performance Analysis

### Profiling Results (concurrent2.db, ListScenarioSummaries, 75 scenarios, 8K tasks)

| Configuration | Wall time | Events | Speedup |
|---|---|---|---|
| Baseline (no optimizer) | 28.4s | 450K | 1.0× |
| Decorrelation only (no Rule 5) | 28.4s | 49K | 1.0× |
| Decorrelation + Rule 5 | 13.7s | 113K | 2.1× |

### Key Finding: Decorrelation Alone Does NOT Improve Wall Time

Decorrelation reduces subqueries from 599 → 4 and events from 450K → 49K,
but wall time is unchanged (28.4s). The correlated path's per-tuple indexed
lookups are fast (~0.6ms each). The decorrelated path replaces them with
full-table scans that cost ~3.5s each — the savings from fewer subquery
executions are offset by more expensive scans.

### Key Finding: Rule 5 Provides All The Speedup

Rule 5 (get-else → or-join with cached DataPattern scans) cuts wall time
from 28.4s → 13.7s. It reduces the intermediate relation sizes feeding
into hash joins, causing the dominant 20.5s join to drop to 6.9s.

**Disproven hypothesis**: "Rule 5 inside decorrelated subqueries is
harmful because LookupAttribute with EA cache is faster than full scans."
Testing showed disabling Rule 5 returns wall time to baseline (28.4s).
Rule 5 helps everywhere, including inside decorrelated subqueries, because
the or-join DataPattern branch cache amortizes the scan cost across all
tuples.

### Remaining Bottleneck: Per-Entity Storage Scans

The 13.7s comes from 23,897 `matcher.Match()` calls — **per-entity
pattern evaluation**, not per-attribute scans. The or-join from Rule 5
evaluates per outer tuple: for each of the ~8K tasks, each get-else
or-join branch calls `matcher.Match()` to scan storage. The branch cache
prevents re-scanning after the first evaluation, but subsequent patterns
(bare DataPatterns in the subquery WHERE) still scan per-entity.

The three slowest individual scans:
```
3.7s  [?t :task/status :status/complete]  (AVET index — correct)
3.4s  [?t :task/key ?key]                 (AETV index — correct)
3.2s  [?t :task/completed-at ?ca]         (AETV index — correct)
```

Index selection is correct (verified via `chooseIndex` code analysis).
The cost is inherent to iterating 8K entities per scan from BadgerDB.

### Rule 6: Entity Prefetch into EA Cache

**Problem**: When a pattern scan produces a set of entity IDs (e.g.,
`[?t :task/root ?s]` → 8K task entities), subsequent patterns referencing
the same entity variable each trigger independent storage scans. With N
entities and M subsequent patterns, this is N × M storage hits.

**Solution**: Once a set of entity IDs is known, batch-prefetch ALL
attributes for those entities into the EA cache in one sequential scan.
Subsequent patterns resolve from cache — O(1) per (entity, attribute).

**Mechanism**:

```
Phase 1: Pattern [?t :task/root ?s] → scan AETV → {t1, t2, ..., t8000}
         → trigger: PrefetchEntities(sorted [t1..t8000])

Phase 2 (concurrent with Phase 1 join processing):
         → open EATV iterator
         → for each entity (sorted by L85 byte order = disk order):
              Seek(EATV_prefix + E_bytes)
              iterate all (E, A, T, V) entries for this entity
              CRDT-resolve each (E, A) pair
              populate EA cache entry
         → one sequential forward scan with 8K forward seeks

Phase 3: Pattern [?t :task/status :status/complete]
         → for each ?t: cache.GetOrResolve(t, :task/status) → HIT
         Pattern [?t :task/key ?key]
         → for each ?t: cache.GetOrResolve(t, :task/key) → HIT
```

**Why sorted order matters**: BadgerDB stores keys sorted. EATV keys are
`prefix(1) + E(20) + A(32) + T(16) + V(...)`. Entity IDs are L85-encoded
SHA1 hashes — L85 preserves sort order. Sorting entities by their 20-byte
hash before scanning means the EATV iterator only moves forward. Each
`Seek()` is cheap (already near the target). Empty ranges between entities
are skipped instantly.

**Cost analysis**:
- Before: N entities × M patterns × ~0.6ms per seek = N×M×0.6ms
  (8K × 3 = 24K seeks × 0.6ms = 14.4s)
- After: N entities × 1 seek each (sorted, forward-only) + N×M cache lookups
  (8K seeks × ~0.1ms + 24K cache hits × ~10ns ≈ 0.8s)

**Concurrency**: The prefetch starts as soon as entity IDs are known,
running in a goroutine. The executor continues processing (hash joins,
etc.) in parallel. By the time subsequent patterns need the data, the
cache is warm.

**Trigger points**:
1. The or-join evaluator: before first per-tuple evaluation, if the outer
   relation contains entity IDs and branches reference those entities,
   prefetch them
2. The clause-by-clause executor: after a DataPattern scan produces entity
   IDs, if subsequent clauses reference the same entity variable, prefetch
3. The matcher itself: when `Match()` returns a Relation with entity
   symbols and bindings reference those entities, trigger prefetch

**What needs to exist**:
1. `Cache.PrefetchEntities(entities []Entity, resolver CacheResolver)` —
   batch load all (E, A) pairs for a set of entities
2. `BadgerMatcher.PrefetchEntities(entities []datalog.Identity)` — sorts
   by key order, scans EATV, resolves CRDT, populates cache
3. Integration in executor or or-join to trigger prefetch at the right time

**Guard conditions**:
- Only prefetch when entity count exceeds a threshold (e.g., >50 entities)
- Only prefetch when subsequent clauses reference the same entity variable
- Don't prefetch for temporal queries (AsOf/History have different cache semantics)

---

## Performance Results (2026-03-16)

### Critical fix: PrefetchValues=false (34× speedup)

The dominant bottleneck was `PrefetchValues=true` in `BadgerStore.Scan()`. All
datom data is encoded in keys — values are never stored. But `PrefetchValues=true`
spawned prefetch goroutines per BadgerDB iterator, causing scheduler thrashing
(`pthread_cond_wait` at 23% CPU) with thousands of short-lived iterators from
per-(E,A) EA cache resolution.

Found via `go tool pprof`, not theorizing. Fix: one line.

| Configuration | Wall time |
|---|---|
| Before (PrefetchValues=true) | 28.4s |
| After (PrefetchValues=false) | 392ms cold, 125ms warm |

### Optimization matrix (bench mode, no annotations, 780K datoms)

| Configuration | Cold | Warm |
|---|---|---|
| No decorrelation | 1.94s | — |
| Decorrelation (Selinger or algebra) | 387ms | 125ms |
| + Scan sharing | 401ms | 125ms |
| + Entity prefetch | 395ms | 125ms |

Decorrelation provides 5× speedup. Scan sharing and entity prefetch are
performance-neutral at this scale — BadgerDB's block cache and the fast
key-only iterators make redundant scans cheap.

### Infrastructure status

| Component | Status | Impact |
|---|---|---|
| PrefetchValues=false | Active, critical | 34× speedup |
| Algebra optimizer (Rules 1, 4, 5) | Active | 5× decorrelation speedup |
| Scan sharing (LazySeq) | Disabled (default off) | Correct but no measurable benefit |
| Entity prefetch | Disabled (default off) | Correct but no measurable benefit |
| Selinger decorrelation | Disabled (default off) | Redundant with algebra optimizer |

---

## Compiler Status and Known Issues

### Removed hacks

1. **Silent error bypass** — algebra compilation errors now propagate (not swallowed)
2. **compileOrFallbackGeneric** — removed, replaced with `compileOrFallbackExclusive`
3. **reflect.TypeOf hack** — replaced with type switch for vector default detection
4. **missing? → AntiJoin** — reverted to Select (preserves predicate through round-trip)
5. **Default branch binding all symbols** — `decompileLateralJoin` now subtracts
   join keys from binding symbols so defaults only bind non-join symbols
6. **extractGroundDefaults rejecting NOT** — now skips NOT/missing? guards (safe
   because LeftOuterJoin handles non-matching tuples via defaults)
7. **Uncorrelated subquery defaults lost** — uncorrelated SubqueryPattern+defaults
   now wraps in LeftOuterJoin (previously only correlated path attached defaults)

### Current compiler coverage

| Clause type | Algebra compilation | Round-trip correct |
|---|---|---|
| DataPattern | Scan | ✅ |
| Expression | Map | ✅ |
| Comparison/Predicate | Select | ✅ |
| DatabaseFunctionPredicate (missing?) | Select | ✅ |
| NotClause | AntiJoin | ✅ |
| NotJoinClause | AntiJoin (explicit) | ✅ |
| OR (pattern-only) | Union | ✅ |
| OR (with correlated predicates) | LateralUnion (detected, routed) | ✅ |
| or-join | Union with JoinVars preserved | ✅ |
| or-default (subquery+ground) | LateralJoin+defaults | ✅ |
| or-default (NOT+ground fallback) | LateralUnion via compileOrFallbackExclusive | ✅ |
| or-default (subquery+NOT+ground) | LateralJoin+defaults (NOT skipped) | ✅ |
| or-default-join | LateralUnion with JoinVars preserved | ✅ |
| SubqueryPattern (correlated) | LateralJoin | ✅ |
| SubqueryPattern (uncorrelated) | LateralJoin (no correlation vars) | ✅ |

### Fixed issues (2026-03-16)

8. **`compileOrJoin` discarded join variables** — added `JoinVars` field to
   `Union` node. `compileOrJoin` now stores the user-specified join vars on
   the Union, and `decompileUnion` emits `OrJoinClause` when join vars are
   present. Round-trip preserves `or-join` vs `or`.

9. **Double join in `compileOrFallback` + `compileOrFallbackExclusive`** —
   `compileOrFallbackExclusive` was returning `joinWith(current, union)`, then
   the caller was joining again, creating `Join(current, Join(current, Union))`.
   Fixed by having `compileOrFallbackExclusive` return just the Union node.

10. **Vector default limitation in get-else rewrite** — added `DefaultAttr
   *datalog.Keyword` to `Join` node. The get-else rewrite stores the attribute
   on the LeftOuterJoin. The decompiler emits `get-else` (not `ground`) in the
   default branch when the attribute is present, so `TypedDefaulter` can
   produce the schema-typed default (e.g., `[]string` instead of `[]interface{}`)
   at execution time. Vector defaults now benefit from Rule 5's scan rewrite.

11. **Defensive fallback in `decompileLeftOuterJoin`** — LeftOuterJoin without
   defaults emitted a meaningless single-branch OrClause. Now returns an error:
   this state cannot occur (decorrelation produces InnerJoin when no defaults).

12. **Defensive fallback in `fromParseNode`** — parse node without
   TransformedValue returned a nil-Data Node that would panic on use. Now
   panics immediately with a descriptive message: every algebra node is created
   with TransformedValue set.
