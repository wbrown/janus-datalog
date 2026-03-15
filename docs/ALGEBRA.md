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

**Decompile:** `Join(LeftOuter, defaults=[0])(left, right)` →

```
decompile(left) ++
(or <decompile(right)>
    <ground defaults>)
```

Left side clauses followed by an OR-fallback:
- Branch 1: decompiled right side (the Aggregate → SubqueryPattern)
- Branch 2: ground expressions for each default value

The OR-fallback evaluates per outer tuple. Branch 1 returns all subquery results;
`filterBranchToOuterTuple` matches to the current outer tuple on shared symbols.
Non-matching tuples get branch 2 (defaults).

**Round-trip invariant:** Left clauses preserved. Right side becomes an OR-fallback
SubqueryPattern. Default values map to ground expressions. Join symbols become
the shared symbols between the outer relation and the SubqueryPattern binding.

**Test:** Build LeftOuterJoin(defaults=[0]) over an outer Scan and an inner
Aggregate. Decompile. Verify: outer DataPattern + OR clause with SubqueryPattern
branch and ground default branch.

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

**Compile:** `(or [?e :a ?x] [?e :b ?x])` (pattern-only OR) → `Union(output=[?e, ?x])(branch1, branch2)`

**Decompile:** `Union()(children...)` → `(or <decompile(child1)> <decompile(child2)> ...)`

**Round-trip invariant:** Each branch's clauses preserved inside an OrClause.

**Test:** Compile pattern-only OR, decompile, assert OrClause with correct branches.

### LateralJoin

**Compile:** `(or [(q [...] $ ?x) [[?count]]] [(ground 0) ?count])` →

```
LateralJoin(
  correlationVars=[?x],
  innerQuery=[:find (count ?t) :in $ ?s :where ...],
  binding=[[?count]],
  defaults=[0]
)(outer)
```

**Decompile:** `LateralJoin(defaults=[0])(outer)` →

```
decompile(outer) ++
(or [(q <innerQuery> $ ?x) <binding>]
    [(ground 0) ?count])
```

Without defaults: `decompile(outer) ++ [(q <innerQuery> $ ?x) <binding>]`

**Round-trip invariant:** Inner query, binding form, correlation variables, and
default values all preserved. Correlation variables appear as SubqueryPattern
inputs. Defaults produce OR-fallback with ground branch.

**Test:** Compile an OR-fallback with correlated subquery + ground, decompile,
assert original clause structure.

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

### Rule 2: Predicate Pushdown (future)

```
σ_p(R ⋈ S) → σ_p(R) ⋈ S     when p references only R's symbols
```

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
| `(or [subq] [ground])` | LateralJoin(outer, inner, defaults) |
| `[(q [...] $ ?x) binding]` correlated | LateralJoin |
| `[(q [...] $) binding]` uncorrelated | Join(outer, inner) |
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

### Phase 2: Decorrelation Transform

With proven round-trip mappings:

1. **Implement the transform** using only standard operators:
   - Input: `LateralJoin(correlationVars, innerQuery, binding, defaults)`
   - Output: `Join(LeftOuter, correlationVars, defaults) [outer, Aggregate(groupBy, fns)(inner)]`
2. **Verify**: `TestCorrelatedSubqueryAlgebraOptimizer` passes (194x speedup)
3. **Verify**: `TestCorrelatedSubqueryAlgebraOptimizerWithDefaults` passes (75/75 results)
4. **Verify**: production profiler returns 75 scenarios

### Phase 3: Production Validation

1. Run profiler against `concurrent2.db` with `--optimize`
2. Verify 75 scenarios returned (correctness)
3. Measure wall time improvement (target: 29s → <5s)
4. Full regression: `go test ./datalog/...` passes
