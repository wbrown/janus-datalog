# Proposal: Recursive Rules

## Summary

Extend janus-datalog with recursive rules using Datalog-style fixpoint semantics. This enables declarative graph traversal, transitive closure computation, and recursive pattern matching without Go-side iteration.

## Motivation

Many real-world queries require transitive closure or recursive graph traversal:

- **Organizational hierarchies**: "Find all reports under this manager"
- **Dependency graphs**: "What are all transitive dependencies of this module?"
- **Social networks**: "Find all friends-of-friends within N degrees"
- **Ontologies**: "Find all subclasses of this type"

Currently, users must implement recursion in Go:

```go
// Manual recursive traversal - error-prone, inefficient
func findAllDeps(db *Database, root string, visited map[string]bool) []string {
    if visited[root] {
        return nil
    }
    visited[root] = true

    deps, _ := db.ExecuteQueryWithInputs(
        `[:find ?dep :in $ ?mod :where [?mod :module/depends-on ?dep]]`,
        root)

    var all []string
    for _, dep := range deps {
        all = append(all, dep[0].(string))
        all = append(all, findAllDeps(db, dep[0].(string), visited)...)
    }
    return all
}
```

With recursive rules:

```clojure
;; Declare the rule once
[[(depends-on* ?mod ?dep)
  [?mod :module/depends-on ?dep]]
 [(depends-on* ?mod ?dep)
  [?mod :module/depends-on ?mid]
  (depends-on* ?mid ?dep)]]

;; Query uses the rule - engine handles recursion
[:find ?dep
 :in $ % ?root
 :where (depends-on* ?root ?dep)]
```

## Design

### Syntax

Rules follow Datomic's syntax - a vector of rule definitions passed via `:in %`:

```clojure
;; Rule set (passed as % input)
[;; Base case: direct parent
 [(ancestor ?x ?y)
  [?x :person/parent ?y]]

 ;; Recursive case: parent of ancestor
 [(ancestor ?x ?z)
  [?x :person/parent ?y]
  (ancestor ?y ?z)]]

;; Query using rules
[:find ?ancestor
 :in $ % ?person
 :where (ancestor ?person ?ancestor)]
```

**Rule structure:**
```
[(rule-name ?arg1 ?arg2 ...)   ; Head - defines the derived relation
 clause1                        ; Body - conditions that derive the head
 clause2
 ...]
```

**Multiple clauses for same rule** define alternatives (logical OR):
```clojure
[[(reachable ?x ?y) [?x :edge ?y]]           ; Direct edge
 [(reachable ?x ?z) [?x :edge ?y] (reachable ?y ?z)]]  ; Transitive
```

### Rule Invocation

Rules are invoked like patterns but with parentheses:

```clojure
[:find ?e
 :in $ %
 :where [?e :type :node]
        (reachable ?e ?target)    ; Rule invocation
        [?target :name "goal"]]
```

### Semantics: Bottom-Up Evaluation

Rules use **bottom-up (forward-chaining)** evaluation with **semi-naive** optimization:

1. **Initialize**: Start with base facts from the database
2. **Iterate**: Apply rules to derive new facts
3. **Repeat**: Continue until no new facts are derived (fixpoint)
4. **Semi-naive**: On each iteration, only consider tuples derived in the previous iteration to avoid redundant computation

```
Iteration 0: Base facts from DB
Iteration 1: Apply rules using Δ₀ (new facts)
Iteration 2: Apply rules using Δ₁
...
Iteration n: Δₙ = ∅, fixpoint reached
```

### Stratification (Handling Negation)

Negation in recursive rules requires **stratification** - organizing rules into layers where negation only refers to lower strata:

```clojure
;; Stratum 1: reachable (positive recursion)
[[(reachable ?x ?y) [?x :edge ?y]]
 [(reachable ?x ?y) [?x :edge ?z] (reachable ?z ?y)]]

;; Stratum 2: unreachable (negation of stratum 1)
[[(unreachable ?x ?y)
  [?x :type :node]
  [?y :type :node]
  (not (reachable ?x ?y))]]   ; Negation OK - reachable fully computed
```

**Stratification algorithm:**
1. Build dependency graph between predicates
2. Mark edges through negation as "negative"
3. Find strongly connected components (SCCs)
4. If any SCC contains a negative edge → **unstratifiable** (error)
5. Topologically sort SCCs into strata
6. Evaluate strata in order

### Go API

```go
// Define rules as EDN string
rules := `[[(ancestor ?x ?y) [?x :parent ?y]]
           [(ancestor ?x ?z) [?x :parent ?y] (ancestor ?y ?z)]]`

// Parse rules
ruleSet, err := datalog.ParseRules(rules)

// Execute query with rules
results, err := db.ExecuteQueryWithInputs(
    `[:find ?a :in $ % ?p :where (ancestor ?p ?a)]`,
    ruleSet,  // Passed as % input
    personID,
)
```

### Query Builder Integration

```go
// Define rules programmatically
ancestorRule := qb.DefineRule("ancestor", x, y).
    Where(qb.Pat(x, Parent, y))

ancestorRecursive := qb.DefineRule("ancestor", x, z).
    Where(
        qb.Pat(x, Parent, y),
        qb.Rule("ancestor", y, z),
    )

rules := qb.RuleSet(ancestorRule, ancestorRecursive)

// Use in query
q := qb.Query().
    Find(a).
    In(qb.DB, rules, qb.Scalar(person)).
    Where(qb.Rule("ancestor", person, a)).
    MustBuild()
```

## Integration with Relational Algebra

The rule evaluator produces a **materialized relation** of derived facts, which then participates in normal RA operations:

```
[base facts from storage]
    → [fixpoint engine: derives (ancestor ?x ?y) relation]
    → [RA executor: joins derived relation with other patterns]
```

This hybrid approach:
- Preserves the existing RA execution model for non-recursive queries
- Encapsulates fixpoint iteration within rule evaluation
- Allows derived relations to compose with other patterns via standard joins

## Implementation Plan

### Phase 1: Rule Parsing and Representation

1. Extend EDN parser to recognize rule syntax `[(name ?args) clauses...]`
2. Add `query.Rule` and `query.RuleSet` types
3. Add `RulesInput` to input spec types
4. Parse `%` in `:in` clause as rules input

**Files:**
- `datalog/query/rules.go` - Rule types
- `datalog/parser/rules.go` - Rule parsing
- `datalog/parser/parser.go` - Integrate rules into query parsing

### Phase 2: Rule Evaluation Engine

1. Implement bottom-up evaluation with semi-naive optimization
2. Implement stratification analysis
3. Add rule invocation pattern type
4. Integrate with existing executor

**Files:**
- `datalog/executor/rules_evaluator.go` - Fixpoint computation
- `datalog/executor/stratify.go` - Stratification analysis
- `datalog/executor/query_executor.go` - Integration

### Phase 3: Query Builder Extensions

1. Add `qb.DefineRule()` and `qb.RuleSet()`
2. Add `qb.Rule()` for rule invocations in Where
3. Add `qb.Rules` input spec

**Files:**
- `datalog/qb/rules.go` - Rule builder
- `datalog/qb/builder.go` - Integration

## Examples

### Example 1: Bill of Materials

Find all components (recursively) needed to build a product:

```clojure
;; Rules
[[(component* ?product ?part)
  [?product :product/contains ?part]]
 [(component* ?product ?part)
  [?product :product/contains ?sub]
  (component* ?sub ?part)]]

;; Query: all parts for product P100
[:find ?part-name ?quantity
 :in $ % ?product-id
 :where [?product :product/id ?product-id]
        (component* ?product ?part)
        [?part :part/name ?part-name]
        [?part :part/quantity ?quantity]]
```

### Example 2: Graph Reachability

Find all nodes reachable from a starting point:

```clojure
[[(reachable ?from ?to)
  [?from :node/edge ?to]]
 [(reachable ?from ?to)
  [?from :node/edge ?mid]
  (reachable ?mid ?to)]]

[:find ?node
 :in $ % ?start
 :where (reachable ?start ?node)]
```

### Example 3: Type Hierarchy

Find all subtypes of a given type:

```clojure
[[(subtype* ?child ?parent)
  [?child :type/extends ?parent]]
 [(subtype* ?child ?ancestor)
  [?child :type/extends ?parent]
  (subtype* ?parent ?ancestor)]]

[:find ?type
 :in $ % ?base
 :where (subtype* ?type ?base)]
```

## Performance Considerations

### Rule Evaluation

- **Semi-naive evaluation** avoids redundant derivations
- **Indexing derived facts** enables efficient lookup during recursion
- **Magic sets optimization** (future) can push filters into recursive evaluation
- **Tabling/memoization** caches intermediate results

### Memory Management

- **Incremental cleanup** releases intermediate results when no longer needed
- **Bounded recursion** can limit depth to prevent runaway evaluation
- Derived relations are materialized (unavoidable for fixpoint detection)

## Future Extensions

### 1. Recursive Aggregation

Support aggregation within recursive rules:

```clojure
[(shortest-path ?from ?to ?dist)
 [?from :edge ?to ?d]
 [(identity ?d) ?dist]]
[(shortest-path ?from ?to ?dist)
 [?from :edge ?mid ?d1]
 (shortest-path ?mid ?to ?d2)
 [(+ ?d1 ?d2) ?dist]
 [(min ?dist)]]  ; Aggregate to keep only shortest
```

### 2. Rule Compilation

Compile frequently-used rules to optimized Go code for maximum performance.

### 3. Incremental View Maintenance

When base facts change, incrementally update derived facts rather than recomputing from scratch.

## References

1. "What You Always Wanted to Know About Datalog" - Ceri, Gottlob, Tanca (1989)
2. "Efficiently Implementing Datalog with Magic Sets" - Bancilhon et al. (1986)
3. Datomic Rules Documentation - https://docs.datomic.com/pro/query/query.html#rules
4. "Datalog and Recursive Query Processing" - Green, Huang, Loo, Zhou (2013)
5. Souffle Datalog Engine - https://souffle-lang.github.io/

## Appendix: Stratification Example

**Unstratifiable (error):**
```clojure
[(p ?x) (not (q ?x))]
[(q ?x) (not (p ?x))]  ; Circular through negation!
```

**Stratifiable:**
```clojure
;; Stratum 1
[(base ?x) [?x :type :node]]

;; Stratum 2 (depends on stratum 1)
[(derived ?x) (base ?x) (not (excluded ?x))]

;; Stratum 3 (depends on stratum 2)
[(final ?x) (derived ?x) [?x :status :active]]
```
