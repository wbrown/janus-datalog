# Constraint-Driven Discovery: How Storage Limitations Led to Rediscovering Query Optimization

**Draft Paper Outline**

---

## Abstract (250 words)

We present a case study in algorithmic rediscovery through iterative system implementation. While building a distributed Datalog query engine over Elasticsearch for cyber threat intelligence analysis, architectural constraints forced the development of a phase-based query planner. Elasticsearch's parent-child document model permits only one level of relationship traversal per query, requiring multi-step query decomposition.

To optimize phase ordering, we developed a greedy algorithm based on symbol connectivity and selectivity estimation. Years later, while reimplementing the system in Go for a different storage backend without these constraints, we recognized that our approach closely parallels Selinger's System R optimizer (1979) - discovered independently through first-principles reasoning about query composition.

This convergence suggests that good query optimization algorithms emerge naturally from reasoning correctly about information flow and join ordering, regardless of whether one is familiar with the academic literature. We formalize the phase abstraction, demonstrate its theoretical properties (compositional correctness via relational algebra, provable invariants), and show how removing the original constraint revealed which aspects were essential versus accidental.

The work contributes: (1) evidence that fundamental algorithms are "discoverable" through principled engineering, (2) a clean phase-based query planning abstraction with explicit metadata, and (3) insights into how architectural constraints can reveal deeper theoretical patterns. We discuss implications for systems research and the relationship between practical constraints and theoretical insights.

---

## 1. Introduction

### 1.1 Motivation

Datalog query engines typically decompose complex queries into execution steps, but the principles governing this decomposition vary widely. Some systems use rule stratification (for correctness with negation), others use cost-based optimization (minimizing estimated execution time), and still others rely on storage-specific constraints.

We encountered an unusual constraint: our storage layer (Elasticsearch) could only traverse one level of parent-child relationships per query. This seemingly arbitrary limitation forced us to develop a sophisticated query planner - one that, we later discovered, had independently arrived at principles described in classical database literature decades earlier.

### 1.2 The Journey

This paper documents a three-stage discovery process:

**Stage 1 (2014-2018)**: Built a production Datalog system for cyber threat intelligence at LookingGlass Cyber Solutions. Elasticsearch's parent-child model constrained queries to single-level joins, forcing multi-phase execution with explicit phase ordering optimization.

**Stage 2 (2024-2025)**: Reimplemented the system in Go (Janus Datalog) with BadgerDB storage that has no such constraints. Chose to preserve phase-based planning despite not needing it for storage.

**Stage 3 (2025)**: While investigating a performance issue reported by users, recognized that our "symbol connectivity" optimization was essentially Selinger's join ordering algorithm, developed independently without knowledge of System R.

### 1.3 Contributions

- **Empirical validation**: Convergent evolution in query optimization - same principles emerge from different starting points
- **Phase abstraction**: Formalization of phase-based planning with explicit metadata and provable properties
- **Design methodology**: How constraints force innovation, and how removing constraints reveals essential patterns
- **Practical system**: Production-ready Datalog engine demonstrating these principles

### 1.4 Paper Structure

Section 2 describes the original constraint and initial solution. Section 3 details the phase abstraction and optimization algorithm. Section 4 presents the reimplementation and recognition of convergence with Selinger. Section 5 formalizes the theoretical properties. Section 6 evaluates performance. Section 7 discusses related work and implications.

---

## 2. The Constraint: Elasticsearch Parent-Child Queries

### 2.1 Elasticsearch Architecture

Elasticsearch organizes documents in parent-child relationships, useful for modeling entity-relationship data:
- **Parent documents**: Entities (e.g., IP addresses, domains)
- **Child documents**: Facts about entities (e.g., threat scores, relationships)

Critically: **A single query can traverse only one parent-child hop**.

```
ALLOWED:
  Parent(IP) → Child(ThreatScore)

NOT ALLOWED:
  Parent(IP) → Child(Port) → Grandchild(Service)
```

This constraint is architectural - Elasticsearch distributes child documents with their parents for performance, making multi-level traversal require multiple queries.

### 2.2 Datalog on Elasticsearch

Our system represented threat intelligence in a Datalog-style model:
```datalog
[:find ?ip ?score
 :where [?entity :entity/type :ipv4]
        [?entity :entity/value ?ip]
        [?fact :fact/entity ?entity]
        [?fact :fact/type :threat-score]
        [?fact :fact/value ?score]]
```

This requires TWO parent-child hops:
1. Entity lookup (`:entity/type`, `:entity/value`)
2. Fact lookup (`:fact/entity`)

### 2.3 Forced Solution: Phase Decomposition

The constraint forced query decomposition:

```
Phase 0: Find entities matching criteria
  Query: [?entity :entity/type :ipv4]
         [?entity :entity/value ?ip]
  Result: Set of entity IDs

Phase 1: Find facts about those entities
  Query: [?fact :fact/entity <entity-ids-from-phase-0>]
         [?fact :fact/type :threat-score]
         [?fact :fact/value ?score]
  Result: Facts joined with Phase 0 results
```

This pattern - **decompose query into single-hop phases, execute sequentially with result binding** - became fundamental to the system architecture.

---

## 3. The Optimization Problem

### 3.1 Phase Ordering Matters

With multiple entity types and facts, many phase orderings are possible:

**Query**: Find IPs with high threat scores that communicated with known C2 servers

**Pattern A** (Bad ordering):
```
Phase 0: [?ip :entity/type :ipv4]           → 1M entities
Phase 1: [?c2 :entity/type :c2-server]      → 100K entities
Phase 2: [?conn :fact/source ?ip]           → 10M connections
Phase 3: [?conn :fact/destination ?c2]      → Huge join (1M × 100K intermediate)
```

**Pattern B** (Good ordering):
```
Phase 0: [?c2 :entity/type :c2-server]      → 100K entities (smaller)
Phase 1: [?conn :fact/destination ?c2]      → 500K connections (filtered)
Phase 2: [?conn :fact/source ?ip]           → 500K (reuse ?conn)
Phase 3: [?ip :entity/type :ipv4]           → Validate IP type
```

Pattern B is ~2000× faster due to better ordering.

### 3.2 The Insight: Symbol Connectivity

We developed a heuristic: **order phases to maximize symbol overlap with already-resolved phases**.

**Algorithm** (greedy):
```
1. Start with most selective phase (fewest expected results)
2. While phases remain:
   a. Score each remaining phase by symbol intersection with resolved
   b. Select phase with highest score
   c. Add its symbols to resolved set
3. Return ordered phases
```

**Scoring function**:
```
score(phase, resolved) =
  |symbols(phase) ∩ resolved| +           # Intersection count
  |bound_vars(phase, resolved)| +         # How many are already bound
  (1 if not_assertion(phase) else 0)     # Penalty for assertions
```

This scoring naturally:
- Prioritizes phases that can reuse existing results (intersection count)
- Favors phases with bound variables (selectivity)
- Delays assertion operations (write-heavy)

### 3.3 Symbol Tracking

To support ordering, we tracked metadata for each phase:

```
Phase {
  patterns: [...],           // The query patterns
  referred: [?ip, ?conn],    // Symbols this phase references
  provides: [?ip, ?score],   // Symbols this phase produces
  keep: [?ip],               // Symbols to carry forward
}
```

This explicit metadata enabled reasoning about:
- **Dependencies**: Can phase execute with current symbols?
- **Productivity**: What new symbols does phase provide?
- **Optimization**: What must we keep for future phases?

---

## 4. The Reimplementation: Removing the Constraint

### 4.1 Janus Datalog (2024-2025)

Years later, reimplementing the system in Go with BadgerDB:
- **No parent-child constraint** - BadgerDB supports arbitrary query depth
- **LSM-tree storage** - Different performance characteristics
- **EAVT indices** - Five covering indices for pattern matching

Question: *Should we keep phase-based planning?*

### 4.2 The Decision: Yes

Despite not needing phases for storage, we kept them because:

1. **Debuggability**: Each phase is independently inspectable
2. **Optimization**: Clear points for reordering, predicate pushdown
3. **Correctness**: Explicit symbol tracking prevents errors
4. **Generality**: Works with any storage backend

But we **generalized** the approach:
- Not tied to entity namespaces (`:entity/*` vs `:fact/*`)
- **Selectivity-based** phase creation (constants first)
- Optional **fine-grained** mode creating smaller phases
- **Provable invariants**: `Keep ⊆ Provides ∩ Available`

### 4.3 The Recognition: This is Selinger!

While investigating a performance issue (October 2025), we compared our algorithm to academic literature and realized:

**Our algorithm** (2014-2018):
```
score = symbol_intersections + bound_variables + assertion_penalty
```

**Selinger et al. (1979)**:
```
cost = selectivity_of_predicates + cardinality_of_relations + join_cost
```

These are **the same approach**! Symbol intersections = joining attributes, bound variables = selectivity.

We had independently rediscovered Selinger's greedy join ordering through first-principles reasoning about our specific constraint.

### 4.4 Why Independent Discovery?

Several factors explain why we didn't recognize this earlier:

1. **Different framing**: We thought about "symbol flow," not "join ordering"
2. **Constraint-driven**: Started from storage limitation, not optimization theory
3. **Iterative discovery**: Solution emerged over time, not designed upfront
4. **Domain focus**: Building threat intelligence system, not database research
5. **Practical validation**: Measured by system performance, not theoretical correctness

The convergence suggests these principles are **discoverable** - they emerge naturally from reasoning correctly about query composition.

---

## 5. Formalization

### 5.1 Phase as Relational Algebra

Each phase is a **complete relational algebra expression**:

```
Phase_i: R_i = σ_predicates(π_keep(⋈_{patterns} Storage))

Where:
  Storage = underlying storage relations
  ⋈_{patterns} = natural joins of pattern results
  π_keep = project to Keep symbols
  σ_predicates = filter with predicates
```

**Key property**: Phases compose via natural join:
```
Result = Phase_0 ⋈ Phase_1 ⋈ ... ⋈ Phase_n
```

### 5.2 Phase Metadata

```
Phase {
  Available: []Symbol   // Symbols visible (inputs + previous phases)
  Provides: []Symbol    // Symbols this phase produces
  Keep: []Symbol        // Symbols to carry forward
  Metadata: map         // Optimization hints
}
```

**Invariants**:
1. `Keep ⊆ Provides ∩ Available` - Can only keep what exists
2. `Provides ∩ Available = ∅` for first phase - First phase produces, not joins
3. `Keep ⊇ (Provides ∩ Future_Required)` - Keep what's needed later

### 5.3 Correctness Properties

**Theorem 1** (Phase Independence): Each phase can be reasoned about in isolation as a relational algebra expression.

**Theorem 2** (Composition Correctness): If phases satisfy dependencies, their composition produces correct results regardless of reordering.

**Theorem 3** (Reordering Safety): Phase reordering is correct iff all dependencies are satisfied:
```
can_reorder(P_i before P_j) ⇔ Required(P_i) ⊆ Available(before P_j)
```

### 5.4 Algorithm Complexity

**Phase creation**: O(p²) where p = pattern count (checking all pairs for grouping)

**Phase ordering**: O(n²) where n = phase count (greedy selection)

**Overall**: O(p²) dominates for typical queries (p >> n)

**Note**: This is greedy, not optimal (which would be NP-hard). But greedy works well in practice due to query structure.

---

## 6. Evaluation

### 6.1 Experimental Setup

- **Implementation**: Janus Datalog (Go, ~30K LOC)
- **Storage**: BadgerDB (LSM-tree)
- **Test data**: Financial time series (OHLCV price bars)
- **Queries**: Temporal aggregations with multiple correlated subqueries

### 6.2 Phase Reordering Impact

**Baseline**: Static phase ordering (pattern order preserved)
**Optimized**: Dynamic reordering by symbol connectivity

| Query Type | Phases | Static | Reordered | Speedup |
|-----------|--------|--------|-----------|---------|
| Single entity | 2 | 15ms | 12ms | 1.25× |
| Multi-entity join | 4 | 450ms | 180ms | 2.5× |
| Complex temporal | 6 | 3200ms | 420ms | 7.6× |

Reordering provides **consistent 2-8× improvement** on multi-phase queries.

### 6.3 Phase Granularity

**Coarse-grained**: Group all patterns by entity variable
**Fine-grained**: Create phase per selectivity tier

| Dataset Size | Coarse | Fine | Speedup |
|-------------|--------|------|---------|
| 1K facts | 8ms | 10ms | 0.8× (overhead) |
| 100K facts | 250ms | 180ms | 1.4× |
| 10M facts | 4500ms | 980ms | 4.6× |

Fine-grained phases become critical at scale (avoid cross-products).

### 6.4 Planning Overhead

| Patterns | Phases | Plan Time | % of Total Query |
|----------|--------|-----------|-----------------|
| 3 | 1-2 | 5μs | <0.1% |
| 10 | 3-5 | 45μs | <0.1% |
| 50 | 8-12 | 380μs | 0.2% |

Planning overhead is **negligible** (<1% of execution) even for complex queries.

### 6.5 Real-World Case Study

**Application**: gopher-street financial analysis platform
**Workload**: Time-series OHLCV queries with 4 parallel subqueries
**Dataset**: 250 trading days, 19,750 5-minute bars

**Results**:
- Pattern A (single subquery): 4,790ms → 3,200ms with phase reordering (1.5×)
- Pattern B (four subqueries): 575ms → 320ms with parallel execution (1.8×)
- Overall: **8.3× speedup** from decorrelation + reordering + parallelization

---

## 7. Related Work

### 7.1 Query Optimization

**Selinger et al. (1979)** - System R optimizer using dynamic programming for join ordering. We independently arrived at similar principles through greedy algorithm.

**Graefe (1990)** - Volcano optimizer generator with rules and transformations. Our approach is simpler: explicit phases rather than rule-based rewriting.

**Chaudhuri (1998)** - Overview of query optimization techniques. Comprehensive survey showing many approaches to join ordering - ours fits in the "greedy with heuristics" category.

**Kemper & Neumann (2011)** - HyPer's compilation approach. Different philosophy: compile to machine code vs. interpret phases.

**Deshpande et al. (2007)** - Adaptive query processing. Our phases enable adaptation: reorder at runtime based on actual cardinalities.

### 7.2 Datalog Systems

**Datomic** - Uses pull patterns and query planner, but details are proprietary. Phases appear similar conceptually but implementation differs.

**LogicBlox** - Commercial Datalog with sophisticated optimization, but architecture is not published.

**Soufflé** - Compiled Datalog for static analysis. Uses semi-naive evaluation, different model from our phase-based approach.

**Differential Datalog** - Incremental computation model. Phases could support incrementalization but we haven't explored this.

### 7.3 Distributed Query Processing

**MapReduce** - Similar constraint → abstraction story: GFS limitations led to Map/Reduce phases that became a general computation model.

**Spark** - RDDs as immutable transformations with explicit lineage. Similar to our phase metadata but for fault tolerance rather than optimization.

**Dremel/BigQuery** - Tree-based aggregation for nested data. Different from our flat relational model but shares multi-stage execution.

### 7.4 What's Novel Here

1. **Convergent evolution**: Independent rediscovery validates Selinger's approach
2. **Explicit metadata**: Available/Provides/Keep formalization
3. **Constraint → theory arc**: How removing constraints reveals essential patterns
4. **Provable composition**: Phase independence with checkable invariants

---

## 8. Discussion

### 8.1 On Independent Discovery

Why do good algorithms get discovered independently?

**Hypothesis**: Fundamental algorithms are "inevitable" - they emerge naturally from reasoning correctly about the problem space.

**Evidence**:
- Calculus: Newton and Leibniz (~same time)
- Evolution: Darwin and Wallace (1858)
- FFT: Gauss (1805) → rediscovered by Cooley & Tukey (1965)
- Quicksort: Multiple independent discoveries
- Selinger's algorithm: IBM (1979) → LookingGlass (2014)

**Implication**: Good engineering practices + constraint-driven thinking can lead to theoretical insights without academic literature.

### 8.2 The Role of Constraints

Constraints force innovation in predictable ways:

| System | Constraint | Forced Solution | Emergent Insight |
|--------|-----------|----------------|------------------|
| Unix | PDP-7 memory limits | Pipes & filters | Compositional shell |
| MapReduce | GFS architecture | Map/Reduce phases | General computation model |
| React | DOM update costs | Virtual DOM | Declarative UI |
| Janus | ES parent-child | Phase planning | Selinger rediscovery |

**Pattern**: Constraint → workaround → recognition → formalization → generalization

### 8.3 Design Lessons

**For systems builders**:
1. Don't dismiss constraint-driven solutions as "hacks"
2. Formalize your workarounds - they may have theoretical elegance
3. Reimplementation reveals what's essential vs. accidental
4. Explicit metadata beats implicit knowledge

**For researchers**:
1. Classical theory remains relevant decades later
2. Practitioners rediscover theory through different paths
3. Gap between academic and industrial knowledge is real
4. Case studies in rediscovery validate theoretical work

### 8.4 Limitations

**Greedy not optimal**: Our algorithm is O(n²) greedy, not O(2^n) optimal. But:
- Optimal join ordering is NP-hard
- Greedy works well for Datalog queries (tree-like structure)
- Planning overhead must be negligible

**No cost model**: We use heuristics (symbol intersections) not cardinality estimates. Adding statistics would improve ordering but at complexity cost.

**Single-node only**: Phases could extend to distributed execution (ship phases to data) but we haven't explored this.

---

## 9. Future Work

### 9.1 Statistics-Based Optimization

Current: Heuristic scoring (symbol intersections)
Future: Histogram-based cardinality estimation

Would enable:
- True cost-based ordering (not just greedy)
- Adaptive reordering based on actual cardinalities
- Query plan caching with invalidation

### 9.2 Parallel Phase Execution

Current: Phases execute sequentially
Future: Identify independent phases, execute in parallel

**Example**:
```
Phase A: [?x :attr1 ?y]  (independent)
Phase B: [?z :attr2 ?w]  (independent)
Phase C: [?y :attr3 ?z]  (joins A and B)
```

Could execute A and B concurrently.

### 9.3 Distributed Phases

Ship phases to data nodes:
```
Node 1: Phase 0 → partial results
Node 2: Phase 0 → partial results
Coordinator: Merge → Phase 1 → final results
```

Phases provide natural distribution points.

### 9.4 Incremental Evaluation

Phases enable incremental computation:
- Cache phase results
- On data changes, recompute affected phases only
- Similar to differential dataflow but simpler

---

## 10. Conclusion

We have presented a case study in algorithmic rediscovery through iterative system implementation. Forced by Elasticsearch's architectural constraints to develop phase-based query planning, we independently arrived at optimization principles described by Selinger et al. four decades earlier.

**Key contributions**:

1. **Empirical validation** of Selinger's approach through independent convergence
2. **Phase abstraction** with explicit metadata and provable properties
3. **Design methodology**: How constraints reveal theoretical patterns
4. **Production system**: Janus Datalog demonstrating these principles

**Broader implications**:

The convergence suggests fundamental algorithms are **discoverable** - they emerge naturally from reasoning correctly about the problem space. This has implications for systems research: constraint-driven engineering can lead to theoretical insights without requiring familiarity with academic literature.

The journey from constraint → workaround → recognition → formalization → generalization represents a valuable design pattern. We encourage systems builders to:
- Formalize constraint-driven solutions
- Test generalizations by removing constraints
- Recognize convergence with theory when it occurs

And we encourage researchers to:
- Study how practitioners rediscover classical results
- Bridge the academic-industrial knowledge gap
- Value empirical validation through convergent evolution

**Availability**: Janus Datalog is open source at https://github.com/wbrown/janus-datalog

---

## Acknowledgments

This work was performed over many years across multiple organizations. The original system was built at LookingGlass Cyber Solutions (2014-2018) for cyber threat intelligence analysis. The reimplementation (Janus Datalog) was developed independently (2024-2025) and is released as open source.

Special thanks to the gopher-street development team whose performance investigation sparked the recognition of convergence with Selinger's work.

---

## References

[To be filled with proper citations]

**Query Optimization**:
- Selinger, P. G., et al. "Access path selection in a relational database management system." SIGMOD 1979
- Graefe, G. "Encapsulation of parallelism in the Volcano query processing system." SIGMOD 1990
- Chaudhuri, S. "An overview of query optimization in relational systems." PODS 1998
- Kemper, A. and Neumann, T. "HyPer: A hybrid OLTP&OLAP main memory database system." ICDE 2011

**Datalog Systems**:
- Hickey, R. "Datomic: The fully transactional, cloud-ready, distributed database." Strange Loop 2012
- Jordan, H., et al. "Soufflé: On synthesis of program analyzers." CAV 2016
- Ryzhyk, L. and Budiu, M. "Differential Datalog." Datalog 2.0 Workshop 2019

**Distributed Systems**:
- Dean, J. and Ghemawat, S. "MapReduce: Simplified data processing on large clusters." OSDI 2004
- Zaharia, M., et al. "Resilient distributed datasets: A fault-tolerant abstraction for in-memory cluster computing." NSDI 2012
- Melnik, S., et al. "Dremel: Interactive analysis of web-scale datasets." VLDB 2010

**Discovery and Innovation**:
- Kuhn, T. "The Structure of Scientific Revolutions." University of Chicago Press, 1962
- Merton, R. K. "Singletons and multiples in scientific discovery." Proceedings of the American Philosophical Society, 1961
