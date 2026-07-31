# Janus Query System: A Game Engine Programmer's Guide

**TLDR for people who know job graphs and entity systems but not SQL.**

## The Core Problem

You have a query like:
```clojure
[:find ?name ?damage
 :where
 [?player :player/weapon ?weapon]
 [?weapon :weapon/damage ?damage]
 [?player :player/name ?name]
 [(> ?damage 50)]]
```

**Questions the engine must answer:**
1. What order to execute these clauses?
2. How to combine results from different patterns?
3. When to apply filters?
4. How to avoid exploding memory?

---

## Think: Job Graph with Data Dependencies

Each clause is a **job** that:
- **Requires** certain symbols (variables) to exist
- **Provides** new symbols to downstream jobs

```
[?player :player/weapon ?weapon]  → Provides: ?player, ?weapon
                                    Requires: nothing

[?weapon :weapon/damage ?damage]  → Provides: ?damage
                                    Requires: ?weapon (need to know WHICH weapon)

[(> ?damage 50)]                  → Provides: nothing (just filters)
                                    Requires: ?damage
```

**This IS a dependency graph.** The planner topologically sorts it.

---

## The Execution Pipeline

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   PLANNER   │ ──▶ │    PLAN     │ ──▶ │  EXECUTOR   │
│ (offline)   │     │ (data)      │     │ (runtime)   │
└─────────────┘     └─────────────┘     └─────────────┘
     │                    │                   │
     ▼                    ▼                   ▼
  Greedy clause      Ordered list of      Stream through
  selection by       clauses with         clauses, joining
  scoring            symbol metadata      as we go
```

---

## Step 1: Clause Scoring (Think: Priority Queue)

The planner scores each clause type:

| Clause Type | Score | Game Analogy |
|-------------|-------|--------------|
| **Pattern** (data fetch) | +100 | Asset load — must happen first |
| **OR clause** | +80 | Alternative asset paths |
| **Expression** (compute) | +50 | Transform calculation |
| **Predicate** (filter) | +10 | Culling pass |
| **NOT clause** | +5 | Anti-join (remove matching) |
| **Subquery** | -50 | Nested query (expensive) |

**Greedy selection**: Pick highest-scoring clause that CAN execute (all required symbols available).

```python
# Pseudocode
available_symbols = set(inputs)

while remaining_clauses:
    best = max(remaining_clauses,
               key=lambda c: score(c) if can_execute(c, available_symbols) else -∞)

    execute(best)
    available_symbols |= best.provides
    remaining_clauses.remove(best)
```

**Game analogy**: Like a task scheduler that runs high-priority ready tasks first.

---

## Step 2: Symbol Flow (Think: Data Dependencies)

Symbols flow through the query like data through a job graph:

```
Phase 0: Available={}
  │
  ├─ Pattern [?e :name ?n]      → +{?e, ?n}
  ├─ Pattern [?e :age ?a]       → +{?a}  (already have ?e)
  └─ Predicate [(> ?a 18)]      → filters, adds nothing
  │
Phase 0: Provides={?e, ?n, ?a}
```

**Key insight**: A clause can only execute when ALL its required symbols are available.

---

## Step 3: Relation Collapsing (Think: Incremental Scene Merging)

After EACH clause, the executor asks: **"Can I join any of these results together?"**

```
After pattern 1:  Relations = [ [?e, ?name] ]
After pattern 2:  Relations = [ [?e, ?name], [?e, ?age] ]

                  Collapse! Both have ?e → join on ?e

                  Relations = [ [?e, ?name, ?age] ]

After pattern 3:  Relations = [ [?e, ?name, ?age], [?weapon, ?damage] ]

                  No shared symbols → keep separate (for now)
```

**Game analogy**: Like merging octree nodes when they share a boundary. You incrementally combine what you can.

---

## Why Join Eagerly?

**Memory explosion prevention.**

Consider:
- Pattern A returns 10,000 players
- Pattern B returns 50,000 weapons
- They share `?weapon` (player's equipped weapon)

**Bad (join last)**: Materialize 10K × 50K = 500M combinations, then filter.

**Good (join early)**: Join on `?weapon` immediately → only ~10K results (each player has one weapon).

**Game analogy**: Like frustum culling BEFORE detailed collision checks. Filter early, process less.

---

## The Collapse Algorithm

```go
func Collapse(relations []Relation) []Relation {
    groups := []Relation{}

    for _, rel := range relations {
        merged := false
        for i, group := range groups {
            if hasSharedSymbols(rel, group) {
                groups[i] = hashJoin(group, rel)  // Join on shared symbols
                merged = true
                break
            }
        }
        if !merged {
            groups = append(groups, rel)  // Keep separate
        }
    }
    return groups
}
```

**Result**: Multiple "disjoint groups" if relations don't share symbols.

**Error case**: If disjoint groups remain at the end → query would require Cartesian product → reject with error.

---

## Join Strategy: Hash Join

When joining two relations on shared symbols:

```
Relation A: [?e, ?name]     (10,000 tuples)
Relation B: [?e, ?age]      (10,000 tuples)
Join on: ?e

Algorithm:
1. Build hash table from SMALLER relation
   hash[?e] → [?name tuples...]

2. Probe with LARGER relation
   for each (?e, ?age) in B:
       lookup hash[?e]
       emit combined tuples

Complexity: O(n + m) instead of O(n × m)
```

**Game analogy**: Like spatial hashing for collision detection. Build grid from static objects, probe with dynamic objects.

---

## Predicate Timing: Apply ASAP

Predicates (filters) execute as soon as their required symbols exist:

```
Pattern 1: [?e :age ?a]          → Available: {?e, ?a}
Predicate: [(> ?a 18)]           → Can execute now! Filter immediately.
Pattern 2: [?e :salary ?s]       → Now works on pre-filtered set
```

**Why not wait?**
- Filtering 10K tuples to 1K is cheap
- Subsequent patterns only process 1K tuples instead of 10K
- Memory stays bounded

**Game analogy**: Like early-out in raymarching. Check cheap bounds before expensive SDF evaluation.

---

## Index Selection: Choosing the Right Scan

When executing a pattern, the storage layer picks the best index:

```
Pattern: [?e :weapon/damage 50]
         ↑       ↑           ↑
      variable  bound      bound

Best index: AVET (Attribute → Value → Entity → Tx)
Scan: All entities where :weapon/damage = 50
```

| What's Bound | Best Index | Why |
|--------------|------------|-----|
| Entity | EAVT | Start with entity, scan its attrs |
| Attribute | AEVT | Start with attr, scan all entities with it |
| Attr + Value | AVET | Direct lookup by attr+value pair |
| Value only | VAET | Rare, but handles "who references X?" |
| Transaction | TAEV | Time-based queries |

**Game analogy**: Like choosing between octree (spatial) and component array (by type) based on query shape.

---

## Binding: Pushing Known Values Into Patterns

When a pattern executes, it can USE values from previous results:

```
Step 1: [?player :player/weapon ?weapon]
        Result: [(player_1, sword_5), (player_2, axe_3), ...]

Step 2: [?weapon :weapon/damage ?damage]

        For each tuple from Step 1:
          Execute pattern with ?weapon = sword_5
          Execute pattern with ?weapon = axe_3
          ...

        This is MUCH faster than scanning ALL weapons
```

**Which binding relation to use?** Score by selectivity:
- Entity position (E): +1000 (most selective)
- Attribute position (A): +100
- Value position (V): +10
- Tie-breaker: prefer smaller relation

**Game analogy**: Like instance culling. Don't check every instance—use the spatial structure to find candidates.

---

## Early Termination: Stop When Empty

If ANY clause produces zero results, stop immediately:

```
Pattern 1: [?e :type :unicorn]     → 0 results (no unicorns!)

STOP. Don't bother with remaining patterns.
Return empty result set.
```

**Game analogy**: Like occlusion culling. If a portal sees nothing, don't traverse what's behind it.

---

## Complete Example Walkthrough

**Query**:
```clojure
[:find ?name ?damage
 :where
 [?player :player/weapon ?weapon]
 [?weapon :weapon/damage ?damage]
 [?player :player/name ?name]
 [(> ?damage 50)]]
```

**Planning Phase**:
```
Available = {}

Round 1:
  Pattern [?player :player/weapon ?weapon] → Score 100, Requires {} ✓
  Pattern [?weapon :weapon/damage ?damage] → Score 100, Requires {?weapon} ✗
  Pattern [?player :player/name ?name]     → Score 100, Requires {} ✓
  Predicate [(> ?damage 50)]               → Score 10, Requires {?damage} ✗

  Pick first pattern (tie-breaker): [?player :player/weapon ?weapon]
  Available = {?player, ?weapon}

Round 2:
  Pattern [?weapon :weapon/damage ?damage] → Score 100, Requires {?weapon} ✓
  Pattern [?player :player/name ?name]     → Score 100, Requires {} ✓
  Predicate [(> ?damage 50)]               → Score 10, Requires {?damage} ✗

  Pick: [?weapon :weapon/damage ?damage] (or [?player :player/name ?name])
  Available = {?player, ?weapon, ?damage}

Round 3:
  Pattern [?player :player/name ?name]     → Score 100 ✓
  Predicate [(> ?damage 50)]               → Score 10 ✓

  Pick: [?player :player/name ?name]
  Available = {?player, ?weapon, ?damage, ?name}

Round 4:
  Predicate [(> ?damage 50)]               → Score 10 ✓

  Pick: predicate
  Done!
```

**Execution Phase**:
```
Step 1: [?player :player/weapon ?weapon]
        Index: AEVT (attribute bound)
        Result: Relation([?player, ?weapon], 10000 tuples)
        Groups: [Rel1]

Step 2: [?weapon :weapon/damage ?damage]
        Binding: Use Rel1's ?weapon values
        Index: EAVT per weapon (entity bound)
        Result: Relation([?weapon, ?damage], 10000 tuples)
        Groups: [Rel1, Rel2]
        Collapse: Rel1 ∩ Rel2 on ?weapon → Rel3([?player, ?weapon, ?damage])
        Groups: [Rel3]

Step 3: [?player :player/name ?name]
        Binding: Use Rel3's ?player values
        Index: EAVT per player
        Result: Relation([?player, ?name], 10000 tuples)
        Collapse: Rel3 ∩ Rel4 on ?player → Rel5([?player, ?weapon, ?damage, ?name])
        Groups: [Rel5]

Step 4: [(> ?damage 50)]
        Filter Rel5 where ?damage > 50
        Result: Rel6 (maybe 2000 tuples)
        Groups: [Rel6]

Project: Keep only [?name, ?damage]
Return: 2000 tuples
```

---

## Summary: Decision Points

| Decision | Method | Game Equivalent |
|----------|--------|-----------------|
| **Clause order** | Score by type, respect dependencies | Task priority + job graph |
| **When to join** | After every clause (collapse) | Incremental scene merging |
| **Join algorithm** | Hash join on shared symbols | Spatial hashing |
| **Index selection** | Based on bound variables | Choosing accel structure |
| **Binding strategy** | Score by selectivity position | Instance culling |
| **Predicate timing** | ASAP when symbols available | Early-out optimization |
| **Termination** | Stop on empty intermediate | Occlusion culling |

---

## Why This Works Well

1. **Greedy is good enough**: Optimal ordering rarely matters when you're filtering early
2. **Progressive joining**: Never materialize more than needed
3. **Symbol tracking**: Clear dependency graph, no magic
4. **Index diversity**: Right tool for each access pattern
5. **Early termination**: Fail fast, succeed incrementally

**The philosophy**: Do the obvious thing correctly rather than the clever thing approximately.
