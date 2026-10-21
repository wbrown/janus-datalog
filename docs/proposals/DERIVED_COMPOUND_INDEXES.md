# Derived Compound Indexes and Query-Time Composite Views

**Builds on:**

- [../reference/KEY_ENCODING_AND_CRDT.md](../reference/KEY_ENCODING_AND_CRDT.md) — binary key layout, eight datom indexes, CRDT resolution
- [../reference/CRDT_UNIQUE_SEMANTICS.md](../reference/CRDT_UNIQUE_SEMANTICS.md) — uniqueness as read-time resolution
- [SCHEMA_SELECTIVITY_HINTS.md](SCHEMA_SELECTIVITY_HINTS.md) — schema-declared physical planning information
- [../ideas/OPTIMIZATION_OPPORTUNITIES.md](../ideas/OPTIMIZATION_OPPORTUNITIES.md) — property-aware planning and the profile-before-optimization rule

---

## Abstract

Datomic's `:db.type/tuple` combines 2–8 scalar values into one atomic value. Its most compelling use is not packing domain data; it is creating a database-maintained compound key from several attributes on the same entity. That supports compound uniqueness and replaces some high-population joins with one indexed lookup.

Janus deliberately avoids opaque serialization and maps as values. A generic stored tuple can become a small record hidden inside `V`, bypassing the EAV model and making individual fields invisible to schema, CRDT resolution, and ordinary indexes. This proposal therefore separates two useful ideas:

1. **Query-time composite view** — a structural expression over existing relation symbols. It is never transacted, encoded, or admitted to the stored `datalog.Value` domain.
2. **Derived compound index** — a schema-declared, rebuildable physical index over independently stored and CRDT-resolved attributes. It is not an authoritative tuple datom.

This distinction matters because Janus's eight indexes are permutations of one authoritative operation record `(E,A,V,Tx,Op)`. A compound index crosses several datoms and therefore indexes _resolved entity state_, not operation records. It must be treated like a validated derived cache, with explicit freshness and fallback rules, rather than disguised as another datom index.

The recommended sequence is conservative:

1. Measure query-time intersection of existing AVET scans.
2. If evidence justifies it, add an equality-only derived candidate index.
3. Add ordered/range compound indexes only after a canonical ordered component encoding is proven.
4. Design compound uniqueness separately; the index accelerates candidate discovery but does not define ownership.

---

## 1. Goals and non-goals

### Goals

- Express compound keys over 2–8 existing attributes without hiding those attributes inside an opaque value.
- Reduce repeated high-population joins for exact compound lookups.
- Preserve Janus's append-only operation log and existing per-attribute CRDT policies.
- Keep constituent attributes independently queryable, indexable, typed, and observable in history.
- Make every derived index rebuildable from authoritative datoms.
- Guarantee that stale derived state cannot produce a wrong answer.
- Leave room for compound range scans and compound uniqueness without requiring either in the first implementation.
- Allow query-time composite expressions without adding a stored tuple type.

### Non-goals

- Adding `schema.TypeTuple` or `:db.type/tuple` as a generic stored value.
- Storing maps, structs, JSON, or arbitrary nested values in `V`.
- Copying Datomic's user-asserted heterogeneous/homogeneous tuple attributes.
- Treating an index cache as authoritative state.
- Defining compound uniqueness as a side effect of adding an index.
- Making History or arbitrary AsOf queries use a latest-state compound cache.
- Adding ordered component encoding before equality lookup demonstrates a measured need.
- Introducing a second matcher, storage engine, or planner path.

---

## 2. Correctness test matrix

Every physical index requires:

### Differential semantics

- Index enabled vs disabled.
- Fresh vs dirty index.
- Rebuilt vs incrementally maintained.
- Cache enabled vs disabled.
- Latest vs History/AsOf decline paths.

### Component values

- String, long, double, boolean, instant, bytes, ref, keyword, symbol.
- `nil`/missing constituents according to the chosen participation rule.
- Equal-content distinct backing arrays for bytes.
- Signed zero and other equality/hash edge cases.
- Minimum/maximum encoded values.

### State transitions

- First constituent added.
- Last missing constituent added.
- Constituent overwrite.
- Constituent tombstone.
- Remove then re-add.
- Several constituents changed in one transaction.
- Concurrent updates to different constituents.
- Out-of-order replicated operations.
- Crash between dirty marking and refresh.
- Rebuild while writes continue.

### Index behavior

- Hash collision candidates.
- Stale false positives.
- Dirty false-negative prevention.
- Duplicate matching entities.
- Empty index.
- Schema index add/remove/change.
- Iterator and close failures.
- Corrupt derived key/state metadata.

### Property proofs

- Every advertised key is unique in produced tuples.
- Every advertised order is empirically satisfied.
- Dirty/fallback relations publish only properties they actually establish.
- Optimized and ordinary query plans return identical results.

---

## 3. Performance acceptance criteria

The equality index should not ship merely because it is faster in a synthetic best case.

Required evidence:

- Real motivating query identified by profile.
- Statistically significant wall-time improvement.
- Bounded additional write amplification.
- Bounded dirty-refresh latency.
- Memory/disk overhead reported per indexed entity.
- Negative lookups remain bounded under dirty fallback.
- No regression when the planner declines the index.
- Full production-shaped checkpoint measured, even if the expected effect is focused.

No fixed percentage is proposed before the baseline measurement (§8) establishes the baseline.

---

## 4. Current Janus index model

Every authoritative Janus write is a datom operation:

```text
(E, A, V, Tx, Op, AfterRef?)
```

The storage layer writes that operation into eight physical orderings:

```text
EAVT  EATV  AEVT  AETV  ATEV  AVET  VAET  TAEV
```

Each index answers a different binding/order shape, but every entry still describes one operation on one attribute. CRDT resolution is performed while reading those operation records:

- CardinalityOne: highest visible operation wins (LWW).
- CardinalityMany: add-wins per value.
- CardinalityVector: RGA element resolution.
- Unique attributes: read-time `(A,V)`-LWW ownership.

The important invariant is:

> Existing indexes reorder authoritative operations; they do not derive new entity facts.

A compound key such as:

```text
(:registration/course,
 :registration/semester,
 :registration/student)
```

does not exist in any one operation. Its current value is computed only after resolving three independent `(E,A)` histories. That makes a compound index a different category of physical structure.

---

## 5. Policy: structural composites, not packed values

### 5.1 Why maps and opaque serializations are rejected

A map or serialized object in `V` hides structure from the database:

- Fields cannot be independently indexed.
- Schema cannot validate field-level types.
- CRDT policy applies to the entire blob, not its logical fields.
- Queries must decode application-specific structure.
- Partial updates replace or rewrite the whole value.
- Ordering and equality depend on an external serialization contract.

### 5.2 Why a Datomic tuple is less opaque

Datomic tuple structure is database-visible:

- Arity is bounded to 2–8.
- Component types are declared.
- Components are scalar and non-nested.
- Equality, hashing, and ordering are component-wise.
- `nil` has defined ordering.
- Query functions can construct and destructure tuples.
- Composite tuples are automatically derived from ordinary attributes.

That is materially better than a blob. However, user-asserted tuple values can still become "mini records in a value" and bypass ordinary EAV modeling.

### 5.3 Janus policy

Janus should retain the useful structural semantics without admitting a generic packed value:

- Constituent data remains ordinary attributes.
- A query composite remains a planner/executor structure, not a `datalog.Value`.
- A physical compound index remains a hidden derived structure, not a datom.
- Result presentation may render a composite view, like Pull renders maps at the terminal boundary, but relational execution lowers it to component symbols before hashing, joining, or storage matching.

---

## 6. Query-time composite views

A possible syntax mirrors Datomic:

```clojure
[(tuple ?year ?season) ?period]
[(untuple ?period) [?year ?season]]
```

The syntax does **not** imply that `?period` is a storable value. The algebra node should retain the component terms:

```text
Composite(?year, ?season)
```

Operations are lowered structurally:

```text
Composite equality:
  (tuple ?a ?b) = (tuple ?x ?y)
    ⇒ ?a = ?x AND ?b = ?y

Composite ordering:
  order by (tuple ?a ?b)
    ⇒ order by ?a, then ?b

Composite join:
  join on (tuple ?a ?b)
    ⇒ natural/equi-join on [?a ?b]
```

This is already close to Janus's internal execution model:

- `TupleKeyMap` hashes selected relation attributes.
- Hash joins accept multiple join symbols.
- Candidate keys are `[][]query.Symbol`.
- Tuple/relation input bindings carry multiple symbols.
- Multi-key ordering is explicit in `[]query.OrderByClause`.

The optimizer should therefore avoid allocating a composite object per row. `tuple` is syntax and result presentation over existing multi-symbol operations.

### 6.1 Result presentation

If a composite appears in top-level `:find`, the result boundary may render it as an immutable positional result object or `[]interface{}`. That representation must not flow back into joins, deduplication, storage, or subquery relations. This follows the same boundary rule used for Pull maps.

### 6.2 Query-time limitations

Query-time composites alone do not provide:

- Write-time or read-time compound uniqueness.
- One-seek compound lookup.
- Compound range scans.
- Persistent statistics.

They improve expression and result ergonomics and provide a logical target for future physical index selection.

---

## 7. Why a synthetic tuple datom is unsafe

The tempting implementation creates a hidden attribute:

```clojure
[?e :registration/course+semester+student
    [course semester student]]
```

Existing AVET and unique-resolution machinery could then index the synthetic value. This is attractive because it reuses all eight index writes.

It is not correct under independent CRDT updates.

Consider two replicas starting from:

```text
course=C1, semester=S1
```

Replica A concurrently writes:

```text
course=C2
derived tuple=(C2,S1)
```

Replica B concurrently writes:

```text
semester=S2
derived tuple=(C1,S2)
```

After merge, per-attribute CRDT resolution may produce:

```text
course=C2, semester=S2
```

but no operation ever asserted `(C2,S2)`. Selecting the latest synthetic tuple operation chooses one replica's incomplete view, not the tuple of resolved constituents.

The defect is categorical:

> A derived operation computed from local replica state cannot authoritatively represent a function of independently mergeable attributes.

Recomputing after every observed merge can repair a cache, but that proves the derived entry is a cache—not an authoritative datom.

---

## 8. Baseline: intersect existing indexes

Before adding physical storage, measure the logical query already available:

```clojure
[:find ?e
 :where
 [?e :registration/course ?course]
 [?e :registration/semester ?semester]
 [?e :registration/student ?student]]
```

With constants or inputs bound, each pattern can use AVET (or another appropriate index) and produce an entity set. The executor intersects/joins the sets on `?e`.

A specialized planner/operator could make this explicit:

```text
CompoundLookup(A1=V1, A2=V2, A3=V3)
  ⇒ intersect(
       AVET(A1,V1) → E,
       AVET(A2,V2) → E,
       AVET(A3,V3) → E)
```

This requires no new correctness model. It establishes:

- Workload frequency.
- Constituent selectivity.
- Intermediate entity-set sizes.
- Whether one-seek lookup would materially improve wall time.
- Which component order would be useful for a future ordered index.

The planner must not introduce the specialized operator without profile and benchmark evidence over realistic populations.

---

## 9. Schema model for a derived index

A possible schema declaration:

```go
type CompoundIndexDefinition struct {
    Ident      datalog.Keyword
    Attributes []datalog.Keyword // 2–8, ordered
    Kind       CompoundIndexKind // equality first; ordered later
}
```

Equivalent EDN is an open design question. One possibility:

```clojure
{:db/indexIdent :registration/by-course-semester-student
 :db/indexAttrs [:registration/course
                 :registration/semester
                 :registration/student]
 :db/indexKind :db.index/equality}
```

The definition names an index, not an attribute:

- It does not appear as an `(E,A,V)` fact.
- It is not returned by wildcard Pull or History.
- It cannot be asserted/retracted through `Transaction`.
- Its components retain their own schema and CRDT cardinality.
- Attribute order is physical and semantically relevant for an ordered index.

### 9.1 Initial restrictions

The first implementation should require:

- 2–8 attributes.
- Every constituent is CardinalityOne.
- Every constituent has a supported scalar storage type.
- One compound index definition has no duplicate attributes.
- No constituent is itself derived.
- Latest-mode exact lookup only.
- No uniqueness claim.

CardinalityMany and CardinalityVector imply a Cartesian product of resolved values and should remain out of scope until a specific use case defines the desired semantics and bounds.

---

## 10. Equality-only physical index

The first physical index should optimize exact matching only.

### 10.1 Candidate layout

```text
[CompoundEqualityPrefix]
[IndexID]
[TypedTupleHash]
[Entity]
```

Where:

- `IndexID` is a stable hash/interned ID of the compound index definition.
- `TypedTupleHash` is a stable content hash over component type tags and bytes.
- `Entity` is the normal 20-byte entity hash.

The key is a candidate locator, not proof that the entity still matches. Hash collisions and stale entries are harmless only because every candidate is validated against current resolved component values.

### 10.2 Stable hashing

The hash contract is the same one now enforced by `TupleKeyMap`:

```text
ValuesEqual(a,b) ⇒ Hash(a)=Hash(b)
```

The persistent index cannot use Go pointer hashes or process-specific intern addresses. It needs a stable versioned hash over canonical storage encodings:

```text
[format-version]
[component-count]
repeat {
  [value-type]
  [encoded-length]
  [canonical-value-bytes]
}
```

Length framing is sufficient for hashing because lexical order is not required in the equality-only index.

### 10.3 Lookup

For a fully bound compound key:

1. Encode/hash the requested components.
2. Scan the exact `(IndexID, TupleHash)` prefix.
3. Resolve every candidate entity's constituent attributes.
4. Compare each resolved component with `datalog.ValuesEqual`.
5. Return only validated candidates.
6. Before trusting an empty result, refresh every dirty entity for that index or fall back to ordinary AVET intersection.

Step 6 is what prevents a stale index from producing false negatives.

---

## 11. Freshness and invalidation

False positives are easy: validate and discard.

False negatives are the hard part. If an entity changed from tuple `T1` to `T2`, a stale index may still contain `T1` and omit `T2`. Validating candidates does not discover the missing `T2` entry.

The design therefore needs a durable dirty protocol.

### 11.1 Dirty marker invariant

Every authoritative write touching a constituent attribute must atomically record:

```text
(CompoundIndexID, Entity) is dirty
```

in the same storage transaction as the datom operation.

The invariant is:

> If a compound index entry may be missing or stale, a durable dirty marker exists before the authoritative write becomes visible.

### 11.2 Refresh

Refreshing one dirty entity:

1. Resolve every constituent `(E,A)` in latest mode.
2. Remove the entity's previous equality-index entry, if known.
3. If all index participation rules are satisfied, write the new entry.
4. Store the constituent winner-version vector used to compute it.
5. Clear the dirty marker atomically with the new index entry.

The previous entry may be tracked in per-entity index metadata:

```text
[CompoundStatePrefix][IndexID][Entity]
  → {tupleHash, componentWinnerTxs}
```

This metadata is derived and rebuildable.

### 11.3 Lookup freshness choices

Before trusting a negative lookup, one of these must hold:

1. **Synchronous write-through:** all dirty entities were refreshed before the committing transaction returned.
2. **Lookup-time drain:** refresh all dirty entities for the index before the index scan.
3. **Fallback:** if dirty markers exist, execute ordinary constituent-index intersection and optionally refresh opportunistically.

The first gives predictable reads but adds write amplification and requires post-resolution work. The second moves spikes to reads. The third is simplest and preserves correctness while measuring whether maintenance is worthwhile.

The recommended first implementation is **fallback on dirty**.

### 11.4 Attribute high-water marks are insufficient alone

ATEV provides O(1) per-attribute high-water marks. They can prove that _some_ constituent changed after an index build, but not which entity is stale. A global high-water check would invalidate or rebuild the entire compound index after every constituent write.

Use attribute high-water marks as a coarse safety/version check, not as the only invalidation mechanism. Per-entity dirty markers are needed for bounded maintenance.

---

## 12. CRDT and temporal semantics

### 12.1 Latest mode

The derived index represents:

```text
Tuple(E) = (
  ResolveLatest(E,A1),
  ResolveLatest(E,A2),
  ...
  ResolveLatest(E,An))
```

Constituents retain their existing CRDT policies. The initial restriction to CardinalityOne keeps each position scalar.

### 12.2 History

History returns operation records, not resolved entity state. A latest-state compound index cannot answer History queries and must not advertise ordering or candidate-key properties there.

History queries continue using ordinary datom indexes.

### 12.3 AsOf

An index built from latest values cannot answer arbitrary `AsOf(basis)`. Options for future work include:

- Decline the compound index and intersect ordinary AsOf scans.
- Build basis-specific ephemeral indexes for repeated analytical queries.
- Store a versioned compound history, which reintroduces the cross-operation derivation problem and is not recommended without a compelling use case.

The first implementation must decline compound index selection whenever `txID != nil`.

### 12.4 Concurrent and replicated writes

Dirty markers are written from operations, not from an assumed mutable current record. Concurrent constituent updates may cause several refresh attempts; the final entry is always recomputed from resolved authoritative state.

No derived entry participates in CRDT conflict resolution. CRDT resolution produces the state; the index follows it.

If future distributed ingestion can bypass `Database.Transaction`, that path must write dirty markers or trigger a rebuild before the derived index is trusted.

---

## 13. Ordered compound indexes

Equality hashing cannot support:

- Prefix lookups.
- Lexicographic range queries.
- Ordered scans or Top-N termination.
- Min/max over compound keys.

An ordered index would use:

```text
[CompoundOrderedPrefix]
[IndexID]
[Component1]
[Component2]
...
[ComponentN]
[Entity]
```

Each component needs:

- A type tag defining cross-type order.
- Canonical sortable bytes.
- An explicit `nil` marker that sorts below values.
- Unambiguous self-delimiting framing.

Existing `ValueBytes` encodings preserve order within supported scalar types, but concatenating variable-length values is not sufficient. A simple length prefix changes lexicographic ordering by making length precede content. Variable-length strings/bytes/symbols require an order-preserving terminator and escape scheme, or an offset table outside the compared component bytes.

The ordered encoding must prove:

```text
CompareValues(a,b) == bytes.Compare(EncodeComponent(a), EncodeComponent(b))
```

for every supported type and then prove the lexicographic extension across all component positions.

Do not add this layout until exact-index benchmarks show that ordered/range use cases justify the additional format and proof burden.

---

## 14. Query planning and execution

### 14.1 Recognition

The planner can recognize a group of patterns sharing the same entity symbol:

```clojure
[?e :registration/course ?course]
[?e :registration/semester ?semester]
[?e :registration/student ?student]
```

If all component values are bound and a matching equality compound index exists, it may compile them into one physical lookup while preserving the logical Datalog clauses.

### 14.2 Logical/physical separation

The `RealizedPhase.Query` should still contain Datalog. Storage sees a one-pattern query today, so compound lookup needs either:

- A new Datalog clause/constraint that represents the logical conjunction, or
- Planner execution that invokes a compound-index-capable relation source and retains the original patterns as validation.

Do not add an opaque matcher request or separate matcher interface. Any physical request must remain expressible in the existing query/constraint contract or below the matcher in the storage backing.

### 14.3 Relation properties

A validated equality compound lookup can claim:

- Candidate key `{?e}` when each matching entity is emitted once.
- No ordering guarantee for a hash layout.

An ordered compound lookup may claim the exact component ordering supplied by its key layout. Those properties must be tested against produced tuples, like the current storage property proofs.

### 14.4 Cost model

Without statistics, a simple conservative gate is possible:

- Use the compound index only when every component is bound.
- Refresh/fallback if dirty.
- Otherwise retain normal pattern planning.

Broader partial-prefix selection should wait for ordered indexes and measured selectivity information.

---

## 15. Compound uniqueness is a separate feature

An index answers:

```text
Which entities currently resolve to component tuple T?
```

Uniqueness answers:

```text
If several entities resolve to T, which entity canonically owns T?
```

Janus's single-attribute unique semantics choose an owner using one value operation's `ElementID`. A compound tuple has several independently resolved winner operations and therefore no single obvious transaction ID.

Possible compound-version rules include:

- Maximum constituent winner `ElementID`.
- Lexicographic vector of constituent winner IDs in schema attribute order.
- A deterministic hash of the version vector plus an explicit total-order tiebreaker.
- A separately asserted ownership operation (which risks returning to the synthetic-datom problem).

Each rule has observable conflict semantics. For example, maximum winner ID may not change when an older component changes the tuple while another component already has a higher winner ID.

This proposal does not choose a rule.

The compound index may accelerate candidate discovery for uniqueness resolution, but the canonical-owner rule must be specified and tested independently. Until then:

- `Unique` is not allowed on a compound index definition.
- Duplicate matching entities are valid output.
- No lookup-ref/upsert semantics are implied.

---

## 16. Failure and recovery model

The authoritative datoms always win.

### Derived index missing or corrupt

- Mark the index unavailable/dirty.
- Fall back to ordinary constituent scans.
- Rebuild from resolved latest state.
- Never return an index-only answer without validation.

### Refresh failure after authoritative commit

- Leave the durable dirty marker.
- Return the authoritative write result according to the chosen maintenance policy.
- Queries fall back until refresh succeeds.

### Process crash during refresh

The dirty marker and replacement entry must be updated atomically. After restart, any remaining dirty marker triggers fallback/rebuild.

### Schema change

Changing constituent order or membership creates a new `IndexID`; it does not reinterpret old keys. The old index can be dropped after the new index is built and validated.

### Disable/rebuild

Compound indexes are optional accelerators. Disabling one must change only performance, never query results. Differential tests with the index enabled and disabled are mandatory.

---

## 17. The choices

Each is a choice the goals leave open, with the alternatives named:

1. **Feature scope:** query-time composites only, the equality index, the ordered index, or a subset of them.
2. **Schema syntax:** Go-only definitions, EDN schema keys, or both.
3. **Missing components:** exclude the entity, include `nil`, or make it configurable.
4. **Maintenance policy:** synchronous write-through, read-time drain, or dirty fallback (§11.3).
5. **Persistence:** a persistent derived index, or an in-memory cache rebuilt at open.
6. **Index key hash:** SHA-1 reuse, a stronger digest, or a versioned custom hash.
7. **Direct store writes and import:** how each ingestion path records dirtiness, or whether it triggers a rebuild instead.
8. **Query syntax:** `tuple`/`untuple` exposed, or composites kept optimizer-only.
9. **Result representation:** a positional presentation type, or a plain slice.
10. **Ordered encoding:** only if the baseline measurement (§8) demonstrates range/order demand.
11. **Compound uniqueness:** the owner rule, in a separate proposal.
12. **Distribution:** how arena/replica consumers learn index dirtiness and rebuild generations.

---

## 18. Alternatives considered

### Generic stored tuple value

Rejected by the proposed policy. It enables packed mini-records and complicates CRDT semantics, storage encoding, and schema visibility.

### Synthetic hidden tuple datom

Rejected as authoritative state. Concurrent constituent updates can resolve to a tuple that no replica generated.

### Application-maintained compound attribute

Possible today but opaque to Janus and vulnerable to the same stale/concurrent derivation problem. It moves correctness into every caller.

### Always intersect existing AVET indexes

Correct and the required baseline. It may remain the final answer if measured performance is adequate.

### Full ordered compound index first

Rejected as a starting point. It adds variable-length ordered encoding and range semantics before exact lookup has justified physical storage.

### Compound index defines uniqueness

Rejected. Candidate discovery and canonical ownership are separate semantics.

---

## 19. Related implementation areas

- `datalog/schema/` — compound index declarations and validation
- `datalog/storage/database.go` — transaction dirty-marker integration
- `datalog/storage/key_encoder_binary.go` — future compound key encoding
- `datalog/storage/matcher.go` — validated lookup and temporal decline gates
- `datalog/storage/atev_index.go` / high-water APIs — coarse freshness signals
- `datalog/planner/` — compound-pattern recognition and physical selection
- `datalog/algebra/` — structural composite expressions and lowering
- `datalog/executor/relation_properties.go` — derived candidate-key/order proofs
- `datalog/executor/tuple_key.go` — typed equality/hash law (in-memory only)
- `datalog/annotations/` — selected/declined/dirty/rebuild events

---

## 20. Recommendation

The baseline measurement (§8) comes before any physical storage.

The most policy-aligned likely endpoint is:

1. Structural query-time composites lowered to existing multi-symbol operators.
2. Ordinary AVET intersection as the correctness baseline.
3. A schema-declared equality-only derived candidate index if profiling proves the baseline materially expensive.
4. Durable per-entity dirtiness, candidate validation, and fallback before trusting negative results.
5. Ordered indexes and compound uniqueness deferred to separate, evidence-led decisions.

This preserves Janus's core rule: authoritative data remains explicit datom operations. Compound structure may guide planning and indexing without becoming an opaque value hidden inside `V`.
