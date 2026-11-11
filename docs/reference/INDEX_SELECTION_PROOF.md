# Index Selection for CRDT-Correct Queries

## The Problem

Given a query pattern `[?e ?a ?v ?t]` where each position may be bound or unbound, select an index that guarantees correct CRDT resolution.

**Constraint**: CRDTResolvingIterator uses "first entry wins" logic. This requires indices where:
1. (E, A) groups are contiguous in the scan
2. Tx is in descending order within each (E, A) group

## Index Anatomy

An index defines a sort order for datoms. Each index name describes its key ordering:
- **EAVT**: E → A → V → Tx↓
- **EATV**: E → A → Tx↓ → V

All indices use Tx↓ (descending via bitwise NOT). The CRDT-correctness distinction comes from **position**, not direction.

**Available indices** (8 total): EAVT, EATV, AEVT, AETV, ATEV, AVET, VAET, TAEV

**Removed**: VTEA (V → Tx↓ → E → A) — With V bound, effective sort is Tx↓ → E → A, making (E, A) groups non-contiguous. Same problem as T-unbound TAEV. VAET provides equivalent candidate discovery with correct (E, A) grouping.

## CRDT-Correctness Definitions

CRDT-correctness is cardinality-dependent:

**For cardinality-one (LWW)**: An index is *CRDT-correct* if, for any prefix scan, the first datom encountered for each (E, A) group has the highest Tx value among ALL datoms for that (E, A) pair in the database.

**For cardinality-many (add-wins)**: An index is *CRDT-correct* if, for any prefix scan constrained to a specific V, the first datom encountered for each (E, A, V) group has the highest Tx value for that specific (E, A, V) triple.

**Critical insight**: For cardinality-one, we must see ALL V values to find the winner. For cardinality-many, we only need the history of one specific V.

## Theorem 1: Valid Tx Positions for Cardinality-One

An index is CRDT-correct for cardinality-one if and only if Tx↓ appears in one of two positions:

**(a) Tx↓ immediately after (E, A)**: The index has form ... → E → A → Tx↓ → ... or ... → A → E → Tx↓ → ...
- Within each contiguous (E, A) group, highest Tx comes first
- Examples: EATV, AETV

**(b) Tx↓ before E and A, with T bound**: The index has form Tx↓ → ... AND T is constrained to a single value.
- Within a single transaction, each (E, A) appears at most once
- Example: TAEV when querying a specific transaction
- The run is the answer in History and in the view as of T. In another resolving view it is a candidate run: a pair it holds a datom of may resolve to a later operation's datom outside the run, so each datom is validated against its pair's resolved datom (`docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md`, T3, T6, T8).

**Proof of (a)**: When Tx↓ is immediately after (E, A) grouping:
1. (E, A) groups are contiguous
2. Within each group, datoms sorted by Tx descending
3. First datom in group has highest Tx ∎

**Proof of (b)**: When T is bound:
1. Scan is constrained to a single transaction
2. Each (E, A) can appear at most once per transaction
3. Resolution within the run is trivial (only one entry per group)
4. In History every datom is its own resolved datom, and in the view as of T no datom of a later operation is read, so the run's datoms are the view's resolved datoms
5. In any other resolving view a later operation may have written the pair, so the run's datom is the pair's resolved datom only when none did, which validation per pair decides ∎

**Note on T-unbound TAEV scans**: Without T bound, the same (E, A) appears at multiple Tx values interleaved with other entities. CRDTResolvingIterator detects group boundaries by comparing consecutive (E, A) pairs, so it would incorrectly treat re-encounters as new groups. Therefore, TAEV is only CRDT-correct when T is bound.

## Theorem 2: V-Bound Scans for Cardinality-One

**Statement**: For cardinality-one attributes, any index that filters by V cannot be CRDT-correct.

**Proof by counterexample**:

Consider:
```
Datom 1: (E=1, A=:name, V="Alice", Tx=5)
Datom 2: (E=1, A=:name, V="Bob",   Tx=10)  -- supersedes Alice
```

CRDT-resolved value of (E=1, A=:name) is "Bob" (highest Tx wins).

Query: `[?e :name "Alice"]` — V bound to "Alice".

With any V-primary index (VAET, AVET):
1. Scan prefix constrains to V="Alice"
2. Only Datom 1 is visible in the scan
3. CRDTResolvingIterator sees one (E=1, A=:name) entry, emits it
4. Query returns entity 1 — **INCORRECT**

The current value is "Bob", not "Alice". Entity 1 should NOT be in the results.

**Root cause**: V-bound scans filter BEFORE CRDT resolution. The iterator cannot see Datom 2 because it has a different V value.

**Corollary**: VAET and AVET are never CRDT-correct for cardinality-one attributes. ∎

## Theorem 3: V-Bound Scans for Cardinality-Many

**Statement**: For cardinality-many attributes, AVET and VAET scans with CRDTResolvingIterator are CRDT-correct.

**Proof**:

For cardinality-many (add-wins semantics):
- A value V is "present" for (E, A) unless explicitly retracted
- To check if V is current, we only need the history of the specific (E, A, V) triple
- Check whether the highest-Tx op for (E, A, V) is an assert or retract
- We do NOT need to see other V values

With AVET scan on (A, V):
1. All (E, A, V) groups for the bound V are contiguous
2. Tx is descending within each group
3. CRDTResolvingIterator correctly identifies assert/retract winner
4. Retracted values are filtered out ∎

**Important**: "Correct" here means CRDTResolvingIterator on the V-bound scan handles add-wins resolution properly. The iterator is still required to process retractions.

## Theorem 3b: Schemaless Defaults to CardinalityOne (LWW)

**Statement**: When no schema is defined, or an attribute has no schema
definition, the attribute resolves as **CardinalityOne (LWW)**, and V-bound
candidates for it **are** post-validated — the Theorem 2 hazard applies to them
exactly as it does to declared cardinality-one.

Consistent at all three layers: `chooseIndex` defaults absent schema to
`CardinalityOne`, `CRDTResolvingIterator` resolves `CardinalityUnknown` as LWW,
and `validatingVBoundIterator.Next` gates on
`card == CardinalityOne || card == CardinalityUnknown`.

**Cardinality hierarchy**:

| Condition | Cardinality | V-Bound Behavior |
|---|---|---|
| Schema defines CardinalityOne | One | Post-validate against the (E, A)'s current value |
| Schema defines CardinalityMany | Many | Add-wins via CRDT iterator |
| Schema defines CardinalityVector | Vector | RGA via CRDT iterator |
| Schema exists but attribute undefined | Unknown → **One** | **Post-validate** |
| No schema at all | Unknown → **One** | **Post-validate** |

**Implementation rule**: post-validate when cardinality is CardinalityOne **or
CardinalityUnknown**. Only explicitly-many and explicitly-vector are resolved by
the CRDT iterator alone.

## Theorem 4: Tx Ties Are Resolved Deterministically

**Concern**: If multiple datoms exist for the same (E, A) at the same Tx, "first entry wins" becomes "arbitrary wins".

**Resolution**: Our Tx is an `ElementID` with structure:
```
ElementID = Lamport (8 bytes) + ReplicaID (8 bytes)
```

This provides a total order:
1. Higher Lamport wins (temporal ordering)
2. Equal Lamport: higher ReplicaID wins (deterministic tiebreaker)

Since Tx↓ sorts by the full 16-byte ElementID, ties are impossible. ∎

## Index CRDT Analysis

| Index | Order             | Card-One CRDT? | Card-Many CRDT? | Notes                                 |
|-------|-------------------|----------------|-----------------|---------------------------------------|
| EAVT  | E → A → V → Tx↓   | ✗ No           | ✓ Yes           | V before Tx breaks card-one           |
| EATV  | E → A → Tx↓ → V   | ✓ Yes          | ✓ Yes           | Tx↓ after (E, A) — Thm 1(a)           |
| AEVT  | A → E → V → Tx↓   | ✗ No           | ✓ Yes           | V before Tx breaks card-one           |
| AETV  | A → E → Tx↓ → V   | ✓ Yes          | ✓ Yes           | Tx↓ after (A, E) — Thm 1(a)           |
| ATEV  | A → Tx↓ → E → V   | T-bound candidate | T-bound candidate | Tx↓ before E breaks (E, A) grouping for plain A-scans. With T bound the run under `[A][Tx]` holds one operation's datoms of A: the answer in History and as of T, a candidate run validated per pair in another resolving view (Thm 1(b), Thm 5). The layout also makes the first entry under `[A]` the global max-Tx datom for A, but nothing consumes that. |
| AVET  | A → V → E → Tx↓   | ✗ No           | ✓ Yes           | V-bound hides other V (card-one only) |
| VAET  | V → A → E → Tx↓   | ✗ No           | ✓ Yes           | V-bound hides other V (card-one only) |
| TAEV  | Tx↓ → A → E → V   | T-bound candidate | T-bound candidate | Tx↓ primary: with T bound the run holds one operation's datoms, the answer in History and as of T, a candidate run in another resolving view (Thm 1(b), Thm 5) |

**Summary**:
- Cardinality-one: EATV, AETV always correct; TAEV and ATEV with T bound are the answer in History and as of T, and candidate runs validated per pair in another resolving view
- Cardinality-many: EAVT, EATV, AEVT, AETV, AVET, VAET correct with CRDTResolvingIterator; TAEV and ATEV with T bound as for cardinality one, each member validated against its own newest assertion

## Theorem 5: Candidate + Validate (Semi-Join Pattern)

**Statement**: For a pattern in a resolving view whose bound V or bound T narrows the run below the (E, A) group, use the narrowed run for candidate discovery, followed by validation against each pair's resolved datom (`docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md`, T6).

**Algorithm**:
1. With T bound to an ElementID: ATEV prefix scan on (A, Tx) if A bound, TAEV prefix scan on Tx if A unbound → stream of candidate datoms, one operation's
2. Otherwise, with V bound: AVET prefix scan on (A, V) if A bound, VAET prefix scan on V if A unbound → stream of candidate datoms carrying V
3. For each candidate, resolve its (E, A) under A's definition, as every reader
   of the pair does: for cardinality one the EA cache's entry, or
   `ResolveLWW`'s answer, the value and the Tx of the assertion it was read
   from, the walk's for a definition declaring a unique and the newest visible
   entry for any other; for a many member its newest assertion; for a vector
   the collection under the group's highest ElementID
4. If `matchesDatom` holds on the resolved datom with the bound V and T → emit the resolved datom
   Otherwise → skip (the candidate is not the pair's datom)

**Proof of correctness**:
- Step 1/2 finds every pair that has a datom in the run: one written by T, or one carrying V
- Step 3 resolves the pair over its whole group, which the narrowed run cannot: another value may be newer, a later operation may have written the pair, and for a unique attribute another entity's later claim may supersede the newest assertion
- Step 4 keeps the pairs whose resolved datom the bound positions admit
- Result: exactly the pairs whose resolved datom carries V, or was written by T, or both ∎

**Complexity**: O(candidates) resolutions, each a cache read or an EATV scan of the pair. The candidates of a T-run are bounded by the operation's size and those of a V-run by the value's history; with both bound the T-run is taken (`docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md`, T7).

**Analogy**: Same pattern PostgreSQL uses: secondary index scan + visibility check against the heap.

## The Selection Matrix

Selection is by **bound position first, cardinality second**. `*` in the Tx column
means Tx is not consulted: a run must contain the whole group, since Tx is what
resolution determines.

| E | A | V | Tx | Card | Index | Prefix | Justification |
|---|---|---|----|------|-------|--------|---------------|
| - | - | - | - | any | EATV | — | Full scan; first entry per (E,A) is the LWW winner |
| ✓ | - | - | * | any | EATV | E | E-primary, whole (E,A) groups |
| ✓ | ✓ | - | * | many | EAVT | E, A | Values grouped for add-wins |
| ✓ | ✓ | - | * | one/vector/unknown | EATV | E, A | Tx↓ first, first entry current |
| ✓ | ✓ | ✓ | * | many | AEVT | A, E, V | V may join the prefix — see below |
| ✓ | ✓ | ✓ | * | one/vector/unknown | EATV | E, A | V compared downstream, never narrowing |
| - | ✓ | - | ✓ | any | **ATEV** | A, Tx | One operation's datoms of A: the answer in History, a candidate run validated per pair in a resolving view |
| - | ✓ | ✓ | * | any | AVET | A, V | Card-one emissions post-validated |
| - | ✓ | - | - | many | AEVT | A | Values grouped for add-wins |
| - | ✓ | - | - | one/vector/unknown | AETV | A | A-primary, Tx↓ first |
| - | - | ✓ | * | per-datom | VAET | V | Per-datom cardinality resolution |
| - | - | - | ✓ | any | TAEV | Tx | One operation's datoms: the answer in History, a candidate run validated per pair in a resolving view |

**A bound V may enter the prefix only for cardinality-many** — its adds and
removes all carry its own value, so the V-filtered run contains the whole
resolution decision. Theorems 2 and 3 at selection time. The binding-driven
match applies the same rule: a single bound V is post-validated when the
attribute resolves as cardinality one, for a binding at or below the
matcher's boundary, and a binding above it on a constant attribute is probed
against one resolved scan of the attribute under AETV
(`docs/proposals/A_VALUE_BINDING_ABOVE_THE_BOUNDARY_SCANS_ONCE_PROOF.md`);
V is the seek dimension among several bound positions only for a declared
many (`docs/proposals/BOUND_VALUE_SEEK_PROOF.md`, T6).

**A bound T narrows the run to one operation's datoms**, which hold no pair's
group whole in a resolving view, so a pattern with E free and T bound to an
ElementID takes the validating route in the matcher (`matchUnboundScan`) and
in the binding-driven match (`analyzeReuseStrategy`) before this selector is
consulted, with T the seek dimension beside a bound V
(`docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md`, T3, T6, T7). The ATEV
and TAEV rows above are the selector's choice in History, where the run is the
answer, and the runs the validating route narrows to in a resolving view.

**Resolution behavior by cardinality**:
- **CardinalityOne**: Post-validate against the (E, A)'s current value, resolved under A's definition (the value-filtered run's winner may not be it)
- **CardinalityMany**: CRDTResolvingIterator with add-wins (handles same-Tx tiebreaking)
- **CardinalityVector**: CRDTResolvingIterator with RGA (per-AfterRef resolution)
- **CardinalityUnknown/schemaless**: **LWW, and post-validated** — same as CardinalityOne, at every layer. See Theorem 3b.

## The State Machine

`chooseIndex` returns a **`ScanBound`** — an index plus the leading components of
that index's order bound to typed values. It performs no encoding, reads the
schema itself rather than taking a cardinality argument, and returns no validation
index: post-validation is the V-bound iterator's concern, not the selector's.

```go
// datalog/storage/matcher.go
func (m *PatternMatcher) chooseIndex(e, a, v, tx interface{}) ScanBound {
    if e != nil {                                  // E first — not Tx
        if a != nil {
            switch m.cardinalityOf(a) {            // absent schema ⇒ CardinalityOne
            case CardinalityMany:
                if v != nil {
                    return ScanBound{AEVT, []Value{a, e, v}}  // V in the prefix
                }
                return ScanBound{EAVT, []Value{e, a}}         // grouped by V
            default:
                return ScanBound{EATV, []Value{e, a}}         // Tx↓ first
            }
        }
        return ScanBound{EATV, []Value{e}}
    }
    if a != nil {
        if tx != nil && v == nil {
            return ScanBound{ATEV, []Value{a, tx}}   // AsOf-by-attribute
        }
        if v != nil {
            return ScanBound{AVET, []Value{a, v}}
        }
        if m.cardinalityOf(a) == CardinalityMany {
            return ScanBound{AEVT, []Value{a}}       // grouped by V
        }
        return ScanBound{AETV, []Value{a}}
    }
    if v != nil {
        // VAET order (V → A → E → Tx↓) groups by A once V is bound, so the
        // per-datom schema lookup is O(1) per distinct A, not per datom.
        return ScanBound{VAET, []Value{v}}
    }
    if tx != nil {
        return ScanBound{TAEV, []Value{tx}}          // reached only here
    }
    return ScanBound{Index: EATV}
}
```

**Critical rule**: post-validate when cardinality is **CardinalityOne or CardinalityUnknown**.
- CardinalityMany: CRDTResolvingIterator applies add-wins
- CardinalityVector: CRDTResolvingIterator applies RGA
- CardinalityUnknown/schemaless: resolves as LWW and **is** post-validated (Theorem 3b)

**V-only bound architecture** (CRDTResolvingIterator wraps VAET scan):

VAET sort order is V → A → E → Tx↓. With V bound, effective order is A → E → Tx↓.
Within this scan, (A, E) groups are **contiguous** — all Tx entries for the same
(A, E) are adjacent. This is exactly what CRDTResolvingIterator requires.

1. Wrap VAET scan with CRDTResolvingIterator
2. CRDTResolvingIterator handles:
   - Group deduplication via contiguous comparison (O(1) space)
   - Add-wins semantics including same-Tx tiebreaking
   - RGA for vectors
   - CardinalityMany/Vector/Unknown all resolved correctly
3. Post-filter: for each emission, check if `lookupCardinality(A) == CardinalityOne`
4. If card-one: validate against the (E, A)'s current value, skip if stale

The type is `validatingVBoundIterator` (`datalog/storage/matcher_relations.go`);
the sketch below is illustrative, not its current field set.

```go
type validatingVBoundIterator struct {
    inner  *CRDTResolvingIterator  // wraps the narrowed scan
    schema schema.SchemaProvider
    boundV any
    boundT any
}

func (it *validatingVBoundIterator) Next() bool {
    for it.inner.Next() {
        datom := it.inner.Datom()

        if def := it.schema.GetAttribute(datom.A); resolvedCardinality(def) == schema.CardinalityOne {
            // CRDTResolvingIterator emitted the "winner" within the narrowed
            // scan, but the pair may resolve to another datom: another value
            // may be newer, a later operation may have written the pair, or
            // another entity's later claim may supersede this one.
            resolved, _, err := it.resolvedDatom(datom.E, datom.A, def)
            if err != nil {
                return false
            }
            if resolved == nil || !it.matcher.matchesDatom(resolved, nil, nil, it.boundV, it.boundT, nil) {
                continue  // the candidate is not the pair's datom
            }
        }
        // Card-many/vector/unknown: handled correctly by CRDTResolvingIterator
        return true
    }
    return false
}
```

**Why CRDTResolvingIterator?**
- No seenCandidates map needed (deduplicates via contiguous group comparison, O(1) space)
- No manual Op checking (handles add-wins correctly, including same-Tx tiebreaking)
- No card-vector special casing (handles RGA)
- Card-unknown defaults to add-wins (Datascript-compatible)
- Only addition: card-one post-validation filter

**Why not manual Op check?** A direct Op check misses add-wins-at-same-Tx:
```
(V="Alice", A=:tags, E=1, Tx={Lamport:10, Replica:2}, Op=Retract)
(V="Alice", A=:tags, E=1, Tx={Lamport:10, Replica:1}, Op=Assert)
```
In Tx↓ ordering, Replica:2 sorts first. A naive check sees Op=Retract and skips.
But add-wins says concurrent assert beats retract — the value should be present.
CRDTResolvingIterator's `processAddWins` handles this correctly.

## Implementation

Live form: `validatingVBoundIterator.resolvedDatom`. It resolves the
candidate's (E, A) under A's definition as every reader of the pair does:
through the EA cache (`Cache.GetOrResolve`) when the cache is on, and through
`PatternMatcher.ResolveLWW` when it is off or the cache holds no entry. The
read seam takes a typed `ScanBound`; no caller constructs byte keys.

The definition decides the resolved datom. For a definition declaring a unique
it is the walk's: the entity's newest assertion no later withdrawal cancels and
no other entity's later claim supersedes, with that assertion's own Tx. For any
other definition it is the newest visible entry, a withdrawal meaning no datom.

`Next` admits the resolved datom through `matchesDatom` with the bound V and T
and emits its tuple, so the tuple's value and Tx are the pair's
(`docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md`, T6).

```go
// The datom a cardinality-one pair resolves to, nil when it holds no value
func (it *validatingVBoundIterator) resolvedDatom(e datalog.Identity, a datalog.Keyword,
    def *schema.AttributeDefinition) (*datalog.Datom, bool, error) {

    // With the cache on, the pair's entry under def and the view's read point
    // answers here instead; the resolver answers when the cache is off.
    v, tx, _, present, err := it.matcher.ResolveLWW(e, a, def, it.report)
    if err != nil {
        return nil, false, err
    }
    // A pair with no visible entry, or a withdrawn value, resolves to no datom,
    // which no bound position matches.
    if !present || v == nil {
        return nil, false, nil
    }
    return &datalog.Datom{E: e, A: a, V: v, Tx: tx}, false, nil
}
```

## Index Utility Analysis

All 8 indices serve a purpose:

| Index  | Purpose                                                                            |
|--------|------------------------------------------------------------------------------------|
| EATV   | E-primary CRDT resolution, validation lookups                                      |
| AETV   | A-primary CRDT resolution                                                          |
| ATEV   | AsOf-by-attribute scans (A-bound + Tx-bound + V-unbound) |
| TAEV   | T-primary queries (time-travel, single transaction)                                |
| AVET   | V-bound with A constant: card-many/vector resolution, card-one candidate discovery |
| VAET   | V-bound with A variable: per-datom cardinality resolution                          |
| EAVT   | Card-many resolution with E+A+V grouping, historical queries                       |
| AEVT   | Card-many resolution with A+E+V grouping, historical queries                       |

**Note**: For card-many, EAVT and AEVT are useful because they group (E, A, V) together, allowing add-wins resolution. They're "broken" only for card-one LWW queries.

## Summary

| Property             | Cardinality-One                            | Cardinality-Many/Vector            |
|----------------------|--------------------------------------------|------------------------------------|
| V-bound direct       | ✗ Broken (LWW winner may have different V) | ✓ Works with CRDTResolvingIterator |
| V-bound solution     | CRDTResolvingIterator + post-validate      | CRDTResolvingIterator directly     |
| T-bound in a resolving view | CRDTResolvingIterator + post-validate (Thm 1(b), Thm 5) | CRDTResolvingIterator + post-validate per member or collection |
| CRDT-correct indices | EATV, AETV; TAEV and ATEV in History and as of T | EAVT, EATV, AEVT, AETV, AVET, VAET; TAEV and ATEV in History and as of T |

**Key insights**:

1. **Cardinality determines correctness**: Card-one requires seeing ALL V values; card-many only needs specific V history.

2. **V-primary indices are useful**: For card-many/vector, they work directly. For card-one, they provide candidates that need validation.

3. **Post-validation pattern**: Wrap the run narrowed to V or to T with CRDTResolvingIterator, then post-validate each emission against its pair's resolved datom, the value and the Tx, with the bound V and T (`docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md`, T6).

4. **V-only bound (A variable)**: VAET scan with CRDTResolvingIterator. Card-one validates against the resolved datom under each candidate's own attribute; card-many/vector/unknown handled by CRDTResolvingIterator.

5. **CardinalityUnknown defaults to One**: a schemaless attribute resolves as LWW and is post-validated (Theorem 3b).

6. **A bound T is a narrowing, not a resolution**: a run under one operation holds no pair's group whole in a resolving view, so its datoms are candidates there, and the answer in History and as of T (Theorem 1(b)).
