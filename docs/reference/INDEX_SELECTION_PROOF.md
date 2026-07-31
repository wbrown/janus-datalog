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

**Proof of (a)**: When Tx↓ is immediately after (E, A) grouping:
1. (E, A) groups are contiguous
2. Within each group, datoms sorted by Tx descending
3. First datom in group has highest Tx ∎

**Proof of (b)**: When T is bound:
1. Scan is constrained to a single transaction
2. Each (E, A) can appear at most once per transaction
3. CRDT resolution is trivially correct (only one entry per group) ∎

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
| Schema defines CardinalityOne | One | Post-validate against the EATV winner |
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
| ATEV  | A → Tx↓ → E → V   | T-or-A only    | T-or-A only     | Tx↓ before E breaks (E, A) grouping for plain A-scans; CRDT-correct when T is bound (Thm 1(b)) or when used for AsOf-bounded A-scans where the Tx↓ prefix gates entries before (E, A) grouping. That is its use: AsOf-by-attribute. The layout also makes the first entry under `[A]` the global max-Tx datom for A, but nothing consumes that. |
| AVET  | A → V → E → Tx↓   | ✗ No           | ✓ Yes           | V-bound hides other V (card-one only) |
| VAET  | V → A → E → Tx↓   | ✗ No           | ✓ Yes           | V-bound hides other V (card-one only) |
| TAEV  | Tx↓ → A → E → V   | T-bound only   | T-bound only    | Tx↓ primary — Thm 1(b)                |

**Summary**:
- Cardinality-one: EATV, AETV always correct; TAEV when T is bound; ATEV when T is bound or used for the bounded-Tx high-water seek
- Cardinality-many: EAVT, EATV, AEVT, AETV, AVET, VAET correct with CRDTResolvingIterator; TAEV and ATEV require T binding (or, for ATEV, the high-water-seek usage pattern)

## Theorem 5: Candidate + Validate (Semi-Join Pattern)

**Statement**: For cardinality-one V-bound queries, use V-primary indices for candidate discovery, followed by CRDT-correct validation.

**Algorithm**:
1. AVET prefix scan on (A, V) → stream of candidate E values (if A bound)
2. VAET prefix scan on V → stream of candidate (E, A) pairs (if A unbound)
3. For each candidate:
   - EATV point scan on (E, A) → first datom = CRDT winner
4. If winner.V == bound V → emit
   Otherwise → skip (stale candidate)

**Proof of correctness**:
- Step 1/2 finds all entities that *ever* had value V
- Step 3 retrieves the *current* value via CRDT-correct index (EATV)
- Step 4 filters stale candidates (entities where V was superseded)
- Result: exactly the entities where V is the current value ∎

**Complexity**: O(candidates) point lookups. The number of stale candidates is bounded by historical updates—small in practice.

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
| - | ✓ | - | ✓ | any | **ATEV** | A, Tx | Direct AsOf-by-attribute seek |
| - | ✓ | ✓ | * | any | AVET | A, V | Card-one emissions post-validated |
| - | ✓ | - | - | many | AEVT | A | Values grouped for add-wins |
| - | ✓ | - | - | one/vector/unknown | AETV | A | A-primary, Tx↓ first |
| - | - | ✓ | * | per-datom | VAET | V | Per-datom cardinality resolution |
| - | - | - | ✓ | any | TAEV | Tx | T-primary, no other position bound |

**A bound V may enter the prefix only for cardinality-many** — its adds and
removes all carry its own value, so the V-filtered run contains the whole
resolution decision. Theorems 2 and 3 at selection time.

**Resolution behavior by cardinality**:
- **CardinalityOne**: Post-validate with an EATV `(E,A)` run (LWW winner may have a different V)
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
4. If card-one: validate with EATV point lookup, skip if stale

The type is `validatingVBoundIterator` (`datalog/storage/matcher_relations.go`);
the sketch below is illustrative, not its current field set.

```go
type validatingVBoundIterator struct {
    inner  *CRDTResolvingIterator  // wraps VAET scan
    schema schema.SchemaProvider
    boundV any
}

func (it *validatingVBoundIterator) Next() bool {
    for it.inner.Next() {
        datom := it.inner.Datom()

        if it.lookupCardinality(datom.A) == schema.CardinalityOne {
            // CRDTResolvingIterator emitted the "winner" within the V-filtered
            // scan, but the real LWW winner might have a different V.
            if !it.validateWithEATV(datom.E, datom.A) {
                continue  // stale candidate
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

Live form: `validatingVBoundIterator.validateCandidate`. The read seam takes a
typed `ScanBound`; no caller constructs byte keys.

```go
// Semi-join validation for V-bound cardinality-one queries
func validateCandidate(r StoreReader, e datalog.Identity, a datalog.Keyword,
    boundV datalog.Value) (bool, error) {

    // The (E, A) run on EATV: Tx↓, so the first entry is the CRDT winner.
    it, err := r.Scan(ScanBound{Index: EATV, Prefix: []datalog.Value{e, a}})
    if err != nil {
        return false, err
    }
    defer it.Close()

    if !it.Next() {
        return false, it.Error()  // no current value
    }
    // Compare through the value domain, never with ==.
    return datalog.ValuesEqual(it.Datom().V, boundV), it.Error()
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
| CRDT-correct indices | EATV, AETV, TAEV*, ATEV†                   | EAVT, EATV, AEVT, AETV, AVET, VAET; TAEV* and ATEV† |

*TAEV only when T is bound
†ATEV is CRDT-correct when T is bound or used for the bounded-Tx high-water seek (first entry under `[A]`)

**Key insights**:

1. **Cardinality determines correctness**: Card-one requires seeing ALL V values; card-many only needs specific V history.

2. **V-primary indices are useful**: For card-many/vector, they work directly. For card-one, they provide candidates that need validation.

3. **Post-validation pattern**: Wrap V-bound scan with CRDTResolvingIterator, then post-validate card-one emissions with EATV point lookup.

4. **V-only bound (A variable)**: VAET scan with CRDTResolvingIterator. Card-one uses EATV validation; card-many/vector/unknown handled by CRDTResolvingIterator.

5. **CardinalityUnknown defaults to Many**: Schemaless attributes use add-wins semantics (Datascript-compatible).

6. **TAEV is conditional**: Only CRDT-correct when T is bound (single transaction).

7. **CRDTResolvingIterator handles edge cases**: Same-Tx tiebreaking for add-wins, RGA for vectors, contiguous group deduplication.
