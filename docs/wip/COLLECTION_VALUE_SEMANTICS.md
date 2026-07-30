# Collection value semantics: absence, emptiness, ordering

Recorded 2026-07-28. Covers how a collection-valued attribute's absence is distinguished from its emptiness, how collections are compared, and the six defects found against that specification.

## 1. Rulings

| Subject | Ruling |
|---|---|
| Absence | A missing vector is different from an empty vector; a missing set is different from an empty set |
| Vector patterns | `[?e :vec "elem"]` is not a membership test |
| Set comparison | Comparison of cardinality-many values must be unordered |
| Cardinality inference | Cardinality must not be inferred from add-shaped ops alone |
| PR #114 scope | P1 only |
| Reproducers | Stay red in the tree pending their fixes |
| This document | Deferred out of P1 |
| Comparator loud defaults | Approved, deferred |

Everything in §2 is either one of these or a consequence of one.

## 2. Specification

### States

Every (E, A) is in exactly one state, decidable from a single group scan. No flag and no new storage representation are required.

- **S0** — no datoms, **or** datoms that are exclusively removes. A remove of a value never added does not bring an attribute into existence.
- **S1** — at least one establishing op (`OpNone` / `OpCRDTAdd` / `OpRGAInsert`) exists, and resolution yields zero live values.
- **S2** — at least one live value.

### S1 is not uniform across cardinalities

| Cardinality | S1 arises from | S1's resolved value | In the value domain | Pattern-observable |
|---|---|---|---|---|
| one | tombstone | none — absence | n/a | n/a |
| many | all members removed | the empty set | **no** | **no** |
| vector | all elements tombstoned, or set to `[]` | `[]` | **yes** | **yes** |

For cardinality-one, S1 is observationally identical to S0 everywhere — a tombstone means absent, and there is no empty scalar to bind.

### Behavior — cardinality-one

| | S0 | S1 | S2 |
|---|---|---|---|
| `[?e :a ?v]` | no tuple | no tuple | `?v` = resolved value |
| `[?e :a X]` | no match | no match | match iff `X` = **resolved** value |
| `(missing? $ ?e :a)` | true | true | false |
| `(pull ?e [:a])` | key absent | key absent | key present |
| `[(get-else $ ?e :a d) ?v]` | `?v` = `d` | `?v` = `d` | `?v` = value |
| `[(get-some $ ?e :a …) ?v]` | falls through | falls through | binds |

The load-bearing row is `[?e :a X]`: the **resolved** value, not "any datom carries V = X".

### Behavior — cardinality-many

| | S0 | S1 | S2 |
|---|---|---|---|
| `[?e :a ?v]` | no tuple | no tuple | one tuple per live member |
| `[?e :a X]` | no match | no match | match iff `X` is a live member |
| `(missing? $ ?e :a)` | true | **false** | false |
| `(pull ?e [:a])` | key absent | key present, empty | key present |
| `[(get-else $ ?e :a d) ?v]` | `?v` = `d` | `?v` = `[]` | `?v` = member slice |
| `[(get-some $ ?e :a …) ?v]` | falls through | **stops**, binds `[]` | binds |

`[?e :a #{}]` is not expressible and should not be — a set-valued literal has no counterpart in the value domain. `missing?`, pull, and `get-some`'s stop-versus-fall-through are therefore the only S0/S1 observables for this cardinality.

`LookupAttribute` returns the whole collection rather than one member, so S1 always has a value to bind. There is no arity question.

**Datomic divergence.** Datomic's current index holds only live datoms, so retracting every member is indistinguishable from never asserting: `missing?` is true for both and pull omits the key. S1 is unreachable in Datomic's model, and Datomic has no cardinality-vector at all. The `missing?` row above is a deliberate janus extension and belongs in `DATOMIC_COMPATIBILITY.md`.

### Behavior — cardinality-vector

| | S0 | S1 | S2 |
|---|---|---|---|
| `[?e :a ?v]` | no tuple | `?v` = `[]` | `?v` = the vector |
| `[?e :a X]`, `X` a vector | no match | match iff `X` = `[]` | match iff `X` = the whole vector |
| `[?e :a X]`, `X` not a vector | no match | no match | no match — typed non-match |
| `(missing? $ ?e :a)` | true | false | false |
| `(pull ?e [:a])` | key absent | key present, `[]` | key present |
| `[(get-else $ ?e :a d) ?v]` | `?v` = `d` | `?v` = `[]` | `?v` = vector |
| `[(get-some $ ?e :a …) ?v]` | falls through | stops, binds `[]` | binds |

The V position of a vector attribute is collection-bound only. A non-vector literal there is an ordinary typed non-match, not an error, which also works schemaless where cardinality is not declared.

The reason is substitution. `[?e :vec ?v]` binds the whole vector, so a membership reading would make one syntactic position mean "the collection" with a variable and "one element" with a constant, breaking

```
[?e :vec ?v] [(= ?v X)]  ≡  [?e :vec X]
```

which predicate pushdown into the V position depends on. The membership operation already exists and is named — `contains?`, `index-of`, `enumerate` — and carries position information a pattern could not.

Downstream vector functions need no special cases: `[]` is an ordinary vector. `(length [])` is 0, `(contains? [] X)` is false, `(enumerate [])` yields zero tuples.

### Required of every read path

- **P1** — V is compared against the resolution output, never placed in the scan prefix, except as permitted below.
- **P2** — "no entry" is distinguished from "entry resolving to zero live values". Never inferred from a count.
- **P3** — a set materialized as a value is in canonical `CompareValues` order.

### When V may enter the scan prefix

Permitted only when the datom's V is the same unit as the pattern's V **and** the resolution decision for that V is contained inside the V-filtered range.

| | Same unit | Decision contained | Prefix-safe |
|---|---|---|---|
| one | yes | **no** — the winner may carry a different V | ✗ |
| many, member-bound | yes | yes — that member's adds and removes all carry its V | **✓** |
| vector | **no** — datom V is an element, pattern V is the collection | n/a | ✗ |

## 3. Defect register

**Evidence** records how each defect was established and does not change. **Status** is where it stands now.

| | Defect | Arm | Evidence | Bug document | Status |
|---|---|---|---|---|---|
| **D1** | V-in-prefix returns the superseded cardinality-one value | non-cache | reproduced **red** — 2 cases of `TestConstantPatternCacheParity` | `BUG_VBOUND_PREFIX_SCAN_MATCHES_SUPERSEDED_VALUE` | **Fixed.** `chooseIndex`'s E+A+V arm is cardinality-aware; V reaches the prefix only for a cardinality-many member |
| ~~**D2**~~ | ~~Collection-bound V can never match a vector; S1 answers as S0~~ | ~~non-cache~~ | ~~derived, no reproducer~~ | same | **Withdrawn: the claim was false.** `[#id … :person/skill [] ?tx]` already matched a cleared vector on both arms before any fix. Derived, never reproduced, and the reproducer written to confirm it refuted it instead. The vector half of P1 still landed — V-in-prefix is wrong in kind there whatever the observable — but it corrected a derivation, not a wrong answer |
| **D3** | Nonexistent entry resolves to `[]`, so S0 matches `[]` | cache | reproduced **red** — `TestCacheMatrix_NeverSetVectorAgainstEmptyVectorLiteral` | `BUG_CACHE_EMPTY_VECTOR_NEVER_SET` | **Fixed** by presence, below |
| **D4** | Cardinality-many S0/S1 conflated by count (`matcher.go:682-692`, `764-766`) | both | read | none | Open — but now small: `SetResolutionResult.Present` exists, so both arms have the fact and need only read it instead of a length |
| **D5** | Set members render in Go map iteration order, so the same set yields unequal values with unequal hashes (`set_resolution.go:14`, `matcher.go:768`) | both | read | none | Open, untouched |
| **D6** | Vectors compare by `%v` rendering | comparator | read | `BUG_VECTOR_VALUES_DEGENERATE_ORDERING` | Open, untouched |
| **D7** | `lookupAttributeViaPattern` returns one arbitrary member for cardinality-many (`pull.go:261`); wildcard pull uses the pattern path, so `(pull ?e [*])` and `(pull ?e [:a])` can disagree on S1 | pull | read | none | Open, untouched |
| **D8** | A V-unbound pattern on a cleared vector produced no tuple, where the same pattern V-bound to `[]` matched — two forms of one query disagreeing, and both disagreeing with `LookupAttribute` | both | reproduced **red** — `TestGroundVectorPatternDistinguishesClearedFromNeverSet`, failing on both arms | none | **Fixed.** An explicit skip-empty early return in each arm, dating from when S0 and S1 were the same state |

D8 was found by the reproducer written to confirm D2 — the same test refuted one claim and established another, which is the case for writing the reproducer even when the defect looks settled by derivation.

D5 is the only remaining defect that corrupts joins and dedup: it violates `ValuesEqual(a,b) ⟹ hash(a) == hash(b)` wherever a cardinality-many value reaches a `TupleKeyMap`. Blast radius is the whole-collection boundary — `get-else`, `get-some`, pull — not ordinary patterns, which yield one scalar tuple per member.

### How D3 and D8 were fixed: presence is resolved

The register above located both in the arms that read the state. The cause was upstream of either: **attribute presence was never modelled.** Four correctness sites decided whether an attribute existed by reading `RGAStats.TotalElements`, a field whose own constructor is documented "calculates statistics for debugging and monitoring", and the cache path — having no statistic to borrow — simply guessed.

- `VectorResolutionResult.Present` and `SetResolutionResult.Present` carry the fact, resolved beside the value rather than counted after it.
- All three `CacheResolver` methods report `present`. An (E, A) with no datoms produces **no cache entry**, so downstream, entry existence *is* attribute existence and nothing reconstructs it from a count or a nil slice.
- `RGAStats` left the correctness path entirely: its four readers are gone, `Stats` came off `VectorResolutionResult`, `ComputeRGAStats` no longer runs per vector resolution, and `MaxElementID` comes from `FindMaxElementID`, which existed for exactly that.
- `ResolveEntry` stopped duplicating the cardinality switch and delegates, so the presence rule has one home instead of two.

**One hazard this opened and closed.** Removing the skip-empty early returns let a cleared vector reach the tuple builder, and `typedVector`'s default arm returns its input — nil — for any element type outside string/long/double/boolean. A declared `TypeRef` vector was enough to put a bare `nil` into a tuple's value position, where `ValuesEqual` and `hashValue` panic. Fixed at `typedVector`, pinned by `TestClearedVectorBindsEmptyVectorNotNil` in both cache modes. It never shipped; it is recorded because the reproducers that existed at the time could not have caught it, all using a `TypeString` vector.

## 4. Settled — do not re-derive

- **Merge join is not exposed to D6.** `chooseJoinStrategy` (`hash_join_matcher.go:66-81`) restricts merge join to `position == 0`; value-position joins take the order-free hash join, because on-disk type-tag order deliberately differs from the comparator's rank order. Pinned by `TestChooseJoinStrategySelectsMergeJoinOnlyForEntityPosition`.
- **Schemaless cardinality reconstruction already honors tombstones.** `cardFromOp` treats `OpRGATombstone` as decisive for vector, and append-only storage means the decisive adds always survive. Schemaless S1 classifies correctly for both collection cardinalities.
- **All-removes is S0**, so `inferSchemaFromStore`'s `default: one` costs nothing — S0 answers identically on every cardinality. No limitation to document. `Transaction.Remove` writes byte-identical datoms for the one and many cases (`database.go:1627-1649`), so the ambiguity is real but unreachable in consequence.
- **Pull's primary path is not a second implementation.** `PullExecutor.lookupAttribute` delegates to the same `LookupAttribute` when the matcher implements `EntityLookupMatcher`, which the storage matcher does. Only the fallback and wildcard paths differ — that is D7.
- **Export is datom-level** and unaffected by D5.
- **Equality and hashing on vectors are correct** — `ValuesEqual` compares slices elementwise across representations (`compare.go:422-437`), and the hashing half was fixed under `BUG_VECTOR_VALUES_DEGENERATE_HASHING`. `=` and `!=` predicates route through `ValuesEqual`, not the comparator (`predicate.go:101-106`).

## 5. Units

**A — P1, scan bounds. Done, in PR #114.** `chooseIndex`'s E+A+V arm is cardinality-aware; cardinality-many member-bound keeps its prefix. D1 fixed, D2 withdrawn.

**B — P2 and determinism. Partially done, in PR #114.** The vector half landed: presence resolved, absent (E, A) uncached, D3 and D8 fixed, `valueCount`'s vector arm back to 1. Remaining: **D5** canonical member ordering, **D4** the cardinality-many arms reading `Present` instead of a length, **D7** the two pull paths. D5 still leads what remains — no assertion about a rendered set is trustworthy while Go randomizes the iteration that produces it. Coverage remaining: the parity matrix across S0/S1/S2 × three cardinalities × the accessor set.

**C — the comparator.** D6, untouched. A vector rank in `typeRank`, elementwise recursion through `CompareValues`, lexicographic-then-length for unequal lengths, plus converting the two silent defaults to panics.

**D — this document's §2**, promoted to reference, with the `DATOMIC_COMPATIBILITY.md` divergence entry.

## 6. Sequencing constraint — dissolved

Recorded because it shaped the plan: P1-only scope plus red-reproducers-in-tree meant #114 could not be pushed while D3's reproducer was red, since the pre-push hook runs `make test`. Fixing D3 inside #114 resolved it. `make test` is green, native and wasm.

## 7. Open

- **The telemetry-as-correctness sweep is not finished.** The ruling was to remove *every* correctness decision reading debugging or monitoring data. `RGAStats` is done — it was the one this work tripped over. Whether other such reads exist has not been derived from the code, and deriving it is the difference between a fix and a class fix.
- Whether D4, D5 and D7 get bug documents before their fixes, or whether this document covers them.
