# The value domain is enforced at one of its three doors

**Status**: Ruled and fixed 2026-07-29, all four parts. Discovered 2026-07-26 by
reading, while auditing the benchmark that disabled the iterator-reuse join
strategy — that benchmark put a `**datalog.identity` in a binding tuple's E
position and, in October 2025, all three of hashing, equality and ordering
accepted it. Two defects are recorded here because they are one class: the closed
value domain is *defined* by three functions and *enforced* by one.

**Ruled 2026-07-29.** `typeRank` enumerates the domain and its default panics;
vectors compare element-wise through `CompareValues`; vectors rank **last**, after
`ElementID`; and `ValuesEqual`'s panic becomes a domain check rather than a
comparability check.

The rank position was chosen on the composite-versus-scalar asymmetry — a vector
is the domain's only type that contains domain values, so it has no natural place
among the scalars. It was explicitly *not* chosen to preserve today's cross-type
order: vectors sort last now only because the silent default put them there, so
that behaviour is an artifact of the defect rather than a baseline.

The fourth part was first presented as a choice between enforcing and correcting
the documents. It is not one. The second branch would document that equality
accepts out-of-domain comparables and compares them by address, which is what
Pillar 2 names as worse than a panic — "a silent fallback (hash-by-address, blind
`==`) … corrupts joins and dedup invisibly." Enforcement is the only branch that
does not require rewriting the discipline to permit the defect. Deferring it also
leaves the recorded class — defined by three, enforced by one — merely advanced to
enforced by two, with three doors then behaving three different ways.

## Summary

Pillar 2 states that `datalog.ValuesEqual` and `executor.hashValue` define the
value domain and enforce it loudly. `datalog.CompareValues` — the third
function that consumes the domain, and the one behind `:order-by`, `min`, `max`,
comparison predicates, and merge join — is not named in that statement and does
not enforce anything.

Two consequences, one live in shipped behaviour:

1. **`CompareValues` has no vector case.** Vectors are domain values, and the
   equality and hashing layers were both taught them. Ordering was not, so a
   vector reaches the unknown-type fallback and is ordered by
   `fmt.Sprintf("%v", …)`. `:order-by` and `min`/`max` over a
   cardinality-vector attribute return wrong answers today.
2. **`ValuesEqual`'s panic is a comparability check, not a domain check.** It
   fires on `!reflect.Value.Comparable()` — maps, funcs, stray slices. Any
   *comparable* non-domain value (any pointer) silently takes `a == b` and
   compares addresses.

## Mechanism

The three doors, for a value outside the domain:

| | behaviour | site |
|---|---|---|
| `executor.hashValue` | panics naming the type | `tuple_key.go:208` |
| `datalog.ValuesEqual` | `a == b` if the type is comparable | `compare.go:446-452` |
| `datalog.CompareValues` | rank 10, then `strings.Compare` of `%v` forms | `compare.go:182`, `:51-57`, `:469-471` |

### Ordering has no vector case

`CompareValues` (`compare.go:73-183`) dispatches on Identity, Keyword, Symbol,
ElementID, the integer widths, `uint64`, `float64`, `string`, `[]byte`, `bool`
and `time.Time`. Everything else reaches `return compareByRank(left, right)` at
`:182`. `typeRank` (`:20-45`) has no slice case beyond `[]byte` (rank 5), so a
vector ranks 10; two vectors therefore share a rank, and `compareByRank`
(`:51-57`) resolves same-rank pairs with
`strings.Compare(stringValue(left), stringValue(right))`. `stringValue`'s
default (`:469-471`) is `fmt.Sprintf("%v", v)`.

The domain's only slice-shaped members are `[]byte`, which has an explicit case
and a rank, and vectors, which have neither.

Worked example, derived from those four sites:

- `[]interface{}{int64(2)}` renders `"[2]"`; `[]interface{}{int64(10)}` renders
  `"[10]"`.
- `strings.Compare("[10]", "[2]")` is `-1` — `'1'` (0x31) precedes `'2'` (0x32).
- So `CompareValues` reports that the vector holding 10 sorts *below* the vector
  holding 2.

Ascending `:order-by` on that variable emits them in that order, and `(min ?v)`
returns the vector holding 10.

### Equality enforces comparability, not the domain

`ValuesEqual` handles every slice kind element-wise via reflection
(`compare.go:418-437`) — this is what makes `[]string{"a"}` equal
`[]interface{}{"a"}`. Its panic (`:446-451`) is guarded by
`ra.IsValid() && !ra.Comparable()`. A pointer is comparable, so a value like
`**identity` never reaches the panic; it falls to `a == b` at `:452` and
compares addresses.

`hashValue` is the only one of the three that actually enumerates. It has an
explicit `[]interface{}` case (`tuple_key.go:172`), a reflective typed-slice
case (`:191-199`), and a panic for everything else (`:208`).

### How the miss happened

`BUG_VECTOR_VALUES_DEGENERATE_HASHING` (resolved 2026-07-04) taught the hash
layer about vectors and cited the equality layer as already correct. Its own
status line records that it was discovered "while enumerating the value domain
for `BUG_PULL_WITH_ORDER_BY_PANICS`'s class fix." The domain was therefore
enumerated twice, and `CompareValues` was in neither enumeration.

## What was verified

By reading, at the sites cited above:

- `CompareValues` has no case reaching any slice other than `[]byte`, and
  `typeRank` gives every other slice the catch-all rank.
- `ValuesEqual` compares slices element-wise and panics only on
  uncomparable types.
- `hashValue` enumerates vectors explicitly and panics on the rest.

Empirically: `go test -bench BenchmarkJoinStrategyThreshold` panics with
`hashValue: **datalog.identity is not a datalog value type`
(`tuple_key.go:208`), reached from `NewMaterializedRelation`'s deduplication —
confirming that the hash door is the only one that stops an out-of-domain value,
and that it stops it at relation construction rather than at comparison.

Vector values do reach relation tuples: the storage vector-matching path
produces typed slices in tuples, recorded in
`BUG_VECTOR_VALUES_DEGENERATE_HASHING`'s status line as the second instance that
its own fix exposed.

Live consumers of `CompareValues`, all production:

- `:order-by` — `executor/executor_utils.go:49` (`SortRelation`), `:94`
  (`compareTuplesByOrder`)
- `min` / `max` — `executor/aggregation.go:585`, `:596`
- binding sort — `executor/relation.go:698`, `:1279` (`Sorted`)
- comparison predicates `<`, `<=`, `>`, `>=` — `query/predicate.go:113-119`,
  `:188-194`
- merge join advance and key equality — three `CompareValues` calls in
  `mergeJoinIterator.Next` (`storage/hash_join_matcher.go`): pairing a datom
  with its key group, advancing past binding groups below the datom's key, and
  the key-equality test that opens a group. Cited by function: these three were
  given as line numbers in round 2, re-derived to different numbers in round 4,
  and had moved again by 2026-07-30 — all three drifts within one unchanged
  function.

## What was not established

- **No shipped wrong answer is on record.** The ordering defect is established
  by reading the four sites and composing them, not from a report or a failing
  test. No reproducer has been written or committed.
- **Whether an out-of-domain value can reach ordering or equality in practice.**
  `hashValue` catches such values at relation construction, so a value that
  passes through any `TupleKeyMap` is stopped before it can be sorted. A value
  that is only ever sorted or compared would slip both remaining doors, but no
  such path has been identified. The October 2025 benchmark is the only observed
  instance and it predates the hash door's panic.
- **Whether the merge-join sites are affected.** They compare E-position
  Identities, which are in the domain and have a rank.
  `chooseJoinStrategy` restricts merge join to position 0 precisely
  because that is where `CompareValues` order and scan order provably agree.
  Noted as adjacent rather than implicated: those sites also use
  `CompareValues(...) == 0` as an *equality* test, and `compare.go:129-133`
  records that `cmp == 0` does not imply `ValuesEqual` within the numeric rank.
  Their safety rests on the position-0 restriction, not on the two agreeing in
  general.

## The correct fix

Not "add a slice case to `CompareValues`" — that repairs the instance and leaves
the class, which is that one of three domain functions was never enumerated.

**Ordering enumerates the domain.** `typeRank` gains a rank for vectors and its
default panics, mirroring `Type()`, `hashValue`, and the convention the other
two doors already follow. Once every domain type has a rank, the unknown arm has
no legitimate traffic, and `stringValue`'s `%v` default stops being reachable
from `compareByRank` for any value that belongs in a relation.

**Vectors compare element-wise**, lexicographically via `CompareValues`
recursively, with the shorter vector lower on a prefix tie. That is the same
traversal `ValuesEqual` performs, so `ValuesEqual(a, b) ⇒ CompareValues(a, b) == 0`
holds by construction and across representations, matching the invariant
`hashValue` already maintains. The converse does not hold and is not required —
it already fails for scalars within the numeric rank (`compare.go:129-133`).

**Where vectors sit in the rank ladder is a user-visible decision**, not an
implementation detail: it fixes how a heterogeneous `:order-by` interleaves
vectors with scalars. *Ruled 2026-07-29: last, after `ElementID`.* A consequence
to know: element-wise recursion means a heterogeneous vector's position follows
its first element's rank, so vectors with mixed element types interleave with
each other by that element rather than clustering.

**Equality enforces the domain.** *Ruled 2026-07-29.* The invariant
`ValuesEqual(a,b) ⇒ hash(a) == hash(b)` is only as strong as the weaker of the two
enforcements, and today equality is the weaker. The alternative — leaving it a
comparability check and correcting `CLAUDE.md`'s Pillar 2 and the Core Model's
"Both panic (mirroring `datalog.Type()`)" — would document address comparison of
out-of-domain values as intended, which is the fallback Pillar 2 names as worse
than a panic. Every domain type is already handled by an explicit arm before the
trailing `a == b`, so what reaches it is nil and out-of-domain values, and both
panic naming what was rejected.

*Amended 2026-07-29: nil gets no arm.* An earlier draft of this part gave the nil
pair one, on the reasoning that `a == b` already answered true for it. That
codified an accidental side effect of the fallback being deleted. nil is absence,
not a member of the domain, so all three doors reject it — including
`hashValue`, whose `case nil: return 0` and nil-`*ElementID` branch made hashing
the *permissive* door once the other two rejected. A permissive hash feeding a
strict equality is worse than either alone: nil-bearing tuples share a bucket and
equality then panics inside the map lookup that was resolving the collision.

**Expect the panic to detonate on first full-suite contact.** That is what
happened when `hashValue`'s address fallback was replaced: the panic immediately
exposed a second instance of the same defect (typed slices from the storage
vector path). Treat what a panicking `typeRank` surfaces as further instances to
fix, not as regressions to suppress.

## Reproducer

*Written 2026-07-29, all four, green.*

1. **Ordering direction** — `TestCompareValues_KnownPairs` gains the vector rows:
   `[10]` vs `[2]` is `+1` (it was `-1`), the reverse, the shorter-first prefix
   tie, cross-representation equality, and vectors ranking above numerics and
   `ElementID`. The direction is what the antisymmetry and transitivity laws
   cannot pin, which is why it belongs in the concrete table.
2. **Cross-layer invariant** — obtained by extending `mixedTypeValues()` rather
   than adding a test: the fixture now carries vectors including `[]int64{2}`
   against `[]interface{}{int64(2)}`, so `_ZeroIsEquality` exercises
   `cmp == 0 ⇒ ValuesEqual` across representations, and `_Antisymmetric`,
   `_Transitive` and `_SortStable` cover the new rank for free.
3. **Taxonomy completeness** — `TestValuesEqualRejectsNonValueTypes` and
   `TestCompareValuesRejectsNonValueTypes`, both doors, **both operand
   positions**.
4. **End-to-end** — `TestVectorOrderingIsElementWiseEndToEnd`
   (`datalog/storage/vector_ordering_test.go`): a cardinality-vector attribute
   holding `[2]` and `[10]`, where ascending `:order-by` returns `[2]` first and
   `(min ?v)` is `[2]`. One-element vectors are the sharpest case because text
   order and element order disagree, so a comparator cannot pass by accident.

The assertions as originally specified, retained because the order they were
written in mattered:

1. **Unit, ordering.** `CompareValues([]interface{}{int64(10)}, []interface{}{int64(2)})`
   must be positive. It is currently `-1`.
2. **Unit, cross-layer invariant.** For a table of vector pairs including
   cross-representation ones (`[]string{"a"}` vs `[]interface{}{"a"}`),
   `ValuesEqual(a, b)` must imply `CompareValues(a, b) == 0`.
3. **Unit, taxonomy completeness.** Every type in the value domain has a
   `typeRank`; an unclassified type panics. Mirrors the shape of the existing
   `hashValue` domain test.
4. **End-to-end.** A cardinality-vector attribute with vectors `[2]` and `[10]`,
   queried with `:order-by [[?v :asc]]`, must return `[2]` first; `(min ?v)`
   must return `[2]`.

Assertion 4 is the one that demonstrates the defect is user-visible rather than
internal, and it should be the first one read.

## What implementing it surfaced

Four things the analysis above did not contain. The first is a second defect of
the same class; the rest are consequences.

**Equality enforced the domain on the left operand only.** Not nil-specific — the
class is general. Every arm of `ValuesEqual` dispatches on `a` and then
*assertion-tests* `b`, and a failed assertion is indistinguishable from a
legitimate type mismatch, so `ValuesEqual("x", map[string]interface{}{...})`
returned `false` while the operands reversed panicked. This part's premise —
"every domain type is already handled by an explicit arm" — is a statement about
`a`; the right operand was never contemplated in it. Ordering never had the bug
because `compareByRank` ranks *both* sides. Fixed by classifying `b` through
`typeRank` at entry: the domain stays enumerated once, and equality gets the
symmetry ordering already had.

**Four pointer-wrapper types were in the domain and shouldn't have been.**
`*uint64` was a Tx representation predating the Lamport `ElementID` — the tell is
`matchesDatom` dereferencing `tx.(*uint64)` immediately before
`tx.(*datalog.ElementID)`, and `ValueBytes` encoding it as raw big-endian while
`int64` gets `orderedInt64`, i.e. a monotonic counter's encoding, not a user
integer's. `Type()` also carried `*Identity`/`*Keyword`/`*Symbol`, which are
*double* pointers given interning, and which nothing constructs. All four removed;
`uint64` itself stays.

**One live production site passed a nil into a door.**
`matcher_relations.go` V-validation handed `entry.OneValue()` — nil for a
tombstoned or never-set (E, A) — straight to `ValuesEqual`, with a comment relying
on nil-vs-value answering false. Five sibling readers of `OneValue()` all guard it;
this one did not. Fixed by testing for absence instead of comparing it. Note the
coverage shape: it needs cache path + CardinalityOne + tombstone + V-bound, and
every Remove test binds E via `:in`, which is the streaming path — the same gap
that produced `BUG_CACHE_CARDINALIY_ONE_TOMBSTONE`.

**Relation construction hashed placeholders.** `perTupleInputBuilder.Session`
pre-wires a relation around `make(Tuple, 1)` and writes the value per iteration,
so `deduplicateTuples` was hashing a slot holding nothing. The nil was never data.
Deduplicating a single tuple is identity — one tuple cannot duplicate itself — so
that case now returns without hashing, which also stops every 1-tuple relation in
the engine from building a `TupleKeyMap` to remove duplicates that cannot exist.
The rejection therefore happens when a tuple is *hashed*, not at construction.

## Adjacent, unverified

`executor.ScanFingerprint` has no `VectorConstant` case, so a vector literal in a
pattern falls to its `default:` and is fingerprinted through `fmt.Sprint` — while
that function's own comment states human formatting is not injective, which is why
every other component is tagged and length-delimited. Two patterns differing only
inside a vector literal may fingerprint identically. What the fingerprint gates has
not been established, so this is recorded rather than claimed.

This is also the correction to a false lead: the vector case was first thought to
be *needed* by `Type()`/`ValueBytes` via `ScanFingerprint`, on the assumption that
a vector literal arrives as a `query.Constant`. It does not — the parser builds
`query.VectorConstant`, a distinct element type. `Type()` and `ValueBytes` are the
database serialization path (Badger key encoding, `EncodeRGAElement`, JDZL export),
and their `default: panic` is what proves a whole vector never reaches storage.
Teaching them a vector encoding would write bytes `ValueFromBytes` cannot decode
and would not be order-preserving in AVET/VAET keys — silent corruption in place of
a loud guard.
