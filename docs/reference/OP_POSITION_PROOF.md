# Op Byte Position in Index Keys: A Correctness Proof

## Model

### Storage model

* The database is an ordered key-value store.
* Keys are compared by **lexicographic byte order**.
* A **prefix scan** over byte string `P` enumerates all keys `K` with prefix
  `P` in increasing lexicographic order.

### Datom

A datom is a tuple:

$$d = (E, A, V, T, Op, AfterRef?)$$

Where:

* `E` (20 bytes) entity id
* `A` (32 bytes) attribute id
* `V` (variable bytes) value encoding (type-tagged, not fixed length)
* `T` (16 bytes) operation id (Lamport + replica); encoded as `T↓` so that
  lexicographic order corresponds to **descending** logical time
* `Op` (1 byte) operation kind: `0=None, 1=Add, 2=Remove, 3=RGAInsert,
  4=RGATombstone`
* `AfterRef` (16 bytes) present iff `Op ∈ {3,4}`

### Invariants (required)

**I1. Unique operation id.** For any two distinct datoms `d1 ≠ d2`,
`T1 ≠ T2`.

**I2. Total order.** The encoding `T↓` is strictly order-preserving for
descending time:

* If `T1 > T2` (newer), then `enc(T1↓) < enc(T2↓)` in lexicographic order.

**I3. Max-T semantics for resolution groups.** For each resolution group `G`
(defined below), the current state is determined solely by the datom in `G`
with maximal `T` (newest). `Op` is interpreted only after that winner is
selected.

This covers:

* **Scalar LWW (cardinality-one):** winner is newest write for `(E,A)`.
* **LWW membership per value (cardinality-many):** membership of `(E,A,V)`
  determined by newest op for that `(E,A,V)` (Add vs Remove).
* **RGA liveness per element id (cardinality-vector):** liveness of an
  element id determined by newest op for that element (Insert vs Tombstone).
  Note: RGA liveness satisfies max-T semantics, but RGA **topology
  reconstruction** (vector ordering) requires reading all entries for an
  `(E,A)` group and building a linked-list from AfterRef pointers. First-
  entry-wins determines alive/dead per element, not vector position.

This is *not* causal add-wins (OR-Set) semantics; causal add-wins needs
additional metadata (version vectors, observed-remove tags) and does not
satisfy I3.

**I4. AfterRef presence is a function of Op.** `AfterRef` exists iff
`Op ∈ {3,4}`.

---

## Keys and resolution groups

A key is a concatenation of components in some index-specific order:

$$K = [\text{IndexPrefix}][\dots \text{sort components} \dots][\dots \text{payload} \dots]$$

The discussion concerns two kinds of components:

1. **Sort-relevant components:** those that must participate in ordering to
   make the "winner" appear first in a scan.
2. **Sort-irrelevant components:** those used only after reading a winner
   (semantic tag, decoding aid).

### Resolution groups

Different indices support different streaming queries:

* **Scalar LWW read:** resolve by scanning keys grouped by `(E,A)` and
  taking newest `T`.
  * A suitable index orders as: `E, A, T↓, ...`.

* **Per-value membership read:** resolve by scanning keys grouped by
  `(E,A,V)` and taking newest `T` for that value.
  * A suitable index orders as: `E, A, V, T↓, ...`.

* **RGA element liveness read:** same group shape `(E,A,V)` where `V`
  encodes the element content.
  * Again requires: `..., V, T↓, ...`.

In all cases, I3 says "winner = argmax_T in the group".

---

## Property to prove

### Desired property (First-entry-wins)

For each resolution group `G`, scanning the keys for `G` yields the newest
datom first, so the state can be determined with **one read per group**.

Formally: for group prefix `P` (e.g., bytes of `[E][A]` or `[E][A][V]`),
let:

$$S(P) = \{ d \mid Key(d) \text{ has prefix } P \}$$

First-entry-wins means:

* the first key returned by prefix scan over `P` corresponds to datom `d*`
  with maximal `T` among `S(P)`.

---

## Theorem 1: If Op is ordered before T, first-entry-wins fails for max-T semantics

### Statement

Assume I1-I3. Consider a group where the winner is the datom with maximal
`T` among **mixed Op values** (e.g., Add vs Remove, Insert vs Tombstone).
If keys are ordered as:

$$[\dots][\text{Op}][T{\downarrow}][\dots]$$

within a fixed group prefix, then first-entry-wins does not hold in general:
the first entry in the scan need not be the global max-`T` datom of the
group, and determining the winner requires reading from multiple `Op`
partitions.

### Proof

Fix a resolution group `(E,A,V)` and assume two ops exist in that group:
`Op=Add` and `Op=Remove`.

Construct three datoms in the same group:

* `d1 = (E,A,V, T=200, Op=Add)`
* `d2 = (E,A,V, T=150, Op=Remove)`
* `d3 = (E,A,V, T=100, Op=Add)`

The global newest is `d1` (T=200).

With ordering `Op` then `T↓`:

* All `Add` keys appear before all `Remove` keys (because `Op` differs
  before `T↓` in the key).
* Inside the `Add` partition, `d1` appears before `d3` (because `T↓` is
  descending).
* The first key in the group is therefore the newest **Add**, i.e., `d1`.

Now change only one fact: add a single remove with higher `T`:

* `d4 = (E,A,V, T=250, Op=Remove)`

Global newest is now `d4` (T=250), so the correct state is "removed" under
I3.

But with `Op` before `T↓`, all `Add` entries still come before all `Remove`
entries; therefore the first entry returned by the scan is still an `Add`
entry (specifically the newest Add). That first entry cannot determine the
correct state, because the true max-`T` might be in the later `Remove`
partition.

Therefore, the scan's first entry is not guaranteed to be the max-`T` datom.
First-entry-wins fails.

Moreover, any correct algorithm must, in the worst case, inspect at least
one entry from each relevant `Op` partition to compare their maxima. For
two partitions (Add/Remove), that implies >=2 reads plus a seek (or
equivalent) to reach the other partition.

∎

---

## Theorem 2: If T is ordered before Op, first-entry-wins holds for all max-T semantics

### Statement

Assume I1-I3. For a resolution group whose winner is the max-`T` datom, if
keys order the group as:

$$[\dots \text{group prefix} \dots][T{\downarrow}][\dots]$$

with `T↓` preceding any other varying fields (including `Op` and
`AfterRef`), then the first key returned by a prefix scan over the group
corresponds to the max-`T` datom. Hence resolution is one read per group.

### Proof

Fix a group prefix `P` (bytes of the group-defining components, e.g.,
`[E][A]` or `[E][A][V]`).

Within all keys sharing prefix `P`, the next compared bytes are the bytes
of `T↓` (by hypothesis). Because `T↓` is strictly descending-order-
preserving (I2), the lexicographically smallest `T↓` among the group
corresponds to the largest logical `T`.

Lexicographic order compares by the first differing byte. Since `T` differs
between any two distinct datoms (I1), `T↓` differs too; thus `T↓` strictly
determines the ordering among all keys in the group. Therefore, the first
key in the scan over `P` is exactly the datom with maximal `T`.

Under I3, the state for the group is a function only of that max-`T`
datom's `Op` (and possibly other payload). So one read suffices.

∎

---

## Theorem 3: Under unique T, any fields after T do not affect scan order

### Statement

Assume I1 and that, within any resolution group, `T↓` appears in the key
before fields `X` and `Y`. Then rearranging `X` and `Y` (including `Op`
and `AfterRef`) after `T↓` does not change the relative order of any two
distinct keys in the group.

### Proof

Take any two distinct datoms `d1 ≠ d2` within the same group prefix `P`.

By I1, `T1 ≠ T2`. Since `T↓` bytes occur before `X` and `Y`, the
lexicographic comparison between the two keys will encounter a difference
in the `T↓` region before it ever examines bytes in `X` or `Y`.

Therefore, the relative order of the two keys depends only on `T↓` (and
earlier group prefix), not on any later fields. Reordering bytes after `T↓`
cannot affect which key comes first.

∎

**Corollary.** Once `T↓` precedes both `Op` and `AfterRef`, the choice
`[...][Op][AfterRef?]` vs `[...][AfterRef?][Op]` is irrelevant to ordering.
It only affects decoding.

---

## Theorem 4: Making Op the last byte yields an unambiguous decoder

### Encoding schema

For every index, place all sort-relevant components first, then optional
`AfterRef`, then `Op` as the final byte:

$$K = [\text{IndexPrefix}][\text{GroupComponents}][T{\downarrow}][\text{OtherComponents}][\text{AfterRef?}][\text{Op}]$$

All 8 indices in this system:

```
EAVT: [prefix][E][A][V][T↓][AfterRef?][Op]
EATV: [prefix][E][A][T↓][V][AfterRef?][Op]
AEVT: [prefix][A][E][V][T↓][AfterRef?][Op]
AETV: [prefix][A][E][T↓][V][AfterRef?][Op]
ATEV: [prefix][A][T↓][E][V][AfterRef?][Op]
AVET: [prefix][A][V][E][T↓][AfterRef?][Op]
VAET: [prefix][V][A][E][T↓][AfterRef?][Op]
TAEV: [prefix][T↓][A][E][V][AfterRef?][Op]
```

### Statement

Assume fixed sizes for `E, A, T, AfterRef, Op` and I4. If `Op` is stored
as the final byte of every key, then:

1. `Op` can be read in O(1) time at `key[len-1]`,
2. presence and location of `AfterRef` is determined without heuristics,
3. the split between `V` and trailing fields is uniquely determined.

### Proof

Let `K` be any key.

1. Since `Op` is last, `Op = K[len(K)-1]` is well-defined.

2. By I4, `AfterRef` is present iff `Op ∈ {3,4}`. If present, it occupies
   the 16 bytes immediately preceding `Op`, so:
   * `AfterRef = K[len(K)-1-16 : len(K)-1]`.
   If absent, no bytes are consumed.

3. All remaining fixed-size components have predetermined offsets in each
   index definition:
   * In layouts like `EAVT`, `T↓` is the 16 bytes immediately preceding
     `AfterRef?/Op`, so it is sliceable from the end once `Op` (and
     optionally `AfterRef`) are removed.
   * In layouts like `EATV`, `E`, `A`, `T↓` are at fixed offsets from the
     start, leaving `V` as the remaining middle segment before the trailing
     `AfterRef?/Op`.

Thus decoding is a total function from bytes to fields with no ambiguity
and no dependence on the contents of `V`.

∎

---

## Theorem 5: If Op is not at a fixed position, any length-based AfterRef heuristic is unsound with variable-length V

### Statement

If `V` is variable-length and `AfterRef` is optional, then any decoder that
decides "has AfterRef" by key length thresholds (or by probing bytes at a
computed offset that is not guaranteed to be `Op`) can misclassify some
valid keys, causing incorrect slicing and possible decode failure.

### Proof (existence)

Let a heuristic be any function `H(K)` that tries to infer AfterRef
presence without reading a known-position `Op` (because `Op` is not at a
fixed location). Because `V` is variable-length, for any fixed threshold or
offset rule there exist keys whose `V` length makes `H` take the wrong
branch:

* keys without `AfterRef` that are long enough to satisfy the threshold, or
* keys with `AfterRef` that are short enough to violate it (if thresholds
  exist), or
* keys where the probed byte lies inside `V` and coincidentally equals a
  value interpreted as "RGA op".

In any such misclassification, the decoder will interpret bytes belonging
to `V` as either `Op` or `AfterRef`, changing boundaries and corrupting
decoding.

Therefore, heuristics are not correctness-preserving in the presence of
variable-length `V` and optional `AfterRef`. A fixed-position tag (Op-last)
eliminates this entire class.

∎

This class of bug was observed in practice: see `docs/bugs/BUG_SHARED_DB_DATOM_LOSS.md`
for the specific instance where a length-based heuristic caused ~0.78%
silent datom loss on reference-valued attributes.

---

## Final conclusion

Under invariants I1-I4 (especially: unique `T` per datom and max-`T` group
semantics), the following are correct:

1. **`T↓` must precede `Op`** in any index where mixed-op groups are
   resolved by selecting the max-`T` datom. Otherwise first-entry-wins
   fails and additional reads are required (Theorem 1 vs Theorem 2).

2. Once `T↓` is before them and `T` is unique, **`Op` and `AfterRef` do not
   affect ordering** and can be placed in any order after `T↓` (Theorem 3).

3. For decoding robustness with variable-length `V` and optional `AfterRef`,
   **`Op` should be the last byte**, with `AfterRef` immediately before it
   when present (Theorem 4). This makes AfterRef detection exact (I4) and
   removes heuristic misparses (Theorem 5).

### Canonical key suffix

For every index:

* Suffix is always `[AfterRef?][Op]`
* `Op = key[len-1]`
* `AfterRef` exists iff `Op ∈ {3,4}` and is the preceding 16 bytes

### Required note

If you ever relax I1 (multiple datoms sharing the same `T`), then some
field **before** `Op` must provide a strict tie-breaker, or the "fields
after T never affect order" claim no longer holds. In that case, keep `Op`
last for decoding, but introduce a deterministic tie-breaker byte sequence
before it.
