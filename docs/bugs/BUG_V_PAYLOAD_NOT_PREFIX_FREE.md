# The V payload is not prefix-free, so key order is not value order

**Status**: Open, recorded 2026-07-31. Found by
`TestDatomComparatorMatchesKeyOrder`, written for the typed memory backend
(`MEMORY_DATOM_INDEXES.md` PR B) to pin that a typed comparator reproduces the
binary key order. It does not, and cannot.

Not a wrong-answer bug. Scans over-cover and are filtered; they never
under-cover. What it costs is the over-coverage itself, the machinery that
filters it, and the ability to state that both storage backends produce one
order.

## The defect

A value is encoded into a key as `[type][payload]` with no length prefix and no
terminator. Whenever a component follows V in an index's order, byte comparison
of two keys whose payloads are prefixes of one another runs the shorter one into
the *next component's first byte*:

```text
EAVT is [prefix][E][A][type][value][Tx↓][AfterRef?][Op]

V="abc"    → …[s]abc  [Tx↓ …]      next byte is Tx↓[0]
V="abcd"   → …[s]abcd [Tx↓ …]      next byte is 'd' (0x64)
```

`Tx↓[0]` is `^(Lamport >> 56)`, which is `0xFF` for every Lamport below 2^56 —
that is, all of them in practice. `0xFF > 0x64`, so **`"abc"` sorts after
`"abcd"`**, while every value-level comparison in the engine says `"abc"` comes
first.

The same happens in the Tx-before-V orders, where V is followed by
`[AfterRef?][Op]`: `AfterRef↓[0]` is also `0xFF`.

## Evidence

`TestDatomComparatorMatchesKeyOrder` compares, pairwise over a fixture spanning
value types, Tx variations and every Op, the sign of `bytes.Compare` on encoded
keys against the sign of the typed comparator. Six of the eight indices report
the disagreement — EAVT, EATV, AEVT, AETV, ATEV, TAEV — always on the
`"abc"`/`"abcd"` pair.

AVET and VAET pass, and that is luck rather than design. There V is followed by
E, and this fixture's entity hash happens to begin below `0x64`. A different
entity flips them. **An order in which the relative position of two strings
depends on an unrelated entity's hash byte is not a designed order.**

## What it does and does not break

- **No under-scan.** A datom whose V equals a bound always sorts inside the
  range the bound produces, because the endpoints bracket the exact payload.
- **Over-scan, already handled.** The range for `"abc"` contains every key whose
  payload begins `"abc"`. `EncodeScanBound` returns an `EncodedRun` carrying a
  `runMembership` for exactly this reason, and both in-tree iterators consult it.
  The filter is correct; it exists because the range over-covers.
- **Resolution is unaffected.** In EATV/AETV/ATEV/TAEV, Tx precedes V, so V
  ordering cannot perturb the Tx sequence resolution walks. In the V-first
  orders, resolution needs a value's datoms to be *contiguous*, not the distinct
  values to be in any particular order — and after the membership filter they
  are.
- **Full-scan sequence differs from value order** in EAVT, AEVT, AVET and VAET.

## Consequence for the typed memory backend

No typed comparator can reproduce this order. Matching it requires knowing which
bytes follow V, which is the encode path — reproducing it inside the comparator
would rebuild the encoder in the hot descent loop, defeating the representation.

So the memory trees order by value (`"abc" < "abcd"`), the Badger keys order by
bytes, and the two differ for prefix pairs. That is a real difference in what a
full scan returns, and it weakens what the differential test can assert to
"agree except across V-prefix pairs".

It also constrains `PRESORTED_INDEX_SECTIONS`: a section shipped pre-sorted is
sorted in *one* order, so a section written by one backend cannot be bulk-loaded
by the other without a re-sort, which is the point of that proposal.

## The fix, if taken

Order-preserving escaping of the payload — the standard `0x00 → 0x00 0xFF` with
a `0x00 0x00` terminator — makes it prefix-free while preserving byte order.

**Not a length prefix.** Length-first would sort `"b"` after `"aa"`, which is a
different wrong answer.

Taking it deletes `runMembership` and the over-scan it compensates for:
`EncodeScanBound` would return a plain range, and both backends would genuinely
produce one order.

## Where it belongs

Not in PR B. That PR's entire safety argument is that the encoder is a fixed
oracle the differential test measures against; changing the encoding in the same
change makes the test prove only that the new comparator agrees with the new
encoder.

It is a candidate to ride the `TRANSACTION_ENVELOPES` hard break, which already
introduces a physical storage-format version, bumps JDZL, adds an EDN version
marker, and rejects pre-envelope databases with a rebuild-required error. The
migration cost is being paid there once. Opening a second, independent break for
this would charge users two rebuilds for one upgrade.
