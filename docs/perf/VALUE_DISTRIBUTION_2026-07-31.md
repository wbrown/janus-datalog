# Value distribution of a production database

Measured 2026-07-31 with `datalog -stats` over a JDZL dump of a production
database: **2,708,364 datoms, 1,024,549 entities, 221 attributes**. This is the
dataset behind the wasm linear-memory ceiling that motivates
[`MEMORY_DATOM_INDEXES.md`](../proposals/MEMORY_DATOM_INDEXES.md).

The raw report is not committed. It enumerates the application's schema
attribute by attribute, which is not this repository's to publish; what follows
is the shape, which is.

## Value bytes

| | |
|---|---:|
| Encoded value bytes, total | 61.11 MiB |
| Encoded value bytes, deduplicated | 29.10 MiB |
| Duplicated | 52.4% |
| **Per datom** | **23.7 B** |

Those are uncompressed encoded value bytes counted once. A key-backed store
embeds each value in all eight index keys, so its value bytes are roughly eight
times the total.

**This settles an assumption rather than a conclusion.** The sizing model in
`MEMORY_DATOM_INDEXES.md` projects at `|V|` of 8 and 24 bytes, chosen without
data. The measured mean is 23.7, at the upper column. The model's per-datom
figures were not calibrated to this dataset and did not need revising after it.

## Type distribution

| Type | Count | Total | Mean |
|------|------:|------:|-----:|
| keyword | 1,093,388 | 15.38 MiB | 14.8 B |
| time | 940,197 | 7.17 MiB | 8 B |
| ref | 436,518 | 8.33 MiB | 20 B |
| int64 | 96,503 | 753.93 KiB | 8 B |
| elementid | 94,069 | 1.44 MiB | 16 B |
| string | 42,247 | 28.06 MiB | 696.3 B |
| bool | 5,442 | 5.31 KiB | 1 B |

Strings are 1.6% of values and 46% of value bytes. Every other type is
fixed-width or nearly so.

## Concentration

Two attributes are two thirds of the database.

| | Datoms | Share | Distinct values |
|---|---:|---:|---:|
| One keyword-valued attribute | 876,803 | 32.4% | **5** |
| `:db/txInstant` | 926,141 | 34.2% | 926,137 |
| Together | 1,802,944 | **66.6%** | |

The first carries 11.82 MiB of value bytes for 82 bytes of distinct content — a
duplication factor of 175,360. A single value in it occurs **495,909 times**;
the next occurs 378,798 times.

The second is what the transaction-envelope work removes, and its share here
matches the figure that work already cites.

## What this implies for the typed representation

The sizing model reasons about bytes per datom uniformly. This dataset is not
uniform, and the non-uniformity favours typed trees by more than the model
projects:

- **Keywords are already interned pointer types.** 1,093,388 keyword
  occurrences totalling 15.38 MiB become one pointer each into a small interned
  population. A key-backed store writes those bytes into eight index keys — on
  the order of 123 MiB of key bytes for content whose distinct form is a few
  hundred bytes.
- **Fixed-width types dominate by count.** keyword, time, ref, int64, elementid
  and bool are 98.4% of values. For these the tree's marginal cost per datom is
  a pointer or an inline word; the key-backed cost is the encoded bytes, eight
  times over.
- **Compression is a small effect here.** Strings are the only compressible
  population at 28.06 MiB. At the codec's documented ~3.6× on prose that is
  roughly 20 MiB saved, against a projected typed-store footprint of ~459 MB —
  about 4%. A typed store that holds values uncompressed is not giving up much
  on this shape, though a string-heavy dataset would say otherwise.

## CRDT operations

| Op | Count | Share |
|----|------:|------:|
| none | 2,555,352 | 94.4% |
| rga-insert | 94,202 | 3.5% |
| add | 58,810 | 2.2% |

No Remove and no RGA tombstone appear at all. `AfterRef` is therefore
meaningful on 3.5% of datoms and zero on the rest, which is consistent with
keeping it inline in `Datom` rather than paying a pointer on every datom to
recover eight bytes from all of them.

## Reproducing

```
go run ./cmd/datalog -db <dump>.jdzl -stats
```

Any figure quoted from a later run should state its dump, since these are
properties of one dataset at one point in its life, not of the engine.
