# December 2025

## What is here

`KEY_ENCODER_CONSOLIDATION_BENCHMARKS.md` — the A/B measurement for
`refactor/consolidate-key-encoders` (f2b047b, 2025-12-24), which extracted the
shared index-ordering logic of the L85 and Binary key encoders into a
`baseKeyEncoder` driven by a pluggable `ComponentEncoder`. It measured a ~10%
time and ~50% allocation regression on storage-heavy benchmarks and attributed
it to interface dispatch plus a per-call `ensureInitialized()` branch.

## Why it is archived

The branch state it measures no longer exists. The consolidation was kept —
`key_encoder_base.go` is in the tree — but both mechanisms the document blames
were removed when L85 stopped being a physical key format: there is no
`ComponentEncoder` interface and no `ensureInitialized()`. `BinaryKeyEncoder` is
the sole physical encoder, so the second implementation the interface existed to
switch between is gone.

The numbers remain a valid record of what pluggable strategy encoding cost, and
of the general point that interface dispatch on a per-component encode path is
not free.

## Where current information lives

- `PERFORMANCE_STATUS.md` — active optimizations and current benchmarks
- `docs/reference/KEY_ENCODING_AND_CRDT.md` — the key layouts as built
- `docs/perf/` — current campaign measurements
