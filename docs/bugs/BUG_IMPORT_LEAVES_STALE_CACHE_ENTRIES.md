# In-process import leaves stale cache entries

**Status**: Open, out of scope by owner ruling (2026-07-25). The mechanism is
real and reproduced; the scenario is unreachable in current deployment, and the
structural fix belongs to [TRANSACTION_ENVELOPES.md](../proposals/TRANSACTION_ENVELOPES.md).
Recorded so the derivation does not have to be repeated. Do not treat as
scheduled work.

## Summary

`Database.ImportBinary` and `Database.Import` write through `Store.Assert`
directly and never touch `d.cache`. An entry cached before the import still
compares fresh afterwards, so a reader is served the pre-import value even
though storage holds a newer datom that wins CRDT resolution.

## Mechanism

Cache freshness is `slot.entry.version == slot.version` (`cache.go`,
`GetOrResolve`). That is a **write-notification** design: `slot.version`
advances only when a writer tells the cache, and the complete production
notification surface is three calls, all inside `Transaction.Commit`:

- `Cache.Invalidate` — `database.go:2287`, `database.go:2351`
- `Cache.InvalidateAttribute` — `database.go:2357`

(`Cache.Clear` has no production callers.) Invalidation is driven off
`t.datoms` / `t.retracts`, the transaction's own buffer.

Both import entry points bypass that path entirely:

- `Database.Import` (EDN) — `export.go:158`, `export.go:171`
- `Database.ImportBinary` (JDZL) — `export_bin.go:247`

Neither constructs a `Transaction`; both call `d.store.Assert` on batches and
never reference `d.cache`. Because nothing advanced `slot.version`, a
pre-import entry satisfies `entry.version == slot.version` and reads as fresh.
Neither guard in `GetOrResolve` helps: the in-flight sentinel is only set by
`BeginInFlight`, which import never calls, and the snapshot bound only refuses
entries that are too *new* — a pre-import entry sits below any bound and
passes.

`TruncateTo` (`truncate.go:76-92`) demonstrates the same hazard handled
correctly, bracketing its delete with `BeginInFlight` before and
`InvalidateRewind` after, on both the success and failure paths.

## What was verified

Reproduced against `MemoryStore` (see below):

- Storage's LWW winner for the (E, A) after import is the imported value.
  Read directly from EATV, whose Tx-descending order makes the first entry for
  an (E, A) the winner — no query path involved, so the assertion isolates the
  layer.
- `Database.ResolveEntityAttributes` returns the pre-import value. It routes
  every attribute through `d.cache.GetOrResolve` (`database.go:2796`,
  `database.go:2819`), so it is unambiguously the cache path.

## What was not established

An earlier version of the reproducer asserted that a `:in`-bound query
(`[:find ?v :in $ ?e :where [?e :attr ?v]]`) resolves from storage without
consulting the cache, and used that as the isolating assertion. It failed,
returning the pre-import value. That observation was then reported as evidence
that ordinary bound-entity queries are also affected.

**That claim is withdrawn.** The observation is real but the mechanism was
never checked: `PatternMatcher` carries `boundOnce` / `readBound` /
`sessionBounded`, so a read bound or session pinned before the import would
produce the same result as correct snapshot behaviour rather than a stale cache
hit. It also added no reach — it is the same warm-cache-then-import sequence
observed through a second reader, not an independent scenario. The isolating
assertion was replaced with the direct EATV read for exactly this reason.

## Scope ruling (2026-07-25)

Out of scope, on two grounds from the owner:

1. Import runs only as a separate process against a database nothing is
   querying. There is no warm cache to serve stale values, so the sequence the
   defect requires does not occur.
2. The structural fix belongs to the transaction-envelope work.

## The correct fix

Not "make import invalidate" — that repairs one instance and leaves the class.
The defect is that a write-notification cache lives in an open world:
`Store.Assert` is exported and `Database.Store()` hands out a writable store,
so any writer that forgets to notify silently corrupts reads. Replication
apply, a repair tool, or envelope apply would each reintroduce it.

The class fix is a single write boundary through which every logical write
passes, maintaining cache bookkeeping itself rather than relying on each caller
to remember. [TRANSACTION_ENVELOPES.md](../proposals/TRANSACTION_ENVELOPES.md)
already specifies exactly this, reached independently from grouping and
replication requirements rather than from caching:

> Low-level index construction remains available inside storage backends, but
> `Store.Assert` is not a public logical-write contract. The injectable Store
> contract applies complete envelopes.

and, in its hard-break section:

> Replace direct logical uses of `Store.Assert` with atomic envelope apply so a
> successfully opened current-format database cannot contain ungrouped datoms.

It also already defines import as two-phase into a fresh, exclusively held
database that refuses queries until an import-complete marker — the same answer
applied to this same path. Once the boundary exists, "advance storage without
advancing the cache" is unrepresentable, and the invariant the cache owes —
that enabling it must not change any answer — holds by construction.

## Reproducer

Was `datalog/storage/import_cache_invalidation_test.go`; moved here rather than
committed red, since the scenario is ruled unreachable. Fails on the final
assertion with `expected: "after-import", actual: "before-import"`.

```go
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestImportBinaryInvalidatesCache pins the invariant that every path which
// advances storage must advance the cache's bookkeeping.
func TestImportBinaryInvalidatesCache(t *testing.T) {
	entity := datalog.NewIdentity("import-cache:subject")
	attr := datalog.NewKeyword(":import-cache/value")

	// Source database carrying the newer value. Its Lamport clock is advanced
	// first so the imported datom outranks the target's on Lamport alone,
	// independent of ReplicaID tie-breaking.
	src, err := NewDatabaseWithOptions(DatabaseOptions{Store: NewMemoryStore(nil)})
	require.NoError(t, err)
	defer src.Close()

	for i := 0; i < 4; i++ {
		tx := src.NewTransaction()
		require.NoError(t, tx.Set(datalog.NewIdentity(fmt.Sprintf("import-cache:filler-%d", i)), attr, int64(i)))
		_, err := tx.Commit()
		require.NoError(t, err)
	}

	srcTx := src.NewTransaction()
	require.NoError(t, srcTx.Set(entity, attr, "after-import"))
	newerTx, err := srcTx.Commit()
	require.NoError(t, err)

	dumpPath := filepath.Join(t.TempDir(), "dump.jdzl")
	dumpFile, err := os.Create(dumpPath)
	require.NoError(t, err)
	require.NoError(t, src.ExportBinary(dumpFile))
	require.NoError(t, dumpFile.Close())

	// Target database, cache enabled.
	dst, err := NewDatabaseWithOptions(DatabaseOptions{Store: NewMemoryStore(nil)})
	require.NoError(t, err)
	defer dst.Close()

	dstTx := dst.NewTransaction()
	require.NoError(t, dstTx.Set(entity, attr, "before-import"))
	olderTx, err := dstTx.Commit()
	require.NoError(t, err)
	require.True(t, olderTx.Less(newerTx),
		"the imported datom must outrank the resident one, or LWW decides nothing")

	// Warm the cache through the cache-backed resolution path.
	before, err := dst.ResolveEntityAttributes(entity, []datalog.Keyword{attr})
	require.NoError(t, err)
	require.Equal(t, "before-import", before[attr])

	in, err := os.Open(dumpPath)
	require.NoError(t, err)
	defer in.Close()
	require.NoError(t, dst.ImportBinary(in))

	// Establish what storage actually holds, without going through any query
	// path: EATV orders Tx descending, so the first entry for this (E, A) is
	// the LWW winner. This isolates the layer under test rather than relying
	// on a belief about which query paths consult the cache.
	sd := ToStorageDatom(datalog.Datom{E: entity, A: attr})
	start, end := dst.Store().Encoder().EncodePrefixRange(EATV, sd.E[:], sd.A[:])
	iter, err := dst.Store().Scan(EATV, start, end)
	require.NoError(t, err)
	require.True(t, iter.Next(), "storage must hold a datom for this (E, A)")
	winner, err := iter.Datom()
	require.NoError(t, err)
	storedWinner := winner.V
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())
	require.Equal(t, "after-import", storedWinner,
		"the imported datom must be storage's LWW winner")

	// The cache-backed read must agree with storage.
	after, err := dst.ResolveEntityAttributes(entity, []datalog.Keyword{attr})
	require.NoError(t, err)
	require.Equal(t, "after-import", after[attr],
		"cache served a value the import superseded")
}
```

Note for whoever revives this: `Store.Scan` and `Store.Encoder()` are being
converted to typed `ScanBound` bounds, and `Encoder()` is leaving `StoreReader`
(see the Ruling 1 section of
[MEMORY_DATOM_INDEXES.md](../proposals/MEMORY_DATOM_INDEXES.md)). The direct
EATV read above will need rewriting against whichever surface exists then.
