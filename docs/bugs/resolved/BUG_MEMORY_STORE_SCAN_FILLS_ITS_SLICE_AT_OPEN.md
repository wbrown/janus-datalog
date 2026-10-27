# `MemoryStore.scan` filled its whole slice when the scan opened

**Status**: Resolved.

The scan answered the run it was asked for, so no answer was wrong. The
defect was a scan that paid for the whole run before the first pull, on the
one scan path that did not go through a read session.

## The two scan paths

A read session's scan on the memory backends walks its run as the consumer
pulls: the session clones the key tree copy-on-write at open, and its
iterator takes the run's keys from the clone a chunk at a time, so a
consumer that stops early, or that only ever asks for the first key, pays
for what it read.

`MemoryStore.scan` (`datalog/storage/memory_store.go`), the store's own
scan outside a session, collected every key of the run into a slice at open
and handed the slice out. It read the live tree under the store's read
lock, and the slice was what let the lock be released before the consumer
read.

## The callers

- `MaxElementID`, which reads one key, the first of the TAEV index, since
  that index orders Tx descending. Every query reads it for its cache bound.
- `MaxTxForEntity`, which walks the entity's EAVT range for its highest
  ElementID.
- `DatomsAfter`, called by `TruncateTo`, which reads the TAEV index until
  it reaches the given ElementID.

`MaxElementID` read one key from a slice built over every key of the store.

## The fix

The store's scan takes the read session's path. `MemoryStore.scan` clones
the key tree under the store's write lock, since `Clone` mutates the source
tree's copy-on-write context, releases the lock, and hands the clone to
`chunkedMemoryIterator`, the one constructor of the memory iterator, which
the read session's scan calls too. The iterator takes the run's keys from
the clone `memoryScanChunkKeys` at a time as its consumer pulls, with no
lock held. A scan answers the store as of its open: a write after the open
reaches the live tree and not the clone.

`MaxElementID` and `MaxTxForEntity` on the store call `maxElementIDByScan`
and `maxTxForEntityByScan` (`datalog/storage/read_session.go`), as the
read session does.

The alternative, holding the store lock across the pulls, was ruled out: it
blocks every writer for the scan's lifetime, where the clone blocks writers
for the clone alone.

## Tests

`TestMemoryStoreScanPaysForWhatItReads`
(`datalog/storage/memory_store_scan_test.go`):

- `MaxElementID` allocates no more over a store holding eighty chunks of
  keys than over one holding eight.
- A scan opened before a write does not yield the written key.
- A walk across chunk boundaries yields every key of the range, in
  strictly increasing order.
