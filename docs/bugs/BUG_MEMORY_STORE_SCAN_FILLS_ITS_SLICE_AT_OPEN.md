# `MemoryStore.scan` fills its whole slice when the scan opens

**Status**: Open.

The scan answers the run it is asked for, so no answer is wrong. The defect
is a scan that pays for the whole run before the first pull, on the one scan
path that does not go through a read session.

## The two scan paths

A read session's scan on the memory backends walks its run as the consumer
pulls, so a consumer that stops early, or that only ever asks for the first
key, pays for what it read.

`MemoryStore.scan` (`datalog/storage`), the store's own scan outside a
session, collects every key of the run into a slice at open and hands the
slice out. It reads the live tree under the store lock, and the slice is what
lets the lock be released before the consumer reads.

## The callers

- `MaxElementID` at open, which wants one key.
- `DatomsAfter`, called by `TruncateTo`.
- `MaxTxForEntity`, which wants one key.

Two of the three read one key from a slice built over the run.

## The fix, and the decision it needs

The fix is a chunked walk, as the session scan does. Walking the live tree
in chunks while the lock is released between pulls is not sound, since a
write between two pulls can move the walk's position. So the walk either
reads a clone taken under the write lock, paying the clone once per scan, or
holds the lock across the pulls, blocking writers for the scan's lifetime.
The maintainer decides which of the two the emulator takes. The scan keeps
its slice until then.

## Tests

A benchmark of `MaxElementID` and `MaxTxForEntity` on a store holding a long
run expects the chunked walk to take their allocations down to one chunk.
