# A locked store directory was reportable only by substring-matching Badger's prose

**Status**: Resolved.

A tool that enumerates the databases in a directory and reports each one's
state has to distinguish "another process is writing this one" from "this
one is unreadable", and the only discriminator the open gave it was
`strings.Contains(err.Error(), "Cannot acquire directory lock")`.

Not a wrong-answer bug. The open correctly failed, it failed fast rather
than blocking, and the message was accurate. What it cost was that the sole
discriminator between a store held by another process and a store that will
not open was a Badger sentence, asserted by callers that do not import
Badger and cannot see which version they are matching against.

## The defect

Under badger v4.9.1, four layers, and the type dies at the second.

**1. Badger takes the lock.** `acquireDirectoryLock` opens the directory
and calls `unix.Flock` with `LOCK_EX | LOCK_NB`. On contention it wraps:

```go
err = unix.Flock(int(f.Fd()), opts)
if err != nil {
    f.Close()
    return nil, y.Wrapf(err,
        "Cannot acquire directory lock on %q.  Another process is using this Badger database.",
        dirPath)
}
```

The wrapped error is `unix.Flock`'s, which for a contended non-blocking lock
is `EWOULDBLOCK`, a `syscall.Errno`, typed and comparable. A caller-usable
type exists at this point.

**2. Badger destroys it.** `y.Wrapf` delegates to `y.Wrap`:

```go
func Wrap(err error, msg string) error {
    if !debugMode {
        if err == nil {
            return nil
        }
        return fmt.Errorf("%s err: %+v", msg, err)
    }
    return fmt.Errorf("%s: %w", msg, err)
}
```

`debugMode` is a package `var` defaulting to `false`, so every ordinary
build takes the `%+v` branch. The errno is rendered into the message text
and the chain ends. `errors.Is(err, unix.EWOULDBLOCK)` cannot succeed from
here on, and Badger's `errors.go` exports 22 sentinels, none of which covers
the directory lock.

**3. `NewBadgerStore` preserved what was left**, wrapping the error with
`%w` and nothing else.

**4. `Open` and `NewDatabaseWithOptions` handed it to the caller.** The `%w`
chain was intact from layer 3 upward, and carried one flat string.

`datalog/storage`'s sentinels, `ErrSnapshotExists`, `ErrSnapshotNotFound`
and `ErrRollbackInProgress`, had nothing for this state, so the caller's
only remaining discriminator was the text.

## Why the location was the defect, not the matching

Someone must match the message, because Badger discards the errno in
non-debug builds. That part is forced. What was wrong is who.

The claim "Badger says this when another process holds the directory" is a
claim about a specific Badger version. `datalog/storage` imports Badger and
pins that version. A consumer asserting the same claim at a distance cannot
see the version, a Badger upgrade that rewords the sentence silently
reclassifies a locked store as unreadable, and the only test it can write is
one that reproduces the string it already hardcodes.

`NewBadgerStore` is the single function that knows it just attempted to
open Badger at a path. That is where the classification belongs, once, next
to the import that makes it true.

## Read-only is not an escape

`acquireDirectoryLock` takes `LOCK_SH` instead of `LOCK_EX` when Badger is
opened read-only, so read-only opens share with each other, and `LOCK_SH`
still fails against a held `LOCK_EX`. A reader cannot open a store that a
writer holds, in any mode. The error is unavoidable; only its shape was in
question.

## The fix

`ErrStoreLocked` (`datalog/storage/backends.go`) reports that a persistent
backend's directory is held by another process. It is declared in that
untagged file, so the name exists under js/wasm, where the state is
unreachable but portable code naming the sentinel still compiles.
`NewBadgerStore` (`datalog/storage/badger_store.go`) matches Badger's
message at its `badger.Open` error branch and returns the sentinel and
Badger's error through two `%w` verbs, so `errors.Is` answers the
classification and the text keeps the diagnostic. `datalog/db` re-exports
the sentinel as `db.ErrStoreLocked`, as `datalog/db/snapshot.go` re-exports
the snapshot sentinels. The returned error keeps its text, so a consumer
still substring-matching keeps working while it moves to `errors.Is`.

## Tests

`TestAPersistentBackendHeldByAnotherOpenIsReportedLocked`
(`datalog/storage/store_locked_native_test.go`), native-only: every
persistent backend this build has, opened at a path and then opened at the
same path again, answers the second open with an error `errors.Is`
`ErrStoreLocked` whose text names the path. This is the test that fails
when a Badger upgrade rewords the message, which no consumer-side match can
have.

`TestADirectoryHeldByAnotherOpenIsReportedLocked`
(`datalog/db/store_locked_native_test.go`), native-only: `db.Open` of a
Badger path held by an earlier `db.Open` answers with an error `errors.Is`
`db.ErrStoreLocked` whose text names the path.
