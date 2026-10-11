# A locked store directory is reportable only by substring-matching Badger's prose

**Status**: Open, recorded 2026-08-03. Found from the caller's side: a tool that
enumerates the databases in a directory and reports each one's state has to
distinguish "another process is writing this one" from "this one is unreadable",
and the only discriminator available is
`strings.Contains(err.Error(), "Cannot acquire directory lock")`.

Not a wrong-answer bug. The open correctly fails, it fails fast rather than
blocking, and the message is accurate. What it costs is that the sole
discriminator between a store held by another process and a store that will not
open is a Badger sentence, asserted by callers that do not import Badger and
cannot see which version they are matching against.

## The defect

Verified against badger v4.9.1 and janus-datalog v0.15.2. Four layers, and the
type dies at the second:

**1. Badger takes the lock.** `acquireDirectoryLock` opens the directory and
calls `unix.Flock` with `LOCK_EX | LOCK_NB`. On contention it wraps:

```go
err = unix.Flock(int(f.Fd()), opts)
if err != nil {
    f.Close()
    return nil, y.Wrapf(err,
        "Cannot acquire directory lock on %q.  Another process is using this Badger database.",
        dirPath)
}
```

The wrapped error is `unix.Flock`'s, which for a contended non-blocking lock is
`EWOULDBLOCK` — a `syscall.Errno`, typed and comparable. **A caller-usable type
exists at this point.**

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

`debugMode` is a package `var` defaulting to `false`, so every ordinary build
takes the `%+v` branch. The errno is rendered into the message text and the chain
ends. `errors.Is(err, unix.EWOULDBLOCK)` cannot succeed from here on, and
Badger's `errors.go` exports 22 sentinels, none of which covers the directory
lock.

**3. `NewBadgerStore` preserves what is left.**

```go
db, err := badger.Open(opts)
if err != nil {
    return nil, fmt.Errorf("failed to open badger: %w", err)
}
```

**4. `Open` / `NewDatabaseWithOptions` hand it to the caller.** The `%w` chain is
intact from layer 3 upward — and carries one flat string.

`datalog/storage`'s error block (`ErrSnapshotExists`, `ErrSnapshotNotFound`,
`ErrRollbackInProgress`) has nothing for this state, so the caller's only
remaining discriminator is the text.

## Why the location is the defect, not the matching

Someone must match the message, because Badger deliberately discards the errno in
non-debug builds. That part is forced. What is wrong is *who*.

The claim "Badger says this when another process holds the directory" is a claim
about a specific Badger version. `datalog/storage` imports Badger and pins that
version. A consumer asserts the same claim at a distance: it cannot see the
version, a Badger upgrade that rewords the sentence silently reclassifies a
locked store as unreadable, and the only test it can write is one that
reproduces the string it already hardcodes.

`NewBadgerStore` is the single function that knows it just attempted to open
Badger at a path. That is where the classification belongs — once, next to the
import that makes it true.

## Read-only is not an escape

`acquireDirectoryLock` takes `LOCK_SH` instead of `LOCK_EX` when Badger is opened
read-only, so read-only opens share with each other — but `LOCK_SH` still fails
against a held `LOCK_EX`. A reader cannot open a store that a writer holds, in
any mode. The error is unavoidable; only its shape is in question. Adding a
read-only option would not remove the need for a typed answer.

## Sites

- `NewBadgerStore` (`datalog/storage/badger_store.go`) — the `badger.Open` error
  branch, where the classification is missing.
- `datalog/storage/snapshot.go` — the existing sentinel block, and the precedent
  for how one is declared.
- `datalog/db/snapshot.go` — the precedent for re-exporting a sentinel to
  db-package users so they need not import `storage`.

## The fix

A sentinel, returned alongside the original error:

```go
// ErrStoreLocked reports that a persistent backend's directory is held by another
// process. Badger reports this by message rather than by a sentinel, and flattens
// the underlying EWOULDBLOCK, so the message match happens here — the one package
// that pins the Badger version and would notice at upgrade time.
var ErrStoreLocked = errors.New("store directory is locked by another process")
```

```go
db, err := badger.Open(opts)
if err != nil {
    if strings.Contains(err.Error(), "Cannot acquire directory lock") {
        return nil, fmt.Errorf("failed to open badger: %w: %w", ErrStoreLocked, err)
    }
    return nil, fmt.Errorf("failed to open badger: %w", err)
}
```

Two `%w` verbs keep both reachable: `errors.Is(err, ErrStoreLocked)` for the
classification, the Badger error for the diagnostic text.

**Declare it in an untagged file, not in `badger_store.go`.** That file is
`//go:build !(js && wasm)`, so a sentinel declared there would not exist under
js/wasm and portable code calling `errors.Is(err, storage.ErrStoreLocked)` would
fail to compile for that target. The state is unreachable under js/wasm — no
Badger, no directory — but the *name* has to be there. Re-export it from
`datalog/db` the way `datalog/db/snapshot.go` re-exports the snapshot sentinels.

The test is native-only and opens one path twice in a single process, asserting
`errors.Is` on the second open. That test is the thing that fails when a Badger
upgrade rewords the message — which is the whole point of moving the match here,
and is exactly what no consumer-side match can have.

## Scope

Self-contained: one sentinel, one error branch, one re-export, one test. No
format change and no API break — the returned error keeps its current text, so
consumers still substring-matching keep working unchanged while they migrate to
`errors.Is`.
