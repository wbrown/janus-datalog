# PR #114 round-4 review — defect families and work list

Head `0c30a7a`. Received 2026-07-27.

## Why this is organized by family

Three rounds have each taken the review's enumeration as the scope of the fix.
Each round closed the instances it was handed; the next round found more of the
same defect elsewhere. Round 3's T10 is the clearest case — it swept the event
names the search could see, declared the vocabulary closed "verified in both
directions", and round 4 finds eleven more in two shapes those four search
tokens cannot match.

So the unit of work here is the **family**: an invariant, the generator that
keeps producing violations of it, and a fix applied from where the invariant
lives. Each family below carries:

- **The invariant** — stated so a violation is decidable.
- **The generator** — why instances keep appearing.
- **Deriving the instance set** — how to enumerate every site *from the code*.
  This is the part every prior round skipped. A fix whose scope is a review's
  bullet list is not a class fix.
- **Instances** — the known ones, as evidence and as a checklist the derived
  sweep must cover. Not as the scope.

**Verification**: **Verified** means the cited code was read at head.
**Unverified** means not yet. Nothing is accepted from the report unread.

**Provenance** is recorded where it changes priority.

**Line citations below are against `0c30a7a`.** The producer sweep recorded at
the end of this document inserted lines throughout `matcher_relations.go` and
`hash_join_matcher.go`, so citations into those two files no longer resolve.
Resolve them by enclosing function, which is what the repository's convention
asks for and what N12 in Family 7 is about — the citations were not rewritten
against a new head because doing that to a finding recorded as *Unverified*
would fix the number by guessing which code the reviewer meant.

---

## Family 1 — Collapsing distinct types into one deleted the compiler's enforcement, and the runtime replacement is partial

**Invariant.** When several closed vocabularies share one Go type, every
admission point validates membership, and every consumer that dispatches on the
vocabulary fails loudly on a value outside it.

**The generator.** `ee6500b` merged `ValueType`, `Cardinality` and `Unique` into
`datalog.Keyword`. That was right — they are keywords — but it deleted the
compile error that had been doing the enforcement, and the replacement was
written at the two entry points that happened to be in view. Nothing derived
"which paths admit an `AttributeDefinition`" or "which code dispatches on
cardinality". The same collapse then repeated one layer out, in the annotation
payload, where `KeyCardinality` is declared to carry a `datalog.Keyword` and one
producer sends a hand-rolled string.

**Deriving the instance set.**

- Every function that stores into `Schema.attributes` or constructs an
  `AttributeDefinition` from caller-supplied values.
- Every `switch` and `==` chain whose subject is a vocabulary keyword — grep the
  three closed-set variables and the type, not the old type names.
- Every predicate that tests a vocabulary field for presence rather than
  membership (`HasUniqueConstraint` is one; there may be more).
- Every annotation payload key declared as carrying a typed value, checked
  against all its producers.

**Instances.**

**1a. `Schema.Add` admits anything; the cardinality switch has no default, so
every datom in the group is silently dropped.** *Verified.* `ee6500b`.

`Schema.Add` (`schema/types.go:231-241`) is exported, tests only for nil,
defaults `Cardinality`, and validates no vocabulary.
`inferSchemaFromStore` (`cardinality_inference.go:93`) is an in-module caller.
`schema/types.go:15-16` asserts that ParseSchema and the builder are *the* entry
points and "nothing else can" store an unchecked keyword — false as written.

`crdt_resolving_iterator.go:204` switches across the four cardinalities with
**no `default`**. A wrong-vocabulary card matches nothing, control reaches `:252`,
the loop advances, and the group is skipped. Zero rows, nil error. This is
CLAUDE.md's taxonomy rule in its worse form: not a silent default but no default.

**Resolved at the admission point, not by adding guards.** *Done 2026-07-27.*
`Add` is the only write into `s.attributes` — `parser.go:94`, `builder.go:34`
and `cardinality_inference.go:93` all reach the map through it — so validating
there closes the admission half completely. With that in place
`def.Cardinality` is necessarily one of the three declared cardinalities:
`CardinalityUnknown` is deliberately outside the set, `parseCardinality` does
not produce it, and the builder cannot. The two sites that mint Unknown for the
schemaless case (`crdt_resolving_iterator.go:265`, `getCardinalityEnum`) feed a
switch with an explicit Unknown arm (`:242`) and an `if` naming both One and
Unknown (`:729`). Every cardinality dispatch is therefore total over its actual
domain, and the missing `default` at `:204` is unreachable.

The review reported the missing `default` as the defect. It is the symptom: the
switch could only meet a value it had no arm for because the third entry point
let one in.

`inferSchemaFromStore` was the one caller that builds definitions while reading
stored data, so it was the candidate for a panic on a user's database. It cannot
reach one — `cardFromOp` returns only the three cardinality constants and
`valueTypeFromValue` is a total type switch with a `TypeString` fallback.

**Still open: the fourth-cardinality case.** Twenty-two three-arm switches would
each mishandle a cardinality added later. Twenty-two unreachable `default` arms
guard that poorly, and under R1's read-path half would mean threading an error
return through `valueCount() int` and `getCardinality() string` for a case that
cannot occur. **Proposed, not ruled**: pin the contents of the `cardinalities`
set. `defineCardinality` is the single registration point, so a test asserting
exactly these three reds the moment a fourth is added — at the moment it is
added, with a message naming the sweep required.

**1b. `HasUniqueConstraint` tests non-nil, not membership.** *Verified as
reported; dissolved by R1a — no change.* `types.go:143`, gating `LookupByUnique`
(`database.go:2663`). A wrong-vocabulary `Unique` can no longer be stored, and
the predicate runs per (E, A) group, so a membership lookup there would be
per-group paranoia against an impossibility.

**1c. `annotations.KeyCardinality` carries two types.** *Verified.* `d41d3fa`.
Declared as a `datalog.Keyword` (`annotations/types.go:156`).
`matcher_relations.go:1191` sends the Keyword; `:962` sends
`getCardinality(a)`, returning `"one"/"many"/"vector"/"unknown"` (`:990-1008`).
`getCardinalityEnum` (`:1023`) is the same lookup returning the Keyword. `:962`
was a literal before `d41d3fa`, so centralising the key created the collision —
in the same commit that added `TestIndexAnnotationKeyCarriesOnlyAnIndexType` for
this exact defect on `KeyIndex`.

**1d. The pointer-identity mechanism is unpinned.** *Unverified.* `ee6500b`.
Reported: replacing all nine `WellKnownKeyword` calls with `NewKeyword` leaves
four packages green, and replacing `def.Cardinality == CardinalityMany` in
`Schema.IsMany` with a rendered-string comparison reds nothing. The closed sets
are keyed by interned pointer and every dispatch is `==` against a package
variable, so an orphaned vocabulary after `ClearInterns` produces 1a's failure
mode. `TestClearInternsPreservesWellKnownIdentity` pins the `datalog` side only.

**1e. `inferSchemaFromStore` compares interned keywords by rendered string.**
*Unverified.* `cardinality_inference.go:108`, under a comment declining to rely
on Keyword equality, where `curA` three lines above already holds the pointer.
Routes around the `Keyword.Equal` panic that exists to catch interning failure —
at the site that reconstructs cardinality for every attribute in the database.

**1f. The breaking changes are absent from the upgrade guide.** *Unverified.*
See the migration family below (Family 5, instance 5d).

---

## Family 2 — An obligation was placed on a set of paths without deriving the set

**Invariant.** ~~Every path that opens a scan on a pattern's behalf~~ announces
the run it addressed and reports what that run cost. A path that reports nothing
is indistinguishable from one that scanned nothing.

**Widened 2026-07-28: every path that opens a scan _for a query_.** "On a
pattern's behalf" licensed the omission it was written to forbid.
`LookupAttribute`, `LookupAllAttributes`, the attribute-fetch fusion path,
`pull_batch` and `PrefetchEntities` serve `get-else`, `missing?`, `get-some` and
pull — clauses and find elements, not data patterns — and each reads real index
ranges while reporting nothing. A trace of a pull-heavy query under-reports by
whatever they spent, which is the failure the invariant names. What the trace
exists to account for is a *query's* index cost, and a data pattern is one cause
of that cost among several.

**The generator.** T1/T2 named some arms; round 3 converted those and wrote
"every dispatch arm". The enumeration came from the review, never from the
dispatch. T7's pins then covered the converted arms, so the unconverted ones are
invisible to the gate as well as to the trace. This is the same family as round
2's N2 and round 3's T1 — three rounds, same generator.

**Deriving the instance set.**

- Every caller of `reader.Scan` / `ScanKeysOnly` — the seam is the definition of
  "opens a scan", so the call sites *are* the obligated set. This is mechanical
  and complete.
- Every call site of `GetOrResolve`, which now returns intake: eleven, of which
  one reads it.
- Every arm reachable from `chooseIndex` / the cardinality dispatch, including
  the arms that are only reachable with the cache **off**, which the current
  pins disable in order to reach.

**Instances.**

**2a. The V-validation arm reports nothing and carries four fields for reporting
it does not do.** *Unverified.* If it holds, T1, T2 and round 3's "Open: none"
are false. `validatingVBoundIterator` declares `scansOpened`, `datomsScanned`,
`datomsMatched`, `scanStart` (`matcher_relations.go:680-683`); reported three
never assigned, one written at `:834` and read by nothing, `Close`
(`:1114-1124`) emitting no event. The arm opens one candidate scan per binding
(`:1076`) plus a validation lookup per candidate (`:911`) — on a unique or
set-once attribute the deepest read in the query. Round 3's T1 contradicts
itself two lines apart (`ROUND3:92` against `:94`).

**2b. Three more arms open scans and report no funnel.** *Unverified.*
`matchWithBindingsFromCache` (discard `:1425`), the attribute-fetch fusion path
(`matcher.go:641`, discard `:671`, default-on), `LookupAllAttributes` (`:850`,
discard `:866`), `PrefetchEntities` (`prefetch.go:51`),
`matchVectorWithBindings` (discard `:1704`).

**2c. `GetOrResolve`'s intake is read at one of eleven call sites.**
*Unverified.* With prefetch on, `pattern/cache-resolve-complete` correctly
reports zero intake while the reads that produced it are reported nowhere.

**2d. Resolvers hand intake back on a result struct, so error paths carry
none.** *Verified — but the report joins two defects that are not the same one,
and its cited emit sites belong to the second.*

**The reported half holds.** `resolveAddWinsSet` returns `nil, err` at
`set_resolution.go:68`, `:84`, `:87`, `:96`, `:104`, `:109`, each after its scan
has read from the index, and `resolveVector` the same. The intake is reachable
only through `finishWithIntake`'s result struct, so when there is no result
there is no count.

**But it does not surface where the report says.** The enclosing callers return
on `err != nil` *before* registering their deferred emit — `m.handler != nil`
and the `defer` both sit below the error check — so the resolver's error path
emits nothing at all. Nothing reports a wrong intake there; the reads are simply
unaccounted.

**What the two cited emits actually do is a different defect.**
`cardinalityManyScanAllEntitiesIterator.Close` and
`vectorScanAllEntitiesIterator.Close` — cited by enclosing function, since the
producer sweep moved both line numbers — emit whenever
`it.matcher.handler != nil`, consulting `it.err` nowhere, so a scan that aborted
mid-iteration reports a completion funnel as though it finished. That is not intake lost on a resolver's error path; it is a
`*-complete` event asserting a completion that did not happen.

The two need different fixes and one of them needs a ruling — see the decision
below. `LookupByUnique` (`database.go:2681-2703`) already answers the second
question one way, emitting before its error check, so the tree currently holds
both answers.

**2e. The unique streaming branch is never executed in the suite.**
*Unverified.* Reported: across the whole suite `uniqueMode` is true 0 times and
`processUniqueEntry` is entered 0 times, so T4's fix and its counter are
unexercised. Deleting the accumulation (`crdt_resolving_iterator.go:544`) reds
nothing.

**2f. The comment recording the arm count miscounts again.** *Unverified.*
`matcher_relations.go:2385-2386`.

---

## Family 3 — A payload key names one datum; nothing enforces that it means one thing

**Invariant.** A shared payload key carries one datum, in one unit, of one type,
across every producer.

**The generator.** `d41d3fa` declared the key constants — the *names* — and
stopped there. Naming a key does not constrain what is filed under it, and
nothing compares producers against the declaration or against each other. So
centralising the names moved the drift from "which key" to "what's in it".

**Deriving the instance set.** For each declared key, enumerate its producers
and compare the type *and the unit* of what each writes. The type half is
mechanical; the unit half needs reading, because `resolved` counting RGA
elements and `resolved` counting datoms are both `int`.

**Instances.**

**3a. The cache event's funnel is inverted.** *Verified; **fixed** under R3.*
`pattern/cache-resolve-complete` no longer carries a funnel. It reports
`values.served` and `datoms.matched`, with `datoms.scanned` beside them as its
own fact — intake and matched keep the meanings they have everywhere, and only
the middle term, which a hit does not have, is replaced.

The arm emits directly instead of through `emitScanCompletion`, which keeps the
funnel a required parameter for the producers that do have one rather than an
optional field for everyone. The formatter renders the shape rather than
`renderScanFunnel`.

Original finding: `29208f7` + `d41d3fa`.
`matcher_relations.go:1184-1191` emits `scanned: spent` — zero on a hit by ruling
10 — beside `resolved: entry.valueCount()`, the entry's lifetime count. A hit
renders `1 matched, 1 resolved, 0 scanned`. `scanFunnel`'s own declaration
(`iterator_validation.go:61-63`) gives its reason for existing as preventing
exactly this shape. `cache.go:53-55` names the mismatch without resolving it.
The test asserts the hit's `scanned` and `matched`, never its `resolved`.

**3b. `binding.size` counts distinct join keys on one strategy and binding
tuples on two.** *Verified; **fixed**.* `buildHashSet` incremented its count
only when a key was new, and `chooseJoinStrategy` selects on
`bindingRel.Size()` — tuples. It now returns a `bindingCounts{keys, tuples}`:
`keys` still decides whether one bound value can narrow the scan, `tuples` is
what `binding.size` reports.

The existing funnel test could not see this — its bindings carry one symbol, and
a Relation is a set, so tuples and distinct keys are the same number there.
`TestBindingSizeCountsTuplesNotDistinctKeys` adds the second symbol that
separates them; it was red on hash join alone.

**3c. On the vector paths `resolved` counts RGA elements against `matched`
counting tuples.** *Verified; **the funnel half does not hold, the cache half
did and is fixed**.*

The two funnel sites are already in datoms. `ReconstructRGA` emits one entry per
live, non-tombstoned element, and each live element is one insert datom that
survived resolution — so `len(result.Elements)` is the same unit as
cardinality-many's `len(result.Members)` and cardinality-one's `1`. Elements are
the surviving datoms named for their role, not a second unit. That `resolved`
and `matched` differ is the funnel working: its three terms are deliberately
three different things, on every arm.

The cache site was real, and R3 moved it: `valueCount`'s vector arm returned
`len(vectorList)` where `values.served` means values the pattern binds, and a
cardinality-vector entry serves one value however many elements it holds. Now 1,
or 0 when the list is empty.

That last case surfaced a correctness divergence between the cache and streaming
arms on a never-set vector attribute — reproduced, red in the tree, and out of
this document's scope: see `BUG_CACHE_EMPTY_VECTOR_NEVER_SET`.

**3d. Thirteen producers still flatten typed values.** *Unverified.* Ten on the
v-validation path in the file that took +342/-117 this round, plus `cache.go:135`
and three in `executor`. `scan_bound.go:106` is the durable half: it flattens
and justifies it at `:117-120` with the rule `d41d3fa` replaced, so it reads as
house style for the next producer.

**3e. Eleven event names are still literals, in two shapes the round-3
verification could not see.** *Unverified.* Seven pass the name as
`RewriteSink.Record`'s positional string parameter; four assign it to a local
and pass `Name: eventName`. Neither matches `Name:`, `AddTiming(`, `emit(` or
`.Emit(` — the four tokens round 3 verified against. `Database.Analyze`
(`database.go:892-894`) additionally spells three constant-backed names as
literals while using constants for others in the same function.

---

## Family 4 — Guards and fields whose claim nobody checked

**Invariant.** A guard encodes a claim about reachability; the claim is verified
against the implementation before the guard is written, and again when the code
it guards changes. A field exists because something writes it and something
reads it.

**The generator.** Ruling 8 made every resolver arm return a non-nil entry or an
error, and every affected line was edited to take the new error — without
re-checking the nil test sitting on the line below. Ruling 9 and the guard sweep
moved guards to call sites without checking whether the moved guard could fire.
Nothing sweeps for guards whose claim has become false.

**Deriving the instance set.** For each guard, name the claim and find the code
that could make it true. For each struct field, find its writers and readers.
Both are mechanical and neither has been done for the touched files.

**Instances.**

**4a. `WarmCache` panics on a cache-disabled database.** *Verified.* Predates
this branch. `cache` is nil under `DisableCache` (`database.go:220-223`);
`WarmCache` calls `d.cache.GetOrResolve` at `:336` with no guard where six
siblings guard; `GetOrResolve` dereferences `c.slots` (`cache.go:232`). The
inverse of the family — a missing guard whose claim is true.

**4b. Unreachable `entry == nil` tests after ruling 8.** *Verified; count
corrected from eleven to **four**.* `matcher_relations.go:1163`, `:1429`,
`database.go:382`, `:426`.

Unreachable because `rebuild` and `ResolveEntry` return either `(nil, err)` or a
non-nil entry on every arm, including the schemaless `default` that routes to
`rebuildOne`. `GetOrResolve`'s three success returns are `entry, entry.scanned,
nil` twice and `slot.entry, 0, nil` once, the last behind a `slot.entry != nil`
guard. There is no path to a nil entry with a nil error.

A fifth match, `cache.go:324`, is **not** an instance: `slot.entry == nil` tests
whether a trie slot holds an entry, which is reachable — invalidation clears the
entry and keeps the version in one swap.

Two of the four are worse than dead. `matcher_relations.go:1163` returns
`nil, false, nil` under the comment "Fallback to storage", so a reader concludes
a storage fallback exists on that path. It does not; `GetOrResolve` cannot
produce the state the guard tests for. The guard documents a path that is not
there, which is the failure `feedback_guards_encode_claims` names.

Mechanical: delete the four, and the two comments with them.

**4c. `Collector.enabled` is always true.** *Unverified.* Set from
`handler != nil` at construction, read nowhere else, with all four non-test call
sites passing non-nil under their own guard — and it is the guard every executor
emit site passes through.

**4d. Both join iterators read `it.iter.Scanned()` then nil-check the same field
two lines later.** *Unverified.* `hash_join_matcher.go:691`/`:701`,
`:832`/`:842`, where the sibling written the same round takes the opposite
stance and documents it (`matcher_iterator_unbound.go:85-90`).

**4e. `RewriteSink.Record`'s nil-receiver test is unreachable from all eight
call sites,** kept alive by one test, with a comment claiming callers rely on it.
*Unverified.* `algebra/provenance.go:64`, `:47`.

**4f. Four V-validation fields with no writers.** See 2a.

---

## Family 5 — An obligation was moved without being written where the obligated party reads it

**Invariant.** When a responsibility moves from one party to another, it is
stated in the contract the new party reads, and every existing party is
converted.

**The generator.** Three separate moves in this branch, none of which enumerated
the parties. Ruling 9 moved handler-wrapping to installers; the engine's own
installers were not converted. `Seek` gained the run's end and the caller-side
check was deleted; the obligation went into the two implementations, not the
interface an external backend reads. `memoryIterator` gained an `end` field;
three of its four position-consulting methods were updated.

**Deriving the instance set.** For each moved obligation: every implementation
of the interface, every installer/caller in the repository including the
engine's own, and the document the external party is pointed at.

**Instances.**

**5a. The wildcard pull path depends on a `Seek` obligation the `Iterator`
interface does not state.** *Partly verified.* `0c30a7a`.
`resolveWildcardEntity` (`pull_batch.go:163-181`) opens no scan — one iterator
over the whole EATV index is handed to it (`:77`) and each entity reached by
`Seek`. That `0c30a7a` deleted both the `key[1:21]` check and the decoded
`datom.E` check is certain, from its own commit message. Unverified: that
`store.go:141-143` documents only repositioning, and that the upgrade guide's
*Custom backends* list gained the membership obligation and `Scanned()` this
round but not this one. Review reports by mutation that a `Seek` implementing
exactly the documented contract reds five Badger tests, and the wrong attributes
are then written into the EA cache under the requested entity's key
(`pull_batch.go:196-201`), outliving the call. `storeContractCases` and
`TestSeekHonoursTheRunItNames` are package-internal, so an external implementer
has nothing to run against the real obligation.

**5b. `memoryIterator.ElementID` does not consult `end`.** *Verified.*
`0c30a7a`. `positioned()`, `Key()` and `Datom()` all test it;
`ElementID()` (`memory_store.go:618-623`) tests only `closed` and the position
bounds. `Scan(wide)` → `Seek(narrower)` → `ElementID()` returns a Tx from
outside the run on memory, the zero value on Badger
(`badger_store.go:525-534`), and `store.go:145-148` documents the Badger answer.

**5c. The engine's own verbose installers do not wrap, and `Synchronized` has
zero callers.** *Verified.* `253df0f` (ruling 9). `db/options.go:73-80` and
`:84-91` each close over one `*OutputFormatter`, stateful across calls
(`lastIndex`/`lastBound`). `Collector.Add` calls the handler outside its mutex
by design; `forkContext` shares one collector across four workers.
`db.Open(path, db.WithVerbose())` is a race. Two documents still state the
removed promise (`annotations/types.go:194-196`, `executor/context.go:65`) —
*unverified*. Reported tree-wide: 40 handlers installed, 27 mutating captured
state unlocked.

**5d. `ee6500b`'s API changes are absent from the upgrade guide,** which this
round extended by 49 lines for two other breaks. *Unverified.* Four exported
names gone; twelve signatures changed, including all three `CacheResolver`
methods, so an external implementation fails to compile. The vocabulary
constants' string values gained a leading colon, so a consumer comparing against
`"db.cardinality/many"` silently mismatches rather than failing to build. The
guide contains no occurrence of "schema", "Cardinality", "ValueType" or
"Keyword". `CRDT_UNIQUE_SEMANTICS.md:403` still gives `def.Unique != ""`.

---

## Family 6 — Pins assert the plumbing, not the producer's choice

**Invariant.** A pin fails when the code under test computes the wrong value —
not merely when the mechanism that carries it is removed.

**The generator.** The funnel keys are written unconditionally by one shared
emitter, so any assertion on key *presence* tests the emitter. The tests were
written alongside the emitter, so they inherited its guarantees as their
subject. Same shape as T7 one round earlier, on the other two terms.

**Deriving the instance set.** For each new assertion, name the mutation it is
supposed to catch and apply it. The review did this and three assertions
survived; the rest of the round's pins have not been mutation-checked.

**Instances.**

**6a. `require.Contains` on `KeyDatomsResolved`/`KeyDatomsMatched`
(`scan_funnel_reporting_test.go:183-184`) tests the emitter.** *Unverified.*
Hardcoding `resolved: 0, matched: 0` in any of the seven arms leaves every row
green; no other test asserts those two values for arms 2 through 7.

**6b. The cache arm's intake is unpinned in value.** *Unverified.* Replacing
`iter.Scanned()` with a constant `1` at all three non-error returns of
`ResolveLWW` leaves the package green — the non-unique arm returns at the first
accepted `Next`, so intake is always 1 in latest mode, and the miss row asserts
only `require.Positive`. The separating input is a concrete `AsOf` handle, which
no test drives through the cache arm.

**6c. `TestSilentSinkBuildsNoProvenance` pins the guard, not the removal.**
*Unverified.* Restoring one deleted rendering unconditionally takes the silent
path from 217 to 240 allocations and the test still passes, because
`require.Less(silent, observing)` holds for any amount of unconditional silent
work.

**6d. Coverage the gate does not reach.** *Unverified.* The six constant-E arms
are asserted only with `DisableCache: true`, so with the cache on — the default —
they are unreachable and their replacement is asserted for cardinality-one only.
`ResolveAddWins`, `ResolveRGA` and `ResolveLWW`'s unique branch are unexercised
through the cache arm. `CacheEntry.scanned`'s documented reader
(`cache.go:40-44`) does not exist and `PopulateFromDatoms` leaves it zero.
Everything under `examples/` is `//go:build example` with no Makefile target, so
this round's `examples/schema.go` edit is covered by no gate.

---

## Family 7 — Status documents assert completion the tree contradicts

**Invariant.** A `Status: Resolved` line is a claim about the tree, checkable
against it.

**The generator.** Statuses are written in the same pass as the fix, from the
intent of the change rather than from a re-read afterwards. Nothing re-checks
them, and each round's document inherits the previous round's claims as premises
— which is how "verified in both directions" survived into round 4 with eleven
counterexamples in the tree.

**Deriving the instance set.** Re-read every `Status:` line against head. The
review reports these already false: T1, T2, "Open: none" (2a); T10's
both-directions claim (3e); T13; T18's `iterator_validation.go:17` claim; T7's
reference to a function absent at head and its coverage claim; round 2's N2, N6,
N7, N12 and the Part 1 preamble; round 3's Part 2 table, seven rows.

**N12 specifically**: `docs/bugs/` is byte-identical between head3 and head4, so
the bug document was never touched and its merge-join citations have drifted a
third time — `:732`, `:766`, `:774` documented against `:748`, `:788`, `:796`
actual, the offsets no longer uniform. Citing by line number is what keeps
regenerating this; the repo's own convention is to cite by enclosing function.

---

## Accepted with no work

Two corrections to round 3, both upheld: `TestPerBindingScanReportsOneCountedEvent`
is not off the mode axis (it never calls `Query`), and the axis-loop count is ten,
not twelve. Round 3's fourteen closed findings are re-verified by the review,
several by mutation.

---

## Owner rulings (2026-07-27)

**R1. Registration-time catches panic; read paths return an error** (Family 1).
Go has two conventions for a fallible call in a chaining position,
and this is the registration one, not the parsing one. `regexp.MustCompile` and
`netip.MustParseAddr` pair a panicking wrapper with a primary error-returning
form because they convert an *input*; the name carries the warning.
`sql.Register` and `http.Handle` panic with no `Must` prefix and no error
sibling, because they *declare* something during initialization, the argument is
always a source literal, and there is no recovery a caller could perform. `Add`
declares an attribute. Every caller's vocabulary comes from the package
constants — including `inferSchemaFromStore`, which reads data from the store
but selects its cardinality from its own enumeration and so cannot produce a
wrong-vocabulary keyword. A failure means a programmer wrote
`Cardinality: schema.TypeString`.

So `Add` keeps `func (s *Schema) Add(def *AttributeDefinition) *Schema` and
panics on a keyword outside the vocabulary its field names.

The ruling's second half: **no per-datom panic**. Where a read path meets a
value it cannot dispatch, it returns an error. This is a constraint on any
future dispatch guard, not a prescription that one be added — see the derivation
below, which finds the guards unnecessary.

The deferred-error builder form was considered and not taken. It is already the
right answer for `AttributeBuilder`, which accumulates into `parent.errors` and
surfaces at `Build` — but `Schema` is the built product, and giving it an error
to carry means every consumer must remember to ask, which is the shape that
produced Family 4's eleven unchecked nil tests.

**Stated cost**: a caller building a `Schema` from genuinely untrusted input —
a config file, an EDN document — gets a panic rather than an error. Nothing does
that today; `ParseSchema` is that path and returns errors properly. If a second
untrusted-input path appears, this ruling is revisited.

**R1a. Validate at the boundary that establishes the invariant; do not
re-validate per datom.** The general form of R1, and it governs every family
here. An invariant is enforced once where it is established and trusted
downstream — a check repeated on the hot path to catch what registration already
made impossible is paranoia, and it is paid on every datom forever.

This decides the fourth-cardinality question against runtime guards on stronger
grounds than reachability: the cardinality switch runs per datom inside
`CRDTResolvingIterator.Next()`, so a validation arm there would be a per-datom
cost guarding an impossibility. The proposed vocabulary-membership pin is a test
and carries no runtime cost, which is why it remains the candidate.

It also settles **1b**. `HasUniqueConstraint` tests non-nil rather than
membership, which the review reports as a gap. It is not one: the predicate runs
per (E, A) group from `startNewGroup`, and consulting the `uniques` map there
would re-derive per group what `Add` guarantees once. Presence is the correct
test *because* registration establishes membership. **No change.**

The two findings share a shape worth naming, since the review is likely to
produce more of it: a missing downstream check is reported as the defect when
the answer is upstream enforcement. Adding the downstream check satisfies the
report and leaves the engine slower and no safer.

**R2. Family 2 takes the seam-level fix.** Reporting moves into
`Scan`/`ScanKeysOnly` so a path physically cannot open a scan without reporting
it, rather than a test that drives every arm. A test manages the family; this
ends it. The existing per-arm test already demonstrates the cost of the other
route: reaching six arms requires `DisableCache: true`, so it asserts a
configuration nobody runs.

**Open within R2**: the seam does not currently have the pattern context the
event carries. Resolving that is part of the work, not a separate decision.

**R2 amended (2026-07-28), after deriving the instance set.** The derivation the
ruling asked for was run, and it falsifies the ruling's premise twice. Both
findings are from reading; the amended mechanism below is **proposed, not
ruled**.

*The instance set.* Twenty-eight production call sites open a scan — twelve
through `Scan`, sixteen through `ScanKeysOnly`. They partition by whether a
pattern exists above them:

- **A — no pattern; must not report** (9). `cmd/datalog/stats.go:90`,
  `export.go:49`, `export_bin.go:80`, `database.go:325` (WarmCache),
  `database.go:1682` (RGA write path), `cardinality_inference.go:78` (schema
  reconstruction at open), `read_session.go:44`/`:58` (MaxElementID,
  MaxTxForEntity), `memory_store.go:157`/`:169`/`:190`.
- **B — a pattern's scan; must report** (16). `matcher_relations.go:510`,
  `:911`, `:1076`, `:2054`, `:2222`, `:2487`; `matcher.go:717`, `:938`, `:983`;
  `matcher_iterator_nonreusing.go:99`; `hash_join_matcher.go:167`, `:576`;
  `pull_batch.go:77`; `database.go:3041`, `:3084`; `prefetch.go:51`.
- **C — nested inside resolution; must not emit, must hand intake back** (7).
  `cache_resolver.go:79`; `set_resolution.go:78`, `:299`;
  `vector_resolution.go:104`; `unique_resolve.go:115`, `:164`, `:241`.

*First premise failure.* "The call sites **are** the obligated set. This is
mechanical and complete" — they are a superset. A third of them must stay
silent, and group C's silence is deliberate: round 3 recorded that one event per
nested read buries the query the stream exists to describe.

*Second, and the one that decides the mechanism.* The seam holds **one of the
funnel's three terms**. `scanned` comes from `Iterator.Scanned`; `resolved` is
what CRDT resolution produced and `matched` is what survived the pattern, both
of which happen in the consumer, after the iterator. `Scan` could never emit a
funnel because it has no access to two thirds of one. Reporting cannot move into
the seam. The pattern-context problem recorded above was the symptom of trying
to make it.

*Family 2 is two failures with different causes, which the ruling conflates.*

- **Forgetting.** The arm never emits. `validatingVBoundIterator.Close` closes
  two iterators and returns nil. Its four reporting fields carry a comment
  explaining why they must accumulate rather than be read at the end, so the
  design was reasoned through and the emit was simply never written. 2a and 2b.
- **Plumbing.** Intake reaches the arm through a dozen bespoke channels — a
  `Scanned` field on `SetResolutionResult`, another on
  `VectorResolutionResult`, a return value from each `CacheResolver` method, an
  int from `GetOrResolve`, `Iterator.Scanned` — and each consumer decides
  whether to look. 2c and 2d.

Every prior round converted arms, which addresses forgetting one instance at a
time and never touches the plumbing. That is the generator this family has been
regenerating from.

*Mechanism, first form. Part 1 superseded by the verification below; part 2
stands.*

1. ~~**Intake accrues at the seam into the active scope.** `scanned` is the one
   term the seam owns, so let the scan accrue it rather than hand it back.~~
2. **Emission rides on the iterator, not on the arm's memory.** An arm that
   obtains a pattern-scanning iterator gets one that emits on Close, fed
   `resolved` and `matched` as the arm filters. Forgetting stops being possible
   because the emit lives in what produced the iterator rather than in a block
   the arm has to remember to write.

*Verifications, run 2026-07-28. Both adverse to part 1 as written.* An "active
scope" needs a container whose lifetime is one pattern's execution. Neither
candidate is one.

- **`ReadSession` is per-query, not per-pattern.** `sessionMatcher`
  (`convenience.go:77-87`) mints one session per query and attaches it to one
  matcher — `AttachReadSession` (`matcher.go:110-113`) sets `m.reader =
  session` — and the session-bound result closes it. A scope there accrues the
  whole query into one number, where the funnel's entire content is per-pattern
  attribution. It is also concurrently shared: the matcher is not cloned when
  the Context forks, and `badgerReadSession` carries a `mu` and an `openIters`
  set, which is what a type built for concurrent use looks like.
- **`Context` is forked per worker.** `forkContext` (`executor/context.go:61-79`)
  hands each parallel worker its own Context precisely because per-query mutable
  state is not concurrency-safe; only the collector is shared. A scope there
  splits one pattern's intake across four containers and the emitting arm reads
  whichever one it holds — the shape Family 5c records for `OutputFormatter`,
  where a mutex would serialize the writes and leave every line wrong.

There is no ambient per-pattern container in the design. Inventing one is a
larger change than this family needs, and it would be hidden state besides.

*Mechanism, amended. Ruled 2026-07-28.*

1. **A scan scope threaded as a required parameter at the resolver boundary** —
   `resolveVector(eBytes, aBytes, scope)` and its siblings. Accrual happens at
   scan time, so an error path keeps the intake it spent rather than losing it
   with the result struct that never gets built (2d). One inward parameter
   replaces four outward channels — the `Scanned` fields on
   `SetResolutionResult` and `VectorResolutionResult`, the `scanned` return on
   all three `CacheResolver` methods, the int from `GetOrResolve` — so a
   consumer cannot decline to look (2c). It cannot be forgotten because it is a
   parameter rather than a value the caller must remember to read, and the nine
   group-A callers pass a discarding scope, which makes "this scan is nobody's
   pattern" a statement rather than an omission.

   **The seam is untouched.** Intake still comes from `Iterator.Scanned`; the
   resolver accrues it. `StoreReader` does not change, so this is not a fourth
   public interface break on this branch.

2. **Emission rides on the iterator**, unchanged from the first form above.

The two halves address the two failures separately, which is the point: prior
rounds converted arms, which treats forgetting one instance at a time and never
touches the plumbing.

*Group B splits once more, and it decides the event vocabulary. Ruled
2026-07-28.* `emitScanCompletion` takes `pattern *query.DataPattern` as a
required parameter, and half of group B has none: `LookupAttribute`,
`LookupAllAttributes`, the fusion path, `pull_batch`, `PrefetchEntities`. They
did not forget to emit — they have no envelope to emit into. That is why 2b
reads as a list of arms that lapsed.

**One event family. The bound and the funnel are mandatory; the cause is
optional.** Not one event name per kind of cause.

The reason is the consumer. `Database.Analyze` sums latency per event name, and
anything asking "what did this query spend on index reads" must find every scan.
Split them across `pattern/storage-scan`, `lookup/storage-scan` and
`pull/storage-scan` and every cost consumer has to enumerate that set, then
silently under-reports the day a fourth appears. That is the silent-default
failure relocated from a `switch` to a consumer's list of names. One name means a
scan cannot escape a cost query by being a new kind of scan.

**Why R3's precedent does not transfer, though it looks like it should.** R3 kept
the cache event off `emitScanCompletion` because it has no funnel, and an empty
one renders `0 scanned, 0 resolved, 0 matched` — an assertion that a scan
happened and found nothing, which is false data. A missing *pattern* key is not
that: the line simply names no cause. Omitting a key that has no value is not the
same act as supplying a zero-valued structure that reads as a measurement. So the
funnel stays required and the pattern does not.

Concretely, `emitScanCompletion` loses its `pattern` parameter. Pattern-bearing
arms pass `KeyPattern` through `extraData` beside `addBoundFields`; pattern-less
ones pass what does name their cause — for `LookupAttribute` the entity and
attribute it was asked for, which is more specific than a pattern would be. Both
still pass a bound and a funnel, because those are what every scan actually has.

`pull_batch` and `PrefetchEntities` are in the family under the widened
invariant. They read on a query's behalf and their silence under-reports it.

**R3. The cache event reports what it did.** `entry.valueCount()` is not a wrong
number — on a hit the call really does take all the entry's values. The funnel
models three stages, index intake → resolution → pattern, and a hit skips the
first two, so `scanned ≥ resolved` fails not because a count is wrong but
because the relationship does not exist for a hit. `pattern/cache-resolve-complete`
therefore reports values served and values matched, with intake as a separate
fact, and does not carry the three-term funnel.

**Consequence to record**: one member of the scan-completion family does not
carry the family's payload, and `emitScanCompletion`'s signature must permit
that without making the funnel optional for everyone else.

**R4. State belongs in the reporter, not the consumer** (Family 5c). Ruled
against wrapping the installers, and against the wrapper existing:
`annotations.Synchronized` is deleted, and the package ships none in its place.

This supersedes the Family 5c entry under *Closed by invariants* below, which
had `WithVerbose` and `WithVerboseCallback` wrapping their formatters. That entry
is retained, struck, because the wrong turn it records is the one worth keeping
visible: a serializing wrapper was reached for on sight, and the affordance
existing is what made "serialize the consumer" look like the answer.

The derivation the ruling settles: a lock serializes the *writes* into a handler
and leaves what the handler *reads* untouched. `OutputFormatter`'s `lastIndex`
and `lastBound` were the defect, and under a mutex one worker's scan line still
renders from another worker's announcement — the race detector goes quiet and
every line stays wrong. The formatter now holds nothing between events.

**Consequence**: an event carries everything the line it produces reports. For
the scan family that means the run, not its index alone. Recorded as completed
work below.

## Closed by invariants — no ruling required

Recorded so the next round does not re-raise them as choices.

- **Family 3's units.** One key, one unit: the funnel's middle term reports
  datoms on the vector paths (element count gets its own key if wanted), and
  `binding.size` reports binding tuples everywhere — the unit two of three
  strategies already use and the one `chooseJoinStrategy` selects on.
- ~~**Family 5c.** Ruling 9's reasoning is against wrapping *at the field*; it
  does not exempt an installer from wrapping itself, which is what it asks of
  every other installer. `WithVerbose` and `WithVerboseCallback` wrap. Ruling 9
  stands unamended.~~ **Superseded by R4.** The installers do not wrap and the
  wrapper is gone; the formatter's cross-event state was the defect the wrap
  would have concealed.
- **Family 4's direction.** `feedback_guards_encode_claims` decides each
  instance mechanically: verify the claim, keep the guard where it is true
  (`WarmCache` gains one), delete it where it is false.

---

## Open decisions

Two, both surfaced by derivation rather than by the review.

**D1. Does a `*-complete` event fire for a scan that aborted?** From 2d. Both
answers are in the tree today: `cardinalityManyScanAllEntitiesIterator.Close`
and `vectorScanAllEntitiesIterator.Close` emit on the handler check alone,
reporting a funnel for a scan that ended in `it.err`; `LookupByUnique` emits
before its error check, deliberately, so the reads a failed lookup performed are
still accounted. The question is what the name asserts — whether `-complete`
means "this scan finished" or "this scan is over, here is what it cost." The
second reading makes the aborted case reportable and is what a reader tracing a
failing query most needs; the first makes the event's absence the signal. Either
is coherent; the tree holding both is not.

**D2. The fourth-cardinality guard.** From Family 1, proposed and not ruled: pin
the contents of the `cardinalities` set, so `defineCardinality` gaining a fourth
member reds a test naming the sweep required. R1a rules out the runtime
alternative — twenty-two per-datom defaults guarding an impossibility — but does
not by itself select the pin.

---

## Completed — the producer sweep (R4)

**Rule, derived rather than enumerated.** A producer that reports what a scan
cost and names the index it walked names the *run*: the positions bound in that
index's component order and the values bound to them. An index alone is half a
run — it gives the component order without the components, so `AETV` cannot be
told from `AETV under :person/name`, and the amplification the funnel reports
cannot be attributed to a bound.

**Deriving the instance set.** Every producer whose event carries the funnel.
`addBoundFields` is the sole writer of both `KeyIndex` and `KeyBound`, so the
sweep is auditable by one search: any other writer of `KeyIndex` on a
funnel-carrying event is an instance.

Nine sites, each of which already held the `ScanBound` and reported
`bound.Index` off it:

| Producer | Held the bound as | Now |
|---|---|---|
| `unboundIterator.Close` | `it.bound` | (fixed earlier in the round) |
| `matchCardinalityManyAsRelation` | `result.Bound` | `addBoundFields` |
| `matchCardinalityVectorAsRelation` | `result.Bound` | `addBoundFields` |
| `matchCardinalityManyMembership` | `bound` | `addBoundFields` |
| `cardinalityManyScanAllEntitiesIterator.Close` | `index IndexType` field | field is `bound ScanBound` |
| `vectorScanAllEntitiesIterator.Close` | `index IndexType` field | field is `bound ScanBound` |
| `cardinalityManyAVETValueIterator.Close` | `index IndexType` field | field is `bound ScanBound` |
| `hashJoinIterator.Close` | `index IndexType` field | field is `bound ScanBound` |
| `mergeJoinIterator.Close` | `index IndexType` field | field is `bound ScanBound` |

Six of the nine were a field swap: the constructor received the bound, stored
`bound.Index`, and threw the rest away.

`v-validation/scan-opened` also carried a bare index and now carries the run.
It is not a completion event, so the rule above does not reach it; it named an
index for a run whose components lived on an event emitted thirty lines earlier,
which is the same reading problem one step removed.

**Not instances.** `storage/reuse-strategy`, `storage/join-strategy` and
`v-validation/entry` carry `strategy.Index` with no bound and are correct: they
report a *selection*, and the bound does not exist yet at those points — it is
built inside the strategy each one dispatches to. Their index is the whole datum.

`pattern/cache-resolve-complete` and `pattern/per-binding-scan-complete` carry
neither, also correctly: the first addresses no run, and the second runs
`chooseIndex` again per binding tuple.

**Formatter.** `PatternHashJoinComplete`/`PatternMergeJoinComplete` rendered the
index alone; both now render `bound:` the way the unbound scan line does. It
matters more on these two than on the unbound line: neither path emits a
`pattern/index-selection`, so the completion is the only line in a trace that
says what was walked.

**Pins.** `TestEveryDispatchArmAnnouncesItsRunAndReportsItsFunnel` gained a row
for the general arm — the one the other six are exceptions to, previously pinned
nowhere — and two assertions per row: a completion naming an index names its
bound, and the announced run equals the priced run. The second is the load-bearing
one: two events built from one `ScanBound` cannot disagree, while an index and a
separately-derived bound can, and that drift is invisible in a trace because both
lines read plausibly. `TestBindingDrivenStrategiesReportTheirFunnel` pins the run
per strategy, including that the per-binding path names none.

Verified red first: every instance failed on the bound assertion, and the
already-converted general arm passed, which is what shows the assertion
discriminates rather than failing everywhere.

## Completed — R3 and Family 3's units

**R3.** Recorded against 3a above. One derived consequence worth stating
separately: the funnel's invariant is `scanned >= resolved`, and only that.
Intake bounds what resolution can produce; the last two terms are *not* ordered,
because merge join emits a row per (datom, binding tuple) pair and can match
more than it resolved. The dispatch-arm table now asserts the one that holds, so
transposing any arm's two counters reds that arm — and the cache hit, `1
resolved` under `0 scanned`, was the only violation in the tree.

**Family 3's units.** 3b fixed; 3c's funnel half found not to hold and its cache
half fixed. Both recorded above.

**One finding referred out.** `BUG_CACHE_EMPTY_VECTOR_NEVER_SET` — a never-set
cardinality-vector attribute matches an empty-vector literal through the cache
and not without it. Reproduced, red in the tree, needs an owner ruling on which
of the two answers is correct before it can be fixed.

## Gate

`make test` was green at head through the producer sweep — native
(`go test -count=1 ./...`, 20 packages) and the js/wasm leg.

It is now **red by one test**, deliberately:
`TestCacheMatrix_NeverSetVectorAgainstEmptyVectorLiteral`, the reproducer for
the bug above. A red reproducer is not held out of the tree to keep a gate
green; whether it stays red, or is ruled out of scope and moved into the bug
document the way `BUG_IMPORT_LEAVES_STALE_CACHE_ENTRIES` was, is the owner's
call.

The review's own run reported `go build`, `go vet` and `go test -count=1
./datalog/... ./tests/...` green — 17 packages, zero failures, zero skips, 272s —
with all ten named tests present and passing, but without the wasm leg.
