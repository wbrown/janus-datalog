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

**Status 2026-07-29: closed.** 1a resolved at the admission point (2026-07-27),
which dissolved 1b — `Add` is the only write into `s.attributes`, so a non-nil
`Unique` is a member and `HasUniqueConstraint`'s presence test is correct rather
than paranoid. 1c and 1e are fixed and pinned; 1d is pinned on the half a test
can reach, with the other half recorded as not test-detectable. 1f is struck: the
surface it called breaking is `datalog/storage`'s cross-package plumbing, not
anything a consumer imports. The fourth-cardinality case raised in 1a's section is
**D2**, ruled and pinned 2026-07-29.

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

**The fourth-cardinality case is D2, ruled and pinned 2026-07-29.** Twenty-two
three-arm switches would each mishandle a cardinality added later. Twenty-two
unreachable `default` arms guard that poorly, and under R1's read-path half would
mean threading an error return through `valueCount() int` and
`getCardinality() string` for a case that cannot occur. The ruling pins the
vocabulary instead: `TestCardinalitySetIsClosedAtThree` asserts `cardinalities`
holds exactly `CardinalityOne`, `CardinalityMany` and `CardinalityVector`, with
`CardinalityUnknown` outside it. `defineCardinality` is the single registration
point, so the test reds at the moment a fourth is added, and its message directs
the author to give every cardinality switch an arm first.

**1b. `HasUniqueConstraint` tests non-nil, not membership.** *Verified as
reported; dissolved by R1a — no change.* `types.go:143`, gating `LookupByUnique`
(`database.go:2663`). A wrong-vocabulary `Unique` can no longer be stored, and
the predicate runs per (E, A) group, so a membership lookup there would be
per-group paranoia against an impossibility.

**1c. `annotations.KeyCardinality` carries two types.** *Verified; **fixed
2026-07-29**.* `d41d3fa`. Declared as a `datalog.Keyword`. Four producers write
it; three sent the Keyword and the v-validation arm sent
`getCardinality(a)`, returning `"one"/"many"/"vector"/"unknown"` from a second
lookup that existed only to render one. Under the formatter's `%v` that arm
printed `one` where the others printed `:db.cardinality/one`, so one event family
spelled cardinality differently from the rest. The call site now uses
`getCardinalityEnum`, the same lookup returning the Keyword, and
`getCardinality` is deleted — that call was its only caller. The collision was
created by `d41d3fa`, which centralised the key, in the same commit that added
`TestIndexAnnotationKeyCarriesOnlyAnIndexType` for this defect on `KeyIndex`;
`TestCardinalityAnnotationKeyCarriesOnlyAKeyword` is its sibling. It asserts the
type on every producer in the trace **and** that the v-validation result is among
them carrying one — two independent flags would go green with a different event
satisfying the type check while the arm that was wrong satisfied only presence.

**1d. The pointer-identity mechanism is unpinned.** *Verified in part; **pinned
2026-07-29**.* `ee6500b`. The three closed sets are `map[datalog.Keyword]struct{}`
keyed by the pointers the `define*` functions captured at init, so an orphaned
vocabulary after `ClearInterns` produces 1a's failure mode.
`TestClearInternsPreservesWellKnownIdentity` pinned the `datalog` side; what it
could not see was this consumer. `TestVocabularyMembershipSurvivesClearInterns`
(schema) closes that: after a clear, a definition whose vocabulary keywords are
interned *fresh from text* — the way a parsed schema's arrive — must still be
admitted, and each package variable must still be the pointer a fresh intern
returns. `require.Same`, not `require.Equal`: `Equal` reflect-compares the
pointed-to structs and so passes for two distinct pointers carrying one string,
which is exactly the orphan case. Under the reported mutation of the nine
`WellKnownKeyword` calls to `NewKeyword`, the variables orphan, the fresh intern
differs, and both halves fail.

**Not test-detectable, and recorded as such rather than left on the list.** The
second reported mutation — `def.Cardinality == CardinalityMany` in
`Schema.IsMany` replaced by a rendered comparison — returns identical answers for
every input. No behaviour test can distinguish it, because there is no behaviour
to distinguish; it is a model violation whose only cost is the allocation and the
lost enforcement. The defence is the rule and review, not a pin, and writing one
would mean asserting on the implementation's text.

**1e. `inferSchemaFromStore` compares interned keywords by rendered string.**
*Verified; **fixed 2026-07-29**.* `cardinality_inference.go:108` rendered both
keywords to detect the ATEV attribute boundary, under a comment that declined to
rely on Keyword equality without giving a reason, while `curA` three lines above
already held the pointer. Keywords are interned, so the boundary is `d.A != curA`
— `datom_decoder.go:38` interns every decoded `A`, and
`CRDTResolvingIterator:187` already detects the same boundary the same way. The
`curStr` field and the `aStr` local are gone, along with one string allocation
per datom across a full ATEV scan, which runs at open on any schemaless database.
`==` is the comparison, not `Equal`: interning makes pointer equality exact, and
`Equal`'s panic branch requires the compared pair to be the orphan and its
replacement, which two attributes from one scan through one decoder cannot be.

**1f. Struck 2026-07-29.** It reported the schema vocabulary's change as a
breaking API change needing a migration note. It is not one: `Cardinality`,
`ValueType` and `Unique` are `datalog.Keyword` fields on `datalog/schema` types
that `datalog/db` never names, and `CacheResolver` is an interface `storage`
exports so `db` and `executor` can reach it. Go's export marker is how a module's
packages compose; it is not a versioning contract, and writing an internal
signature into a consumer-facing document would create the obligation rather than
report it.

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

**Status 2026-07-29: closed, generator included.**

The *plumbing* failure is ended: intake reaches its reporter through one inward
parameter instead of a channel per resolver. `walkUniqueEntityValue`,
`resolveMaxOtherTxForValue`, `resolveAVLWW`, `checkSetMembership` and
`lookupAllAttributesFallback` no longer return counts for a caller to add up.

The *forgetting* failure is ended: every acquisition in the package goes through
`openScan`/`openKeyScan`, which attach the accounting to acquiring the scan. Part
2 as ruled was falsified and amended — see the mechanism below — and the amended
form is what landed.

**The generator is closed by `TestScanAcquisitionGoesThroughAReport`.** It parses
**the whole module** on every run, finds every one-argument
`Scan`/`ScanKeysOnly`, and attributes each to its enclosing function. Anything
not on an exemption list reds the gate; a stale exemption reds it too.

`OpenScan`, `OpenKeyScan` and `DiscardIntake` are exported for exactly that
reach. Go visibility is not API surface — half this package's exported names are
already internal seams — and an opener only `storage` could call would leave a
query path added in `db`, `executor` or a command with the store as its shortest
route, invisible to the gate: the generator relocated one package over. The
report type stays unexported, so an outside caller can say "nothing accounts for
this" and cannot fabricate an accounting; the day a real out-of-package query
path exists, the check reds and that decision gets made deliberately.

Detection is by call shape, not receiver name. The first version whitelisted
`reader`/`store` receivers and would have missed `db.Store().Scan(bound)`, whose
receiver is a call — arity separates a storage acquisition from
`bufio.Scanner.Scan`, which takes none.

**The exemption list holds five functions, and no read a query causes.** They are
the seam implementing itself: `MemoryStore`'s `MaxElementID`, `MaxTxForEntity`
and `DatomsAfter`, plus the shared `maxElementIDByScan`/`maxTxForEntityByScan`
derivations behind the first two. These sit below the layer a report lives at —
routing them through an opener would be a store calling a module-level function
to call itself, and there is no arm on whose behalf they read. A sixth reds the
gate.

The check earned itself three times over. Of the twenty-two sites enumerated by
hand for this section, six names did not exist and seven real acquisitions were
missing — wrong in thirteen ways, written minutes earlier. Widening it to the
module surfaced the five seam-internal reads that no receiver-name detector would
have found. And a wildcard-pull bug had already slipped past the conversion:
`ResolveAllAttributes` announced EATV while its history branch walked EAVT,
which is the announce-versus-price defect on a path with no pin. An enumeration
is not a derivation, including this one.

**What the check does not do.** It enforces that an event exists, not that its
payload is right — which is exactly how the EATV/EAVT mismatch survived. The
per-arm pins cover the dispatch arms;
`prefetch`, `pull_batch` and `ResolveAllAttributes` have none.

**Two arms still hand-write their completion**, holding a report as an intake
sink and calling `emitScanCompletion` themselves rather than `report.close()`:
`matchCardinalityVectorAsRelation` and `matchVectorWithBindings`. Both report,
so neither is a Family 2 instance; the cost is two idioms for one thing, which
is what invites the next divergence. Converting them wants `resolveVector` to
declare the run it walks when the report has none, which is what
`resolveAddWinsSet` already does — the asymmetry between the two resolvers is
the actual outstanding item.

**An arm declares its run; it is not inferred.** `scanReport.run` is set by arms
that address a single bound, and `peers` is counted by arms that do not — the
per-binding iterator, v-validation, prefetch — standing in the bound's place.
Counting acquisitions cannot make that distinction: an arm that drives resolvers
acquires many scans and still has exactly one run, and the earlier `opens == 1`
inference dropped the bound precisely where an arm had announced an index it then
had to be seen to price. Subordinate reads are neither run nor peer; they are
cost, carried by `scanned`.

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

**Verified and fixed 2026-07-28.** The arm reports through
`storage/scan-complete` as `ScanVValidation`, pinned by
`TestVValidationReportsWhatItsScansCost` over both branches it emits from.
Three corrections to the report, each found by implementing it:

- **The field written at `:834` is not dead.** It is
  `it.datomsScanned += funnel.scanned` in `tryEmitUniqueWinner`, accumulating
  the claimant walk's intake from `resolveAVLWW`'s funnel. It read to nobody
  only because the emit that would have read it was never written — it was the
  one half of the accounting somebody built. `funnel.resolved` now travels back
  the same way.
- **The unique short-circuit emits without touching the candidate loop.**
  `Next` returns at the `emitted` branch, so a `datomsMatched++` in the loop
  below counts nothing for a unique attribute. Both points increment now. The
  arm also needed a fourth field: the funnel has three terms and only two had
  counters.
- **Intake must be harvested where the scan is released, not at Close.** Each
  binding's candidate scan is dropped before the next opens, so `Close` sees
  only the last. This is what the field comment meant by the counts having to
  accumulate as it goes, and it is the one line the fix turns on.

**The first emit guard was this family's own defect, in the fix.** Gating on
`scansOpened > 0` as shorthand for "did this arm do work" silenced the unique
branch, which does its deepest reading through a walk without opening a
candidate scan. The arm now reports whenever it ran, with `scanStart` taken at
construction. Only the two-branch pin caught it; the review records the unique
branch as unexercised by the suite, so a counter added to it would have stayed
wrong indefinitely.

**Two reachability facts, both established by the pin after being guessed wrong
twice.** The arm is selected only when the *binding relation supplies V* —
`analyzeReuseStrategy` reaches it at `case 2: // V is bound`
(`matcher_strategy.go:120`), so a binding over the entity takes a join strategy
instead. And with the cache on it is not reached at all:
`matchWithBindingsFromCache` answers first. Reaching it in a test means
`DisableCache: true`, which is not the test being contrived — it is 2b, since
the arm that answers by default reports nothing either.

`datomsScanned` still does not cover the validation lookup `validateCandidate`
opens per candidate: that returns `(ok, error)` and its intake reaches nobody.
That is the nested-read gap the scan scope is for, and the field comment says so
rather than claiming coverage the iterator cannot see.

Not to be confused with 2e, which is a different unique branch —
`uniqueMode`/`processUniqueEntry` in `CRDTResolvingIterator`, still unexercised.

**2b. Three more arms open scans and report no funnel.** *Verified and fixed
2026-07-28.*
`matchWithBindingsFromCache` (discard `:1425`), the attribute-fetch fusion path
(`matcher.go:641`, discard `:671`, default-on), `LookupAllAttributes` (`:850`,
discard `:866`), `PrefetchEntities` (`prefetch.go:51`),
`matchVectorWithBindings` (discard `:1704`).

**`matchWithBindingsFromCache` confirmed silent, by accident.** Driving a
binding-driven V-bound query with the cache on for 2a's pin produced **zero**
scan completions of any strategy — the arm answered the whole query and
reported nothing. It is also the default: with the cache on it intercepts before
`analyzeReuseStrategy` is consulted, so it is what runs for this query shape in
production while the arms that do report are the ones a test has to disable the
cache to reach.

That inverts how 2b reads. These are not peripheral arms that forgot; the one
verified so far is the common path, and the reporting arms are the exception.

**The five paths do not share one shape, and the fix is a field contract rather
than five events.** They split three ways by what names their subject and what
they produce, and the payload carries both:

| path | cause | run | output |
|---|---|---|---|
| `matchWithBindingsFromCache` | pattern | none — resolves many entries | served / matched, `binding.size` |
| `LookupAttribute` | entity + attribute | named — one run per arm | served |
| `LookupAllAttributes` | entity + attribute | none — peek plus resolve is two | served, `scans.opened` |
| `PrefetchEntities` | none — an entity set | none — one run per entity | `entries.populated` |
| `ResolveAllAttributesMany` | none — an entity set | named — one shared EATV traversal | served |

A run is named exactly when the call addressed one, which is the rule
`ScanPerBinding` and `ScanVValidation` already follow. Its presence is also what
separates the mechanisms: the cache picks an index per entry inside resolution
and a hit walks none, so no run means the cache answered.

`pattern/cache-resolve-complete` became `storage/resolve-complete`. It named one
cause and one mechanism, and three of the five paths above have neither — two
carry no pattern, and their fallback arms never touch the cache. The rename is
the same collapse R3's event family took: one name, the distinguishing facts in
the payload. `entries.populated` is the one new key, for a read that answers
nobody — `values.served` and `datoms.matched` would both be zero for a prefetch
that filled a thousand entries, and zero on those keys means "found nothing".

Pinned by `TestPatternLessReadsReportUnderEntityAndAttribute` and
`TestBulkReadsReportUnderNoSingleSubject`. `Database.Analyze` reports the family
and its total intake alongside the scan family.

**An absent (E, A) costs no intake, and the event is its only trace.** Absence
is resolved and deliberately not cached, so it is re-established on every call —
but its prefix names an empty run, so the seek reads zero datoms. Nothing about
the counts distinguishes it from a call that never ran; `binding.size` is what
says it happened.

**2c. `GetOrResolve`'s intake is read at one of eleven call sites.**
*Fixed 2026-07-28, by the ruled inward carrier rather than by converting
consumers.* Twelve call sites, not eleven. The count no longer travels outward
for any of them: the carrier goes in as a required parameter and the four
channels the ruling named are deleted — the `Scanned` fields on
`SetResolutionResult` and `VectorResolutionResult`, the `scanned` return on all
three `CacheResolver` methods, and the int from `GetOrResolve`.

It was `scanScope` when this entry was written and is `scanReport` now; the
amendment below records why the two collapsed into one.

A caller that reports nothing now passes `discardIntake`, which is the ruling's
point: the eight sites that discarded with `_` were mostly group A and correct
to, but nothing distinguished them from the one that was a defect — the
v-validation arm's cache read, which discarded in a path that reports. It
accrues into that iterator's own report now and appears in its funnel.

The carrier has no nil-safe methods and no accessors. Accrual is written where
the number is, under `if report != nil`, so a discarding read costs neither the
call nor `iter.Scanned()` in its argument. A nil-safe method called
unconditionally gates neither, which is the same defect as an emit guarded
inside the emitter while the caller builds its payload regardless.

`entry.scanned` survives and is measured as a delta across the resolver call:
the caller's carrier spans however many entries it resolves, the entry's is its
own build cost. Under `discardIntake` the delta is zero, which is the same
statement the nil carrier makes.

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

**Both fixed 2026-07-28.** The first by the scan scope: intake accrues at scan
time in a defer before Close, so a resolution that fails keeps what it read.
The two iterator comments reading "accounting comes after the error check: a
failed resolution returns no result to read a count from" are gone, because the
constraint they described is gone.

The second by D1, ruled below. `emitScanCompletion` takes the outcome as a
required parameter and writes `annotations.KeySuccess`, so every scan completion
states whether its funnel is a total or as far as it got. Required for the
funnel's reason: a producer that cannot abort passes true, which is a claim,
where a default would make "finished" the answer for producers that never
considered the question.

Deferred emits read a named error return, so the outcome follows whatever the
function returns rather than a comment asserting today's control flow. Inline
emits past an error check pass the literal, because a named return read there is
`nil == nil` — a constant wearing a disguise.

Pinned by `TestAbortedScanReportsThatItDidNotFinish` over both failure shapes,
with `TestCompletedScanSaysSo` as its other half: without it an arm could
hardcode the aborted answer and the first would still pass.

**2e. The unique streaming branch is never executed in the suite.**
*Verified and fixed 2026-07-28.* `uniqueMode` needs an attribute that is
CardinalityOne *and* unique, scanned with E unbound — a bound E goes to the
cache and a bound V to the claimant lookup, so neither shape a unique attribute
is usually queried by reaches it, which is why the suite never did.

`TestUniqueAttributeWalkReportsItsOwnScans` enters it. Measured rather than
asserted: 24 source datoms against 32 reported, the walk adding one supersession
read per group, so the assertion is a floor of source + groups rather than a
bare inequality.

**2f. The comment recording the arm count miscounts again.** *Verified and fixed
2026-07-28.* Seven `emitIndexSelection` call sites against the comment's six,
and its other claim — that `matchFromCache` is the *one* dispatch arm that does
not emit — went stale when `matchWithBindingsFromCache` joined it. Both counts
removed rather than corrected, per the rule already recorded beside the event
names: a number in prose is a claim about the tree that nothing rechecks.

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
`storage/resolve-complete` no longer carries a funnel. It reports
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
arms on a never-set vector attribute, out of this document's scope:
`BUG_CACHE_EMPTY_VECTOR_NEVER_SET`, **fixed 2026-07-28** and resolved. This
paragraph read "reproduced, red in the tree" until 2026-07-29 — written the day
before the fix and stale from then on, which is this document's own Family 7:
a status claim the tree contradicts.

**3d. Producers still flatten typed values.** *Class verified 2026-07-29; the
counts and one citation do not hold.*

- **The durable half is real.** `describeRun` renders every bound value with
  `fmt.Sprintf("%v", v)`, and its doc comment justifies it — "an event's Data is a
  display surface… a consumer that needs the typed value has the query" — which is
  the rule `d41d3fa` replaced, so it reads as house style for the next producer.
  It is the shared seam helper, so every arm reporting a bound inherits it. The
  `KeyIndex` assignment beside it is correctly typed.
- Its sibling defect: `"bound.values"` is a bare literal key, not a declared
  constant, which is this family's other half.
- **The counts are wrong.** `matcher_relations.go` carries five emit sites and
  fifteen flattened fields in the v-validation region, not ten; `executor` has
  nine, not three.
- **The `cache.go` citation is wrong.** `annotateRebuild` is already correct: it
  carries the attribute as a `Keyword` under a comment stating that interning
  already produced the canonical pointer and `String()` would spend an allocation
  per rebuild.

**3e-consumers. A third shape, found 2026-07-28 by collapsing the scan
completion names: literals in *consumers*.** *Verified — all four were live.*

The derivation below enumerates producers. Consumers spell names too, and when
they spell them as literals a constant rename cannot find them: the code
compiles, matches nothing, and reports zero forever.

- `Database.Analyze` listed three of the five completion names as string
  literals (`database.go:906-908`) while reading a fourth through its constant.
  Deleting the constants did not break it. It would have compiled clean and
  reported zero binding-driven scans, and it never counted unique lookups at
  all.
- Three test captures matched `"pattern/storage-scan"` or
  `"pattern/hash-join-complete"` as literals — `history_index_order_limit_test.go`,
  `index_order_limit_test.go`, `aevt_matcher_bug_test.go`. Each compiled and
  silently captured nothing; only the behaviour change surfaced them, as index
  assertions comparing against `""`.

All four are converted. The lesson for the rest of 3e: sweeping producers is
half the family, and the consumer half cannot be found by searching for the
constants, because a literal consumer is exactly the one that does not use them.

**3e. Eleven event names are still literals, in two shapes the round-3
verification could not see.** *Verified 2026-07-29, exactly eleven, none declared
in `annotations/types.go`.* Neither shape matches `Name:`, `AddTiming(`, `emit(`
or `.Emit(` — the four tokens round 3 verified against.

- Seven pass the name as `RewriteSink.Record`'s positional string parameter:
  `algebra_bridge.go` (join-project skip/apply), `rewrite_decorrelate.go`
  (decorrelate check/skip/apply), `rewrite_getelse.go` (getelse-scan skip/apply).
- Four assign it to a local: `query_executor.go` (`pattern/fused-fetch`,
  `pattern/fused-constraint`), `scan_sharing_matcher.go`
  (`scan-sharing/cache-miss`, `cache-hit`).
- `Database.Analyze` additionally spells three constant-backed names as literals
  while using constants for others in the same function.

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

**Status 2026-07-30: closed.** 4a was already fixed before it was read; 4b is
withdrawn (its premise false — the guards it called dead are what every reader
downstream treats as the attribute's absence); 4c resolved by removing its subject;
4d struck, its subject gone. 4e is fixed, and its framing inverted on derivation:
the unreachable guard is contract, and the defect was the five-spelling predicate
around it. 4f belongs to 2a.

Two of this family's six instances turned out to be about code that had already
changed, and a third was false. That ratio is the family's own generator applied to
the review: a guard's claim goes unchecked, and so does an instance's.

**Deriving the instance set.** For each guard, name the claim and find the code
that could make it true. For each struct field, find its writers and readers.
Both are mechanical and neither has been done for the touched files.

**Instances.**

**4a. `WarmCache` panics on a cache-disabled database.** *Was verified; **already
fixed in the tree**, found 2026-07-29.* `WarmCache` returns nil at
`database.go:311-313` when `d.cache == nil`, under a doc comment stating that with
`DisableCache` there is nothing to warm and that warming has no storage fallback
because warming is the whole of what it does. The instance's own text was accurate
when written and stale by the time it was read.

**4b. Withdrawn 2026-07-29: the four `entry == nil` tests are reachable and
load-bearing.** The premise — that `rebuild` and `ResolveEntry` return either
`(nil, err)` or a non-nil entry on every arm — is false. `rebuildOne`,
`rebuildMany` and `rebuildVector` each return `(nil, nil)` when the resolver
reports the (E, A) absent (`cache.go:550`, `:575`, `:600`), and `GetOrResolve`
returns `(nil, nil)` itself at `:300-307`, under a comment stating that absence is
deliberately not cached because **entry existence is what every reader downstream
reads as the attribute's existence**. Those readers are these four guards.

The sites are `matcher_relations.go:1272`, `:1557`, `database.go:392`, `:438` —
each roughly ten lines below the cited numbers, which is instance N12's disease in
this document's own work list. `matcher_relations.go:1272` returns an empty
relation under a comment reading "the cache answering, not declining: absence is a
resolved state," not a storage fallback.

Deleting them would turn a missing attribute into a nil dereference. The
enumeration of `GetOrResolve`'s returns as `entry, entry.scanned, nil` dates the
claim: that signature predates Family 2's plumbing change, so the instance was
derived against a shape the tree no longer had.

**4c. `Collector.enabled` is always true.** *Verified 2026-07-29; **resolved
2026-07-29 by removing the subject**, ledger item 36 — `annotations.Collector` is
deleted, and with it the field, the event slice nothing read, and the pooled data
maps.* As reported: set from `handler != nil` at construction and read only by
`Add`/`AddTiming`, with all four production construction sites guaranteeing
non-nil — `executor/context.go` returning a `BaseContext` on nil,
`storage/matcher.go` guarding with `handler != nil`,
`executor/annotated_matcher.go` returning the unwrapped matcher on nil, and
`storage/database.go` passing a literal closure. The field encoded a claim three
explicit upstream checks already made.

Three of those four constructions are gone with the type: `executor/context.go`'s
`BaseContext` arm and `storage/matcher.go`'s `SetHandler` are deleted, and
`storage/database.go`'s closure is now registered on the options directly.
`WrapMatcher`'s nil check remains, and still means what it meant — a nil handler
returns the matcher unwrapped, for zero overhead — but it no longer constructs
anything.

**4d. Struck 2026-07-29: the subject is gone.** It cited both join iterators
reading `it.iter.Scanned()` then nil-checking the same field. `Scanned` no longer
appears anywhere in `hash_join_matcher.go` — Family 2's inward `*scanReport`
replaced those outward reads. The only remaining `Scanned()` reads are the two
backend implementations, `CRDTResolvingIterator`'s delegation, and the reported
iterator's own accrual.

**4e. `RewriteSink.Record`'s nil-receiver test is unreachable.** *Verified
2026-07-29, with the count corrected from eight to **five**.* Every production
call site — three in `rewrite_decorrelate.go`, two in `rewrite_getelse.go` — sits
behind `observing := sink != nil && (sink.Collect || sink.Handler != nil)`, so
`if s == nil` cannot be reached. The doc comment claims the opposite ("a nil sink
is valid and does nothing, so passes call it unconditionally"); the passes
deliberately compute `observing` first, precisely so they do not prepare the
payload arguments.

**Reframed and fixed 2026-07-30** (ledger item 37). The guard is not the defect:
nil sinks are live — `DecorrelationPass(nil)`, `GetElseScanRewritePass(nil)` — and
the guard is what makes "a nil sink is valid" true. The defect is the predicate
around it. Two questions, "will a record be consumed" and "will an event be
consumed", had five spellings across four sites (the `planner` site dropping the
nil check under a comment explaining why it could), because the nil test was copied
rather than derived. `Recording()` and `Emitting()` are now each question's single
home, both nil-safe, and `Emit`'s guard *is* `!s.Emitting()`.

The unreachability stands and is now documented as the division of labour it is:
nil is valid at the entry points **and** callers still gate, because Go evaluates
arguments eagerly and no guard inside `Record` can prevent a payload map or a
`hasAggregates` walk. That is why the entry guards go unreached from inside the
module — the fact 4e read as the defect, missing only its reason.

Verified free before landing, at owner instruction: `-gcflags=-m` reports `can
inline` for both predicates and `inlining call to` at every site, cross-package
included. Unchecked, the change would have placed a real call on the decorrelation
pass's per-`LateralJoin` path — the accessor-wrapper shape, introduced while
claiming to remove duplication.

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

**Status 2026-07-30: closed.** 5a's free half is fixed — the `Iterator` contract
now states the run's end and membership rule alongside its start — and its decision
half (publishing a runnable conformance suite) is governed by 5d's ruling. 5b is
fixed; 5c is superseded by R4, with its one live clause corrected; 5d is struck.

**5d's surviving residue is itself false — derived 2026-07-30.** It claimed
`CRDT_UNIQUE_SEMANTICS.md:403` shows `def.Unique != ""`, a reference document
displaying code that cannot build. That line reads `if def.HasUniqueConstraint()`,
which compiles: the method exists as `schema.AttributeDefinition.HasUniqueConstraint`.
The string `def.Unique != ""` occurs nowhere in that document — its only two
occurrences in the repository are this file's own claim and the status line that
repeated it. Third round-4 instance to fall on derivation, after 4d and 6c.

The search that settled it did find three genuinely stale snippets, none of them
5d's subject and none a status claim: `BUG_UNIQUENESS_VALIDATION_TOCTOU.md` (in
`resolved/`, showing `def.Unique == ""` as the code was when the bug was written),
and `CRDT_COMPOSABLE_TOOLKIT.md` and `SCHEMA_SELECTIVITY_HINTS.md`, which use
`def.Unique` as a bool in proposed code. A resolved bug document quoting the code
of its own era is a record, not a false claim; the two proposals describe code that
does not exist yet and would need rewriting whenever they are taken up.

**Deriving the instance set.** For each moved obligation: every implementation
of the interface, every installer/caller in the repository including the
engine's own, and the document the external party is pointed at.

**Instances.**

**5a. The wildcard pull path depends on a `Seek` obligation the `Iterator`
interface does not state.** *Partly verified.* `0c30a7a`.
`resolveWildcardEntity` (`pull_batch.go:163-181`) opens no scan — one iterator
over the whole EATV index is handed to it (`:77`) and each entity reached by
`Seek`. That `0c30a7a` deleted both the `key[1:21]` check and the decoded
`datom.E` check is certain, from its own commit message. **Verified 2026-07-29:**
the `Seek` doc comment on the interface states repositioning at or after the
bound's start, plus the sticky-error rule for an unencodable bound, and nothing
about `end` or the membership rule — both of which `Seek` sets and which
`positioned()` enforces. Review reports by mutation that a `Seek` implementing
exactly the documented contract reds five Badger tests, and the wrong attributes
are then written into the EA cache under the requested entity's key, outliving the
call.

**The instance is two things, and only one of them waits on anything — split
2026-07-30.** Filing both under the extension-point question is what kept the
free half open.

- **Stating the obligation is a defect outright, and needs no ruling.** `Seek`
  sets three things from one `EncodedRun` — the start, the run's `end`, and the
  membership rule — and the interface comment names the first. Writing the other
  two in describes code that already behaves that way. It is owed to the two
  in-tree implementations before any hypothetical third: **5b is this defect
  landing**, `memoryIterator.ElementID` having dropped `end` and membership
  because nothing on the contract said to consult them.

  **Fixed 2026-07-30** (ledger item 37). The contract states all three, and states
  what follows from taking only the start: the iterator walks past the sought bound
  into whatever the scan's wider range still holds, and the caller is left deriving
  its own run's end from the encoded key — which is the `pull_batch.go` arithmetic
  `0c30a7a` deleted, re-arriving from above the seam. `memoryIterator.Seek` and
  `KeyOnlyIterator.Seek` already carried that reasoning in their bodies; the
  interface an external backend reads did not, which is this family's invariant
  exactly. `TestSeekHonoursTheRunItNames` already pinned the behaviour, so no test
  changed.
- **Publishing a runnable contract is the decision.** `storeContractCases` and
  `TestSeekHonoursTheRunItNames` are in `_test.go` files, so nothing outside the
  package can execute the real obligation. Exporting a conformance suite is API
  surface, and 5d's ruling governs it: recording internal surface in a
  consumer-facing form manufactures the compatibility obligation rather than
  reporting it. Open, and it is the only part that is.

**5b. `memoryIterator.ElementID` does not consult `end`.** *Verified; **fixed
2026-07-29**.* It now returns the zero value unless `positioned()` holds — the
same predicate `Key()` and `Datom()` use, covering `closed`, the position bounds,
`end` and the membership rule, rather than duplicating the first half of it.
`TestSeekHonoursTheRunItNames` gained the assertion, across both backends: an
exhausted run positions nothing. The pin discriminates on any entity but the last,
where `Next()` stops because the key at the cursor lies past `end` and so leaves a
valid index on the next entity's first key.

As reported, against `0c30a7a`: `positioned()`, `Key()` and `Datom()` all test
`end`; `ElementID()` (`memory_store.go:618-623`) tested only `closed` and the
position bounds. `Scan(wide)` → `Seek(narrower)` → `ElementID()` returned a Tx
from outside the run on memory, the zero value on Badger
(`badger_store.go:525-534`), and `store.go:145-148` documents the Badger answer —
which is the one the memory backend now gives.

**5c. The engine's own verbose installers do not wrap, and `Synchronized` has
zero callers.** *Verified.* `253df0f` (ruling 9). `db/options.go:73-80` and
`:84-91` each close over one `*OutputFormatter`, stateful across calls
(`lastIndex`/`lastBound`). `Collector.Add` calls the handler outside its mutex
by design; `forkContext` shares one collector across four workers.
`db.Open(path, db.WithVerbose())` is a race. Reported tree-wide: 40 handlers
installed, 27 mutating captured state unlocked.

**Closed 2026-07-29: the race premise is stale.** It rests on the installers
closing over a formatter "stateful across calls (`lastIndex`/`lastBound`)". Those
fields no longer exist — they survive only in the comment on
`TestScanLineRendersFromItsOwnEvent`, which pins that a scan line needs no memory
of the event before it, the matcher having been made to carry the bound on the scan
event. `OutputFormatter` holds `useColor`, `writer` and `renderer`;
`RelationRenderer` holds `useColor`. All are set at construction and never
mutated, so a formatter shared across workers has no cross-call state to race on.
What remains is that concurrent `Fprintln` calls can interleave whole lines on
stdout — output ordering, not shared state.

`Synchronized` likewise has zero occurrences: it went with `253df0f`, before this
instance was read.

One clause was live. Of the two documents said to state the removed promise, one
did not — `annotations/types.go`'s Handler text is the *post*-removal statement,
and it is the authority the other now cites. The other did:
`executor/context.go:65` said the shared collector "is internally synchronized,"
while `Collector.Add` unlocks at `:365` and calls the handler at `:369`, so the
mutex covers accumulation only and a shared collector enters the handler
concurrently. Corrected — which matters for the next handler someone writes, since
that sentence read as license to keep state in one.

**And then resolved by removing its subject, 2026-07-29** (ledger item 36). The
corrected sentence is gone with the collector it described: `Context` carries
`scanRegistry` and `env` only, and `annotations.Collector` is deleted. Ruling 9's
mechanism — wrap a handler at the installer — goes with it. There is no wrapping
step for an installer to forget, because a handler is registered on the options
every executor, matcher and relation is constructed with, and `WrapMatcher` now
times the `Match()` boundary and claims nothing about what happens inside it.

**5d. Struck 2026-07-29,** together with the guides it was measured against.
Its counts were of Go-visible symbols, not of consumer-visible breaks: the four
names and twelve signatures are `datalog/storage` and `datalog/schema` internals,
and `CacheResolver` is exported so `db` and `executor` can reach it. Recording
them in a consumer-facing document would have declared that surface to be API,
manufacturing the compatibility obligation the instance claimed to report — and
binding every later refactor of it to a migration note.

One part of 5d survives as its own item, unrelated to any guide:
`CRDT_UNIQUE_SEMANTICS.md:403` gives `def.Unique != ""`, which no longer compiles
now that `Unique` is a `datalog.Keyword`. That is a reference document showing
code that cannot build — plain staleness, **open**.

---

## Family 6 — Pins assert the plumbing, not the producer's choice

**Invariant.** A pin fails when the code under test computes the wrong value —
not merely when the mechanism that carries it is removed.

**The generator.** The funnel keys are written unconditionally by one shared
emitter, so any assertion on key *presence* tests the emitter. The tests were
written alongside the emitter, so they inherited its guarantees as their
subject. Same shape as T7 one round earlier, on the other two terms.

**Status 2026-07-30: closed.** 6a and 6b are fixed — every funnel term is asserted
in value, and the cache arm's table covers all three cardinalities rather than
cardinality-one alone. 6c is **struck as false**: the absolute floor it asked for
was already the line above the one it read. 6d's coverage clause is addressed, its
two later clauses were derived away, and its surviving gate hole is closed by
`make test-examples`.

**Deriving the instance set.** For each new assertion, name the mutation it is
supposed to catch and apply it. The review did this and three assertions
survived; the rest of the round's pins have not been mutation-checked.

**Instances.**

**6a. `require.Contains` on `KeyDatomsResolved`/`KeyDatomsMatched` tests the
emitter.** *Verified 2026-07-29 by reading the assertions.* `scanned` is asserted
in value — `require.Positive`, plus the `scanned >= resolved` ordering. `resolved`
appears **only** in that inequality, which `0` satisfies, and `matched` is never
value-asserted at all. Hardcoding either to `0` in any arm leaves every row green.

**6b. The cache arm's intake is unpinned in value.** *Verified 2026-07-29.* The
cache path's two assertions are `require.Positive` on the cold row and
`require.Equal(0, …)` on the warm one, both satisfied by a constant intake. The
two exact intake assertions in the package are on `ScanDirect`, not
`resolve-complete`, so neither covers `ResolveLWW`. The separating input is a
concrete `AsOf` handle, which no test drives through the cache arm.

**6c. Struck as false — derived 2026-07-30.** The instance read one assertion and
missed the one above it. `require.Less(silent, observing)` is indeed relative, but
the preceding line is `require.Equal(t, noSink, silent)` — a silent sink must
allocate *exactly* what a **nil** sink does, and a nil sink cannot build provenance
at all. That is the absolute floor the instance asks for, already present. `Less` is
the discrimination check keeping `Equal` from passing vacuously if the pass built
nothing in all three cases.

The cited counterexample refutes itself: restoring a deleted rendering takes the
silent path from 217 allocations to 240, which is exactly what `Equal` reds on. The
"verified 2026-07-29" marker recorded reading `Less`, not the assertion pair.

Second instance to fall on derivation, after 4d. Both were *Verified*.

**6d. Coverage the gate does not reach.** *First clause verified 2026-07-29:* the
arm table sets `DisableCache: true` for every case, so with the cache on — the
default — the six constant-E arms are unreachable in this test, and their
replacement is asserted for cardinality-one only.

**The resolver clause is narrower than written — derived 2026-07-30.** It said
`ResolveAddWins`, `ResolveRGA` and `ResolveLWW`'s unique branch are unexercised
through the cache arm. All four are exercised: `ResolveLWW` and `ResolveAddWins`
directly in `cache_integration_test.go`, `ResolveRGA` in `cache_vector_test.go`,
`ResolveLWW`'s tombstone arms across `crdt_one_remove_cache_test.go`, and its
unique branch — the `walkUniqueEntityValue` path gated on schema, uniqueness and
non-history mode — by `TestPullInto_UniqueFallback`, which pulls a superseded
unique attribute through the cache. What is unexercised is their **funnel
reporting**: `TestCacheResolvedPatternReportsItsCostAndAnnouncesNoRun` queries
`:person/name`, `One()` in `funnelSchema`, so the cache arm's report is pinned for
cardinality-one and for neither of the other two. That is 6b narrowed by
cardinality, not uncovered resolution — the resolvers' answers are tested, what
they say they cost is not.

**The `CacheEntry.scanned` clause does not hold either — derived 2026-07-30.**
Its reader exists: `cache_test.go:116` asserts `entry.scanned` against the build
cost, and that test is the pin for the very distinction the field's comment
states — build cost forever, against the per-call `datoms.scanned` a hit reports
as zero. `matcher_relations.go:1743` and `:1852` write it besides the three
rebuild arms. And `PopulateFromDatoms` leaving it zero is correct rather than
missed: it resolves through `ResolveLWWFromDatoms`/`AddWins`/`RGA` over datoms
already in hand and opens no scan, and both its callers — `prefetch.go` and
`pull_batch.go` — opened and reported their own scan, so charging those reads to
each entry as well would count them twice.

What survived of 6d was the coverage clause it opened with, now addressed, and the
gate hole: everything under `examples/` is `//go:build example` with no Makefile
target, so this round's `examples/schema.go` edit was covered by no gate.

**Closed 2026-07-30.** `make test-examples` vets each file separately and is part
of `make test`. Separately because `go vet ./examples/` reports `main redeclared`
— every example is its own `package main`, which is why sixteen files had no gate
rather than an oversight. The file list is an overridable variable, which is how
the gate was shown able to red at all: `make test-examples
EXAMPLE_FILES=examples/does_not_exist.go` exits Error 1. A gate with no
demonstrated failure path is 6a/6b's defect one layer out — an assertion
satisfied by a constant.

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

*Part 2 falsified and amended, 2026-07-28, by deriving it against the arms that
actually forgot.* Riding on the iterator reaches an arm that holds one. Of the
six silent paths, one does:

| path | scanning iterator to ride on |
|---|---|
| `matchWithBindingsFromCache` | none — resolves per binding through the cache |
| `matchVectorWithBindings` | none — one resolver call per binding |
| `PrefetchEntities` | one per entity, not one |
| `ResolveAllAttributesMany` | one shared, but the arm is a Database method |
| `LookupAllAttributes` | two or three, by arm |
| `LookupAttribute` | one, in the cardinality-one arm only |

Having no natural place to hang an emit is *why* they forgot, so a mechanism
keyed to that place covers the arms that never needed it.

**Amended: bind the obligation to acquisition.** What all six share is going
through `Scan`/`ScanKeysOnly` for a query. `PatternMatcher` acquires through
`openScan`/`openKeyScan`, which take the report that accounts for the scan and
return the iterator with the accounting attached. Acquisition also subsumes part
1: the report *is* the intake carrier, which is why `scanScope` no longer exists
— an arm's report could not name a run its resolver acquired while the two were
separate types.

An arm cannot open a scan on the query path without naming what it is for. What
it can still get wrong is feeding `resolved` and `matched`, which shows as zeros
under a positive intake rather than as silence.

**No constructor, no accessors, no nil-safe methods.** The arm builds the report,
its cause map, the clock read and the deferred close inside one
`if m.handler != nil`. A constructor that tested the handler itself would be
handed a cause map the caller had already allocated, which is the same defect as
an emit guarded inside the emitter while its caller builds the payload anyway.

**Acquisition-binding forced a contract change in `CRDTResolvingIterator`, and
that change is load-bearing.** `Scanned()` used to return
`source.Scanned() + uniqueScanned` — the unique walk's AVET supersession reads
folded into the wrapper's own total. Once the source is acquired through a
report, a report attached beneath the wrapper sees only the source, so the walk's
reads vanish: an under-report of the deepest read on the path, introduced by the
fix for under-reporting, and one the gate would not have caught because
`TestUniqueAttributeWalkReportsItsOwnScans` asserts on the event, which would
still exist and still look plausible.

The walk is a nested read and now accrues into the arm's report where it
happens. `Scanned()` returns the source's intake and only that. The two other
shapes both fail: attaching the report after the wrapping means the arm must
remember a second call, and reading the outermost `Scanned()` at close
double-counts against the wrapper's accrual.

Arms not yet converted carry the walk's intake in a `nested *scanReport` added to
the funnel they already build. That invariant — `nested` allocated iff
`handler != nil` — is maintained by hand at three construction sites, and one of
the three was missed on the first pass; `TestBindingDrivenStrategiesReportTheirFunnel`
panicked on the nil. The gate is what enforces it until those arms convert.

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

**Collapse done 2026-07-28.** `pattern/storage-scan`,
`pattern/hash-join-complete`, `pattern/merge-join-complete`,
`pattern/per-binding-scan-complete` and `unique/lookup-complete` are one
`storage/scan-complete`, with `annotations.ScanStrategy` under
`annotations.KeyStrategy` carrying what the names used to. The strategy set
draws exactly the distinctions the five names drew, so the collapse asserts
nothing new. The formatter's four arms are one, dispatching on the strategy,
with an explicit `ScanDirect` case and a `default` that names an unrecognised
strategy rather than rendering it as a direct scan.

Two things the implementation found that the argument for it had understated.
`Database.Analyze` was not merely enumerating names — it was enumerating three
of them as string literals, so it would have silently reported zero rather than
failing to compile; see 3e-consumers above. And
`scan_funnel_reporting_test.go` lost its `completions []string` column outright:
the field existed only because the name varied by strategy, and every arm now
reports through the same one.

`VValidationOpenScan` and `VValidationScanOpened` are untouched. They are
announcements, not completions, and the collapse is over the completion family
only.

**R3. The cache event reports what it did.** `entry.valueCount()` is not a wrong
number — on a hit the call really does take all the entry's values. The funnel
models three stages, index intake → resolution → pattern, and a hit skips the
first two, so `scanned ≥ resolved` fails not because a count is wrong but
because the relationship does not exist for a hit. `storage/resolve-complete`
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

Two, both surfaced by derivation rather than by the review. Both are now ruled,
so nothing in this document awaits a decision.

**D1. Does a `*-complete` event fire for a scan that aborted?** From 2d.
*Ruled 2026-07-28: it fires, and says which.*

Both answers were in the tree: `cardinalityManyScanAllEntitiesIterator.Close`
and `vectorScanAllEntitiesIterator.Close` emitted on the handler check alone,
reporting a funnel for a scan that ended in `it.err`; `LookupByUnique` emitted
before its error check, deliberately, so the reads a failed lookup performed
were still accounted. The question was what the name asserts — "this scan
finished" or "this scan is over, here is what it cost."

The wider tree already answered it. Eleven producers across `executor`,
`reflect` and `annotations` fire their completion unconditionally and carry
`success: err == nil`, including `annotated_matcher.go` — the matcher decorator,
the closest analogue to a scan completion. The review surveyed three scan sites
and found two answers; the convention outside them is one.

So `-complete` means "this operation is over, here is what it cost", and the
outcome travels in the payload. `success` was a bare string literal at all
eleven producers, which is Family 3's defect in a key the formatter reads; it is
now `annotations.KeySuccess`, declared once and written by every producer
including the scan family. `renderScanFunnel` marks an aborted funnel, since
read without that its counts are a total.

**D2. The fourth-cardinality guard.** From Family 1.
*Ruled 2026-07-29: pin the set.*

`TestCardinalitySetIsClosedAtThree` (schema) asserts `cardinalities` holds
exactly `CardinalityOne`, `CardinalityMany` and `CardinalityVector`, and that
`CardinalityUnknown` — which marks an attribute with no definition — stays
outside it.

R1a had already ruled out the runtime alternative: twenty-two unreachable
`default` arms guarding an impossibility, which on the read paths would mean
threading an error return through signatures like `valueCount() int` for a value
admission makes unreachable. What remained was the exposure R1a does not cover —
not a bad value arriving, but the set growing while the switches do not.

The set rather than the switches, because `defineCardinality` is the single
registration point: a test enumerating the twenty-two dispatch sites is a list
that rots, while this one cannot be satisfied except by noticing. The failure
message directs the author to give every cardinality switch an arm first, since a
member with no arm is skipped silently — zero rows, nil error.

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

`storage/resolve-complete` and the `per-binding` strategy carry neither, also
correctly: the first addresses no run, and the second runs `chooseIndex` again
per binding tuple.

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

## Found while implementing — the existence guard placed where it gates nothing

Not a review instance. Surfaced by the owner while reviewing 2b, and the class
is wider than the code 2b touched.

The rule is that the handler-existence guard lives at the **call site**, because
it must gate the caller's argument preparation and not only the emit. The shape
in the tree was the guard wrapped around the emit with the clock read left
outside it:

```go
opened := time.Now()          // unconditional
if m.handler != nil { ... }   // guard starts here
```

`time.Now()` is not free, and its only consumer is the event. **21 sites** paid
it whether or not anything was listening: the eight cardinality dispatch arms,
three iterator constructors (`unboundIterator`, `nonReusingIterator`,
`validatingVBoundIterator`), `hashJoinIterator` and the merge-join scan,
`matchFromCache`, `matchWithBindingsFromCache`, both `Lookup*` functions,
prefetch, batch pull, the unique-value lookup in `database.go`, the phase loop
in `executor.go`, and two in `executeOrClause`/`executeOrClauseUnion`.

Several of those sites already had the guard *one line below* the clock read.
`database.go`'s unique lookup carries a comment describing itself as the
dominant read in an upsert loop.

The paths where this costs most are the ones that can return having touched no
storage: a cache hit, an absent (E, A), a fused attribute fetch per row. There
is no scan to amortize against on those, and the memory backend has no disk to
amortize against on any of them.

`context.go`, `pull_context.go`, `annotated_matcher.go`, `or_fallback_relation.go`
and `reflect/context.go` were checked and are correct by construction — either
decorator types whose `Base*` counterparts are no-ops, or already inside the
guard. `planner/cache.go`'s clock is cache expiry, not annotation.

**A second defect in the same sweep: `aggregation/executed` fired before the
aggregation.** `AddTiming(name, time.Now(), data)` took its start at the emit,
and the emit sat ahead of the fold, so the event named for the execution
reported a duration that could not contain it. Start is now taken before the
work and the three return paths converge on one emit after it. The streaming
path builds a relation and folds at consumption, which `aggregation/materialized`
already reports, so a near-zero latency there is now a true statement about what
the call did rather than an artifact of every path; `aggregation_mode`
distinguishes them. The same block read `ctx.Collector()` directly while the
strategy event two blocks above used the `opts.Collector` fallback, so an
options-supplied collector saw the decision and never what it led to.

**That last clause is now structurally impossible** (ledger item 36): there is one
observer, `opts.Handler`, and `ExecuteAggregationsWithContext` — the arm that
supplied the second — is deleted. A producer cannot pick between two sources for
one handler when the options are the only source.

Pinned by `TestAggregationExecutedTimesTheAggregation`. The pin took two
attempts, both instructive: `require.Positive` on the latency passes against the
defect, because `AddTiming` computes `end.Sub(start)` and a start taken at the
emit still yields a few hundred nanoseconds. Calibrating against a separately
measured fold then failed under wasm, where the cold first run and the warm
second differ six-fold — more than the effect. It now compares against the same
call's own wall time, which the reported duration is a sub-interval of.

`join/build.copy` had the same shape and worse: two consecutive clock reads and
no `Latency` field at all, so the duration was exactly zero while `End - Start`
was a few nanoseconds. Its interval is the build drain, which is where the
copies it counts are made, and it ends at the loop rather than at the emit —
grouping runs in between and the copy statistics do not describe it. Pinned by
`TestJoinBuildCopyReportsTheIntervalItsCountsWereMadeIn`, whose load-bearing
assertion is that `Latency` agrees with the event's own `Start` and `End`: a
disagreement is not a fast build, it is timing that was never taken.

## Gate

`make test` was green at head through the producer sweep — native
(`go test -count=1 ./...`, 20 packages) and the js/wasm leg.

It was red by one test for the duration of that bug —
`TestCacheMatrix_NeverSetVectorAgainstEmptyVectorLiteral`, held in the tree
rather than out of it while the ruling was pending. `BUG_CACHE_EMPTY_VECTOR_NEVER_SET`
is now resolved and the gate is green again, native and wasm, through 2a, 2b,
the guard sweep and the aggregation timing fix.

Green again 2026-07-29 through 5b and the handler registration of ledger item 36
— native (20 packages) and the js/wasm leg.

The review's own run reported `go build`, `go vet` and `go test -count=1
./datalog/... ./tests/...` green — 17 packages, zero failures, zero skips, 272s —
with all ten named tests present and passing, but without the wasm leg.
