# Four proposals carry a statement their own text or the engine's contracts contradict

**Status**: Open.

These are defects in design documents, not in the engine. Each document
says one thing in one place and another in a second, so a reader cannot
tell which the design means, or it states a choice the engine's standing
contracts already rule out. The maintainer resolves each one. Either one
statement stays and the other goes, or the choice is restated with the
alternative beside it.

## `docs/proposals/DISTRIBUTED_JANUS.md`

**Retractions.** "Append-only writes" says "No retractions, only appends with
new ElementID" and, two lines down, "Cardinality-many: add-wins semantics
with tombstones". A tombstone is a written record of a removal, which the
first line says is never written.

**The sharding hash.** "Sharding by Datom Identity" hashes `Hash(E, A, I, V)`.
The Write Path's code hashes `hash(d.E, d.A, d.ElementID, d.V)`. The two agree
only if `I` is the ElementID, which the document does not say.

**The shard map's name.** The Write Path's map is declared `bySherd` and used
as `byShard`.

## `docs/proposals/CRDT_COMPOSABLE_TOOLKIT.md`

**The `One()` combination.** The convenience method `One()` in the
Implementation sets `OrderingNone`, `unique = false` and `ConflictLWW`. The
Invalid Combinations table lists `Ordering:None + Unique:false + Conflict:LWW`
as invalid, "Bag with LWW is meaningless". The combination the method builds
is one the builder is said to reject.

**The LSeq ordered set's name.** The LSeq ordered set has no convenience
method. The Valid Combinations table shows none for LSeq-OrderedSet, since
`OrderedSet()` is the RGA ordered set the engine has built.

## `docs/proposals/REGISTERED_FUNCTIONS.md`

**Data access.** "Execution Semantics" says a registered function cannot
access data sources. Example 2's `resolve-subject` calls
`getWorldEntity(base)` and `getProperty(base, prop)`, each a read of data.

**The default error mode.** "Error handling" makes the default mode filter:
an error from the function drops the tuple. The engine's iterator-error
contract (`TODO.md`, "Iterator-Error Propagation Contract") has every
deferred error ride through to the result instead of being laundered into a
clean partial result, and a default that drops the erroring tuple is that
laundering at the function.

**The registry.** `RegisterFunction` registers into a package-level registry.
The engine's designs exclude package-level settings in favour of options on
the executor or the database (`docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md`
§4.2, `docs/reference/RULES.md` §2.1 on the fold setting), and a
package-level registry is one.

## `docs/proposals/PRESORTED_INDEX_SECTIONS.md`

**The Stored column.** The derivation table's Stored column reads "Probably"
for ATEV and AVET and "Unclear" for TAEV. The document's "What decides it"
names the profile that settles the column, and the column waits on it.
