# Four proposals carried a statement their own text or the engine's contracts contradicted

**Status**: Resolved.

These were defects in design documents, not in the engine. Each document
said one thing in one place and another in a second, so a reader could not
tell which the design meant, or it stated a choice the engine's standing
contracts rule out. Each document now states each fact once. What each
states is recorded below, beside the contradiction it replaced.

## `docs/proposals/DISTRIBUTED_JANUS.md`

**Retractions.** "Append-only writes" said "No retractions, only appends with
new ElementID" and, two lines down, "Cardinality-many: add-wins semantics
with tombstones". A tombstone is a written record of a removal, which the
first line said is never written. The first line now reads "No in-place
deletion: a removal is an appended tombstone with its own ElementID". Both
lines state the one rule: every write, a removal included, is an append.

**The sharding hash.** "Sharding by Datom Identity" hashed `Hash(E, A, I, V)`
while the Write Path's code hashed `hash(d.E, d.A, d.ElementID, d.V)`. The
derivation now reads `DatomIdentity = Hash(E, A, ElementID, V)`, the hash the
code computes.

**The shard map's name.** The Write Path's map was declared `bySherd` and
used as `byShard`. It is declared and used as `byShard`.

## `docs/proposals/CRDT_COMPOSABLE_TOOLKIT.md`

**The `One()` combination.** The convenience method `One()` set
`OrderingNone`, `unique = false` and `ConflictLWW`, the combination the
Invalid Combinations table listed as "Bag with LWW is meaningless". The
three primitives describe a collection's members. A cardinality-one
attribute holds one value, so ordering and uniqueness do not apply to it,
and only the conflict rule does. The proposal now says so:

- the proposed struct keeps `Cardinality`, and its dispatch's first arm is
  cardinality one
- `One()` sets cardinality one and `ConflictLWW`, the register
- `MVRegister()` sets cardinality one and `ConflictMV`, the MV-register
- the register and the MV-register are stated above the composition
  matrix, whose combinations describe collections
- the Compatibility section maps `CardinalityOne` to no composition
- the invalid combination "Bag with LWW is meaningless" stays, since it
  describes a collection

**The LSeq ordered set's name.** The LSeq ordered set had no convenience
method. The Valid Combinations table names it `.LSeqOrderedSet()`.
`OrderedSet()` stays the RGA ordered set the engine has built.

## `docs/proposals/REGISTERED_FUNCTIONS.md`

**Data access.** "Execution Semantics" said a registered function cannot
access data sources, and Example 2's `resolve-subject` called
`getWorldEntity(base)` and `getProperty(base, prop)`, each a read of data.
Datomic's query grammar lets a function clause take a source variable as an
argument, `fn-arg = variable | constant | src-var`, as in
`[(datomic.api/entity $ ?e) ?ent]`. "Execution Semantics" now says a
function is deterministic in its arguments, performs no side effects, and
reads data only through a source passed to it as an argument. Example 2's
call is `[(resolve-subject $db ?expr ?base ?prop) ?subject]`, and its Go
function receives the source first and reads the world entity and the
property through it. The Go value a source argument carries is stated as a
choice, under "The choices".

**The default error mode.** "Error handling" made the default mode filter:
an error from the function dropped the tuple. The engine's iterator-error
contract (`TODO.md`, "Iterator-Error Propagation Contract") has every error
reach the result, and a dropped tuple is that error turned into a smaller
clean answer. Fail is now the default, and filter is the opt-in,
`datalog.OnErrorFilter()`.

**The registry.** `RegisterFunction` registered into a package-level
registry. The engine's settings live on the database or the executor
(`docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` §4.2), and a function is
one. "Registry and Scoping" now names two homes, the database
(`db.RegisterFunction`) and a set passed with the query
(`datalog.NewFunctionSet()`), and every example registers on one of them.

## `docs/proposals/PRESORTED_INDEX_SECTIONS.md`

**The Stored column.** The derivation table's Stored column reads "Probably"
for ATEV and AVET and "Unclear" for TAEV. The column states that the fact
is undecided, and the document's "What decides it" names the profile that
settles it. That is one statement, not two, and it stands.
