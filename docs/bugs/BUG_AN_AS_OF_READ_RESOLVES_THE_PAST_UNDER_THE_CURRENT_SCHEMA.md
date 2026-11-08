# An as-of read resolves the past under the current schema

**Status**: Open. `Database.AsOf` resolves the facts up to its target under the definitions in force when the handle is made, not under those in force at the target.

An as-of read is the database as it stood at its target: the facts up to the target, resolved under the definitions in force there. `docs/reference/CRDT.md` states the mode as "CRDT-resolved state at a specific point in time", and `DATOMIC_COMPATIBILITY.md` as "CRDT-resolved state at a specific point in causal time". The store keeps what that read needs. A later declaration moves a definition's ref to its declaration, "so the ref's history is the definition's history" (`docs/reference/SCHEMA.md`, "Schema as Facts").

## The route

1. `Database.AsOf` (`datalog/storage/database.go`) copies the parent's installed value into the handle: `handle.installed.Store(d.installed.Load())`. That value holds the definitions current when the handle is made.
2. `TestTemporalHandleFieldClassification` (`datalog/storage/temporal_handle_inheritance_test.go`) pins the copy. Its table holds `installed` as inherited, "the schema in force and the store's definitions, one value the handle shares with the parent".
3. The handle's matcher resolves each attribute under those definitions, and reads no datom after the target (`PatternMatcher.shouldFilterTx`).
4. So a declaration made after the target decides how the facts before it resolve.

## The shape: cardinality one declared again as a vector

- An attribute declared cardinality-one holds a value whose commit's mark is T1. A later `SetSchema` declares the attribute cardinality-vector.
- A cardinality-one write records its value with no op.
- A vector resolves from its RGA inserts and tombstones alone. `CRDTResolvingIterator`, `PatternMatcher.loadRGAElements` and `ResolveRGAFromDatoms` gather through `vectorElements.gather`, which passes over every other datom ("A datom whose op is neither an insert nor a withdrawal is passed over"). `TestResolveRGAFromDatoms_IgnoresNonRGAOps` pins the skip.
- With no RGA datom at the entity, `PatternMatcher.ResolveRGA` answers the attribute as absent.
- So `d.AsOf(T1)` answers the attribute as absent, where a read at T1 answered the value.
- A handle made before the declaration keeps the definitions it copied, so two handles on one target answer differently.

## Reach

- An as-of read of a point before a declaration that changed a word resolution reads:
  - the cardinality, which decides how an attribute's datoms combine
  - a unique constraint, which decides whether the unique rule's walk resolves the value (`PatternMatcher.ResolveLWW`, from the definition its caller passes)
- An attribute no declaration covers resolves under the definition inferred at the last install, from the ops of every datom in the store then, and not from the datoms up to the target.
- A transaction open across an install writes under the schema it opened with (`Transaction.schema`, read once in `Database.NewTransaction`). Each write draws its ElementID when it is made: `Transaction.Add`, `Set` and `Remove` call `t.db.clock.Next()`. So the writes it makes after the install lie after the declaration, and no point in the store's order resolves them under the definitions they were written under.

## What was not established

- No reproduction runs the shape above. It rests on reading `Database.AsOf` and the vector resolution named there.
- Whether a `History()` read depends on the definitions. A history read resolves no datom, and whether its matcher reads the schema was not read.

## The fix, if taken

- An as-of handle resolves under the definitions in force at its target:
  - for each definition, the declaration its ref resolves to as of the target
  - the engine's words
  - for an attribute no declaration covers at the target, the definition the ops of its datoms up to the target infer
- On a database that refuses unknown attributes, an as-of read checks a query against those declarations, so it refuses an attribute declared after its target. `docs/reference/SCHEMA.md` ("Not refused") states the current answer for such an attribute: no tuple.
- An as-of handle holds an installed value of its own, and `TestTemporalHandleFieldClassification`'s disposition of `installed` changes with it.
- A schema change is rare, and the transaction is its boundary. A transaction's writes are admitted and written under the schema in force when it opened (`SetSchema`: "Schema changes take effect for new transactions"). A change that took effect inside a transaction would tear it.
- So an install waits for every open transaction to finish, and holds new transactions until it is done. Each transaction's facts then lie wholly before or wholly after each declaration, and the definitions in force at a point are the ones every fact there was written under.
- The rewind drains open transactions already (`Database.TruncateTo`: `rollbackInProgress`, `activeTx`, `drainCond`, `onDrainWait`). An install holds a transaction begun while it runs until it finishes, where the rewind drops one (`ErrRollbackInProgress`).
- A goroutine that holds an open transaction and calls `SetSchema` or `Import` then waits on its own transaction, as one that calls `TruncateTo` does.

## Reproducer

The steps follow from the reads in "The route" and were not run.

1. Open a database whose schema declares `:a` cardinality-one, add a value at an entity, and commit. The commit's mark is T1.
2. `SetSchema` declares `:a` cardinality-vector.
3. `d.AsOf(T1).Query("[:find ?v :where [?e :a ?v]]")` is to answer the value. By the reads above it answers no tuple.

## Related

- `docs/reference/SCHEMA.md`, "Schema as Facts": the declarations and the definitions' refs to them.
- `docs/reference/CRDT.md`, "Three database modes".
