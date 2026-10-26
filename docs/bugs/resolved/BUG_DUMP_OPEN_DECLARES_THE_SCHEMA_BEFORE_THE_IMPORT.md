# `db.Open` on a dump declares the schema into the empty store and then imports the dump's own declarations

**Status**: Resolved.

The current view resolved each definition to one declaration, so no answer
was wrong. The defect was a store that held every declaration fact twice,
and a dump that grew by one copy of the schema on every load-and-export
cycle.

## The order

`db.Open` (`datalog/db`) built its `storage.DatabaseOptions` with the
caller's schema and, for a dump source, called
`storage.NewDatabaseWithOptions` first and `Database.ImportDump` second.

Opening with a schema declares. `docs/reference/SCHEMA.md`, "Schema as
Facts": the definitions the store holds are read, and each given definition
the store does not hold word for word is declared in one transaction. The
store was empty at that moment, so every definition was declared, under the
opening database's replica.

Importing asserts. The same section: a dump carries the declarations and the
definitions' refs to them, and the import asserts them with the rest of the
dump, as a replay of the writer's facts under the writer's ElementIDs.

A declaration entity is named by its words and a definition entity by its
ident, so the open and the import wrote datoms of the same entity, attribute
and value, at two ElementIDs on two replicas. The current view resolved them
to one. History, and an export, carried both.

## What was observed

A loader opened an empty destination with its schema, imported a dump, and
exported. The export held every declaration datom twice, at one Lamport under
two replica ids, and was twice the size of the dump it had loaded. The loader
was corrected to open with no schema, import, and then call
`Database.SetSchema`, after which its round trip was byte-stable; `db.Open`
took the uncorrected order for every dump it opened with a schema.

The growth was per cycle: a second load declared again under its own replica
and then imported both earlier copies.

## The fix

`storage.DatabaseOptions` carries `Dump`, an `io.ReadSeeker` holding an
exported dump. `NewDatabaseWithOptions` opens the store, replays the dump
into it (`replayDump`, which reads the format from the dump's own bytes and
asserts its datoms under the writer's ElementIDs), and then runs its one
`installSchema(true)`. The declarations the dump carries are in the store
when the install reads it, so a given definition the dump states word for
word is found and not declared a second time, and a load-and-export round
trip under the writer's schema is byte-stable. `db.Open` sets `Dump` for a
dump source, a path, an open file or bytes, and imports nothing after
construction.

The order the loader was corrected to, an open with no schema, an import and
then `SetSchema`, is not the fix: `RefuseUnknownAttributes` requires a schema
at construction (`installLocked` refuses the option with no schema), so an
open with no schema cannot carry it. The constructor holds the schema and
declares it after the dump's facts are in.

`Import`, `ImportBinary` and `ImportDump` each replay (`replayEDN`,
`replayBinary`, `replayDump`) and then install, so a database that imports a
dump after construction composes its schema from the dump's definitions as
before. A database constructed with a schema and then given a dump carrying
the same declarations through those methods holds both copies, in the
caller's order.

## Tests

`TestOpenDumpWithTheWritersSchemaDeclaresNothing`
(`datalog/db/dump_source_test.go`) opens a dump with the writer's own schema,
in both formats and over every backend and planner mode, and asserts that
History holds one declaration of the attribute and that an export of the
database is the dump it loaded, byte for byte.

`TestReplayLoadsTheDumpAndComposesNoSchema`
(`datalog/storage/dump_replay_test.go`) replays a dump through `replayEDN`,
`replayBinary` and `replayDump` and asserts that the store holds the dump's
one declaration and its members and that the database runs with no
definition of the attribute until `installSchema` composes the one the dump
carried.
