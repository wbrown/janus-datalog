# `db.Open` on a dump declares the schema into the empty store and then imports the dump's own declarations

**Status**: Open.

The current view resolves each definition to one declaration, so no answer
is wrong. The defect is a store that holds every declaration fact twice, and
a dump that grows by one copy of the schema on every load-and-export cycle.

## The order

`db.Open` (`datalog/db`) builds its `storage.DatabaseOptions` with the
caller's schema and, for a dump source, calls
`storage.NewDatabaseWithOptions` first and `Database.ImportDump` second.

Opening with a schema declares. `docs/reference/SCHEMA.md`, "Schema as
Facts": the definitions the store holds are read, and each given definition
the store does not hold word for word is declared in one transaction. The
store is empty at that moment, so every definition is declared, under the
opening database's replica.

Importing asserts. The same section: a dump carries the declarations and the
definitions' refs to them, and the import asserts them with the rest of the
dump, as a replay of the writer's facts under the writer's ElementIDs.

A declaration entity is named by its words and a definition entity by its
ident, so the open and the import write datoms of the same entity, attribute
and value, at two ElementIDs on two replicas. The current view resolves them
to one. History, and an export, carry both.

## What was observed

A loader opened an empty destination with its schema, imported a dump, and
exported. The export held every declaration datom twice, at one Lamport under
two replica ids, and was twice the size of the dump it had loaded. The loader
was corrected to open with no schema, import, and then call
`Database.SetSchema`, after which its round trip was byte-stable; `db.Open`
takes the uncorrected order for every dump it opens with a schema.

The growth is per cycle: a second load declares again under its own replica
and then imports both earlier copies.

## The fix

`db.Open` on a dump source opens the destination with no schema, imports,
and then declares the schema through `Database.SetSchema`.
`docs/reference/SCHEMA.md`, "Schema as Facts", states that `SetSchema`
declares a schema the way an open does: each given definition the store
does not hold word for word is declared, and the rest write nothing. A
definition the dump lacks is declared then, as it is now. One the dump
states is read from the dump. The import's replay is exempt from every
schema check (`docs/proposals/NESTED_SCOPES_AS_QUERIES.md` §8), so the
import needs no schema in hand.

## Tests

A test for the fix opens a dump with a schema, exports, and expects the
export to hold each declaration once, so that a load-and-export round trip
is byte-stable.
