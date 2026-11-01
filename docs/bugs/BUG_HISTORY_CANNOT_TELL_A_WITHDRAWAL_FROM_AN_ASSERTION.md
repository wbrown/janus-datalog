# History cannot tell a withdrawal from an assertion

**Status**: Open.

A History read returns a datom that withdraws a value exactly as it returns one that asserts it. Datomic's form that tells them apart, a data pattern's fifth position, is refused. Goal 12 of `docs/proposals/NESTED_SCOPES_AS_QUERIES.md` names that position among the forms that take Datomic's meaning, so the refusal is a gap.

## Datomic's form

Datomic's grammar is `data-pattern = [ src-var? (variable | constant | '_')+ ]`. A datom's fifth component says whether it asserts or retracts. Datomic's history view "includes the present and the unfiltered past, including retractions", and its example `[?e ?a ?v ?tx true]` selects the assertions alone.

## What janus stores

A datom carries its operation in `Op` (`datalog.Datom`, a `datalog.CRDTOp`):

- `OpNone`, `OpCRDTAdd` and `OpRGAInsert` assert a value.
- `OpCRDTRemove` and `OpRGATombstone` withdraw one.

`Transaction.Remove` writes `OpCRDTRemove` for a cardinality-one or cardinality-many attribute and `OpRGATombstone` for a vector. `Transaction.Retract` reaches the store's `Retract` on the commit path, and the memory-tree store's `Retract` removes the matching datoms, so History holds nothing for it. `TRANSACTION_ENVELOPES.md` removes `Transaction.Retract` from the write surface.

## What the language does

- `DataPattern.Validate` (`datalog/query/clause_scope.go`) refuses a pattern of more than `PatternPositions` elements, four, with `data pattern must have 1 to 4 elements, got 5`.
- `unboundIterator.Next` (`datalog/storage`) matches each scanned datom through `matchesDatom` on the entity, attribute, value and transaction, and builds its tuple from them. Nothing reads `Op`.
- `History()` reads the raw datoms with no resolution. So `[?e :a ?v]` on it binds a withdrawn value as it binds an asserted one, and no query can tell the two apart.

## The use

A downstream build runs fixpoints over its store, and a least fixpoint requires that no iteration withdraw a value an earlier one held. Its check is whether the datoms an iteration wrote include a withdrawal. That needs the operation of each datom, which no query reaches.

## The fix

- `PatternPositions` admits a fifth position, read as whether the datom asserts. The scan fingerprint, which sizes its elements by `PatternPositions`, reads it too.
- The matcher binds it from `Op`: true for `OpNone`, `OpCRDTAdd` and `OpRGAInsert`, false for `OpCRDTRemove` and `OpRGATombstone`. A constant at that position selects the datoms whose operation matches.
- In the latest view and in an `AsOf` view every datom a pattern matches is a resolved assertion, so the position binds true there.
- A pattern of six elements is refused, naming the count.
- The tests that pin the refusal at five elements move it to six: `datalog/parser/parser_test.go`, `datalog/qb/shape_test.go`, `datalog/query/shape_test.go` and `datalog/storage/shape_refusal_test.go`.

## Tests

- On `History()`, `[?e :a ?v ?tx ?added]` binds true for an asserted value and false for its withdrawal, for a cardinality-one attribute, a cardinality-many attribute and a vector.
- On `History()`, `[?e :a ?v ?tx false]` answers the withdrawals alone, and `[?e :a ?v ?tx true]` the assertions alone.
- On the latest view and on an `AsOf` view, the fifth position binds true for every tuple.
- A pattern of six elements is refused, naming the count.
