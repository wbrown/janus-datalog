# History cannot tell a withdrawal from an assertion

**Status**: Resolved. The body states the defect as filed; the Resolution at the end states the fix.

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

## Resolution

- A data pattern holds one to five elements. `query.PatternPositions` is 5, and `DataPattern.GetAdded` reads the fifth position, whether the datom asserts.
- `datalog.CRDTOp.Asserts` answers whether an operation asserts its value: true for `OpNone`, `OpCRDTAdd` and `OpRGAInsert`, false for `OpCRDTRemove` and `OpRGATombstone`.
- On a History handle the position reads each datom's own operation:
  - `PatternMatcher.matchesDatom` matches a datom whose `Op.Asserts` equals the position's bound value, and a value that is not a boolean matches none.
  - `query.InternedTupleBuilder` and `query.DatomToTuple` bind the position's variable from `Op.Asserts`.
  - The routes that read raw datoms, the scan, the per-binding route, the hash join and the merge join, filter through `matchesDatom` and build through the interned builder.
- In the latest view and an as-of view every tuple is a resolved value, so `PatternMatcher.matchAsAssertions` (`datalog/storage/matcher_relations.go`) decides the position once for every route:
  - the constant true and a blank answer the pattern's first four positions
  - a variable is bound to true beside each of their tuples
  - false and any other constant answer nothing
- A change of a cardinality-one value is a supersession: the store records the new value alone and no withdrawal of the old one, and History records nothing more. So `[?e :a ?v _ false]` answers one tuple per `Remove`, binding the value the call named, and nothing for a superseded value. A cardinality-one `Remove` withdraws whatever value the attribute holds, so the value it names need not be one the attribute held, and the false tuple binds the named value, not the value the view lost (`TestAWithdrawalBindsTheValueTheRemoveNamed`). `DATOMIC_COMPATIBILITY.md` states the difference from Datomic's history, which carries the old value's retraction.
- `executor.MockPatternMatcher`, which reads datoms as written, reads the position from the datom's operation.
- The explain rendering names a bound fifth position on its `Bound:` line (`planner.BoundMask.Added`).
- `qb.Pat` builds a pattern of five elements, and `QUERY_BUILDER.md`, the `qb` docs and `DATOMIC_COMPATIBILITY.md` list the form.
- The tests:
  - `TestHistoryBindsWhetherEachDatomAssertsOrWithdraws` (`datalog/storage/assertion_position_test.go`), every backend
  - `TestTheLatestAndAsOfViewsBindEveryTupleAsAnAssertion`, with the cache enabled and disabled
  - `TestScanFingerprint_FifthPosition`
  - `TestMockPatternMatcherBindsWhetherEachDatomAsserts`
  - `TestExplainRendersABoundFifthPosition`
  - `TestAFiveElementPatternParsesRendersAndParsesBack`
  - `TestPatOfFiveElementsEqualsItsParsedForm`
  - the four refusal tests, at six elements
