# History answers a value-bound pattern from the current view

**Status**: Resolved.

A History query over a pattern that left the entity unbound, bound a
cardinality-one attribute and wrote the value as a constant was answered from
the current view. A datom carrying the constant that a later assertion
superseded was not read. Two datoms carrying the constant that differed only
in the replica that wrote them were read as one. The same query with the
value unbound read every datom, so the answer depended on whether the value
was written in the pattern or filtered after it. The wrong answer was silent.

## The route

`matchUnboundScan` (`datalog/storage/matcher_relations.go`) takes a pattern
with no bindings and chooses its route from the positions the pattern binds.
Most routes set a resolution flag and fall through to one block that clears
every flag in history mode, after which the pattern goes to the raw scan over
the index `chooseIndex` answers, with no resolution. Two routes returned
before that block in every mode.

The first is the arm for an unbound entity, a bound attribute, a constant
value and cardinality one. It returns through `matchWithVValidation`, whose
`validatingVBoundIterator` scans AVET for the attribute and the value, wraps
the scan in `CRDTResolvingIterator`, and validates each candidate the
resolution emits against the entity's first entry on EATV
(`validateCandidate`). The resolution's cardinality-one arm emits the first
entry of each entity-and-attribute group and skips the rest, so two datoms of
one value in one group reach validation as one. The validation reads the
group's first entry on EATV, which `shouldFilterTx` never filters under
History, and emits the candidate only when that entry's value equals the
pattern's, so a superseded value's candidate is rejected. Both steps are the
current view's, and the matcher's mode was read only after the arm had
returned.

The second is the arm for an unbound entity and a bound cardinality-vector
attribute, which returns through `matchVectorScanAllEntities`. Its
`vectorScanAllEntitiesIterator` resolves each entity's vector
(`resolveVector`) and compares the resolved vector against the pattern's
value, and it ran under History as well.

## What was observed

`datalog/db/db_test.go` holds two tests of the first arm, each over every
backend and planner mode.

`TestHistoryReadsASupersededValueBoundInThePattern` writes "Alice" and then
"Alicia" to one entity's name. History
`[:find ?name :where [?e :person/name ?name]]` reads both. History
`[:find ?e :where [?e :person/name "Alice"]]` expects the entity and read
an empty relation: the AVET scan found the "Alice" datom, and validation
read the entity's current name.

`TestHistoryReadsDatomsDifferingOnlyByReplica` writes one value to one
entity on replica 1 and on replica 2 at one Lamport, one locally and one
imported from a dump. History
`[:find ?tx :where [?e :replicated/name "same" ?tx]]` expects two ElementIDs
and read one, `L1@R2`, the entry the resolution emitted first.

Before the fix, both failed that way on the memory, memory-trees and badger
backends, with the algebra bridge on and off.

## The fix

`matchUnboundScan` reads the matcher's mode at the arms. The cardinality-one
validating arm and the vector arm are taken in the current and as-of views
only. Under History a pattern of either shape goes to the raw scan over the
index `chooseIndex` answers, as a pattern of the flagged arms does once the
block clears its flags. For the first shape that scan is AVET over the
attribute and the value, and `unboundIterator` compares each datom's value
against the pattern's exactly (`matchesDatom`), so every datom written with
that value is read, each at its own ElementID. For the vector shape the scan
reads each element datom as written, which is what the same pattern reads
under History with the entity bound. A vector literal at the value position
names a collection, which nothing resolves under History, so there it
answers the empty relation under a vector attribute as it does under any
other.

`MatchWithConstraints` intercepts a pattern on a declared vector attribute
for RGA resolution in the current and as-of views only, and
`analyzeReuseStrategy` sets a binding's `NeedsValidation` in those views
only, so the binding route under History reads every candidate the V-primary
index finds.

The view a matcher reads is a field. `PatternMatcher` and `Database` carry
`mode`, one of `latestMode`, `historyMode` and `asOfMode`, and `asOf`, the
target of an as-of view. A reader writes `m.mode == historyMode`, and
`shouldFilterTx` reads the two fields (`docs/reference/CRDT.md`, "Three
database modes").

The vector test found a second site. On the binding route,
`compileBindingMatchPlan` and `matchesWithBindingTuple` compare a vector
literal at the value position as the collection it is, which equals no
datom's value. Before, the literal bound neither a constant nor a binding at
that slot and matched every datom of the attribute, in the current view
without the EA cache and under History.

## Tests

`datalog/db/db_test.go`, each over every backend and planner mode:

- `TestHistoryReadsASupersededValueBoundInThePattern`: after "Alice" and
  then "Alicia" on one entity, History reads the entity for the name
  "Alice", with the value written in the pattern and with it bound from an
  input.
- `TestHistoryReadsDatomsDifferingOnlyByReplica`: one value written on two
  replicas at one Lamport reads back as two ElementIDs, one per replica,
  with the value written in the pattern and with it bound from an input.
- `TestHistoryReadsAVectorAttributeAsItsElementDatoms`: under History a
  vector attribute reads as the element datoms its writer wrote, the same
  three with the entity written as a constant, unbound and bound from an
  input. A vector literal at the value position reads with the entity
  unbound or from an input what it reads with the entity written as a
  constant, and the current view resolves the vector the literal names.
- `TestAVectorLiteralEqualsNoDatomOfAScalarAttribute`: a vector literal at
  the value position of a scalar attribute reads nothing, with the entity
  written as a constant, unbound and bound from an input, with the EA cache
  and without it.

## Related

`BUG_VBOUND_PREFIX_SCAN_MATCHES_SUPERSEDED_VALUE.md`, resolved, is the
current view's side of the first arm's route. Its note records that the arm
is defended by `TestVBoundScanWithUnboundEntityRejectsSupersededValue`: in
the current view a superseded value is not read, which the candidate-and-
validate route guarantees. Under History the superseded value is read, and
the route is not taken.

`BUG_ASOF_WILDCARD_PULL_IGNORES_TEMPORAL_VIEW.md`, resolved, is the class: a
temporal handle answered from the current view on one route while another
route honoured it. Its resolution put the mode in front of the cache guards
in `matcher_relations.go`; the two arms here were reached after that guard
and before the mode was read.
