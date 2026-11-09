# A constant value with a free attribute answers a superseded value

**Status**: Resolved. The body states the routes as filed; the Resolution at the end states the fix.

## Resolution

`matchUnboundScan` sends a constant-value pattern with the entity free and the
attribute free or cardinality-one, outside History, to `matchWithVValidation`.
With the attribute free the candidates come from VAET, and
`validatingVBoundIterator.Next` skips a vector candidate, since a scalar value
equals no collection.
`TestAValueWithAFreeAttributeAnswersWhatTheAttributeBoundPatternAnswers`
(`datalog/storage/free_attribute_value_test.go`) pins the contract on every leg
of the optimizer matrix with the EA cache on and off: the value as a constant
and as a binding, the latest view and the view as of the first commit, over
cardinality-one, cardinality-many, vector and undeclared attributes.

## What the query answers

An entity's cardinality-one attribute holds `"room"`, and a later assertion
supersedes it with `"cave"`. In the latest view, and in an as-of view after
the second assertion, `[:find ?e :where [?e ?a "room"]]` answers the entity.
Its current value is `"cave"`, so the pattern does not hold of it.

`TestAValueBoundMatchOverEveryAttributeResolvesEachCandidateUnderItsAttribute`
(`datalog/storage/resolution_definition_test.go`) reproduces it with the EA
cache on and off.

## Why

- `PatternMatcher.matchUnboundScan` (`datalog/storage/matcher_relations.go`)
  sends a constant-value pattern to `matchWithVValidation` only when the
  attribute is bound: `e == nil && a != nil && v != nil && card ==
  schema.CardinalityOne`.
- With the attribute free, the pattern falls through to the resolved scan
  `chooseIndex` picks: VAET, with the value as its prefix.
- `CRDTResolvingIterator` groups that run by entity and attribute. Each group
  holds only the datoms carrying the bound value.
- A cardinality-one group's first entry is emitted as its current value. In
  the run, that entry is the superseded assertion of the bound value.
- No candidate is checked against the entity's current value.

## The binding route validates

A value bound by a binding reaches the same scan through
`PatternMatcher.analyzeReuseStrategy`. With the attribute free, it returns a
VAET strategy with `NeedsValidation` set outside History.
`validatingVBoundIterator` then reads each candidate's own definition and
checks a cardinality-one candidate against its entity's current value.

So two sites decide whether a value-bound candidate is validated, and they
decide it by different rules.

## The fix

`matchUnboundScan` sends a constant-value pattern with the entity and the
attribute free, outside History, to `matchWithVValidation` with the strategy
the binding route builds: VAET candidates, EATV validation, no bound
attribute.

## Tests

The contract: in the latest and as-of views, `[?e ?a V]` answers an entity
and attribute exactly where `[?e a V]` with that attribute constant answers
the entity.

The cases, each with the EA cache on and off, under both optimizer modes:

- the value as a constant and as a binding
- the latest view, and an as-of view before the supersession
- a cardinality-one value since superseded, and one still held
- a cardinality-many member since withdrawn, and one still held
- an attribute the schema does not declare, which resolves as cardinality one
- a vector element
