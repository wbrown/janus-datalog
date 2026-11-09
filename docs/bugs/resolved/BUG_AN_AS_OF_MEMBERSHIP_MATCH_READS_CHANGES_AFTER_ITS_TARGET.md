# An as-of membership match reads changes after its target

**Status**: Resolved. The body states the routes as filed; the Resolution at the end states the fix.

## Resolution

`cardinalityManyAVETValueIterator.Next` and `checkSetMembership` skip a datom
`shouldFilterTx` filters, as every other resolver does.
`TestAnAsOfMembershipMatchAnswersTheMembershipAsOfItsTarget`
(`datalog/storage/as_of_membership_test.go`) pins the contract on every leg of
the optimizer matrix with the EA cache on and off: a member held throughout,
one withdrawn after the first commit and one added after it, with the entity
free and bound through an `#id` literal, in the latest view and the view as of
the first commit.

## What the query answers

An entity's cardinality-many attribute holds the member `"x"`, and a later
assertion withdraws it. As of a point before the withdrawal, `[:find ?e
:where [?e :item/many "x"]]` does not answer the entity. As of that point,
`"x"` is a member.

`TestAValueWithAFreeAttributeAnswersWhatTheAttributeBoundPatternAnswers`
(`datalog/storage/free_attribute_value_test.go`) shows it with the EA cache
on and off. As of the first commit, `[?e ?a "x"]` answers the entity under
`:item/many`, and `[?e :item/many "x"]` does not.

## Why

An as-of view holds the datoms at or before its target. Every resolver of a
current value skips the others through `PatternMatcher.shouldFilterTx`. Two
cardinality-many membership resolvers do not:

- `cardinalityManyAVETValueIterator.Next` (`datalog/storage/matcher_relations.go`)
  reads every datom of its AVET run into its add-wins state.
  `matchCardinalityManyFindEntitiesWithValue` builds it for a pattern whose
  entity is free and whose attribute and value are bound, with the cache on
  and off.
- `PatternMatcher.checkSetMembership` (`datalog/storage/set_resolution.go`)
  reads every datom of its EAVT run. `matchCardinalityManyMembership` calls it
  for a pattern whose entity, attribute and value are all bound. With the
  cache on, `matchFromCache` answers that pattern before it.

So a withdrawal after the target hides a member the view holds, and an add
after the target answers a member the view does not hold.

## The other resolvers

Every other site that resolves a current value from a scan applies the filter,
or resolves through one that does:

- `ResolveLWW` and the unique walk
- `CRDTResolvingIterator`, which the hash join, the non-reusing route, the
  value-bound candidate scan and the resolved scan wrap their scans in
- `resolveAddWinsSet`, `resolveVector`, `PrefetchEntities` and
  `resolveWildcardDatoms`

`WarmCache`, `matchVectorScanAllEntities` and
`matchCardinalityManyScanAllEntities` scan only to find entities, and resolve
each through a filtered resolver.

## The fix

`cardinalityManyAVETValueIterator.Next` and `checkSetMembership` skip a datom
`shouldFilterTx` filters, as every other resolver does.

## Tests

The contract: in an as-of view, a cardinality-many value match answers the
membership as of the view's target.

The cases, each with the EA cache on and off, under both optimizer modes:

- the entity free, and the entity bound
- a member withdrawn after the target, which the view holds
- a member added after the target, which the view does not hold
- the latest view, which holds both changes
