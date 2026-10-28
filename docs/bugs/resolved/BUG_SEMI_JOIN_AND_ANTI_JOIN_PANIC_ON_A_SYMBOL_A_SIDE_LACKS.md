# `SemiJoin` and `AntiJoin` keyed on a symbol a side lacked panicked, and `ExecuteAggregations` with no aggregate answered an empty relation with no error

**Status**: Resolved.

Three keyed operations mishandled a key symbol a side lacked. Two panicked,
and one answered an empty relation with no error. The hash join found a
missing key symbol and returned the lookup's error. These three did not.

## The defect

- `executor.SemiJoin` and `executor.AntiJoin` (`datalog/executor/join.go`),
  and the `SemiJoin` and `AntiJoin` methods of every relation kind, which
  delegate to them, keyed on a symbol the left side lacked or the right side
  lacked, panicked with `runtime error: index out of range [-1]`: each key's
  position was computed through `SymbolIndex` with no check, and
  `NewTupleKey` and `TupleKeyMap.GetPositions`
  (`datalog/executor/tuple_key.go`) then indexed the tuple at `-1`.
- `ExecuteAggregations` (`datalog/executor/aggregation.go`) with no
  aggregate in the find, over a symbol the relation lacked, discarded the
  projection's refusal and returned an empty relation with no error, and
  `Relation.Aggregate`, which delegates to it, answered the same way.

The or-fallback iterator's environment narrowing
(`OrFallbackIterator.buildBranchFromEACache`,
`datalog/executor/or_fallback_relation.go`) keys `SemiJoin` on the value
symbol, which the branch relation carries at its second position and the
environment's projection onto that symbol carries at its one. The relation
interface offers all three operations, so a caller of the executor's API
reached the panic and the empty answer.

## The fix

`SemiJoin` and `AntiJoin` derive both sides' key positions through
`query.SymbolIndexTable` and, when it refuses a symbol, return a result over
the left side's symbols carrying that error, as `HashJoinWithOptions` and
`SymmetricHashJoinWithOptions` do. The no-aggregate branch of
`ExecuteAggregations` returns a result over the find's variables carrying the
projection's refusal, the shape its aggregating branches use for an absent
group-by or argument symbol. Every relation kind's `SemiJoin`, `AntiJoin`
and `Aggregate` method delegates to these, so each answers the error.

## Tests

- `TestSemiAndAntiJoin_AKeySymbolASideLacksIsAnError`
  (`datalog/executor/join_missing_symbol_test.go`), beside the hash join's
  `TestHashJoin_AKeySymbolASideLacksIsAnError`: `SemiJoin`, `AntiJoin`,
  `Relation.SemiJoin` and `Relation.AntiJoin`, keyed on a symbol the right
  side lacks, one the left side lacks and one both lack, answer an error
  naming the symbol and never a panic. Keyed on a symbol both sides carry,
  the semi join answers the left tuples with a match and the anti join the
  left tuples without one, under the left side's symbols.
- `TestAggregate_AFindSymbolTheRelationLacksIsAnError`
  (`datalog/executor/aggregation_validation_test.go`), beside
  `TestAggregationRejectsAbsentSymbols`: `ExecuteAggregations` and
  `Relation.Aggregate`, over a find naming a symbol the relation lacks,
  alone, beside a symbol it carries, or beside an aggregate, answer an error
  naming the symbol. Over a find of symbols it carries they project onto
  them.
