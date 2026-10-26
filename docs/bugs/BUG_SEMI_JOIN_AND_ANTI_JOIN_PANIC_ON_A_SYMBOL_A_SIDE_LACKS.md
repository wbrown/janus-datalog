# `SemiJoin` and `AntiJoin` keyed on a symbol a side lacks panic, and `ExecuteAggregations` with no aggregate answers an empty relation with no error

**Status**: Open.

Three keyed operations the query language does not route to today mishandle
a key symbol a side lacks. Two of them panic, and one answers an empty
relation with no error. The hash join finds a missing key symbol and returns
the lookup's error. These three do not.

## What is observed

- `executor.SemiJoin` and `executor.AntiJoin` (`datalog/executor/join.go`),
  and the `SemiJoin` and `AntiJoin` methods of `MaterializedRelation`, keyed
  on a symbol the left side lacks or the right side lacks, panic with
  `runtime error: index out of range [-1]`: the key's position is computed
  with no check, and `TupleKeyMap.GetPositions` indexes the tuple through
  `hashTuplePositions` (`datalog/executor/tuple_key.go`) at `-1`.
- `ExecuteAggregations` with no aggregate in the find, over a symbol the
  relation lacks, and `Relation.Aggregate` the same way, return an empty
  relation and a nil error.

The query language reaches neither join. The executor's forms run on the
windowed primitive and the hash join, and no production caller invokes
`SemiJoin` or `AntiJoin`. The relation interface still offers them, so a
caller of the executor's API reaches the panic.

## The fix

Every keyed operation derives its positions through the one required
lookup, `query.SymbolIndexTable`. It returns that lookup's error for a
symbol a side lacks, as `HashJoin`, the projection, the union and the rename
do. A find over a symbol the relation lacks is an error at the aggregation
boundary too, never an empty relation.

## Tests

A test for the fix expects each of the eight join entries, keyed on an
absent symbol, to return an error naming it. It expects
`ExecuteAggregations` and `Relation.Aggregate`, over an absent find symbol,
to return an error naming it.
