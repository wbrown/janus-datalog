# A failed tuple raises at an operator that does not carry it

**Status**: Open. A windowed form's iterator answers the output it answered for a failed window tuple as its failed tuple (`windowedIterator.failedTuple`, `resume`), so a windowed form over another form's result decides the tuple at its key. A hash join of several groups and a stream marked for caching answer no failed tuple.

A windowed form decides a failed tuple of its subject at the tuple's key. It raises the failure where it answers an output for the tuple, and drops the tuple where it answers none (`docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md`, T13). An operator between the failing evaluation and the form that answers no failed tuple ends the form's reader with the failure, though the form may hold at no assignment that extends the tuple. The failure contract has the query answer there.

## The route

1. The iterators that answer a failed tuple are those `failedTupleIterator` names (`datalog/executor/iterator_composition.go`; P13):
   - `PredicateFilterIterator` and `FunctionEvaluatorIterator`, which answer the tuple their own failure arose on and resume past it
   - `CountingIterator`, which answers the failed tuple of the iterator it counts
   - `windowedIterator`, which answers the output it answered for a failed window tuple, with the positions the window tuple holds no value at (`failedTuple`, `windowReader.unknownAt`), and which the subquery's, the not-join's and the fallback's iterators embed
   - `patternJoinIterator`, which answers its windowed join's (`datalog/executor/bound_pattern.go`)
2. A form's window reader takes a failed tuple holding a value at each position the form reads into its window, marked with its failure, and resumes its subject (`windowReader.carriedFailure`). Any other failure of its subject ends the reader with that failure (P13).
3. `hashJoinIterator` and `CachingIterator` answer no failed tuple. So where a form's subject is a join of several groups or a stream marked for caching, over a failing evaluation, the form's reader ends with the failure, and the form never runs at the failed tuple's key.

## Contracts it contradicts

- The failure contract (`PULL_BASED_HASH_JOIN_PROOF.md` §1): an evaluation failure fails the query only at an assignment where every other clause holds. Where the form holds at no assignment extending the failed tuple, the failure fails no query.
- T13 states a form's decision for a subject whose iterator answers the failed tuple. The proof's §7 lists a subject that is a join of several groups or a caching stream as outside it.

## Reach

- Any windowed form whose subject passes a failed tuple through a hash join of several groups or a caching stream, where the form holds at no assignment extending the tuple.
- The windowed forms are the subquery, which also serves the bound pattern and the bound invocation, the not-join and the fallback (P13).

## What was not established

- A failed tuple through a hash join of several groups or a caching stream into a form. That rests on P13 and §7, and no reproduction runs it.

## The fix, if taken

- `hashJoinIterator` and `CachingIterator` answer the failed tuples of their probe side and their source, as the windowed forms do.
- A probe tuple that holds no value at a key position matches every build tuple agreeing at the key's other positions.

## Reproducer

`TestAFormOverAnotherFormsOutputDecidesAFailedTupleAtItsKey` (`datalog/storage/failure_contract_reach_test.go`) asserts the contract for a windowed form over another form's result, under `algebra_on` and `algebra_off`. Each query is `[:find ?q :where [?q :item/v ?a] [(+ ?a 1) ?b] <first> <second>]`, so `(+ ?a 1)` fails at the one item whose `:item/v` is a string. Its cases:

- A first form holds at the failing tuple: a not-join, a subquery, an or-join, an or-default-join or a pattern.
- With a second subquery, not-join or pattern holding nowhere there, the test asserts the sixteen numeric items and no error.
- With a second clause holding there, the test asserts the failure as the query's error.

The open half above has no reproducer.

## Related

- `PULL_BASED_HASH_JOIN_PROOF.md` §7 lists the hash join and the caching stream among what the proof does not cover.
- `BUG_OPTIMIZER_PATHS_ANSWER_DIFFERENTLY_WHEN_A_CORRELATED_SUBQUERY_FAILS_ON_A_VALUE.md` is the failure contract's other recorded departure.
