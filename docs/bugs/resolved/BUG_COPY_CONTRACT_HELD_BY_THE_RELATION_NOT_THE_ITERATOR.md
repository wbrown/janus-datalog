# Whether a tuple must be copied is the iterator's fact, and the relations answer it as a constant

**Status**: Resolved.

A reader that retains a tuple copies it when the iterator's next pull would
overwrite it. A relation that answers that question as a constant makes
every retaining reader copy every tuple, one allocation per tuple, whether
or not the iterator underneath would overwrite it. No answer is wrong under
the constant, since it is the worst case; the cost is the allocation.

## The fact and where it lives

An iterator's `Tuple()` either hands back a slice the next `Next()` overwrites
or hands back a slice the caller may keep. That is a property of the
iterator: the workspace-reusing iterators overwrite, the materialized ones
do not.

A relation asked the question answers for the iterator it hands out. A
relation that answers as a constant answers for iterators it was never
told about: a streaming relation that says true wraps iterators that hand
out stable tuples as often as ones that reuse a workspace.

## The cost

Every retaining reader asks the relation and copies when it says so: the
window reader, the kept branch's collect, the derived set's absorb, the
limit, `collectTuplesInto` and the lazy-seq wrap. Under a constant true
every one of them copies every tuple of every streaming relation. What the
copy costs on a query whose retaining reader is the limit, the derived
set's absorb or the lazy-seq wrap is not measured.

## The fix

The `Iterator` interface states the contract: `RequiresCopy` reports whether
the slice `Tuple` hands back is one the next `Next` overwrites. Each
implementation answers from what its own `Tuple` does:

- An iterator that fills one workspace per pull answers true: the storage
  matcher's unbound, hash-join, merge-join and per-binding iterators.
- An iterator that builds a tuple per pull, or reads from a slice it keeps,
  answers false.
- The counting, filtering, selecting and session-bound wrappers answer their
  source's.
- The function evaluator answers its source's when it unifies and false when
  it extends.
- The dedup iterator answers false when it copies each admitted tuple and
  its source's otherwise.
- The invocation iterator answers its inner's when it yields the source's
  tuple as it is and false when it reads out the kept positions.

`StreamingRelation.RequiresCopy` is the answer of the iterator its
`Iterator` hands out: false once the relation caches, the wrapped iterator's
otherwise. Each other relation mints one iterator type and answers that
type's answer. The readers keep asking the relation.

## Tests

`TestAStreamingRelationAnswersTheCopyContractFromItsIterator`
(`datalog/executor/copy_contract_test.go`):

- A streaming relation over an iterator handing out stable tuples answers
  false, and `collectTuplesInto` over it keeps the iterator's own slices and
  allocates no more over three hundred tuples than over three.
- Over a workspace-reusing iterator it answers true, and the collected
  tuples are copies.
- After `Materialize` it answers false, and every read hands out the cache's
  own tuples.

`TestAQueryResultAnswersTheCopyContractFromItsIterator`
(`datalog/storage/copy_contract_test.go`), over every backend and planner
mode: for an unbound scan, a scan driven by a collection input, a bound
value on a cardinality-one attribute, a cardinality-many attribute with the
entity unbound, a projection, a sorted result and an aggregate, the result's
answer equals whether the slice its iterator hands back after one pull is
the slice it hands back after the next.
