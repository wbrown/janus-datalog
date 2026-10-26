# Whether a tuple must be copied is the iterator's fact, and the relations answer it as a constant

**Status**: Open.

No answer is wrong. The constant is the worst case, so every reader that
retains a tuple copies it, and no reader holds a slice an iterator later
overwrites. The cost is one allocation per tuple, paid for a fact nobody
checks, on every streaming relation, including those over iterators that
hand out stable tuples.

## The fact and where it lives

An iterator's `Tuple()` either hands back a slice the next `Next()` overwrites
or hands back a slice the caller may keep. That is a property of the
iterator: the workspace-reusing iterators overwrite, the materialized ones
do not.

The `Iterator` interface (`datalog/executor`) carries no such fact. The
relations answer the question instead, through `RequiresCopy`, for iterators
they were never told about:

- `StreamingRelation.RequiresCopy` returns `true` for every instance, its doc
  saying the relation wraps iterators that may reuse workspace memory.
- `OrFallbackRelation.RequiresCopy` returns `false` for its own iterator the
  same way.
- `MaterializedRelation.Rename` copies the fact into a `requiresCopy` field
  from its source at construction, a second copy of a fact that belongs to
  the iterator underneath.

## The cost

Every retaining reader asks the relation and copies when it says so: the
window reader, the kept branch's collect, the derived set's absorb, the
limit, `collectTuplesInto` and the lazy-seq wrap. Under the constant every
one of them copies every tuple of every streaming relation, one allocation
per tuple, whether or not the iterator underneath would have overwritten it.

On `BenchmarkComplexQueryCheckpoint` (`datalog/storage`, badger) the cost
is small. The allocation profile of one run attributes 54,615 of
19,236,644 sampled objects to `copyTuple`, 0.28%, the fixture's writes
among the total. Every one of them is under the windowed iterator's branch
collection, `windowedIterator.buildBranch` through `collectBranch`, and
38,231 of them under `windowReader.next`. The benchmark read 21.4 ms/op,
23.09 MB/op and 203,474 allocs/op on that run, on the tree at `8565cdd` on
an Apple M5. What the copy costs on a query whose retaining reader is the
limit, the derived set's absorb or the lazy-seq wrap is not measured.

## The fix

The `Iterator` interface states the contract. Each implementation answers it
from what its own `Tuple()` does. Every relation delegates to its iterator.
The `requiresCopy` field the rename sets goes, since it is that fact copied
at construction.

## Tests

A benchmark runs a retaining reader over a streaming relation whose
iterator hands out stable tuples, and expects the delegation to take its
per-tuple allocation to zero. A test beside it expects a reader over a
workspace-reusing iterator to still copy.
