# TupleKeyMap optimization for set membership

## The problem

A `Relation` is a set, so `StreamingRelation.Project()` deduplicates its output through `DedupIterator`. `DedupIterator` keys a `TupleKeyMap` by the whole tuple. The map stores a value beside every key. Deduplication asks one question, whether the tuple was seen, and reads no value. The value and the entry that carries it are the cost this proposal removes.

`PERFORMANCE_STATUS.md` carries the measurements. The set-semantics fix's cost is under "Known Performance Regressions (Correctness Fixes)". The single-walk insertion is entry 19, "Single-Lookup Dedup Insertion", with the gain it measured.

## What exists

`DedupIterator` (`datalog/executor/iterator_composition.go`) deduplicates in one walk per tuple:

```go
key := NewTupleKeyFull(tuple)
if !it.seen.PutIfAbsent(key, true) {
    it.current = tuple
    return true
}
```

`TupleKeyMap` (`datalog/executor/tuple_key.go`) holds each hash's first entry inline in `entries map[uint64]mapEntry`. Second and later entries sharing a hash spill into `overflow map[uint64][]mapEntry`, which stays nil until the first collision. Every lookup compares full values, so a collision costs time and never correctness.

`mapEntry` carries two fields. `values []interface{}` holds the tuple's values for the collision comparison. `value interface{}` holds the stored value. For deduplication the stored value is `true`, and nothing reads it.

For every unique tuple the map computes the FNV-1a hash over the values, looks the hash up, compares values on a collision, and stores a `mapEntry` holding the values and the `true`.

## Its tests

- `TestDedupIterator` (`datalog/executor/iterator_composition_test.go`).
- `TestSetSemantics_Projection` and `TestSetSemantics_Contract_ProjectionAlwaysDeduplicates` (`datalog/executor/set_semantics_test.go`). Every option below keeps these green, since none changes which tuples pass.

## The alternatives

### A `TupleSet` that stores no value

```go
type TupleSet struct {
    m map[uint64][]Tuple  // Just tuples, no mapEntry wrapper
}

func (s *TupleSet) Add(tuple Tuple) bool {
    hash := hashValues(tuple)
    for _, existing := range s.m[hash] {
        if tuplesEqual(tuple, existing) {
            return false  // Already exists
        }
    }
    s.m[hash] = append(s.m[hash], tuple)
    return true  // Was added
}

func (s *TupleSet) Contains(tuple Tuple) bool {
    hash := hashValues(tuple)
    for _, existing := range s.m[hash] {
        if tuplesEqual(tuple, existing) {
            return true
        }
    }
    return false
}
```

It removes the `mapEntry` wrapper and the `value` field per unique tuple. It changes no behaviour. Its cost is a second keyed type beside `TupleKeyMap`, with the same hashing to keep in step. The sketch above keys a bucket slice per hash. `TupleKeyMap` has since moved to an inline first entry with an overflow map, and a `TupleSet` takes the same layout.

### An interned-value fast path

A projection to one symbol of an interned type (`*Identity`, `*Keyword`) can key a native Go map by the pointer:

```go
type SingleValueSet struct {
    m map[interface{}]struct{}
}

func (s *SingleValueSet) Add(val interface{}) bool {
    if _, exists := s.m[val]; exists {
        return false
    }
    s.m[val] = struct{}{}
    return true
}
```

It avoids hashing altogether on that shape. Its cost is detecting the projection's symbol type, which the iterator does not know today.

### An adaptive strategy

```go
func NewDedupIterator(source Iterator, symbols []Symbol) Iterator {
    if len(symbols) == 1 && isInternedType(symbols[0]) {
        return &SingleSymbolDedupIterator{...}
    }
    if expectedCardinality < 100 {
        return &LinearScanDedupIterator{...}  // O(n²) but cache-friendly
    }
    return &HashDedupIterator{...}  // Current approach with TupleSet
}
```

It picks the representation by the projection's shape. Its cost is three iterator types and a cardinality estimate the planner does not carry.

### A probabilistic set

A Bloom filter answers membership approximately. A false positive drops a valid tuple, so the relation is no longer the set the query defines. That cost excludes it.

## What decides it

- Each alternative is measured against the deduplicating implementation, never against the pre-fix one that returned duplicates. The gain is a smaller correctness tax, not its removal.
- The `TupleSet` changes no behaviour and needs no new information, so its measurement comes first. The fast path pays off only where the single-symbol projection is the shape that remains slow after it. The adaptive strategy pays off only where a profile shows the cardinality split it keys on.
