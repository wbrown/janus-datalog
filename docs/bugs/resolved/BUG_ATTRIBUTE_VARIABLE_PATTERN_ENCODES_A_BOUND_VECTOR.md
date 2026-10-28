# `[?e ?a ?v]` with `?v` bound to a vector panicked in the encoder

**Status**: Resolved.

`[:find ?e :in $ ?v :where [?e ?a ?v]]` given a vector for `?v` panicked
with `unknown value type: []interface {}` on every backend and in both
optimizer modes. `[:find ?e :where [?e ?a [1 2]]]`, the same vector written
into the pattern, panicked the same way.

## The route

A data pattern with a constant attribute and a vector bound at its value
position was filtered at the entry of the binding-driven match, before any
index was addressed, and answered nothing. The attribute-variable pattern
took a different route, through `validatingVBoundIterator`
(`datalog/storage/matcher_relations.go`), which put the bound value into the
scan bound and handed it to the backend. The written vector under an open
attribute took the unbound scan, whose index choice put the vector into a
VAET prefix the same way.

On the memory backend, `openCRDTScan` opened the scan through `OpenKeyScan`.
`EncodeScanBound`, `encodeBoundComponent` and `encodeValueForSearch` encoded
the bound, and `EncodeValueBytes` (`datalog/storage/key_encoder_binary.go`)
asked `datalog.Type` for the slice's type. `Type` has no case for a slice
and panics.

On memory-trees, the iterator's `Next` sought the tree cursor. The seek
compared the bound with each node through `comparePrefixInOrder`,
`compareComponent` and `CompareValuesTagOrder`, which asks `datalog.Type` the
same way and panics the same way.

## The fact

A vector at the value position names a declared vector attribute's whole
collection, which resolution reconstructs from the collection's element
datoms, each its own datom in the index. Whether the vector matches depends
on the attribute: under a declared vector attribute it matches the entities
whose collection equals it, and under any other attribute it equals no
datom. A pattern bound from a relation answers the tuples the unbound
pattern answers that agree with the binding at the bound positions, so with
the attribute open the vector matches every entity whose collection under
some declared vector attribute equals it.

## The fix

The entry selection of the binding-driven match, `typedPositionBindingCheck`
(`datalog/storage/matcher.go`), drops a vector at the value position under a
constant attribute, where the vector equals no datom the selection's routes
read: outside History a pattern on a vector attribute is intercepted before
the selection by the vector arm of `MatchWithConstraints`, and under History
a datom is read as written, an element and never a collection. Under a
variable attribute the tuple passes, and the attribute decides downstream.

`matchVectorUnderOpenAttribute` (`datalog/storage/matcher_relations.go`)
answers a vector under an open attribute: for every attribute the schema
declares a vector, `matchVectorScanAllEntities` resolves each entity's
collection, the pattern's entity alone when it writes one, and compares it
with the vector, and the answers form one relation. A matcher with no schema
declares no vector attribute and answers nothing. `validatingVBoundIterator`
asks it for a vector-valued binding in place of a candidate scan and drains
the answers before the next binding, and `matchUnboundScan` asks it for a
written vector under an open attribute. Under History nothing resolves and a
vector equals no datom as written, so both answer the empty relation there.

The hash and merge joins compare a vector bound beside an entity or an
attribute against the collection resolution produced, through
`matchesDatom` and value equality, which reads a resolved collection and the
vector it was written from as one value.

## Tests

`TestABoundPatternAnswersWhatTheUnboundPatternAnswersUnderTheBinding`
(`datalog/storage/vector_binding_test.go`), in both optimizer modes, in the
current view and under History, over x and y carrying a long at `:r` and a
unique string at `:code` and t carrying the vector `[1 2]` at `:tags`,
declared a vector of longs. Each case's expected answer is the unbound
pattern's answer filtered in Go at the bound positions and projected; a case
binds its positions from inputs, or writes some into the pattern as
literals:

- the vector bound with the attribute open;
- the vector bound under `:r`, and under `:tags`;
- the vector bound on x, and on t, with the attribute open;
- t bound with the value open;
- a relation binding the vector beside a scalar at `:r`;
- a relation binding the vector under `:r` and under `:tags`;
- a stored scalar bound;
- the vector written with the attribute open, under `:r`, and under `:tags`;
- x, and t, written beside the vector bound, and beside the vector written;
- x, and t, written under the written `:tags` with the vector bound;
- x, and y, written beside a scalar bound, with the attribute open and under
  the written `:r`;
- x written under the written unique `:code` with y's code bound, and with
  its own.
