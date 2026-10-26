# `[?e ?a ?v]` with `?v` bound to a vector panics in the encoder

**Status**: Open.

`[:find ?e :in $ ?v :where [?e ?a ?v]]` given a vector for `?v` panics with
`unknown value type: []interface {}` on every backend and in both optimizer
modes.

## The route

A data pattern with a constant attribute and a vector bound at its value
position is filtered at the entry of the binding-driven match, before any
index is addressed. It answers nothing. The attribute-variable pattern takes
a different route, through `validatingVBoundIterator`
(`datalog/storage/matcher_relations.go`), which puts the bound value into
the scan bound and hands it to the backend.

On the memory backend, `openCRDTScan` opens the scan through `OpenKeyScan`.
`EncodeScanBound`, `encodeBoundComponent` and `encodeValueForSearch` encode
the bound, and `EncodeValueBytes` (`datalog/storage/key_encoder_binary.go`)
asks `datalog.Type` for the slice's type. `Type` has no case for a slice and
panics.

On memory-trees, the iterator's `Next` seeks the tree cursor. The seek
compares the bound with each node through `comparePrefixInOrder`,
`compareComponent` and `CompareValuesTagOrder`, which asks `datalog.Type`
the same way and panics the same way.

## The fix

The typed-position filter runs where `validatingVBoundIterator` builds its
scan bound, before any backend reads the bound. A bound value the index
cannot hold at that position answers no tuple for that binding. Neither the
encoder nor the tree cursor is asked to compare it. One site decides which
values a position can carry, as the constant-attribute route already does.

## Tests

The query above, given a vector, answers no tuple on every backend and in
both modes. The same query, given a stored value, answers the entities that
carry it.
