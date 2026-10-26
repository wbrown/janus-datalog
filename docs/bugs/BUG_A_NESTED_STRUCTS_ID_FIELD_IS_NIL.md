# A struct read from a pull leaves a nested struct's `db/id` field nil

**Status**: Open.

The reflect reader fills a top-level struct's `db/id`-tagged field on
`PullInto` and `PullIntoMany`. A struct nested under it, reached through a
ref, a slice of refs, or a pull map that carries the nested entity's
`db/id`, is filled with its attributes, and its id field stays nil.

## What is observed

The cases run on every backend, in both optimizer modes, with the cache on
and off, and with and without a schema.

- `PullInto` of a struct holding a `*T` ref field: the nested `T`'s id is
  nil, where the pulled entity is `b`.
- `PullIntoMany` of the same shape: nil.
- `PullInto` of a struct holding a `[]*T` field over a cardinality-many ref:
  `Refs[0].ID` is nil, where the pulled entity is `c`.
- `reflect.ReadStruct` over a `Pull` map whose nested map holds `db/id`: the
  nested struct's id is nil, where the map holds `b`.

## The mechanism

`ReadStructWithID` (`datalog/reflect`) reads the struct through
`StructReader.Read` from the pull result and then sets the id field through
`SetIDField` from the entity the caller passed. That runs once, at the top.
A nested struct is filled by the reader from the nested map, and the reader
sets no id on it, whether the nested map carries a `db/id` or not, as the
`ReadStruct` case shows.

## The fix

The reader fills a nested struct's id field from the nested map's `db/id`,
at every depth. The pull pattern the reflect package generates for a nested
struct names `:db/id`, so the map carries it. The top-level id keeps its
source, the entity the caller passed. Where the top-level map carries a
`db/id` too, the two agree.

## Tests

A test for the fix runs each shape above and expects the nested entity's
identity in the nested struct's id field, on every backend, in both modes,
with and without a schema.
