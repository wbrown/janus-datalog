# A struct read from a pull left a nested struct's `db/id` field nil

**Status**: Resolved.

The reflect reader filled a top-level struct's `db/id`-tagged field on
`PullInto` and `PullIntoMany`. A struct nested under it, reached through a
ref, a slice of refs, or a pull map that carried the nested entity's
`db/id`, was filled with its attributes, and its id field stayed nil.

## What was observed

The cases ran on every backend, in both optimizer modes, with the cache on
and off, and with and without a schema.

- `PullInto` of a struct holding a `*T` ref field: the nested `T`'s id was
  nil, where the pulled entity was `b`.
- `PullIntoMany` of the same shape: nil.
- `PullInto` of a struct holding a `[]*T` field over a cardinality-many ref:
  `Refs[0].ID` was nil, where the pulled entity was `c`.
- `reflect.ReadStruct` over a `Pull` map whose nested map held `db/id`: the
  nested struct's id was nil, where the map held `b`.

## The mechanism

`ReadStructWithID` (`datalog/reflect`) reads the struct through
`StructReader.Read` from the pull result and then sets the id field through
`SetIDField` from the entity the caller passed. That ran once, at the top.
A nested struct was filled by the reader from the nested map, and the
reader set no id on it, whether the nested map carried a `db/id` or not,
as the `ReadStruct` case showed. The map a `PullInto` produced for a nested
struct carried no `db/id` either: `generatePullPatternRecursive` lists a
struct's id field apart from its attribute fields and named the attribute
fields alone.

## The fix

`generatePullPatternRecursive` names `:db/id` in the pattern of a nested
struct with an id field, so the nested map carries the entity's id. The
top level's pattern does not name it; the top-level id keeps its source,
the entity the caller passed. `readIntoStruct` fills a struct's id field
from the map's `db/id` when the map carries it, at every depth, so a
`ReadStruct` over a map that carries nested ids fills them too.

The fixture, which saves a struct holding a nested struct pointer through
`SaveStruct`, exposed a second defect: `extractSingleValue`'s nested-struct
arm dereferenced the nested id field when its kind was pointer, and
`datalog.Identity` is the interned pointer type, so the dereference
stripped the type the assertion then looked for and every such save failed
with "ID field is not Identity". The arm reads the field's `Identity` as it
is, and reports a nil id as before.

## Tests

`TestANestedStructReadFromAPullCarriesItsEntitysID`
(`datalog/reflect/nested_id_test.go`), on every backend, in both optimizer
modes, with a schema and without one: a struct saved with a peer under a
ref and two peers under a many ref is pulled with `PullInto` and
`PullIntoMany`, and the peer and each of the two carry the identity of the
entity pulled for them.

`TestReadStructFillsANestedStructsIDFromItsMap` (the same file): a struct
read from a map whose nested map carries `db/id` holds that identity in
the nested struct's id field.
