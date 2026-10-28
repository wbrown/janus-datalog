# A 33-byte attribute keyword at a read entry point was truncated to 32 bytes and aliased the 32-byte attribute

**Status**: Resolved.

A write refuses an attribute keyword wider than the store holds.
`Transaction.Add` (`datalog/storage`) runs `validateAttributeStorable`,
which runs `datalog.ValidateAttributeWidth`. Its doc states why: the storage
attribute is a fixed 32-byte array, so a longer name would be truncated and
alias the attributes sharing its first 32 bytes. No read entry point ran
that check. The keyword was copied into the 32-byte attribute, and the read
went to whatever attribute those bytes name.

## What each read route answered

The store held `:t/aaaaaaaaaaaaaaaaaaaaaaaaaaaaa` (32 bytes) with the value
`short`, and each route was asked for `:t/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaz`
(33 bytes), on every backend, in both optimizer modes, with the cache on
and off.

- `Pull`, `PullMany`, `(pull …)` in a `:find`, a `PullInto` or `PullIntoMany`
  struct tag, a nested pull under a 32-byte ref, an `AsOf` pull: the 32-byte
  attribute's value, `short`.
- `get-else`, `get-some`, an `AsOf` query's `get-else`: `short`.
- `missing?`: the entity was not missing.
- `ResolveEntityAttributes`: `short` under the 33-byte name.
- A text constant, a builder constant, a keyword input at the attribute
  position, a `History` query's constant, a rule body's pattern: no tuple,
  and no refusal.
- `GetString`, `GetInt`, `GetRef`, `GetStrings`: the 32-byte attribute's
  value with `found` true under the cache, and not found with it off.
- `GetVectorNth`, `GetVectorLength`: `attribute … is not a vector`, naming
  the 33-byte keyword, instead of the width refusal.

## The mechanism

The read paths build their storage datom through `ToStorageDatom`
(`datalog/storage`). It copies the keyword's text into the 32-byte attribute
array, which drops the 33rd byte. The `LookupAttribute` cache key and the
`ResolveLWW` prefix read the truncated attribute. The scan prefixes are
encoded from it. `matchesDatom` then compares each datom's attribute with
the constant the query holds and matches none. `GetVectorNth` and
`GetVectorLength` copy the attribute into the array directly.

## The fix

`ValidateAttributeWidth` runs once where a keyword enters a read, and never
on a match, an encode or a lookup: `ToStorageDatom`, `LookupAttribute`, the
cache key and the scan prefixes are untouched.

- `DataPattern.Validate` (`datalog/query/clause_scope.go`) refuses a
  constant attribute keyword wider than storage holds, so `ValidateQuery`
  refuses it for a text, a built and a hand-made query, for an inner query
  through `SubqueryPattern.Validate`, and for a rule body through
  `NewRuleSet`.
- `Expression.Validate` and `DatabaseFunctionPredicate.Validate` refuse the
  attribute of `get-else`, `get-some` and `missing?` through
  `validateDatabaseFunctionAttributes`, which enumerates the database
  functions as the roster does.
- `PullPattern.Validate` (`datalog/query/pull.go`) refuses an attribute the
  pattern names, through its map specs, over `PullPatternAttributes`.
  `ValidateQuery` runs it for a find pull, and `Database.pullMatcher`
  (`datalog/storage/database.go`) runs it for `Pull`, `PullMany`, `PullInto`
  and `PullIntoMany`, through a temporal handle too.
- `convertInputValue` (`datalog/storage/database.go`) refuses a keyword
  bound into an attribute position that is wider than storage holds, once
  per input value. The typed accessors `GetString`, `GetInt`, `GetFloat`,
  `GetBool`, `GetRef` and `GetStrings` read through one shared query that
  binds the attribute as an input, so they are refused there.
- `GetVectorNth`, `GetVectorLength` and `ResolveEntityAttributes` refuse at
  their entry, beside the unknown-attribute check.

Each refusal is the width rule's one message, `attribute … is N bytes,
exceeds the 32-byte storage limit`.

## Tests

`TestValidateQueryRefusesAnAttributeWiderThanStorageHolds`
(`datalog/query/attribute_width_test.go`):

- a constant attribute, a `get-else`, a `get-some` and a `missing?`
  attribute, each refused by `ValidateQuery`, by `Prepare`, in an inner
  query and in a rule body;
- a pull attribute, one under a map spec and a pull default's attribute,
  each refused by `ValidateQuery` and `Prepare`;
- the keyword exactly as wide as storage holds, accepted at each position.

`TestBuild_RefusesAnAttributeWiderThanStorageHolds`
(`datalog/qb/attribute_width_test.go`):

- `Build` refuses a pattern's constant attribute and a `get-else`'s
  attribute;
- `Build` accepts the keyword at the limit.

`TestAnAttributeWiderThanStorageHoldsIsRefusedAtEveryReadEntry`
(`datalog/storage/attribute_width_read_test.go`): every route above, on
every backend, in both optimizer modes, with the cache on and off, refuses
the 33-byte keyword with the width rule's message and answers the 32-byte
keyword without error.
