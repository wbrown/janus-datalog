# A 33-byte attribute keyword at a read entry point is truncated to 32 bytes and aliases the 32-byte attribute

**Status**: Open.

A write refuses an attribute keyword wider than the store holds.
`Transaction.Add` (`datalog/storage`) runs `validateAttributeStorable`,
which runs `datalog.ValidateAttributeWidth`. Its doc states why: the storage
attribute is a fixed 32-byte array, so a longer name would be truncated and
alias the attributes sharing its first 32 bytes. No read entry point runs
that check. The keyword is copied into the 32-byte attribute, and the read
goes to whatever attribute those bytes name.

## What each read route answers

The store holds `:t/aaaaaaaaaaaaaaaaaaaaaaaaaaaaa` (32 bytes) with the value
`short`, and each route is asked for `:t/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaz`
(33 bytes). The cases run on every backend, in both optimizer modes, with
the cache on and off.

- `Pull`, `PullMany`, `(pull …)` in a `:find`, a `PullInto` or `PullIntoMany`
  struct tag, a nested pull under a 32-byte ref, an `AsOf` pull: the 32-byte
  attribute's value, `short`.
- `get-else`, `get-some`, an `AsOf` query's `get-else`: `short`.
- `missing?`: the entity is not missing.
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

`ValidateAttributeWidth` runs at every read entry. Those are the query
parser and the builder for a constant attribute, the pull-pattern parser,
the struct-tag reader, the typed accessors, `ResolveEntityAttributes`, and
the executor's entry for a keyword an input binds into an attribute
position. Each refuses with the width message, `exceeds the 32-byte storage
limit`, before any key is built. No read then reaches the copy.

## Tests

A test for the fix asks each route above for the 33-byte keyword and
expects the width refusal on every backend, in both modes, with the cache
on and off. Asked for the 32-byte keyword, each route is expected to answer
`short`.
