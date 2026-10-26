# An unsigned integer is equal to a signed one under `ValuesEqual` and has no arm in `Type`, so it reaches the encoder's panic

**Status**: Open.

The engine's equality admits `uint64` as an integer. Its normalizer and its
encoder do not. The two disagree on the one path a caller's `uint64` takes
into the store, and that path ends in a panic.

## The three sites

`NormalizeValue` (`datalog/value_encoding.go`) canonicalizes `int`, `int8`,
`int16` and `int32` to `int64`, the engine's canonical integer
representation, and a float's signed zero to `0.0`; every other value passes
through. No unsigned type is among them.

`Type` (the same file) normalizes and then switches on `string`, `int64`,
`float64`, `bool`, `time.Time`, `[]byte`, `Identity`, `Keyword`, `Symbol`,
`ElementID` and `*ElementID`; its default arm panics with
`unknown value type: %T`. No unsigned type is among them either.

`ValuesEqual` holds `uint64(5)` equal to `int64(5)`, and `typeRank` ranks
`uint64` among the integers, so the engine's comparison treats an unsigned
value as an integer the store holds.

## The path

A `uint64` written through a transaction, bound at a query's value position
from an input, or given as a builder constant passes through
`NormalizeValue` untouched. It reaches `Type` through `EncodeValueBytes`
(`datalog/storage/key_encoder_binary.go`) when its key or its seek bound is
encoded, and panics the write or the query. No run has carried the case.
The path is read from the switch.

## The fix

The encoding agrees with the equality. `NormalizeValue` takes an unsigned
integer within `int64`'s range as `int64`, as it takes the smaller signed
integers, so a value `ValuesEqual` already holds equal to a stored integer
encodes as that integer. An unsigned value above `math.MaxInt64` is outside
the domain and is refused where values enter the engine, at the transaction's
`Add` and at a query's input conversion, with an error naming the value,
never by the encoder's panic.

## Tests

A write and a query input of `uint64(5)` find the entity holding
`int64(5)`. A write of `uint64(math.MaxInt64) + 1` is refused at `Add`.
