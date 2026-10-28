# An unsigned integer was equal to a signed one under `ValuesEqual` and had no arm in `Type`, so it reached the encoder's panic

**Status**: Resolved.

The engine's equality admitted `uint64` as an integer. Its normalizer and
its encoder did not. The two disagreed on the one path a caller's `uint64`
took into the store, and that path ended in a panic.

## The three sites

`NormalizeValue` (`datalog/value_encoding.go`) canonicalized `int`, `int8`,
`int16` and `int32` to `int64`, the engine's canonical integer
representation, and a float's signed zero to `0.0`; every other value passed
through. No unsigned type was among them.

`Type` (the same file) normalizes and then switches on `string`, `int64`,
`float64`, `bool`, `time.Time`, `[]byte`, `Identity`, `Keyword`, `Symbol`,
`ElementID` and `*ElementID`; its default arm panics with
`unknown value type: %T`. No unsigned type was among them either.

`ValuesEqual` holds `uint64(5)` equal to `int64(5)`, and `typeRank` ranks
`uint64` among the integers, so the engine's comparison treated an unsigned
value as an integer the store holds.

## The path

A `uint64` written through a transaction, bound at a query's value position
from an input, or given as a builder constant passed through
`NormalizeValue` untouched. It reached `Type` through `EncodeValueBytes`
(`datalog/storage/key_encoder_binary.go`) when its key or its seek bound was
encoded, and panicked the write or the query. The write boundary and the
input boundary refused NaN alone, through `containsNaN`
(`datalog/storage/database.go`), so nothing before the encoder refused the
value.

## The fix

The encoding agrees with the equality. `NormalizeValue` takes an unsigned
integer within `int64`'s range as `int64`, as it takes the smaller signed
widths, so a value `ValuesEqual` holds equal to a stored integer encodes as
that integer. An unsigned value above `math.MaxInt64` passes through
unchanged for `ValidateValue` (`datalog`) to refuse, with an error naming
the value, never by the encoder's panic. `ValidateValue` is the domain's one
statement, and every boundary a value enters the engine by refuses through
it: a transaction's `Add`, `Set`, `Remove` and `Retract`, through
`validateValueStorable`, which puts the attribute in front of the value's
own refusal; a query's input conversion, through `convertInputValue`, which
puts the input's symbol in front; and a query's constants, through
`ValidateQuery`. `containsNaN` is gone, and NaN is `ValidateValue`'s
refusal. The renderers' side of the same domain is
`BUG_PULL_DEFAULTS_AND_BUILT_VALUES_RENDER_AS_TEXT_THE_PARSER_DOES_NOT_READ.md`.

## Tests

`TestAnUnsignedIntegerWithinRangeIsTheIntegerItEquals`
(`datalog/storage/value_domain_test.go`), over every backend and optimizer
mode: with `int64(5)` written on one entity and `uint64(5)` on another, a
query input of `uint64(5)` finds both, a pattern constant `5` finds both,
and the value written as `uint64(5)` reads back as `int64(5)`.

`TestAValueOutsideTheDomainIsRefusedWhereItEnters` (the same file), over
every backend and optimizer mode: `uint64(math.MaxInt64) + 1` and a
`float32` are each refused by `Add`, `Set`, `Remove` and `Retract` naming
the value or the type, and refused as a query input naming it.

`TestWritesRejectNaN` and `TestInputsRejectNaN`
(`datalog/storage/nan_exclusion_test.go`): the NaN refusal at a write and
at a query input still names NaN.

`TestNormalizeValueTakesAnUnsignedIntegerWithinRangeAsInt64`
(`datalog/value_domain_test.go`): every unsigned width at 5 normalizes to
`int64(5)`, `uint64(math.MaxInt64)` to its `int64`, and one above the range
passes through unchanged.
