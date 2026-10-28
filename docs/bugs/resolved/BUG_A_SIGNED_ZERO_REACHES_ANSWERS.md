# A signed zero reached answers through a text literal, built-in arithmetic and the EDN import

**Status**: Resolved.

The engine's equality holds `-0.0` and `0.0` to be one value.
`datalog.NormalizeValue` takes a float's signed zero as `0.0`, and
`Transaction.Add` runs it on every value it writes. Three entry points did
not run it. A `-0.0` that entered through one of them reached an answer,
where `str` rendered it `"-0"` and a bitwise comparison found two values.

## What was observed

The cases ran on every backend and in both optimizer modes.

- A text literal `-0.0` ground into the find answered `-0`, and `(str ?z)`
  of it answered `"-0"`.
- `(* -1.0 ?a)`, `(- ?a)` and `(/ ?a -1.0)` over a stored `0.0` answered
  `-0`.
- An EDN import of `-0.0` read back `-0` in a pattern, a pull and `str` on
  memory-trees, in both modes, with the cache on and off. It read back `0`
  on memory and badger, whose value encoding carries one zero.

## The mechanism

`parsePatternElement` (`datalog/parser`) read a float literal with
`strconv.ParseFloat` and put the result into the constant as it was, at the
literal and at a vector literal's element. `ArithmeticFunction.Eval`
(`datalog/query`) computed its float result and returned it as it was, so a
negation, a product with a negative factor or a division by a negative over
a stored `0.0` yielded the negative zero. The EDN import read each value
through `ParseValueNode` and asserted it through the store directly, not
through the transaction that normalizes, and on memory-trees the store
carried the value as the dump stated it.

## The fix

`NormalizeValue` runs at the three entries, as it runs at the transaction's
write: `parsePatternElement`'s float literal and vector element, the float
branch of `ArithmeticFunction.Eval`, whose early returns fold into the one
return that normalizes, and `ParseValueNode`'s float. No path puts a signed
zero into a relation or the store, and the encoding agrees with the equality
at every value a query can hold. The binary dump needs no entry of its own:
its values pass through the storage encoding, which carries one zero.

## Tests

`TestASignedZeroIsOneValueAtEveryEntry` (`datalog/storage/signed_zero_test.go`),
on every backend, in both optimizer modes, with the cache on and off:

- an imported `-0.0` matches `0.0`, reads back as `0.0` in a pattern, renders
  as `"0"` under `str` and pulls as `0.0`;
- a grounded `-0.0` answers `0.0` and renders as `"0"` under `str`;
- a product of a stored `0.0` with a negative factor, a negation of it and
  a division of it by a negative each answer `0.0`.

Each float answer is checked by its sign bit, so `-0.0` and `0.0` are two
values to the test where the engine's equality holds them one.
