# A signed zero reaches answers through a text literal, built-in arithmetic and the EDN import

**Status**: Open.

The engine's equality holds `-0.0` and `0.0` to be one value.
`datalog.NormalizeValue` takes a float's signed zero as `0.0`, and
`Transaction.Add` runs it on every value it writes. Three entry points do
not run it. A `-0.0` that enters through one of them reaches an answer,
where `str` renders it `"-0"` and a bitwise comparison finds two values.

## What is observed

The cases run on every backend and in both optimizer modes.

- A text literal `-0.0` ground into the find answers `-0`, and `(str ?z)`
  of it answers `"-0"`.
- `(* -1.0 ?a)`, `(- ?a)` and `(/ ?a -1.0)` over a stored `0.0` answer `-0`.
- An EDN import of `-0.0` reads back `-0` in a pattern, a pull and `str` on
  memory-trees, in both modes, with the cache on and off. It reads back `0`
  on memory and badger.

## The mechanism

`parsePatternElement` (`datalog/parser`) reads a float literal with
`strconv.ParseFloat` and puts the result into the constant as it is.
`ArithmeticFunction.Eval` (`datalog/query`) computes its float result and
returns it as it is, so a negation, a product with a negative factor or a
division by a negative over a stored `0.0` yields the negative zero. The
EDN import asserts each value through the store directly, not through the
transaction that normalizes, and on memory-trees the store carries the
value as the dump stated it.

## The fix

`NormalizeValue` runs at the three entries, as it runs at the transaction's
write. The entries are the parser's float literal, the arithmetic function's
result, and the import's value before the assert. No path then puts a signed
zero into a relation or the store. The encoding agrees with the equality at
every value a query can hold.

## Tests

A test for the fix expects each case above to answer `0` and `"0"` on every
backend and in both modes, and an EDN import of `-0.0` to read back `0` on
memory-trees.
