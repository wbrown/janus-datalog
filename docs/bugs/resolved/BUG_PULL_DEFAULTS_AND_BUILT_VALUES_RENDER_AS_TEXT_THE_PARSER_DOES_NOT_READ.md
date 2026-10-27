# A pull default and several built values rendered as text the parser did not read back as the same query

**Status**: Resolved.

Two renderers wrote text the parser refused or read as another query. The
plan cache keys on a structural hash of the query, so a second query whose
default or constant differed only in type got its own plan. One built value
panicked the cache's key.

## The pull default

`PullDefaultExpr.String` (`datalog/query`) rendered the default with `%v`:

| Default     | Rendered as            | Parsed back                     |
| ----------- | ---------------------- | ------------------------------- |
| `"unknown"` | `(default :b unknown)` | refused, unsupported default    |
| `"a b"`     | `(default :b a b)`     | refused, three arguments        |
| `nil`       | `(default :b <nil>)`   | refused                         |
| `1.0`       | `(default :b 1)`       | `int64(1)`                      |
| `"1"`       | `(default :b 1)`       | `int64(1)`                      |

The builder's pull default rendered the same way: `qb.Default` of a string
rendered it unquoted.

## The built values

`FormatValueEDN` rendered a value the builder admitted as text the parser
did not read as that value:

| Value, through `qb.Ground`     | Rendered as  | Parsed back            |
| ------------------------------ | ------------ | ---------------------- |
| `+Inf`, `-Inf`                 | `+Inf.0`     | a symbol               |
| `float32(2)`                   | `2`          | `int64(2)`             |
| `uint64(7)`                    | `7`          | `int64(7)`             |
| `datalog.ElementID`            | `L5@R1`      | an EDN error           |
| `map[string]interface{}`       | `map[a:1]`   | an EDN error           |
| keyword `:a b`                 | `:a b`       | two arguments          |
| keyword `:`                    | `:`          | an EDN error           |

A transaction value the engine binds, a `*datalog.ElementID` given through
`qb.V` at a pattern's transaction position, rendered `L2@R…` and did not
parse either.

## The plan cache

The cache keys on `Query.Hash`, a structural hash (`datalog/query/hash.go`).
So a text default of `1.0` and one of `"1"` were two plans, and a built
`+Inf` or `uint64` beside its text spelling were two plans. A built
`float32` was neither: `datalog.HashValue` (`datalog/value_hash.go`) has no
case for it and panicked with `HashValue:float32 is not a datalog value
type`, from `PlanCache.computeKeyWithOptions` on the query's first
execution.

## The fix

The value domain has one statement, `ValidateValue` (`datalog`): a string,
an `int64`, a `float64` other than NaN, a bool, a `time.Time`, a `[]byte`,
an `Identity`, a `Keyword`, a `Symbol`, an `ElementID` by value or by
non-nil pointer, or a vector of those. Its error names the type of a value
outside the domain, and the value itself for NaN, a nil pointer and an
unsigned integer above `int64`'s range. `NormalizeValue` takes an unsigned
integer within `int64`'s range as the integer it equals, the rule
`BUG_UNSIGNED_INTEGER_REACHES_THE_ENCODER.md` records, which is part of
this fix.

`ValidateQuery` (`datalog/query`) refuses a constant outside the domain at
every position a query holds one: a pattern element, a predicate term, a
function's argument or ground value, a get-else default, a subquery argument
and a pull default, at the top level and inside every form, an inner query's
constants through its own `ValidateQuery`. `ParseQuery`, `qb.Build` and
`Prepare` each run it, so the builder refuses a `float32`, a map or an
unsigned integer above `int64`'s range at `Build`, naming the type or the
value.

A keyword with no name after the colon, or holding a space, is refused where
the keyword is made: `InternKeyword` panics naming the keyword, so no query
ever holds one.

`PullDefaultExpr.String` renders its default through `FormatValueEDN`,
quoted and typed as the parser reads it, and `nil` as `nil`.
`parseDefaultValue` reads a default as a pattern constant is read, so every
value the value grammar admits is a default, and `nil` is the one default
that is no value.

`FormatValueEDN` renders a `Symbol` by its text, an `ElementID` by value or
by pointer as `#eid [lamport replica]`, the infinities as `##Inf` and
`##-Inf`, and NaN as `##NaN`. The EDN reader reads `##Inf`, `##-Inf` and
`##NaN` as floats, `TaggedLiteralValue` reads `#eid`, and the dump's
`ValueNode` renders an infinite float without the `.0` suffix. The random
query draw (`datalog/internal/queryfuzz`) draws the infinities and `#eid`,
so the round trip over random queries covers them.

## Tests

`TestBuild_AdmitsTheDomainsValuesAndRendersThemAsTheParserReads`
(`datalog/qb/value_domain_test.go`): an infinity of either sign, an
unsigned integer, a transaction value by value and by pointer, a
whole-valued float, a string of digits, a string with a space and a
keyword, each as a ground, as a pattern constant and as a pull default, and
a transaction value at the transaction position, build, and each query's
text parses back as the same text and the same query. A `float32`, a map
and an unsigned integer above `int64`'s range are refused at `Build` at
each of the three positions, naming the type or the value.

`TestKw_RefusesAKeywordWithASpaceOrNoName` (the same file) and
`TestAKeywordWithASpaceOrNoNameIsRefusedWhereItIsMade`
(`datalog/value_domain_test.go`): `:a b`, `:`, the empty string and a
keyword holding a tab are refused where they are made, and `:a.b/c-d` is
not.

`TestValidateQueryRefusesAConstantOutsideTheDomain`
(`datalog/query/value_domain_test.go`): a `float32`, a map, an unsigned
integer above `int64`'s range, NaN and a vector holding a `float32`, each
as a pattern element, a ground, a predicate term, a get-else default, a
subquery argument, an inner query's ground, a pattern inside a `not` and a
pull default, is refused naming the type or the value. A query holding a
transaction value as a pull default, an infinity as a pattern constant and a
vector ground holding a pointer transaction value is accepted.

`TestNormalizeValueTakesAnUnsignedIntegerWithinRangeAsInt64`
(`datalog/value_domain_test.go`): every unsigned width at 5 normalizes to
`int64(5)`, `uint64(math.MaxInt64)` to its `int64`, and one above the range
passes through unchanged.

`TestCompareValuesTagOrderRejectsOutsideDomain`
(`datalog/compare_tag_order_test.go`): an unsigned integer above `int64`'s
range, a `float32` and a vector are refused by the tag order.

`TestQueryStringRoundTripsEveryValueType`
(`datalog/parser/format_values_test.go`): an infinite float as a pattern
constant and in a predicate, and a transaction value at the transaction
position and in a ground, each render to text that parses back as the same
query and renders the same again. `TestQueryStringRoundTripsPullDefaults`
(the same file): a string, a string with a space, a string of digits, `nil`,
a whole-valued float, an integer, a keyword, a boolean, an instant, a
transaction value and an identity as a pull default each do the same.
