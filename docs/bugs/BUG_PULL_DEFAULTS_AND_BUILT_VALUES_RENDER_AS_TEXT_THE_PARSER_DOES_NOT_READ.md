# A pull default and several built values render as text the parser does not read back as the same query

**Status**: Open.

Two renderers write text the parser refuses or reads as another query. The
plan cache keys on a structural hash of the query, so a second query whose
default or constant differs only in type gets its own plan. The renderings
stand, and one built value panics the cache's key.

## The pull default

`PullDefaultExpr.String` (`datalog/query`) renders the default with `%v`:

| Default     | Renders as            | Parsed back                     |
| ----------- | --------------------- | ------------------------------- |
| `"unknown"` | `(default :b unknown)` | refused, unsupported default   |
| `"a b"`     | `(default :b a b)`    | refused, three arguments        |
| `nil`       | `(default :b <nil>)`  | refused                         |
| `1.0`       | `(default :b 1)`      | `int64(1)`                      |
| `"1"`       | `(default :b 1)`      | `int64(1)`                      |

The builder's pull default renders the same way: `qb.Default` of a string
renders it unquoted.

## The built values

`FormatValueEDN` renders a value the builder admits as text the parser does
not read as that value:

| Value, through `qb.Ground`     | Renders as   | Parsed back            |
| ------------------------------ | ------------ | ---------------------- |
| `+Inf`, `-Inf`                 | `+Inf.0`     | a symbol               |
| `float32(2)`                   | `2`          | `int64(2)`             |
| `uint64(7)`                    | `7`          | `int64(7)`             |
| `datalog.ElementID`            | `L5@R1`      | an EDN error           |
| `map[string]interface{}`       | `map[a:1]`   | an EDN error           |
| keyword `:a b`                 | `:a b`       | two arguments          |
| keyword `:`                    | `:`          | an EDN error           |

A transaction value the engine binds, a `*datalog.ElementID` given through
`qb.V` at a pattern's transaction position, renders `L2@R…` and does not
parse either.

## The plan cache

The cache keys on `Query.Hash`, a structural hash (`datalog/query/hash.go`).
So a text default of `1.0` and one of `"1"` are two plans, and a built `+Inf`
or `uint64` beside its text spelling are two plans. A built `float32` is
neither. `datalog.HashValue` (`datalog/value_hash.go`) has no case for it
and panics with `HashValue:float32 is not a datalog value type`, from
`PlanCache.computeKeyWithOptions` on the query's first execution.

## The fix

The builder admits the domain's values and refuses the rest at `Build`.
`float32`, the unsigned integers, a map and an `ElementID` given as a
ground constant are refused naming the type. A keyword with a space or no
name is refused where the keyword is made. `PullDefaultExpr.String` renders
its default through the one value renderer, quoted and typed as the parser
reads it. `FormatValueEDN` renders an infinity and a transaction value in a
form the parser reads, or the parser gains the form. After that, every
query the builder accepts renders to text the parser reads back as the same
query, which the round-trip test over random queries pins.

## Tests

A test for the fix expects each default and each value above, built,
rendered and parsed back, to be the same query, or to be refused at `Build`
with its type named.
