# Several query shapes are refused by `ParseQuery` alone and accepted by `qb.Build`, `Prepare`, `NewRuleSet` and `Database.Query`

**Status**: Open.

`query.ValidateQuery` runs at `ParseQuery`, `qb.Build` and `Prepare`. It
carries every static check but the ones below, which the parser alone
makes. A query holding one of those shapes, built or hand-made, runs where
its own rendering would be refused. Two of the shapes answer tuples.

## The shapes

| Shape                                      | Refused by the parser as                             | Accepted by                                   |
| ------------------------------------------ | ---------------------------------------------------- | --------------------------------------------- |
| `(or)` with no branch                      | or clause must have at least one branch              | `Build`; `Build` of an inner query            |
| `(or (and) …)`, an `and` with no clause    | and branch must have at least one clause             | `Build`; `Query` with the optimizer off answers |
| `(not)` with no clause                     |                                                      | `Build`                                       |
| `(not-join [?e])` with no clause           |                                                      | `Build`                                       |
| `(or-default …)` with fewer than two branches | or-default clause must have at least two branches | `Build`; `NewRuleSet`; `Query` with the optimizer off answers |
| a data pattern of two or five elements     | data pattern must have 3 or 4 elements               | `Build`; an inner query; `NewRuleSet`; `Query` answers |
| `:limit -1`                                | :limit must be non-negative                          | `Prepare`; `Query` answers no tuple           |
| `(median ?v)`, an unknown aggregate        | unknown aggregate function                           | `Prepare`                                     |

`Database.Query` of `[:find ?e :where [?e :a]]` answers every entity
carrying `:a`, in both optimizer modes. `:limit -1` answers no tuple. With
the optimizer off, `(or (and) [?e :a ?v])` and a one-branch `or-default`
answer. With it on, the algebra compiler refuses both, so one path refuses
what the other runs. `qb.Limit` refuses a negative limit, and the builder's
aggregates are the five it names. So the last two shapes reach `Prepare`
from a hand-built `query.Query` alone.

## The fix

Each check moves from the parser into `ValidateQuery`, where the three
boundaries and every inner query and rule body run it. A form with no
branch, a branch with no clause, an `or-default` with fewer than two
branches, a data pattern of other than three or four elements, a negative
limit and an unknown aggregate are then refused with the parser's message
at every boundary. The parser carries no check of its own.

## Tests

A test for the fix expects each shape above to be refused with the message
`ParseQuery` gives its rendering, whether built through `qb`, hand-made and
given to `Prepare`, declared in a rule body, or placed in an inner query.
It expects `Database.Query` to refuse the shape in both optimizer modes.
