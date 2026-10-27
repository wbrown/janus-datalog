# Several query shapes were refused by `ParseQuery` alone and accepted by `qb.Build`, `Prepare`, `NewRuleSet` and `Database.Query`

**Status**: Resolved.

`query.ValidateQuery` runs at `ParseQuery`, `qb.Build` and `Prepare`. It
carried every static check but the ones below, which the parser alone made.
A query holding one of those shapes, built or hand-made, ran where its own
rendering would be refused. Two of the shapes answered tuples.

## The shapes

| Shape                                      | Refused by the parser as                             | Accepted by                                   |
| ------------------------------------------ | ---------------------------------------------------- | --------------------------------------------- |
| `(or)` with no branch                      | or clause must have at least one branch              | `Build`; `Build` of an inner query            |
| `(or (and) …)`, an `and` with no clause    | and branch must have at least one clause             | `Build`; `Query` with the optimizer off answered |
| `(not)` with no clause                     |                                                      | `Build`                                       |
| `(not-join [?e])` with no clause           |                                                      | `Build`                                       |
| `(or-default …)` with fewer than two branches | or-default clause must have at least two branches | `Build`; `NewRuleSet`; `Query` with the optimizer off answered |
| a data pattern of five elements            | data pattern must have 3 or 4 elements               | `Build`; an inner query; `NewRuleSet`; `Query` answered |
| `:limit -1`                                | :limit must be non-negative                          | `Prepare`; `Query` answered no tuple          |
| `(median ?v)`, an unknown aggregate        | unknown aggregate function                           | `Prepare`                                     |

`:limit -1` answered no tuple. With the optimizer off, `(or (and) [?e :a ?v])`
and a one-branch `or-default` answered. With it on, the algebra compiler
refused both, so one path refused what the other ran. `qb.Limit` refuses a
negative limit, and the builder's aggregates are the five it names, so the
last two shapes reached `Prepare` from a hand-built `query.Query` alone.

## The data pattern's count

The parser's count check refused a pattern of one or two elements with the
same message as one of five, and that refusal was the parser's own
divergence, not a shape for every boundary to refuse. A pattern of five
elements names a position the engine has no reading of: history is a
handle's mode, never a fifth position, and no boundary ever ran one. A
pattern of one or two elements is Datomic's grammar, a source and one to
four positions with the omitted trailing ones unbound, and the engine
already read it so: `Database.Query` of `[:find ?e :where [?e :a]]` answered
every entity carrying `:a`, in both optimizer modes. The contract is one to
four elements, entity, attribute, value and transaction in that order, a
position the text omits being unbound; no element, or more than four, is
refused at every boundary.

The builder's doc said `Pat` creates a pattern of any arity, with
`Pat(name, age)` matching a two-tuple input relation and
`Pat(e, a, v, tx, op)` a history pattern. Nothing in the engine matches a
pattern against an input relation, which is bound as a relation joined once,
and no fifth position exists. The claim entered with the builder package and
was never true of the engine.

## The fix

Each check moved from the parser into `ValidateQuery`
(`datalog/query/clause_scope.go`, `datalog/query/validate.go`), where the
three boundaries and every inner query and rule body run it. The parser
carries no check of its own; the `-join` forms still refuse a missing header
vector, which is a parse shape.

- `DataPattern.Validate` refuses no element and more than four, naming the
  count. `DataPattern.GetA` and `GetV` (`datalog/query/types.go`) answer a
  blank for a position the pattern omits, so every reader of a pattern
  reads an omitted position as unbound. `GetT` answers nil, since its
  readers test it for the transaction position's presence.
- `NotClause.Validate` and `NotJoinClause.Validate` refuse a body with no
  clause and validate each clause's shape through `validateClauseElements`,
  which names a refused clause by the form and its position counted from
  one.
- `OrClause.Validate`, `OrJoinClause.Validate`, `OrDefaultClause.Validate`
  and `OrDefaultJoinClause.Validate` refuse too few branches and validate
  each branch through `validateBranches`: a branch with no clause is refused
  naming the form and the branch, and a refusal inside a branch is wrapped
  with the form and the branch. The headed forms validate the branches'
  shapes before reading the header, so a shape refusal comes before a scope
  refusal.
- `ValidateStaticClauseShapes` walks the clauses and stops at a compound
  clause whose Validate descended into it, so a nested clause is validated
  once, with its position.
- `ValidateQuery` refuses an aggregate outside `aggregateFunctions` and a
  negative `:limit`.
- The fuzz drawer (`datalog/internal/queryfuzz`) draws one- and two-element
  patterns, so the round-trip and mode-parity tests cover them inside every
  form, subquery and rule body.

## Tests

- `TestValidateQueryRefusesTheShapesTheParserRefuses`
  (`datalog/query/shape_test.go`): each shape is refused with one message
  by `ValidateQuery`, by `Prepare`, over an inner query and over a rule
  body, and a refusal inside a form names the form and the branch, or the
  clause of a not or a not-join, by its position counted from one.
  `TestValidateQueryAcceptsAShortDataPattern`: a pattern of one or two
  elements passes `ValidateQuery` and `Prepare`.
- `TestBuild_RefusesTheShapesTheParserRefuses` and
  `TestBuild_AcceptsAShortDataPattern` (`datalog/qb/shape_test.go`,
  `datalog/qb/short_pattern_test.go`): the same at `Build`, a short pattern
  rendering as written.
- `TestQuery_RefusesTheShapesTheParserRefuses`
  (`datalog/storage/shape_refusal_test.go`): a hand-built query holding a
  shape is refused by `Query` in both optimizer modes.
  `TestQuery_AShortPatternLeavesItsTrailingPositionsUnbound`
  (`datalog/storage/short_pattern_test.go`): a short pattern answers what
  its blank-padded spelling answers, in both modes, with and without a
  source named.
- `TestParseQueryReadsAShortDataPattern`
  (`datalog/parser/short_pattern_test.go`): a short pattern parses to its
  count and its rendering reads back as the same query.
  `TestARefusalInsideAFormNamesTheBranchCountedFromOne`
  (`datalog/parser/not_or_test.go`): the parser's wrapper names the branch
  when its text does not parse, and the validator's names it when a clause
  the branch parses to is refused.
- `TestQueryStringRoundTripsRandomQueries` (`datalog/parser`) and
  `TestQueriesAnswerAlikeUnderBothOptimizerModes` (`datalog/storage`) run
  over the draw that includes short patterns.
