# A blank at a binding position is a Datomic form the language refuses

**Status**: Open.

The blank is refused loudly, so no answer is wrong. The defect is a gap
against goal 12 of `docs/proposals/NESTED_SCOPES_AS_QUERIES.md`, under which
the language takes Datomic's forms with Datomic's meanings.

## Datomic's forms

Datomic's bind-tuple is `[(variable | '_')+]` and its bind-rel is
`[[(variable | '_')+]]`. Each admits `_` at a position whose value the
binding does not name. Its bind-coll, `[variable '...']`, admits no blank.

## What the language does with them

`parseTupleBinding`, `parseRelationBinding` and `inputVariables`
(`datalog/parser`) each refuse an element that is not a variable.
`parseTupleBinding`'s refusal reads `tuple binding element %d must be a
variable, got %s`. So a blank is refused in an `:in` tuple or relation
input, in an expression's tuple or relation binding, and in a subquery's
relation binding. A caller who wants one position of a tuple names the
others with variables nothing reads.

## The fix

The tuple and relation forms admit the blank at every binding site. The
collection form refuses it, as Datomic's does. The parser's three readers
take `_` as a position carrying no symbol. `Query.String` renders it back as
`_`.

## What the fix costs

The binding machinery does change. A binding's symbol list has two kinds of
reader. Positional readers pair a position with a value or a find symbol and
need the blank kept as a position: the subquery frame's substitution in
`planner/framing.go`, the lateral join's arity check in
`algebra/analysis.go`, and the expression alignment in
`executor/relation_ops.go` and `executor/query_executor.go`. Set readers
need the blank absent: `ScopeOf`'s Provides, which header inference reads;
the outputs in `algebra/compile.go`, `analysis.go`, `decompile.go` and
`rewrite_decorrelate.go`, which also rebuilds a relation binding from its
output symbols; the executor's subquery symbols; the or-fallback and explain
analyses. Both read `BoundVariables()` today. `query.SymbolGroups`
(`datalog/query/symbol_index.go`) puts a nil entry in no group, and the
storage boundary's `convertInputsToRelations` builds an input's relation over
`spec.Symbols` as written, so a blank there needs the value at its position
dropped. A blank's representation is a position holding no symbol: either a
nil symbol, the convention `SymbolGroups` and `InputPosition` carry, or a
pattern element of its own type beside `Variable`, the form a pattern's `_`
takes. Either way the four fields `TupleBinding.Variables`,
`RelationBinding.Variables`, `TupleInput.Symbols` and `RelationInput.Symbols`
change what they hold, and every constructor and reader of them follows, in
the parser, the query package, the planner, the algebra, the executor, the
storage boundary and the builder.

No run, import or test produced a wrong outcome because `_` is refused: the
refusal is loud and names the position, and the caller writes a variable
nothing reads. The fix is a representation change across the engine for one
spelling the language already expresses another way. It is taken up when a
use needs the spelling, which is the failure the cost answers.

## Tests

A test for the fix expects these outcomes. `[(ground [1 "x"]) [_ ?s]]`
binds `?s` alone. `:in $ [[_ ?age]]` over a relation of two positions binds
`?age` alone. `[?x _ ...]` as a collection input is refused, naming the
form. A query carrying a blank in a binding renders and parses back to
itself.
