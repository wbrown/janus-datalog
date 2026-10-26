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
`_`. The binding machinery needs no change. `query.SymbolGroups`
(`datalog/query/symbol_index.go`) already puts a position carrying no symbol
in no group, so the blank binds nothing and joins no unification.
`BindQueryInputs` and the expression's `bindingAlignment` read their groups
from it.

## Tests

A test for the fix expects these outcomes. `[(ground [1 "x"]) [_ ?s]]`
binds `?s` alone. `:in $ [[_ ?age]]` over a relation of two positions binds
`?age` alone. `[?x _ ...]` as a collection input is refused, naming the
form. A query carrying a blank in a binding renders and parses back to
itself.
