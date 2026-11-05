# A source on a database function or a rule invocation is a Datomic form the language refuses

**Status**: Open for the invocation's prefix. The database function's form is fixed, as *The fix* states for it. `TestDatabaseFunction_ReadsTheSourceItNames` and `TestDatabaseFunction_InASubqueryReadsTheSourceTheCallPasses` (storage), `TestDatabaseFunction_ParsesTheSourceItNames` (parser) and `TestDatabaseFunctionFrom_BuildsTheQueryTheEDNParsesTo` (qb) pin it.

Each shape is refused loudly, so no answer is wrong. The defect is a gap
against goal 12 of `docs/proposals/NESTED_SCOPES_AS_QUERIES.md`. Under that
goal the language takes Datomic's forms with Datomic's meanings, and a
janus spelling is a synonym of a Datomic form, never a form with a meaning
of its own.

## Datomic's forms

A database function's first argument is a source: `(get-else $src ?e :a "d")`
reads `$src`. A rule expression may carry a source prefix, `[$src (rule ...)]`
or `($src rule ...)`, under which the rules evaluate over that source.

## What the language does with them

`validateDatabaseRef` (`datalog/parser`) admits `$` alone as a database
function's first argument and refuses any other symbol with
`expected database reference ($)`, so `(get-else $src ?e :a "d")` does not
parse. `SourceRouter` delegates `get-else`, `get-some` and `missing?` to `$`
through `EntityLookupMatcher`, so the one source a function can name is the
one it reads.

`parseRuleInvocation` (`datalog/parser`) reads a list clause whose head
names no reserved form as an invocation of the predicate the head names.
`($src reach ?a ?b)` is read as an invocation of a predicate called `$src`,
with `reach` as its first argument. `Prepare` refuses it as an invocation
of a predicate the set does not define. A vector clause is a data pattern,
so `[$src (reach ?a ?b)]` is read as one and refused as such. A
`query.RuleInvocation` holds its name and its arguments and no source.

## The fix

The parser reads a source at a database function's first argument and as an
invocation's prefix, each form carrying the source it names. The function
reads that source, and the invocation's rules evaluate over it, through the
same routing a data pattern on a named source takes. The vocabulary check's
roster names an attribute on whichever source the function or the rule body
reads, where today a rule body's constants are collected on the default
source alone.

## What the fix costs

Each form is refused where it is written, naming what the parser expected,
so no run, import or test produced a wrong outcome from it, and a fallback
over a named source is already written as an `or-default` over a data
pattern on that source. The fix touches the parser for both forms; the
routing of `get-else`, `get-some` and `missing?`, which `SourceRouter`
delegates to `$` alone through `EntityLookupMatcher`; `query.RuleInvocation`,
which carries no source, and the evaluation of its rules over one; and the
vocabulary roster, which collects a rule body's attributes on `$`. That is
the parser, the query package, the executor's routing, the rule evaluation
and the roster, for two spellings the language expresses another way.

## The use

A downstream build keeps two stores and joins them in its queries. It reads
attributes that a run may lack from the second store, and declares them
`NeverZeroValue`, so a missing value is an absent datom. This query is
refused:

```clojure
[:find ?r ?f
 :in $ $build
 :where [$build ?r :run/how _]
        [(get-else $build ?r :run/failed "") ?f]]
```

The refusal reads `get-else: expected database reference ($), got $build`.

The use reaches the database function's form alone. No use reaches the
invocation's prefix. The two fixes touch different code:

- The function's source touches the parser's `validateDatabaseRef`, the
  function types, which carry no source, the routing through
  `EntityLookupMatcher`, and the roster for a function's attribute.
- The invocation's prefix touches `parseRuleInvocation`,
  `query.RuleInvocation`, the evaluation of its rules over a source, and the
  roster for a rule body's attributes.

So the function's form can be fixed without the invocation's.

## Tests

`(get-else $src ?e :a "d")` over two sources answers from `$src` and not
from `$`. `($src reach ?a ?b)` over two sources derives the closure from
`$src`'s edges alone. A refusing `$src` refuses an attribute a rule body
names on it, and admits the same name on `$`.
