# Rules

**Status**: Current

A rule set declares predicates. A rule gives one way to derive a predicate's
tuples from clauses. A query or another rule invokes a predicate as a clause.
A rule that invokes itself computes a transitive closure. The derived relation
joins the rest of the query as a pattern's relation does.

## Syntax

The forms are Datomic's. A rule set is a vector of rules. Each rule is a
vector: the head, `(name ?args...)`, then at least one body clause. Several
rules for one name are alternatives. The predicate holds a tuple when any of
them derives it.

```clojure
[[(reach ?a ?b) [?a :edge ?b]]
 [(reach ?a ?c) [?a :edge ?b] (reach ?b ?c)]]
```

The rule set is the query's `%` input. The query invokes a predicate as a list
clause in `:where`:

```clojure
[:find ?y
 :in $ % ?start
 :where (reach ?start ?y)]
```

A head's arguments are distinct variables. Every rule for one name has the
same arity. Every invocation uses that arity. A predicate's name lies outside
the namespaces the engine names its own predicates in, `adorned`, `magic`
and `delta`; a rule set naming a predicate there is refused.

A head may begin with a vector of variables. This is Datomic's required
binding form. The caller binds those variables. The body may use them without
binding them, so a rule can compute from its arguments:

```clojure
[[(succ [?n] ?m) [(+ ?n 1) ?m]]]
```

Every rule for one name has the same required run. An invocation binds those
positions before it runs.

A body is a `:where`. It may hold patterns, predicates, expressions, database
functions, `not`, `not-join`, `or`, `or-join`, `or-default`,
`or-default-join`, subqueries, and invocations of any predicate the set
defines.

## The `%` input

The value given for `%` is a `*query.RuleSet` or the text of a rule set.
`parser.ParseRules` and the query builder produce a `*query.RuleSet`, which is
passed through. Text is parsed once per text, through the database's parse
cache, as a query string is:

```go
rules, err := parser.ParseRules(`[[(reach ?a ?b) [?a :edge ?b]]
                                   [(reach ?a ?c) [?a :edge ?b] (reach ?b ?c)]]`)

rel, err := d.Query(`[:find ?y :in $ % ?start :where (reach ?start ?y)]`, rules, start)
rel, err := d.Query(`[:find ?y :in $ % ?start :where (reach ?start ?y)]`, rulesText, start)
```

A value of any other Go type at the `%` position is an error naming the type.
`Explain`, `ExplainAlgebra` and `Analyze` take the rule set the same way.

A clause uses the query's own `%` when it invokes a predicate, at any depth
short of a subquery body, or when it is a subquery call passing `%`. A query
holding such a clause declares `%` in its own `:in`. A query that declares
none is a parse error naming the clause. A subquery that invokes a predicate
declares `%` in its `:in` and is passed `%`, and the call passing it is a use
of the enclosing query's `%`:

```clojure
[:find ?x ?n
 :in $ %
 :where [?x :edge _]
        [(q [:find (count ?y) :in $ % ?x :where (reach ?x ?y)] $ % ?x) [[?n]]]]
```

## Invocation arguments

An argument is a variable, a constant, or `_`. The invocation reads the
derived relation under its arguments:

- a variable binds the position
- a variable repeated across positions keeps the tuples whose positions are
  equal
- a constant keeps the tuples whose position holds it
- `_` drops the position
- a variable the query's inputs bind keeps the tuples whose position holds the
  input's value

An invocation with no variable left is a verdict. The clause holds when the
predicate holds the tuple. Otherwise it fails.

An invocation stands wherever a pattern stands: at the top level of a
`:where`, inside `not` and `not-join`, inside `or` and `or-join` branches,
inside `or-default` and `or-default-join` branches, and inside a subquery.

A position the predicate requires bound is the invocation's correlate. The
planner orders the invocation after the clauses that bind it, inside a
subquery's inner query as at the top level. When nothing binds it, the
planner refuses the query naming the invocation and the variable. A
positive invocation inside a rule body whose rewrite leaves a required
position free, or an invocation evaluated at its clause with the position
unbound there, is refused as `*query.RequiredBindingError` naming the
predicate, the variable and the repair.

## Safety

A rule is safe when every variable it must bind is bound. The variables it
must bind are the head variables and every variable a body clause consumes: a
predicate's inputs, an expression's inputs, a subquery's inputs, an
explicit-join header, and a body invocation's required positions. A variable
is bound by a positive body clause or by the head's required run. An
invocation binds its variable arguments at the positions its predicate does
not require. An unsafe rule is refused when the rule set is built, as
`*query.UnsafeRuleError` naming the rule and the variable.

## Stratification

A rule's predicate depends on every predicate its body invokes, at any depth.
The dependency is negative when the invocation sits under `not` or
`not-join`. The strongly connected components of that graph are the strata.
They are ordered so that every predicate a stratum depends on is in an
earlier one. A component holding a negative edge is refused as
`*query.UnstratifiableError` naming its predicates. No evaluation order
satisfies predicates that depend on each other through negation.

```clojure
[[(reach ?a ?b) [?a :edge ?b]]
 [(reach ?a ?c) [?a :edge ?b] (reach ?b ?c)]
 [(unreached ?n) [?n :type :node] (not (reach _ ?n))]]
```

`reach` is one recursive stratum. `unreached` is a later one. Its negation
reads a predicate already complete.

A recursive invocation is one of a predicate in the invoking rule's own
stratum. It stands at the top level of the body or inside an `or` or
`or-join` branch, where evaluation is monotone. Inside `or-default`,
`or-default-join` or a subquery it is refused as
`*query.NestedRecursionError` naming the rule, the predicate and the form. An
invocation of a predicate from an earlier stratum stands anywhere.

A rule invoking a predicate the set does not define is refused as
`*query.UnknownRuleError` naming the invoked name and the defined names. A
rule invoking it at another arity is refused as `*query.RuleArityError`. A
query's own invocations are checked the same way when it executes.

## Evaluation

An invocation is evaluated when its clause runs. The planner has ordered it
among the query's other clauses by then. The arguments bound at that point
decide what is derived.

An invocation with no bound argument derives its predicate whole. The strata
it needs are those holding the predicate or a predicate a needed body
invokes. They evaluate in order, each to a fixpoint. The invocation reads the
predicate's materialized relation.

An invocation with a bound argument derives only what the bindings reach. A
bound argument is a constant, or a variable the query's inputs or earlier
clauses bind. The rule set is rewritten for the invocation's binding pattern.
This is the magic-set rewrite:

- Each predicate the bindings reach gets a copy named for the pattern.
  `adorned.bf/reach` is `reach` with its first position bound.
- The copy's rules are guarded by a magic predicate, `magic.bf/reach`. It
  holds the bindings the predicate is asked with.
- The bindings pass through each body in the order the planner gives it
  into the invocations after them, and through each branch of a form in the
  planner's order under the form's header.
- An invocation under `not`, `not-join`, `or-default`, `or-default-join` or
  a subquery whose inner query aggregates receives the bindings of the
  clauses before the first that invokes a predicate of the rule's own
  stratum. A binding that invocation provides would make the inner
  predicate's magic rule depend on the adorned predicate, which depends on
  the inner one through the form.
- Such an invocation keeps the original rules where adorning it would place
  its adorned predicate in the rule's own stratum, as when the same
  predicate is invoked positively after the recursion in another rule, or
  where the bindings it receives leave a position its predicate requires
  free. The set is rebuilt with that site left as it is, so no rewrite is
  refused.
- A body invocation that nothing before it binds keeps the original rules.
- A subquery's inner query is a scope of the rewrite: its `:in` symbols
  stand for the call's arguments, and an invocation inside it is adorned by
  the arguments the clauses before the call bind and the inner clauses
  before it.

The invocation's bound values seed the magic predicate. The rewritten set
evaluates as any set does. The invocation reads the adorned predicate, and
an invocation left with its original name reads the predicate derived whole
in its own stratum, or, for a predicate with required positions, derived at
the clause under the bindings the branch holds there.

A recursive stratum iterates semi-naively. The seeding run executes the rules
with no recursive invocation. Each later iteration executes each rule once
per recursive invocation in its body. In that execution the chosen invocation
invokes the predicate's delta symbol, `delta/reach`, under which the scope
holds the tuples the previous iteration added. Every other invocation reads
the whole relation. The tuples not yet derived become the next iteration's
delta. The stratum ends when an iteration adds nothing.

Every iteration reads the one snapshot the query reads. A write committed
while a closure is under evaluation is invisible to it.

An invocation inside an `or-join` or `or-default-join` branch, a `not-join`
body or a subquery's inner query is evaluated once per execution of the
form, continued across the windows of the form's subject. The branch runs
over each window's keys. A window's keys enter the seed predicate's stratum
as a delta, the sets of every stratum and the derived-tuple count are
carried from window to window, and the strata run again from the lowest one
that changed, so each derived tuple is derived in one window.
`rules/stratum.begin` fires at the first window and `rules/stratum.complete`
at the last, and `rules/iteration` numbers each window's iterations from
zero.

A rule body is planned by the query planner as a query of its own, under the
database's planner options. Its plan enters the plan cache under its own
text, which carries `%` and the delta symbol. An invocation of a predicate
the seed restricts, `magic.bf/reach`, `adorned.bf/reach` or the delta of
either, reads a relation holding only what the seed reaches, so the planner
schedules it before the body's patterns, which then seek on the bindings it
provides. An invocation of any other predicate, `reach` or `delta/reach`,
reads an unrestricted relation. The body's patterns scan first and it joins
them. A database opened with `RefuseUnknownAttributes` refuses a rule body
naming an attribute its schema does not declare, as it refuses the query's
own.

## Bounds

Two planner options bound an evaluation. Both are opt-in. Both are zero by
default, and zero is unbounded:

- `MaxRuleIterations`: the iterations one stratum may run, counted per window
  for an invocation inside a form's branch. Exceeded, the query returns
  `*query.RuleIterationLimitError` naming the stratum's predicates, the limit
  and the iteration reached.
- `MaxDerivedTuples`: the tuples one invocation's evaluation may derive,
  carried across the windows for an invocation inside a form's branch. A
  rewritten set's magic predicates count too. Exceeded, the query returns
  `*query.DerivedTupleLimitError` naming the predicates, the limit and the
  count reached.

```go
opts := storage.DefaultPlannerOptions()
opts.MaxRuleIterations = 1000
opts.MaxDerivedTuples = 1_000_000
d, err := db.Open(path, db.WithPlannerOptions(opts))
```

## Cancellation

`WithContext` is a query option. It is passed among the inputs of `Query`,
`QueryInto`, `QueryOneInto`, `Explain`, `ExplainAlgebra` and `Analyze`. A
query whose context is done is refused at entry, before any work. A fixpoint
whose context is done stops at its next iteration. The context's error is the
error `Query`, `QueryInto`, `QueryOneInto` and `Analyze` return.

```go
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()
err := d.QueryInto(&pairs, closure, rules, db.WithContext(ctx))
```

The context is read at entry and once per fixpoint iteration. No tuple is
checked, so the option costs nothing per tuple.

## Observability

An annotation handler sees three events per stratum and one per bound
invocation:

| Event | Payload |
|---|---|
| `rules/magic-sets` | `predicate`, `adornment` (bound, per position) |
| `rules/stratum.begin` | `predicates`, `recursive` |
| `rules/iteration` | `predicates`, `iteration`, `delta` (tuples added, per predicate) |
| `rules/stratum.complete` | `predicates`, `iterations`, `tuples` |

`Explain` and the CLI's `-plan-only` render an invocation in its phase under
`Invocations:`. The arguments the phase binds before it runs are shown with
it.

## The CLI

The `-in` value at the `%` position is the rule set's text:

```bash
datalog -db graph.jdzl \
  -query '[:find ?y :in $ % ?x :where (reach ?x ?y)]' \
  -in '[[(reach ?a ?b) [?a :edge ?b]] [(reach ?a ?c) [?a :edge ?b] (reach ?b ?c)]]' \
  -in '#id "node/n1"'
```

## The query builder

`qb.Rules` is the `%` input spec. `qb.Rule` is an invocation. `qb.DefineRule`
with `qb.RuleSet` builds a rule set equal to its parsed text:

```go
a, b, c := qb.NewVar("a"), qb.NewVar("b"), qb.NewVar("c")
reach := qb.MustRuleSet(
    qb.DefineRule("reach", a, b).Where(qb.Pat(a, Edge, b)),
    qb.DefineRule("reach", a, c).Where(qb.Pat(a, Edge, b), qb.Rule("reach", b, c)),
)

start, y := qb.NewVar("start"), qb.NewVar("y")
q := qb.Query().
    Find(y).
    In(qb.DB, qb.Rules, qb.Scalar(start)).
    Where(qb.Rule("reach", start, y)).
    MustBuild()

rel, err := d.Query(q, reach, startID)
```

`Require` names the head variables the caller binds, the required run:

```go
n, m := qb.NewVar("n"), qb.NewVar("m")
succ := qb.MustRuleSet(qb.DefineRule("succ", n, m).Require(n).Where(qb.Add(n, int64(1)).As(m)))
```

`qb.RuleSet` returns the errors `query.NewRuleSet` returns. `qb.MustRuleSet`
panics on them. See [QUERY_BUILDER.md](QUERY_BUILDER.md).

## Errors

| Error | Named parts | Raised by |
|---|---|---|
| `*query.RuleArityError` | predicate, expected arity, arity found | building the set, executing a query |
| `*query.UnsafeRuleError` | rule, variable | building the set |
| `*query.UnknownRuleError` | invoked name, defined names | building the set, executing a query |
| `*query.UnstratifiableError` | the predicates on the negative cycle | building the set |
| `*query.NestedRecursionError` | rule, predicate, form | building the set |
| `*query.RequiredBindingError` | predicate, variable, invoking rule | the rewrite at a positive invocation, an invocation evaluated at its clause |
| `*query.RuleIterationLimitError` | predicates, limit, iteration reached | evaluation |
| `*query.DerivedTupleLimitError` | predicates, limit, count reached | evaluation |

`Query` wraps an execution error with `%w`, so `errors.As` reaches each.

## Deferred

- **Recursive aggregation**: an aggregate over a recursive predicate inside
  its own stratum (shortest paths, counts over closures). A body aggregates
  over a completed stratum through a subquery, as a query does.

## Measurement

`BenchmarkTransitiveClosure` in `datalog/executor` and `datalog/storage`
measures the closure over chains of ten edges. It runs at a thousand, ten
thousand and a hundred thousand edges. `BenchmarkBoundInvocation` beside each
measures the same closure with its start bound to one node. The numbers are
in [PERFORMANCE_STATUS.md](../../PERFORMANCE_STATUS.md), the 2026-10-12 entry
of its session history.

## Tests

`datalog/query/rules_test.go` (the set and its refusals) and
`datalog/query/magic_test.go` (the rewrite), `datalog/parser/rules_test.go`,
`datalog/executor/rules_test.go` (the fixpoint, the invocation and the bound
invocation), `datalog/executor/cancellation_test.go`,
`datalog/storage/rules_test.go` and `datalog/storage/cancellation_test.go`
(through `Database.Query` on the backend and optimizer matrix),
`datalog/qb/rules_test.go`, `datalog/db/cancellation_test.go` and
`cmd/datalog/rules_test.go`.
