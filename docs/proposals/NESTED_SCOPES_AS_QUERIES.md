# Nested Scopes as Queries

**Status**: Proposed

Every nested form of a query states its interface in a header. The header
is computed once, from the text and the rule set, before the planner or the
algebra engine sees the query. Every component after that point reads the
header and computes nothing about it. This document states the goals the
design answers to, the semantics of the forms, the architecture that
delivers them, and what each component is against `5b152c9`.

## 1. Goals

The design answers to these, in the owner's words.

1. The most correct fix, not the tactical one. "Let's consider the design
   question and derive the most correct fix for Janus Datalog, not the
   tactical fix."
2. Clarity is not diluted. "Janus Datalog is something special, with an
   unusual clarity that we don't want to dilute."
3. Correct and consistent outranks compatible. "I am willing to break
   compatibility if it means that things are *correct* conceptually and
   consistent."
4. The algebra package keeps `main`'s clarity. "The goal was to retain the
   `algebra` package's clarity against `main`."
5. No accreted arguments. "Remember, do not accrete arguments."
6. The algebra engine is a drop-in oracle of correctness. "What is the goal
   of having the algebra engine be something that you can drop in and out?
   It's an oracle of correctness, isn't it?"
7. Each component does its own job, and inference is one pass. "Keep
   inference as one pass."
8. The architecture is simple. "Restore the simplicity of the software
   architecture."
9. The design is concrete. "It's a mishmash of rulings. It's not a concrete
   design."

## 2. The language

### 2.1 The forms

A query's `:where` holds data patterns, expressions, predicates, rule
invocations, subqueries, and six nested forms:

| Form | Header | Meaning |
|---|---|---|
| `(or branch…)` | inferred | union of the branches |
| `(or-join [[req…] out…] branch…)` | written | union of the branches |
| `(not clause…)` | inferred | the body matches nothing |
| `(not-join [req…] clause…)` | written | the body matches nothing |
| `(or-default branch…)` | inferred | the first branch with a result, per tuple of the required run |
| `(or-default-join [[req…] out…] branch…)` | written | the first branch with a result, per tuple of the required run |

A header has a required run and outputs. The required run is the variables
the enclosing scope binds before the form runs. The outputs are the variables
every branch binds and the form provides to the enclosing scope. A flat
header, `[out…]`, is all outputs. A `not-join` header is all required, since
a `not` provides nothing.

Every other variable inside a form is local to the branch that holds it. A
local never correlates with the enclosing scope, whatever reads it: a
pattern, a predicate, an expression, or a nested form. Renaming a local to
a name that occurs nowhere else in its scope changes nothing.

### 2.2 The interface of a form

The header is the whole interface of a form. These rules state it:

- A branch sees the required run and nothing else of the enclosing scope. A
  variable a branch binds that shares the name of an input the header does
  not name is the branch's own.
- Sources (`$`, `$name`) and the rule set (`%`) are not logic variables and
  reach every scope.
- The enclosing scope sees the outputs and nothing else of a branch.
- An `or-default` or `or-default-join` decides its fallback per tuple of the
  required run. A branch runs over that tuple alone, so the decision is the
  same in every order the planner chooses for the clauses that read the
  outputs.
- A form runs once its required run is bound, wherever it was written. The
  refusal when nothing binds it is §2.4's.

### 2.3 The inference

A form without a header gets one by this rule, under the rule set the query
runs with:

- A variable inside the form is shared when it occurs in the interface of
  another clause of the enclosing list, or in what the enclosing scope
  exposes to that list. To the query's own list the enclosing scope exposes
  the variables of `:in`, `:find` and `:order-by`. To a branch it exposes
  the enclosing form's header. A clause's interface is what
  it provides and what it reads from its scope: a pattern's, expression's,
  predicate's or invocation's variables, a subquery's arguments and binding,
  a header form's header, and, for a headerless form in the same list, every
  variable of its branches short of a nested header. Every other variable is
  local to the branch that holds it.
- A shared variable the form binds in every branch is an output.
- Every other shared variable is required.
- A `not` binds nothing for its scope, so every shared variable of a `not`
  is required.

A branch binds a variable through a pattern, an expression's binding, a
subquery's binding, an invocation's free positions, or a nested form's
outputs. An invocation of a predicate with required positions consumes the
variables at those positions and does not bind them. That is why the rule
reads the rule set.

The rule is the same for `or` and `or-default`. The zero-default count of
`OR_FALLBACK_SEMANTICS.md` is the case it answers:

```clojure
[:find ?category ?count
 :where [?c :category/name ?category]
        (or-default [(q [:find (count ?p)
                         :in $ ?c
                         :where [?p :product/category ?c]]
                        $ ?c) [[?count]]]
                    [(ground 0) ?count])]
```

Both branches bind `?count`, so it is an output. The subquery reads `?c` and
the ground does not mention it, so `?c` is required. The header is
`[[?c] ?count]`, and each category gets its count or zero.

An `or` whose branches bind different variables is legal. In

```clojure
[:find ?q
 :where [?q :t/n _]
        (or [?q ?a ?v] [?q :t/tag :t/none])
        [(ground :t/tag) ?a]]
```

`?q` is shared and every branch binds it, so it is an output. `?a` is shared
and the second branch does not bind it, so it is required. The header is
`[[?a] ?q]`. The ground binds `?a` before the form in every written order,
and the query has one answer.

An output that another clause also binds is joined, never superseded. In

```clojure
[:find ?e
 :where [?w :p ?e]
        [?y :q ?e]
        (or-default [?e :p ?m]
                    (and [?m :p ?e] [?y ?a ?m]))]
```

`?e` is an output and `?y` is required. For each `?y`, the form answers the
`?e` values the first branch with a result produces. The enclosing scope
keeps a tuple when its `?e` is among them.

A `not` provides nothing, so every variable it shares is required. In

```clojure
[:find ?e
 :where [?e :p _]
        (not [?e :q ?x])]
```

`?e` is shared and required. `?x` occurs only inside the form, so it is
local, and the form holds for an `?e` that has no `:q` at all. The header is
`[?e]`.

### 2.4 Strictness

- A written header is read strictly.
- An `or-join` or `or-default-join` output that some branch does not bind is
  a query error naming the form, the branch by its position counted from
  one, and the variable, with the repair of moving the variable into the
  required run.
- A variable a `not-join` body reads and neither binds nor names in its
  header is a query error naming the form and the variable.
- A `not-join` header variable the body neither binds nor reads is a query
  error naming the form and the variable.
- Whether a branch binds a variable is read under the rule set.
- A variable a branch's predicate, expression, subquery input or required
  position reads, which neither the branch nor the header binds, is a query
  error naming the form and the variable.
- A form whose variables occur nowhere else in its scope is a query error
  naming the form.
- A required run that no written order binds before the form is a query
  error naming the form and the variable. Two forms that each partially bind
  one variable nothing else binds are refused by this rule.

A form sharing nothing is checked after `InferHeaders`' other refusals, so a
form that also reads a variable neither its branch nor its header binds is
refused naming the variable.

### 2.5 Subqueries and invocations

A subquery's inner query is a scope of its own. Its inputs are what its `:in`
declares and its call passes, and nothing else of the enclosing scope. Its
clauses are ordered like any scope's, never run in written order.

An invocation inside a scope is a clause at its planned position. An
invocation of a predicate with required positions is ordered after the
clauses that bind them, inside a branch as at the top level.

## 3. The contracts the tests pin

Each contract is pinned by a test the plan names, and the plan states for
each whether it is red on the tree it continues from or already green.

- The four worked examples of §2.3 give one answer on both planner paths in
  every written order.
- A form sharing nothing is refused naming the form.
- A required run no order binds is refused naming the form and the variable.
- A flat `or-join` header variable some branch does not bind is refused
  naming the form, the branch and the variable.
- A variable a `not-join` body binds that shares the name of an input its
  header does not name is the body's own.
- A branch variable that shares an outer variable's name reads nothing of
  the outer.
- An invocation of a required predicate inside a branch is ordered after its
  binder on both paths.
- A rule body holding an `or` whose branch holds a `not` with a variable of
  its own is a safe rule.
- Generated queries over random graphs, in both optimizer modes, match a
  reference computed in Go from the datoms under §2, and every query §2.4
  refuses is refused by the engine. That a refusal names the form and the
  variable is pinned by the step-3 tests the plan names.

## 4. The fact that decides the architecture

The interface of a headerless form is not a local property of the form.
Whether a variable inside it is shared depends on where else the variable
occurs. Whether a shared variable is an output depends on what every branch
binds, which depends on the rule set.

No reader that holds the form alone can compute its interface. `ScopeOf`
holds the clause alone. The executor holds the form and the relation in
hand. The algebra compiler holds the form and the symbols the fold has bound.
A reader that computes the interface from what it holds substitutes its own
context for the scope, and two readers with two contexts give two answers.
At `5b152c9` the readers that held a form alone, `ScopeOf`'s headerless
cases, the algebra compiler's headerless compilers and the executor's form
handlers, each did, and every disagreement between them was a defect.

So the interface is computed once, with the whole scope and the rule set in
hand, and written into the text as the header. After that it is local, and
every reader reads it.

The fact is a consequence of the rule §2.3 states. A different rule would
remove it, at a price. The rest of this section proves the fact, bounds it,
and states the trade the rule makes.

### 4.1 Two rules

A rule for a headerless form's interface is local when the interface is a
function of the form's text alone. It is non-local when the interface is a
function of the form and its scope. Two rules are in question for the
branch forms, `or` and `or-default`.

- The same-set rule, Datomic's rule for `or`: every branch mentions one set
  of variables, every variable of the set is shared with the enclosing
  scope, and a form whose branches mention different sets is refused.
- The occurrence rule, §2.3's: a variable inside the form is shared when it
  occurs elsewhere in its scope, and local otherwise.

Under both rules a shared variable every branch binds is an output, and
every other shared variable is required.

The same-set rule is local. The set is read from any one branch, since
every branch mentions it. Which of its variables every branch binds is read
from the branches. Nothing outside the form is read.

The occurrence rule is non-local. One form in two scopes shows it. The form
is

```clojure
(or [?q ?a ?v] [?q :t/tag :t/none])
```

Its first scope is the second worked example of §2.3:

```clojure
[:find ?q
 :where [?q :t/n _]
        (or [?q ?a ?v] [?q :t/tag :t/none])
        [(ground :t/tag) ?a]]
```

`?a` occurs in the ground, so it is shared. The second branch does not bind
it, so it is required. The header is `[[?a] ?q]`. The form holds for a `?q`
that carries `:t/tag` with some value, or carries `:t/tag` as `:t/none`.

Its second scope drops the ground:

```clojure
[:find ?q
 :where [?q :t/n _]
        (or [?q ?a ?v] [?q :t/tag :t/none])]
```

`?a` occurs nowhere else, so it is local. The header is `[?q]`. The form
holds for a `?q` that carries some attribute with some value, or carries
`:t/tag` as `:t/none`.

One text, two headers, two relations. No function of the text gives both.

### 4.2 Where the rules differ

The two rules agree on every variable every branch mentions. When it occurs
elsewhere in the scope, both rules share it and split it the same way. When
it occurs nowhere else, the occurrence rule makes it local, and nothing
changes: a local every branch binds gives the answer the shared variable
gives once the find boundary projects it away, and a local some branch
reads without binding is refused under either rule, by §2.4 under this one.

The rules differ only on a variable some branch mentions and another does
not. The same-set rule refuses every form that holds one. So the occurrence
rule is non-local only on forms the same-set rule refuses, and every form
the same-set rule accepts gets the same answer under either rule.

Such a variable cannot be an output. A relation has no unbound position,
and a branch that does not bind the variable would yield tuples with none.
The same-set rule meets the constraint by refusing the form. The occurrence
rule meets it by making the variable required: it is bound before the form
runs, so every branch's tuple carries it, and §2.4 refuses the query when
no written order binds it.

`not` has one body, so no variable of it is asymmetric. A rule under which
every variable of a `not` is shared with its scope is local, and it refuses
the fourth worked example of §2.3, where `?x` is an existential nothing
else names. The occurrence rule reads the existential from the scope, by
the same non-local reading.

### 4.3 The cost

The cost of the occurrence rule is action at a distance. A clause added
elsewhere in the scope that names a variable a branch held as a local makes
that variable shared. When the clause reads the variable and nothing binds
it before the form, §2.4 refuses the query, and the change is loud. When
the clause binds it, the form joins on it, and the change is silent: the
form's answer changes with no edit to the form.

Two facts bound the cost.

- It falls only on a variable some branch does not bind. For a variable
  every branch binds, the same-set rule shares it from the start, and the
  added clause joins on it under either rule. So the cost falls only on the
  forms the same-set rule refuses, the forms the occurrence rule admits.
- In Datalog a name used twice is a join. Every pattern in a query carries
  the same hazard for every variable it names, and the same-set rule
  removes one case of it.

### 4.4 The alternatives and the ruling

Four rules were weighed.

- (a) The same-set rule for `or` and `or-default`, with `not` under the
  occurrence rule. Local for the branch forms. An asymmetric form is
  written as `or-join` by its author, which is the design's own principle
  applied by hand. The language carries two interface rules.
- (b) The intersection rule: a headerless branch form is read as the header
  form whose outputs are the variables every branch mentions, with no
  required run. Local. A branch-only variable that shares an outer name
  does not unify with it, and nothing in the text says so.
- (c) The occurrence rule for `or`, `or-default` and `not`. Non-local. One
  rule for the three forms. Every form the same-set rule accepts gets the
  same answer, and the asymmetric forms are admitted at the cost of §4.3.
- (d) A header on every nested form, with no headerless form in the
  language. Local in the text as written. Every headerless form in every
  query is rewritten, and the inference has nothing to do.

The design's rule is (c) (§9). It is one rule for the three forms, which
goal 3 ranks over the compatibility (a) keeps. Its price is the one goal 2
names: a reader of a headerless form reads its scope to know its
interface. Two things answer the price. `Explain` plans the prepared query
(§5.1), so its phases carry the header `InferHeaders` wrote. The written
forms, `or-join`, `or-default-join` and `not-join`, state the interface in
the text wherever the scope would leave it unclear.

## 5. The architecture

Four stages, one job each, and an optional rewrite between the second and
the third:

```
parser.ParseQuery(text)          Query      syntax
query.Prepare(q, rules)          Query      shapes checked, every nested form stating its interface, every invocation resolved
algebra.Compile → passes → Decompile        optional: an equivalent, better query
QueryPlanner.PlanQuery(q, bindings, handler)   Plan       ordering
executor                         Relations  running
```

### 5.1 Preparation and the inference

`query.Prepare(q *Query, rules *RuleSet) (*Query, error)` takes a parsed
query and the rule set bound to `%` and returns the query ready to plan. It
composes the four static steps, each a function of the text and the rule
set: `ValidateInputShapes` on `q.In`, `ValidateStaticClauseShapes` on
`q.Where`, `InferHeaders`, and the check that a rule set was given when the
query invokes a predicate and that every invocation names a predicate it
defines at its arity. It runs once per execution,
before planning, and its result is not held on the query.

The executor's entry runs in this order: the cancellation check, the
`QueryInvoked` event rendering the query as received, `Prepare`,
`PlanQuery`, the vocabulary check against the roster the plan carries and
this execution's inputs, and execution. A preparation error completes the
query with that error, as a planning error does. `Explain`, `ExplainAlgebra`
and `Analyze` call `Prepare` before they plan. `Analyze` prepares the query
it plans and again in the execution it runs through `Query`'s path. Nothing
after preparation sees a headerless form.

`query.InferHeaders(q *Query, rules *RuleSet) (*Query, error)` returns `q`
with every `or`, `not` and `or-default` at any depth rewritten as the header
form that states its interface, by the rule of §2.3, and refuses every query
§2.4 names as a query error but the one its last bullet names. That one, a
required run no written order binds before the form, is the planner's
refusal on both paths, `orderClausesForCompile` on the algebra path and
`createPhasesGreedy` on the clause-based path, each naming the form and the
variable. It infers
a form's branches under the form's header and a
subquery's inner query as a scope of its own. A form on the path to a
rewritten form is copied, and every other clause is shared with `q`. A query
of header forms is returned as it is. The function is the one place the rule
of §2.3 exists.

`NewRuleSet` calls `InferHeaders` on each rule body once the heads are
collected, inferring the body as a query that finds the head with the
required run as its inputs, so the safety check, the magic rewrite, the
roster and the body planner read bodies of header forms. A body's shapes and
invocations are checked by `NewRuleSet`'s own validation.

Downstream of preparation there are three nested forms. `ScopeOf` panics on
a headerless form as on an unknown clause, on the planner's path and the
fold's alike. `orderClausesForCompile` reads every clause's scope before any
compiles, so the fold's path panics before a clause compiles.
`DefaultQueryExecutor.Execute`'s dispatch returns its unsupported-clause
error naming the clause's type. A skipped preparation therefore surfaces at
once.

### 5.2 The planner

`QueryPlanner.PlanQuery(q, bindings, handler)` takes a prepared query. The
planner orders
and phases. It computes no interface. Readiness is `ClauseReady` over
`ScopeOf`, and `ScopeOf` on a nested form reads its header alone: the
required run correlates, mandatory, and the outputs are provided. The gate
that held an `or`, `not` or `or-default` back by a closure of pending
clauses, the `not` check `createPhasesGreedy` carried, and the inference
forms' arms of the clause scoring have nothing to read and do not exist.

The planner's scheduling preferences stand. `patternDependsOnPendingExpression`
holds a pattern back while a pending expression binds one of its variables,
and `subqueryDependsOnPendingProvider` and `invocationDependsOnPendingProvider`
hold a clause back while a pending selectable clause does, so a binder is
placed before its consumer. They order and decide no interface.

Both optimizer modes phase a prepared query. With the algebra engine on,
the planner phases the clauses the decompiler returns. With it off, the
planner phases the prepared clauses.

### 5.3 The algebra engine

`algebra.Compile(q *query.Query, rules *query.RuleSet) (*Node, error)` takes
a prepared query and the rule set its invocations are ordered under. It is
`Query → tree`, the passes are `tree → tree`, and `Decompile` is
`tree → clauses`, which `optimizeViaAlgebra` sets into a query of the
prepared query's parts. The engine is a rewrite from a query to an
equivalent query.
It infers nothing and is optional. Where a header came from is not a fact
the engine reads: an or-join the inference wrote and the same or-join a user
wrote are one query, and the engine's output is a function of that text
alone.

Each header form compiles to the operator `main` compiles it to, in `main`'s
shape, with the header where the operator's keys come from:

- `not-join` compiles to an `AntiJoin` keyed on its header. The body
  compiles under the header alone.
- `or-join` compiles to a `Union`. `Output` is the operator's schema, the
  required run and the outputs. `RequiredVars` and `OutputVars` are the
  header's two halves, `LateralUnion`'s fields with the same meanings.
  `Required` is the required run and the outputs the current relation
  binds. `Output` and `Required` keep each operator's own meaning: a
  `LateralUnion`'s `Output` is the outer schema with the symbols every
  branch produces, and its `Required` is the outer schema. The branches
  compile under the required run alone.
- `or-default-join` compiles to a `LateralUnion` whose outer schema is the
  current relation's symbols and the required run. The branches compile
  under the required run alone. The subquery lowering to a `LateralJoin`
  with defaults fires only when branch 1 is one subquery and branch 2 is
  ground expressions alone.
- A `LateralJoin` carries the rule set its call's `%` binds, so the
  decorrelation pass compiles the inner query it rewrites under that rule
  set and the inner `:in`.

The fold orders a form after the clauses that bind its required run.
`orderClausesForCompile` reads `ScopeOf`, which on a header form reports the
required run as mandatory correlates, so a form's position in the fold
depends on its header and never on where it was written.

The decompiler renders every header: an or-join's required run and outputs,
a not-join's header, and an `or-default-join` for every fallback. A
fallback's header comes from the operator the fallback compiled or rewrote
to:

- From a `LateralUnion`, its `RequiredVars` and `OutputVars`.
- From a `LateralJoin` with defaults, where the lowering fired, its
  `CorrelationVars` as the required run, or the symbols its binding shares
  with the outer when its call passes no variable, and the binding's other
  symbols as the outputs.
- From a `LeftOuterJoin`, decorrelation's or the get-else rewrite's, its
  `JoinSymbols` as the required run and the right child's other symbols as
  the outputs, the rewrite's default branch a `get-else` expression.

### 5.4 The executor

The executor runs three nested forms, each a scope with a header. One
runner, `runScope(ctx, scope, outer)`, serves them and the subquery. A scope
is a header and a clause list: each form's handler builds one from each
branch, and the subquery's handler one from its inner query's `:in`, `:find`
and `:where`.

The runner frames the scope as a query finding its header, whose `:in` is
`$` and `%` and whose `:where` opens with an invocation of the minted
predicate `scope/required` over the required run, followed by the scope's
clauses. A scope whose required run is empty reads no group, so its outer
relation is the unit relation. It opens with `(scope/required)` over that
relation's zero-symbol projection, a verdict that holds, and runs once. An
empty group standing beside it is a group the required run does not read.
`SeedRestricted` and
`engineNamespace` read a fourth kind, `scope`, beside `delta`, `magic` and
`adorned`. The planner therefore leads with `scope/required`. A rule set may
not declare into `scope`. The text carries the binding, so the plan caches
under the text.

The required run enters the scope as a relation:

- The projection of `outer` joined with the environment's tuple onto the
  required run is held in the scope's context through `Context.WithDerived`.
- A required variable the environment alone binds thereby takes its value in
  every tuple of the projection.
- The map it is held in carries every relation the enclosing context holds
  beside it.
- `evaluateStratum` holds each delta under its delta symbol in the map that
  `stratumEvaluation.run` enters through the same method.
- `invocationSource` reads the projection at the invocation as it reads a
  delta.
- The scope's context carries no environment: a query input reaches a branch
  only through the header.
- Every caller of `WithDerived` outside the tests is in the rules evaluator,
  `datalog/executor/rules.go`.

Of the three form handlers the runner replaces, `executeOrDefaultJoinClause`
keys the outer relation per tuple through `OrFallbackRelation` on the
required run. `executeOrJoinClause`, through
`executeOrJoinClauseCorrelatedUnion`, keys it on the outputs.
`executeNotJoinClause` runs the body through `executeInnerClauses` once per
key combination, through `filterWithNotJoinClause`.

The runner runs each scope's plan once. Then:

- `or-join` unions the branch results over the header and joins the outer
  relation on the required run and the outputs.
- `or-default-join` runs branch 1 over the projection, branch 2 over the
  projection minus the tuples of the required run branch 1 answered, and so
  on, and joins the outer relation on the required run and the outputs.
- `not-join` anti-joins the outer relation on the header with the body's
  result.
- An output the environment binds is joined with the environment's tuple,
  as an output another clause binds is joined with that clause's relation
  (§2.3).
- A relation group the required run does not read stands as it is, unjoined,
  until the find boundary.
- `Relations.Collapse` joins only groups that share a symbol.
- The find boundary of `DefaultQueryExecutor.Execute` returns the group
  holding every `:find` symbol, or the product of the groups that together
  hold them. It asks every group outside that set for one tuple, by one
  pull of the group's iterator, and a group with none empties the result.
- A subquery runs once over the relation of its argument combinations.
- An aggregating inner query carries its inputs as group keys.
- A tuple or scalar binding keeps its one-tuple-per-combination check, per
  group.
- An argument bound to an attribute position is checked against the
  vocabulary once, over the relation of combinations, before the run.
- An invocation inside a scope evaluates at its clause, seeded from the
  scope's bindings.

A rule body already runs this way through `planBody`. The join on the
outputs is what makes an output that another clause binds a join and never a
supersession (§2.3's third example).

### 5.5 The language package

`query` owns the forms, the header grammar, `ScopeOf`, `InferHeaders` and
`Prepare`. `OrJoinClause` carries `RequiredVars` and `OutputVars`, the names
`OrDefaultJoinClause` gives the same halves of its header. `NotJoinClause`
carries its header as `RequiredVars`, the name its all-required header has.
The parser,
`qb.OrJoin` with `Require`, and `FormatQuery` carry the grammar, so every
phase the planner emits parses back to itself.

Each header form has a `Validate` method, `OrJoinClause`'s new, checking the
shape the text alone decides: a non-empty header for every form, and for
the two branch forms at least two branches and a required run disjoint from
the outputs. `ValidateStaticClauseShapes` calls the three beside
`SubqueryPattern.Validate` and `FunctionPredicate.Validate`. Whether every
branch binds every output, and whether a body reads
a variable it neither binds nor names in its header, are `InferHeaders`'
checks, under the rule set, since an invocation's binding depends on it.

`NewRuleSet` infers each body once the heads are collected and holds each
rule with its body inferred, and `checkRuleSafety` reads the inferred body.
A branch input nothing binds is `InferHeaders`' refusal (§2.4), a typed
error carrying the form and the variable. `NewRuleSet` returns that refusal
as `UnsafeRuleError` naming the rule and the variable, so a rule holding
one is refused at build.

The magic rewrite reads headers, and its order comes from its caller:

- `Magic` takes a function from a scope, its header and its clauses, to the
  scope's planned clauses, which the executor implements by planning the
  scope as the runner frames it (§5.4) and reading the phases' clauses after
  the `scope/required` invocation that opens them.
- The planned clauses are the plan's own, in phase order, in the
  decompiler's shape under the algebra mode, since the rewritten body is
  planned again and parses back. The plan the rewrite reads is made with
  join-project insertion off, so no decompiled projection stands between a
  rule and its invocations.
- `rewriteBody` walks the clauses that function returns and computes no
  order of its own: the planner imports `query`, and the order depends on
  the optimizer mode.
- What is bound at an invocation at a body's top level is the head's bound
  positions and what the planned clauses before it provide. The body is the
  scope whose header is the head, with the bound positions as its required
  run.
- What is bound at an invocation inside a form's branch is the form's
  required run and what the branch's planned clauses before it provide. The
  branch is the scope whose header is the form's.
- The magic rule reads the planned clauses before the invocation.
- The magic rule of an invocation inside a form's branch reads the body's
  planned clauses before the form and the branch's planned clauses before
  the invocation. `passing.within` keeps the body's prefix alone, so the
  design adds the branch's clauses to it, with the branch's locals renamed
  apart from the body's variables: a local sharing a body variable's name
  outside the header (§2.1) would otherwise unify with it and narrow the
  magic set below what the adorned rule's guard needs.
- A negation is not read, since it binds nothing, and a form is read with
  each negation at any depth left out.
- A fallback form is read as the union of its branches over its header,
  which binds what the form binds and holds every tuple the form yields.
- An invocation under a negation is not adorned: its predicate joins the
  rewritten set unchanged and is derived whole in its own stratum, so the
  rewritten set's negative edges are the original's and every rewrite
  stratifies.
- A predicate with required positions under a negation has no evaluation:
  a negated invocation is never adorned, and a required predicate is never
  derived whole. A rule invoking one under `not` or `not-join`, at any
  depth short of a subquery body, is refused by `NewRuleSet`, since every
  invocation reaching the rule goes through the rewrite and none can run.
  The refusal reads the invocation edges the dependency walk collects, each
  marked negative when its invocation sits under `not` or `not-join` at any
  depth, against the invoked predicate's required positions.
  `forInvocations` passes the negative mark into a subquery body unchanged
  and carries no record of the body, so the design adds one: the walk gains
  a mark that an invocation sits inside a subquery body, and the refusal
  leaves such an invocation out, since the subquery's clause evaluates it
  with the subquery's bindings (§2.5). The refusal is a typed error naming
  the rule and the predicate.
- A free invocation of a predicate whose needed strata hold a predicate
  with required positions is rewritten too, its head all free, so each
  required predicate is adorned from the clauses before its invocation.
  `NewRuleSet` refuses no recursion between a required predicate and a free
  one. A positive invocation whose planned clauses before it leave a
  required position free is refused at the rewrite as
  `RequiredBindingError`.

The roster reads headers: an attribute variable inside a form is the form's
own unless the header names it. The plan holds the roster the prepared text
names under no rule set, collected once at planning, and the vocabulary
check reads it there. The keywords the query's invocations pass at positions
the rule set reads as attributes are read at the check through
`RuleSet.invocationAttributes`, which maps the plan's invocation constants
through the rule set's attribute positions on each execution. The rule
bodies' own roster is the rule set's, collected once.

## 6. The components against `5b152c9`

The baseline is `5b152c9`. The algebra rows hold on `main` as well, which
is the baseline goal 4 names.

| Component | `5b152c9` | This design |
|---|---|---|
| `query.ScopeOf` | computes a headerless form's interface from its branches | reads a header, with no headerless case |
| `query.InferHeaders` | absent | the one computation of §2.3 |
| `query.Prepare` | absent | the four static steps as one function, four callers |
| `OrJoinClause` | `JoinVars` | `RequiredVars`, `OutputVars` |
| planner gate | `inferenceFormDependsOnPendingProvider` over the pending closure | absent |
| `algebra.Compile` | `Compile(q)`, with `compileOr`, `compileNot` and `compileOrDefault` taking keys from the fold's symbols | `Compile(q, rules)`, with three compilers for the header forms |
| `algebra.Union` | `Output`, `JoinVars`, `Required` | `Output`, `RequiredVars`, `OutputVars`, `Required`, the fields of `LateralUnion` |
| `algebra.LateralJoin` | no rule set | `Rules`, the set its call's `%` binds |
| executor nested forms | six handlers, branches reading the outer relation's free variables | three, each a scope run over its required run |
| executor invocations in a scope | hoisted before the scope, over the whole outer relation | at the clause, seeded from the scope |
| subquery bodies | run in written order | ordered by the planner |
| rule bodies in `RuleSet` | as given | inferred |

The algebra package's shape is `main`'s:

- One operator per form, one compiler per operator, the passes and the
  decompiler as they are.
- `compileOrJoin` keeps its two union routes as on `main`: the one for
  branches holding no `not-join` or `missing?`, and `compileOrUnionCorrelated`,
  which compiles the branches over a placeholder for the required run, how
  an anti-join inside a branch reads the header.
- The compilers for the headerless forms are gone, since their input never
  arrives.
- `Union` carries the header's two halves under the names `LateralUnion`
  already uses.
- `JoinVars`, a second name for `Output` once every union comes from an
  or-join, goes.
- `AntiJoin.ExplicitJoin`, which `compileNot` and `compileNotJoin` both set
  true on `main`, goes with its readers, and `decompileAntiJoin` emits a
  `NotJoinClause` alone.
- Nothing in the package gains an argument beside the rule set `Compile`
  orders invocations under.

## 7. The two oracles

The algebra engine on against off checks whether the optimizer preserves
meaning: compile fidelity, decorrelation, the get-else rewrite, project
insertion, and any order the decompiler produces, each against the
clause-based path on the same prepared query. The optimizer-mode matrix runs
every query under both.

The Go reference in the differential tests checks the semantics of §2: an
interpreter of the generated query, from which the text the engine parses
is rendered, over the generated datoms, and for the rules shapes the
closures `closureOf` and `parityClosureOf` compute, which share no code
with the inference, the planner or the executor. When the two
engine paths disagree, the matrix says that one is wrong, and the reference
says which. When both are wrong alike, only the reference sees it.

Each oracle checks one question. The inference and the executor's handling
of header forms are shared by both paths and are checked by the reference.

Cost is measured, not claimed. The path the runner replaces under
`or-default-join` evaluates a cacheable branch once, narrowed to the outer
relation's join keys through `OrFallbackIterator.outerJoinKeys`, and probes
it per outer tuple. It runs every other branch per outer tuple through
`executeInnerClauses`. The path it replaces under `or-join`,
`OrFallbackIterator.nextCorrelatedUnion`, runs every branch per outer tuple
through `executeInnerClauses`. The path it replaces under `not-join` runs the
body once per key combination through `executeInnerClauses`. The runner seeks
through the matcher's binding-driven scan, `HashJoinScan` or its merge join
in `hash_join_matcher.go`, as a top-level pattern with bound symbols does.
The plan names the benchmarks that compare them, and the one that measures
preparation, which runs per execution.

## 8. The engine's vocabulary

The principle of §4, applied to the schema: the engine's words are declared
once, into every running schema, and every reader reads them there. The
engine writes and reads
attributes it never declares: `:db/ident` and the other six words of a
declaration, `:db.attribute/declaration`, `:db/txInstant`, `:db/id`, which
pull patterns name, and the snapshot and branch words. Under the design they are a schema the engine installs into
every running schema. It is installed in memory and never declared as
facts, and each of its words resolves by its declaration, never by
inference, so an attribute entity's ref to its current declaration resolves
to one declaration whatever the ops hold.

- A user's write of an engine attribute is refused in
  `validateAttributeStorable`, which `Transaction.Add`, `Transaction.Set`,
  `Transaction.Remove` and `Transaction.Retract` run on every datom's
  attribute, so the check takes the transaction beside the keyword.
- Import's replay is exempt, as it is from every schema check.
- The engine's own writers, `assertSchemaFacts`, `addBranchRecord` and the
  marker `Snapshot` writes, pass because the transaction the engine opens
  for itself carries that provenance, which the check reads.
- A given schema that redeclares an engine word is refused where a schema is
  given, at open and at `SetSchema`.
- `Schema.Add` panics on an ident longer than `datalog.MaxAttributeBytes`,
  as it panics on a wrong-vocabulary keyword, since a definition is
  constructed there, and `ParseSchema` returns the error.
- `SetSchema` and `adoptDeclarations` compose the whole next state, validate
  it, and then publish it under the lock, so no field records a step that
  failed.
- `adoptDeclarations` composes the branch's running schema from the
  definitions its store states after the parent's declarations are asserted,
  as an open composes its own, so the branch runs with what its store
  states.
- A cache entry carries the resolution rule it was resolved under,
  cardinality and uniqueness together.

## 9. Rulings

Ruled 2026-10-14:

- Asymmetric `or` branches stay legal. "I agree about the permissive
  derivation."
- `or-default` infers its keys from the text. "I agree about the or-default
  behavior."
- Subqueries take the runner in the same change, after the three nested
  forms.
  "If we don't have subqueries ride it, does it make things more complicated
  and not follow the clarity that Janus Datalog strives for?"
- One interface rule for `or`, `or-default` and `not`. "I agree with your
  Q1-Q4 inferences."
- Headers are read strictly.
- A variable a body binds that shares the name of an input its header does
  not name is the body's own.
- Sources and `%` stay ambient.
- The algebra package keeps `main`'s clarity, with no accreted arguments.
  "Should we reset the algebra package to `main`, and then start from there?
  Remember, do not accrete arguments." "The goal was to retain the `algebra`
  package's clarity against `main`."
- A lateral join carries the rule set bound to `%`. "Yes, it should carry
  the ruleset bound on %."
- The algebra engine is a drop-in oracle of correctness. "It's an oracle of
  correctness, isn't it?"
- Inference is one pass. "Keep inference as one pass."
- The inference is one stage in `query`, called before either planner path,
  derived in §4 from the interface being non-local, and ratified: "Do it."
- `Union` carries `LateralUnion`'s fields, and `JoinVars` goes as a second
  name for `Output`. "1, 2, 3. Agreed."
- Preparation is one function, `query.Prepare`, with four callers.
- The runner for the forms lands before the algebra package's clarity work,
  and the runner for subqueries after it.
- A scope's required run enters its query as a relation held in the context
  and read by an invocation of `scope/required`, as a delta enters a body's
  context (§5.4). Excluded: the required run as a
  `query.RelationInput` in `:in`, which
  `executeRealizedWithRelationInputIteration` runs the plan once per tuple
  of, and as the planner's bound symbols, which keeps the plan out of the
  cache.
- The magic rewrite takes each body's planned order from the executor
  (§5.5). This supersedes the first sentence of the decision in
  `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §9, "Sideways information
  passing in the rewrite is left to right. It stops at a subquery." Its
  second sentence stands. Excluded: a readiness walk in `query`, a third
  orderer beside the planner and the algebra fold.
- An invocation inside a scope is seeded from its own scope's bindings
  (§5.4). This supersedes the held union of two invocations adorned alike,
  which `hoistWalk` makes.
- A subquery's arguments bound to attribute positions are checked once,
  over the relation of its combinations (§5.4). This supersedes the check
  once per combination of its arguments.
- A rule set holds each body with its headers inferred (§5.1). This
  supersedes `newRuleSet` storing the rules as given, which guarded against
  a field the text does not render.
- The fallback lowering to a `LateralJoin` with defaults fires only when
  branch 1 is one subquery and branch 2 is ground expressions alone (§5.3).
  The failure it removes: at `5b152c9` the lowering fired with a predicate
  beside the subquery and with a `not` beside the ground, and applied the
  predicate outside the fallback and the `not` not at all.
- The roster is held with the plan, collected once at planning from the
  prepared text (§5.5). This supersedes the roster held on `query.Query`
  under a `sync.Once`, `rostersOnce`.

Ruled 2026-10-15, "Accept all.":

- A magic rule reads a fallback form as the union of its branches over its
  header (§5.5).
- A negated invocation is never adorned, and its predicate is derived whole
  in its own stratum (§5.5). This supersedes the passing sealed at the first
  same-stratum invocation, the decline of a rewrite that does not stratify,
  and the error for a declined rewrite of a required predicate, since the
  rule removes their cause.
- The find boundary asks every group it does not return whether it is
  empty, and an empty one empties the result (§5.4). This supersedes the
  discard of an unread group.
- §8 is ratified: the engine's words are a schema installed in memory into
  every running schema, never declared as facts. This supersedes the
  decision that the engine's words get no definitions, stored or in memory,
  which was made while the refusal was gated on schema facts being present.
- `Magic` takes a function from a scope to its planned clauses, and an
  invocation inside a branch is bound by the form's required run and the
  branch's planned clauses before it (§5.5).
- An output the environment binds is joined with the environment's tuple
  (§5.4).

Ruled 2026-10-15, later the same day:

- The occurrence rule stands for `or`, `or-default` and `not`, and §4
  carries the proof that it is non-local, the bound, the cost and the
  alternatives. "Yes, (c), but we do need to document the trade and why
  with maximum clarity." Excluded, each by §4.4: the same-set rule for the
  branch forms, the intersection rule, and a header on every nested form.
