# Nested Scopes as Queries

Every nested form of a query states its interface in a header. The header
is computed once, from the text and the rule set, before the planner or the
algebra engine sees the query. Every component after that point reads the
header and computes nothing about it. This document states the goals the
design answers to, the semantics of the forms, the architecture that
delivers them, and what each component is against `5b152c9`.

## 1. Goals

The design answers to these eleven goals. Goal 10 is a product choice the
others do not settle, with the alternatives' costs beside it.

1. The most correct fix, not the tactical one: the design question is
   derived to the most correct fix for Janus Datalog.
2. Clarity is not diluted: Janus Datalog's unusual clarity is kept.
3. Correct and consistent outranks compatible: compatibility is broken
   where conceptual correctness and consistency require it.
4. The `algebra` package keeps `main`'s clarity.
5. No accreted arguments.
6. The algebra engine is a drop-in oracle of correctness: it can be dropped
   in and out, and its job is to check the other path.
7. Each component does its own job, and inference is one pass.
8. The architecture is simple.
9. The design is concrete: a design, not a collection of rulings.
10. The language keeps headerless `or`, `or-default` and `not`, the
    existential `not` among them. The alternatives' costs: under a header
    on every nested form, every headerless form in every query is rewritten
    by its author; under the uniform same-set rule, the fourth example of
    §2.3 is refused and written as `not-join`.
11. The engine's own queries run on every database it opens, whatever the
    user's schema declares and whether or not the database refuses unknown
    attributes.

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
  not name is the branch's own. The alternative is inputs ambient in every
  scope. Under it a written form's interface depends on the query's `:in`,
  which no reader holding the form computes (§4), and at `5b152c9` a
  branch-local sharing an outer variable's name was adorned bound, the
  failure §3 pins.
- Sources (`$`, `$name`) and the rule set (`%`) are not logic variables and
  reach every branch. A subquery's inner query takes the sources and the
  rule set its call passes (§2.5). The alternative is sources and the rule
  set passed through a form's header. A form cannot pass another source or
  rule set, as a subquery's call can, so such a header would restate the
  query's own.
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

The rule is the same for `or` and `or-default`. §4.4 weighs the one rule
for `or`, `or-default` and `not` against the alternatives. The alternative
for `or-default` is a
headerless `or-default` with no required run, its decision global, and
per-tuple keys declared through `or-default-join` alone. That is a second
interface rule, for one form beside the rule for `or` and `not`, which goal
3 excludes. The zero-default count of `OR_FALLBACK_SEMANTICS.md` is the case
the rule answers:

```clojure
[:find ?category ?count
 :where [?c :category/name ?category]
        (or-default [(q [:find (count ?p)
                         :in $ ?c
                         :where [?p :product/category ?c]]
                        $ ?c) [?count ...]]
                    [(ground 0) ?count])]
```

Both branches bind `?count`, so it is an output. The subquery reads `?c` and
the ground does not mention it, so `?c` is required. The header is
`[[?c] ?count]`, and each category gets its count or zero.

An `or` whose branches bind different variables is legal (§4.4). In

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

- A written header is read strictly. The alternative is a tolerant header,
  under which a branch's read of a variable the header does not name is
  bound from the enclosing scope. Under it a `not-join` body reading an
  unnamed outer variable is bound to it, and the same query with that
  variable renamed answers differently, against §2.1's rule that renaming
  a local changes nothing.
- An `or-join` or `or-default-join` output that some branch does not bind is
  a query error naming the form, the branch by its position counted from
  one, and the variable, with the repair of moving the variable into the
  required run.
- A variable a `not-join` body reads and neither binds nor names in its
  header is a query error naming the form and the variable, with the repair
  of naming the variable in the header or binding it in the body.
- A `not-join` header variable the body neither binds nor reads is a query
  error naming the form and the variable, with the repair of dropping the
  variable from the header or reading it in the body.
- Whether a branch binds a variable is read under the rule set.
- A variable a branch's predicate, expression, subquery input or required
  position reads, which neither the branch nor the header binds, is a query
  error naming the form and the variable, with the repair of binding the
  variable in the branch or naming it in the header.
- A form whose variables occur nowhere else in its scope is a query error
  naming the form, with the repair of naming in the form a variable the
  scope binds.
- A required run that no written order binds before the form is a query
  error naming the form and the variable, with the repair of binding the
  variable before the form. Two forms that each partially bind one variable
  nothing else binds are refused by this rule.

A form sharing nothing is checked after `InferHeaders`' other refusals, so a
form that also reads a variable neither its branch nor its header binds is
refused naming the variable.

### 2.5 Subqueries and invocations

A subquery's inner query is a scope of its own. Its inputs are what its `:in`
declares and its call passes, and nothing else of the enclosing scope. Its
clauses are ordered like any scope's, never run in written order.

A subquery's binding names the inner query's outputs in the enclosing scope,
and a binding asserts nothing about cardinality (§5.4). A name written at two
places of a binding unifies the two outputs it names, as a variable repeated
across an invocation's arguments does: the tuples whose two outputs agree
enter the scope under the one name. Its spellings are
the relation form, `[[?a ?b] ...]`, and the collection form, `[?a ...]`, a
relation of one symbol. The scalar form `?a` and the tuple form `[[?a ?b]]`
are refused, naming the relation form: in the grammar of a query's inputs
each names a single value, and a binding is never one. The alternative is
every spelling as one meaning, under which `[[?a ?b]]` reads as one tuple
and answers a relation. Goal 3 ranks the consistent grammar over the
compatible one.

An invocation inside a scope is a clause at its planned position. An
invocation of a predicate with required positions is ordered after the
clauses that bind them, inside a branch as at the top level.

An invocation of a predicate of its rule's own stratum stands inside a
subquery whose inner query does not aggregate, as it stands inside an `or`
or `or-join` branch: the subquery joins the inner query's result with the
scope over its combinations, monotone as a branch is. Under an aggregating
inner query, at any depth, it is refused, since aggregation inside a
recursive component is refused
(`RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.2).

## 3. The contracts the tests pin

Each contract below is the subject of one test.

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
  variable is a contract of its own, beside the differential.

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

Its first scope is the second worked example of §2.3, where a ground binds
`?a` after the form. `?a` occurs in the ground, so it is shared. The second
branch does not bind it, so it is required. The header is `[[?a] ?q]`. The
form holds for a `?q` that carries `:t/tag` with some value, or carries
`:t/tag` as `:t/none`.

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
every variable of a `not` is shared with its scope, the uniform same-set
rule of §4.4's (e), is local, and it refuses the fourth worked example of
§2.3, where `?x` is an existential nothing else names. The occurrence rule
reads the existential from the scope, by the same non-local reading.

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

### 4.4 The alternatives

Five rules state a headerless form's interface:

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
- (e) The uniform same-set rule: the same-set rule for `or` and
  `or-default`, and for `not` every variable of the body shared with its
  scope. Local for every form. The fourth worked example of §2.3 is
  refused and written as `not-join`.

The design's rule is (c). Goal 3 excludes (a), which carries two interface
rules, since consistency outranks the compatibility (a) keeps. §4.3's fact
excludes (b): in Datalog a name used twice is a join, and under (b) a
branch-only variable sharing an outer name is not joined with it, with
nothing in the text to say so. Two rules remain that no goal of §1
excludes: (d) and (e). The choice of (c) over them
is goal 10, which states the alternatives' costs. The price of (c) falls
on goal 2's clarity: a reader of a headerless form reads its scope to know
its interface. Two things answer the price. `Explain` plans the prepared query
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
before planning, and its result is not held on the query. The alternative
is each caller composing the four steps itself, four copies of one fact the
callers must agree on, under which two boundaries that compose the steps
differently refuse different queries.

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
of §2.3 exists. Two alternatives are excluded. Inference at each reader is
excluded by §4: two readers with two contexts give two answers. Inference
in the planner is excluded by §5's stage order, under which
`algebra.Compile` runs before `QueryPlanner.PlanQuery`, and by §4, under
which an engine inferring for itself is a second reader.

`NewRuleSet` calls `InferHeaders` on each rule body once the heads are
collected, inferring the body as a query that finds the head with the
required run as its inputs, so the safety check, the magic rewrite, the
roster and the body planner read bodies of header forms. A body's shapes and
invocations are checked by `NewRuleSet`'s own validation. The alternative is
a rule set holding each body as given, with the inference at each of those
four readers, which goal 7 excludes. Holding the rules as given guarded
against a field the text does not render. An inferred body holds header
forms, which the text renders, and every phase the planner emits parses
back to itself (§5.5), so no such field exists.

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
holds a pattern back while a pending expression that can run before it binds
one of its variables, and `subqueryDependsOnPendingProvider` holds a subquery
back while a pending clause provides one of its binding variables and waits,
through the pending clauses, on no output of the subquery, so a binder is
placed before its consumer and no gate waits on the clause it holds
(`REWRITE_ORDER_PROOF.md` §6). `invocationDependsOnPendingProvider`
holds an invocation with no available argument back behind any pending
selectable clause that provides one of its arguments, and an invocation with
an available argument behind a provider with visible selectivity alone, since
a scan of a whole extent placed before a bound invocation multiplies its
magic set by the scan's extent (`REWRITE_ORDER_PROOF.md` T2, T3). The
selection within a phase takes a rank before the score: a clause without
visible selectivity, a constant beyond a pattern's attribute or an
invocation's name, a variable available when the clause is selected, or a
seed-restricted predicate, is selected only when no other clause is
selectable, and among the rest the scores stand as they are
(`REWRITE_ORDER_PROOF.md` §6). An
expression that waits, through the pending clauses binding what it needs, on
a variable the pattern itself binds cannot run before it: the pattern runs
first, and the expression unifies with the variable the pattern bound. They
order and decide no interface.

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
  header's two halves, `LateralUnion`'s fields with the same meanings. The
  alternative is the halves under names of `Union`'s own, two names for one
  fact across the two operators.
  `Required` is the required run and the outputs the current relation
  binds. `Output` and `Required` keep each operator's own meaning: a
  `LateralUnion`'s `Output` is the outer schema with the symbols every
  branch produces, and its `Required` is the outer schema. The branches
  compile under the required run alone.
- `or-default-join` compiles to a `LateralUnion` whose outer schema is the
  current relation's symbols and the required run. The branches compile
  under the required run alone.
- The lowering of an `or-default-join` to a `LateralJoin` with defaults
  fires only when branch 1 is one subquery and branch 2 is ground
  expressions alone. The failure the condition removes: at `5b152c9` the
  lowering fired with a predicate beside the subquery and with a `not`
  beside the ground, and applied the predicate outside the fallback and the
  `not` not at all.
- A `LateralJoin` carries the rule set its call's `%` binds, so the
  decorrelation pass compiles the inner query it rewrites under that rule
  set and the inner `:in`. The alternative is a trailing rule-set parameter
  on both pass constructors, a second home for a fact the node's call
  carries, which goal 5 excludes.

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
and `:where`. The alternative is a runner of the subquery's own. Running
the inner query in its written order is excluded by §2.5, and running an
inner query holding an invocation per combination by the cost goal 2 of
`SCOPE_EXECUTION_AND_CONTRACTS.md` names, that invocation seeding one
fixpoint per combination. The window an inner query runs over is that
design's §4.2, stated for the subquery in its §4.4. A second runner with
neither defect is a second component for one job, which goal 7 excludes.

The runner frames the scope as a query finding its header, whose `:in` is
`$` and `%` and whose `:where` opens with an invocation of the minted
predicate `scope/required` over the required run, followed by the scope's
clauses. A scope whose required run is empty reads no group, so its outer
relation is the unit relation. It opens with `(scope/required)` over that
relation's zero-symbol projection, a verdict that holds, and runs once. An
empty group standing beside it is a group the required run does not read.
`scope` is the fourth of the engine's namespaces
`RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.1 states, and its §1.3
states the scheduling that leads with `scope/required`. The text carries
the binding, so the plan caches under the text. Two alternatives are excluded. The required run as a
`query.RelationInput` in the scope's `:in`:
`executeRealizedWithRelationInputIteration` (`datalog/executor/executor.go`)
runs a plan once per tuple of such an input, where a branch holding an
invocation runs over the window with its fixpoint continued across the
windows, one evaluation deriving each tuple once
(`SCOPE_EXECUTION_AND_CONTRACTS.md` §4.2). The required run as the
planner's bound symbols, which keeps the plan out of the cache.

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
- Before this design every caller of `WithDerived` outside the tests is in
  the rules evaluator, `datalog/executor/rules.go`. `runScope`
  (`datalog/executor/scope.go`) is the second.

At `5b152c9` the three form handlers ran this way: `executeOrDefaultJoinClause`
keyed the outer relation per tuple through `OrFallbackRelation` on the
required run; `executeOrJoinClause`, through
`executeOrJoinClauseCorrelatedUnion`, keyed it on the outputs;
`executeNotJoinClause` ran the body through `executeInnerClauses` once per
key combination, through `filterWithNotJoinClause`. Under this design the
two branch forms' handlers construct `OrFallbackRelation` on the primitive
of `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.1, as its §4.3 states, and the
`not-join` handler runs its body once per window over the window's distinct
projection onto its header (that design's §4.4).

The window a scope's plan runs over is `SCOPE_EXECUTION_AND_CONTRACTS.md`
§4.2's. Then:

- `or-join` unions the branch results over the header and joins the outer
  relation on the required run and the outputs.
- `or-default-join` answers each tuple of the required run from the first
  branch with a result for it, and joins the outer relation on the required
  run and the outputs (`SCOPE_EXECUTION_AND_CONTRACTS.md` §4.4).
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
  pull of the group's iterator, and a group with none empties the result
  (`SCOPE_EXECUTION_AND_CONTRACTS.md` §3 derives the verdict).
- An iterator filling a group's replay cache is read to its end before it
  is closed, so the cache is whole for the group's later readers.
- A product ending on an empty relation reads the iterators it has pulled
  to their ends the same way, and never opens the relations after the empty
  one.
- A subquery is a scope under the same window, run over the relation of its
  argument combinations (`SCOPE_EXECUTION_AND_CONTRACTS.md` §4.4).
- An aggregating inner query whose body reads an input carries its inputs
  as group keys, and one whose body reads no input is a pure aggregate,
  aggregated once and paired with every run tuple, since adding group keys
  to a pure aggregate changes its kind
  (`SCOPE_EXECUTION_AND_CONTRACTS.md` §4.4).
- The binding names the inner query's outputs in the enclosing scope, and
  the result enters the scope as the relation over them; a binding asserts
  nothing about cardinality (`SCOPE_EXECUTION_AND_CONTRACTS.md` §4.4).
- An invocation inside a scope evaluates at its clause, seeded from the
  scope's bindings. The alternative is the hoist: at `5b152c9` `hoistWalk`
  derived a scope's invocations before the scope, over the whole outer
  relation. Its failures: an invocation seeded from the whole outer relation
  rather than the header, and one derived before any clause of the form had
  run, so an argument the form's own clauses bind counted as unbound.

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
shape the text alone decides: a non-empty header for every form, at least
one branch for an `or-join`, at least two for an `or-default-join`, and for
the two branch forms a required run disjoint from the outputs. A union of
one branch is that branch over the header, the form the inference writes
from a one-branch `or`. The alternative is a two-branch minimum for
`or-join`, under which the inference writes from a one-branch `or` a form
the written form's check refuses, against goal 5 of
`SCOPE_EXECUTION_AND_CONTRACTS.md`, that every form the inference writes is
a form an author may write. `ValidateStaticClauseShapes` calls the three beside
`SubqueryPattern.Validate` and `FunctionPredicate.Validate`. Whether every
branch binds every output, and whether a body reads a variable it neither
binds nor names in its header, are checked at every text boundary. The
alternative is both checks in `InferHeaders` alone, whose reason, that an
invocation's binding depends on the rule set, covers only a query that
declares `%`. Goal 5 of `SCOPE_EXECUTION_AND_CONTRACTS.md`, one grammar
whose boundaries accept and refuse the same text, excludes it.

`NewRuleSet` infers each body once the heads are collected and holds each
rule with its body inferred (§5.1), and `checkRuleSafety` reads the
inferred body.
A branch input nothing binds is `InferHeaders`' refusal (§2.4), a typed
error carrying the form and the variable. `NewRuleSet` returns that refusal
as `UnsafeRuleError` naming the rule and the variable, so a rule holding
one is refused at build.

The magic rewrite reads headers, and its order is the planner's:

- The rewrite reads each scope's order from the planner, so it lives above
  the planner, in the planner package, and asks the planner for a scope's
  clauses in its order (`SCOPE_EXECUTION_AND_CONTRACTS.md` §4.1). The
  alternative is the rewrite in `query`, handed the order as a function by
  the executor. The planner is the one orderer (that design's §4.1), and
  that alternative breaks it twice: the executor chooses which passes the
  planner runs, and the rewrite's tests in `query`, which cannot import the
  planner, run under an order of their own that no production path uses.
- The planned clauses are the plan's own, in phase order, in the
  decompiler's shape under the algebra mode, since the rewritten body is
  planned again and parses back. The rewrite reads the scope's plan as the
  planner made it, the phases' clauses less the `scope/required` invocation
  that opens them. Which passes a plan runs is the planner's decision.
- What the plan places before an invocation decides its adornment and its
  magic rule, and the rewrite is correct under every order
  (`REWRITE_ORDER_PROOF.md` T1, T2).
- The planner of §5.2 places no scan of a whole extent before an invocation
  the bound positions reach, so the closure adorns under the planner's order
  as under the written one (`REWRITE_ORDER_PROOF.md` T3, T4).
- The rewrite walks the clauses the planner answers and computes no order
  of its own, so the order depends on the optimizer mode.
- What is bound at an invocation at a body's top level is the head's bound
  positions and what the planned clauses before it provide. The body is the
  scope whose header is the head, with the bound positions as its required
  run.
- What is bound at an invocation inside a form's branch is the form's
  required run and what the branch's planned clauses before it provide. The
  branch is the scope whose header is the form's. The alternative is the
  hoist's bound set, excluded in §5.4.
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
  The alternative is a rewrite that skips a fallback form, under which the
  form's outputs never join the bound set, so a required invocation after
  an `or-default` that binds its position is refused.
- A subquery in a body is read as a branch is: a scope whose required run
  is the inner query's `:in` symbols, each bound to the argument its call
  passes, and whose clauses are the inner query's planned clauses. What is
  bound at an invocation inside the inner query is that run and what the
  inner query's planned clauses before it provide. The magic rule of such
  an invocation reads the body's planned clauses before the subquery and
  the inner query's planned clauses before the invocation, the inner
  query's variables renamed apart from the body's and each `:in` symbol
  unified with the argument its call passes. The alternative is a subquery
  the rewrite does not enter, its invocations evaluating at their own
  clauses with the subquery's bindings. Under a bound invocation the
  rewritten set holds the adorned predicate and not the original
  (`RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.3), so an invocation of
  the rule's own stratum inside such a subquery names a predicate the
  evaluation does not hold.
- An invocation of the rule's own stratum inside a subquery is an
  occurrence of the semi-naive step: the body copy for that occurrence
  invokes the delta symbol inside the inner query, and the subquery's scope
  reads the delta from the map that carries every relation the enclosing
  context holds (§5.4). The alternative counts the occurrences at a body's
  top level and inside its branches alone, under which a rule whose only
  occurrence of its stratum is inside a subquery runs at iteration 0 alone
  (`RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.3) and derives nothing
  from a later delta.
- An invocation under a non-monotone form, a negation, a fallback form or
  an aggregating inner query, is adorned by the passing sealed at the first
  clause that invokes a predicate of the rule's own stratum, and its magic
  rule reads the sealed clauses. The site is left unadorned, its
  predicate's rules joining the set unchanged, where adorning it would
  place its adorned predicate in the rule's own stratum or where the sealed
  passing leaves a required position free. The rewrite rebuilds with such a
  site excluded when the constructor refuses the set, so every rewrite is
  accepted and none is declined (`NON_MONOTONE_FORMS_PROOF.md` T6, L4).
  Two alternatives are excluded. No adornment under a non-monotone form
  costs the extent of every predicate under one, a number the engine cannot
  see, and evaluates a required predicate at its clause once per execution
  of the form at every iteration of the stratum (that proof's T4, T5). The
  sealed passing alone costs the decline on the shape of its T3, where the
  rule's own predicate is derived whole, and refuses a required predicate
  whose position is bound after the recursion, which the evaluation at the
  clause answers (its T5).
- The dependency walk records every form on an invocation's path, at any
  depth: `not` and `not-join`, `or-default` and `or-default-join`, an
  aggregating inner query, and a subquery body. The refusal below reads
  that record. The alternative is a record of the innermost form
  alone, as `forInvocations` keeps it, under which a fallback inside a
  subquery body overwrites the subquery body's mark, and a fallback
  enclosing an admitted subquery reads as the subquery.
- A predicate with required positions under a non-monotone form is adorned
  when the sealed passing binds every position it requires, and its
  adorned copy, having no required run, is derived once in the rewritten
  set's fixpoint. Otherwise the site is left unadorned, and the executor
  derives the predicate at its clause under the positions the branch binds
  there, once per execution of the form, since a fixpoint never holds a
  predicate with required positions (`NON_MONOTONE_FORMS_PROOF.md` T5).
  The branch binds the required position before the invocation (§2.4,
  §2.5), so that evaluation answers, and no refusal at `NewRuleSet` and
  none at the rewrite reads such a site. The alternative, a refusal of the
  rule, records the rewrite's limitation as the query's error, which goal 6
  of `SCOPE_EXECUTION_AND_CONTRACTS.md` excludes.
- `NewRuleSet` refuses an invocation of the rule's own stratum under
  `or-default`, `or-default-join` or an aggregating inner query, at any
  depth, and admits one under a subquery whose inner query does not
  aggregate (§2.5). The refusal reads every fallback form and aggregating
  inner query the walk recorded on the invocation's path, and is a typed
  error naming the rule, the predicate and the form.
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
check reads it there. The alternative is the roster held on the parsed
`query.Query`, as `Query.collectRosters` (`datalog/query/attribute_names.go`)
holds it under `rostersOnce`. `Prepare`'s result is not held on the query
(§5.1), so a roster collected there reads headerless forms, where this
roster reads headers. The keywords the query's invocations pass at positions
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
| `algebra.Compile` | `Compile(q)`, with `compileOr`, `compileNot` and `compileOrDefault` taking keys from the fold's symbols, and `compileOr`'s correlated route from `ScopeOf` on the form | `Compile(q, rules)`, with three compilers for the header forms |
| `algebra.Union` | `Output`, `JoinVars`, `Required` | `Output`, `RequiredVars`, `OutputVars`, `Required`, the fields of `LateralUnion` |
| `algebra.LateralJoin` | no rule set | `Rules`, the set its call's `%` binds |
| executor nested forms | six handlers, the headerless three selecting the outer groups by the branches' free variables and the header three by the header | three, each a scope run over its required run |
| executor invocations in a scope | hoisted before the scope, over the whole outer relation | at the clause, seeded from the scope |
| subquery bodies | run in written order | ordered by the planner |
| rule bodies in `RuleSet` | as given | inferred |

The algebra package's shape is `main`'s:

- One operator per form, one compiler per operator, the passes and the
  decompiler as they are.
- `compileOrJoin` keeps its two union routes as on `main`: the one for
  branches holding no `not`, `not-join` or `missing?`, and
  `compileOrUnionCorrelated`, which compiles the branches over a
  placeholder, on `main` for every symbol of the current relation and under
  this design for the required run, how an anti-join inside a branch reads
  the header.
- The compilers for the headerless forms are gone, since their input never
  arrives.
- `Union` carries the header's two halves under the names `LateralUnion`
  already uses (§5.3).
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

Cost is measured, not claimed. The executor's evaluation of the two branch
forms, each branch run once per window of the subject through the primitive
and probed per tuple of the window, is `SCOPE_EXECUTION_AND_CONTRACTS.md`
§4.2 to §4.4, which also states the `not-join` body run once per window over
the window's distinct projection onto its header. A window's keys are sought
as that design's §4.6 states: per key below its crossover, and above it
through the matcher's binding-driven scan, `HashJoinScan` or its merge join
in `hash_join_matcher.go`, as a top-level pattern with bound symbols does.
`SCOPE_EXECUTION_AND_CONTRACTS.md` §8 names the benchmarks, and
`BenchmarkPrepare` (`datalog/query/prepare_test.go`) measures preparation,
which runs per execution.

## 8. The engine's vocabulary

This section answers goal 11 of §1, that the engine's own queries run on
every database it opens. The principle of §4, applied to the schema,
delivers it: the engine's words are declared once, into every running
schema, and every reader reads them there. The engine writes and reads
attributes it never declares, the roster
`engineAttributes` (`datalog/storage/schema_facts.go`) that
`RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §4 states. Under the design
they are a schema the engine installs into every running schema. It is installed in memory and never declared as
facts, and each of its words resolves by its declaration, never by
inference, so an attribute entity's ref to its current declaration resolves
to one declaration whatever the ops hold. Two alternatives are excluded. No
definitions for the engine's words: `:db.attribute/declaration` is then
undeclared on a database with a given schema, resolves by inference, and
the V-bound path of a pattern on it emits every ref datom. The engine's
words declared as facts: a user transaction writing
`:db.attribute/declaration` then commits and every later open fails, and a
many-valued declaration of `:db/cardinality` or `:db/ident` fails every
later `SetSchema` and `Import`.

- A user's write of an engine attribute is refused in
  `validateAttributeStorable`, which `Transaction.Add`, `Transaction.Set`,
  `Transaction.Remove` and `Transaction.Retract` run on every datom's
  attribute, so the check takes the transaction beside the keyword.
- Import's replay is exempt, as it is from every schema check.
- The engine's own writers, `assertSchemaFacts`, `addBranchRecord` and the
  marker `Snapshot` writes, pass because the transaction the engine opens
  for itself carries that provenance, which the check reads.
- A given schema that redeclares an engine word is refused where a schema is
  given, at open and at `SetSchema`. The alternative is accepting it, under
  which the word resolves by the given definition, and a many-valued
  `:db/cardinality` or `:db/ident` then fails every later `SetSchema` and
  `Import`, the failure named above.
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
  cardinality and uniqueness together. The alternative is an entry keyed on
  cardinality alone, which a change of uniqueness leaves in place, so a
  matcher minted before a change from unique to plain caches, after the
  change, a value resolved under the unique rule, which later reads serve.
