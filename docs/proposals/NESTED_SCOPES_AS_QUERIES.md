# Nested Scopes as Queries

Every nested form of a query states its interface in a header. The header is computed once, from the text and the rule set, before the planner or the algebra engine sees the query. After that point every component reads the header and computes nothing about it. This document states the goals the design answers to, the semantics of the forms, and the architecture that delivers them.

## 1. Goals

The design answers to the goals below. Two of them are product choices the other goals do not settle: the headerless forms (goal 10) and the binding grammar (goal 12). Each of those two states the alternative's cost beside it.

1. The most correct fix, not the tactical one: the design question is derived to the most correct fix for Janus Datalog.
2. Clarity is not diluted: Janus Datalog's unusual clarity is kept.
3. Correct and consistent outranks compatible: compatibility is broken where conceptual correctness and consistency require it.
4. The `algebra` package keeps `main`'s clarity.
5. No accreted arguments.
6. The algebra engine is a drop-in oracle of correctness: it can be dropped in and out, and its job is to check the other path.
7. Each component does its own job, and inference is one pass.
8. The architecture is simple.
9. The design is concrete: a design, not a collection of rulings.
10. The language keeps headerless `or`, `or-default` and `not`, including the existential `not`. Two alternatives are priced. A header on every nested form makes every author rewrite every headerless form in every query. The uniform same-set rule refuses the fourth example of §2.3, which would then have to be written as `not-join`.
11. The engine's own queries run on every database it opens, whatever the user's schema declares and whether or not the database refuses unknown attributes.
12. The binding grammar is Datomic's. A subquery's binding, an expression's destructuring and an `:in` entry take the forms Datomic's query reference gives (`bind-scalar`, `bind-tuple`, `bind-coll`, `bind-rel`), with Datomic's meanings. A spelling of janus's own is a synonym of a Datomic form, never a form with a meaning of its own. The alternative's cost: a query written for Datomic would read differently here, and a janus spelling with its own meaning would give one text two readings across the two engines.

## 2. The language

### 2.1 The forms

A query's `:where` holds data patterns, expressions, predicates, rule invocations, subqueries, and six nested forms:

| Form | Header | Meaning |
| --- | --- | --- |
| `(or branch…)` | inferred | union of the branches |
| `(or-join [[req…] out…] branch…)` | written | union of the branches |
| `(not clause…)` | inferred | the body matches nothing |
| `(not-join [req…] clause…)` | written | the body matches nothing |
| `(or-default branch…)` | inferred | the first branch with a result, per tuple of the required run |
| `(or-default-join [[req…] out…] branch…)` | written | the first branch with a result, per tuple of the required run |

A header has a required run and outputs. The required run is the variables the enclosing scope binds before the form runs. The outputs are the variables every branch binds and the form provides to the enclosing scope. A flat header, `[out…]`, is all outputs. A `not-join` header is all required, since a `not` provides nothing.

Every other variable inside a form is local to the branch that holds it. A local never correlates with the enclosing scope, whatever reads it: a pattern, a predicate, an expression, or a nested form. Renaming a local to a name that occurs nowhere else in its scope changes nothing.

### 2.2 The interface of a form

The header is the whole interface of a form. These rules state it:

- A branch sees the required run and nothing else of the enclosing scope. A variable a branch binds that shares its name with an input is the branch's own, unless the header names that input. The alternative is inputs ambient in every scope. Under it a written form's interface would depend on the query's `:in`, which no reader holding the form alone computes (§4). And a branch-local sharing an outer variable's name would be adorned as bound, the failure §3 pins.
- Sources (`$`, `$name`) and the rule set (`%`) are not logic variables, and they reach every branch. A subquery's inner query takes the sources and the rule set its call passes (§2.5). The alternative is passing sources and the rule set through a form's header. A form cannot pass another source or rule set, as a subquery's call can, so such a header would only restate the query's own.
- The enclosing scope sees the outputs and nothing else of a branch.
- An `or-default` or `or-default-join` decides its fallback per tuple of the required run. A branch runs over that tuple alone. So the decision is the same in every order the planner chooses for the clauses that read the outputs.
- A form runs once its required run is bound, wherever it was written. When nothing binds the required run, §2.4 states the refusal.

### 2.3 The inference

A form without a header gets one by this rule, under the rule set the query runs with:

- A variable inside the form is shared when it occurs in the interface of another clause of the enclosing list, or in what the enclosing scope exposes to that list. To the query's own list the enclosing scope exposes the variables of `:in`, `:find` and `:order-by`. To a branch it exposes the enclosing form's header. A clause's interface is what it provides and what it reads from its scope. For a pattern, an expression, a predicate or an invocation, that is its variables. For a subquery, it is its arguments and its binding. For a header form, it is its header. For a headerless form in the same list, it is every variable of its branches short of a nested header. Every other variable is local to the branch that holds it.
- A shared variable the form binds in every branch is an output.
- Every other shared variable is required.
- A `not` binds nothing for its scope, so every shared variable of a `not` is required.

A branch binds a variable through a pattern, an expression's binding, a subquery's binding, an invocation's free positions, or a nested form's outputs. An invocation of a predicate with required positions consumes the variables at those positions and does not bind them. That is why the rule reads the rule set.

The rule is the same for `or` and `or-default`. §4.4 weighs this one rule for `or`, `or-default` and `not` against the alternatives. The alternative for `or-default` is a headerless `or-default` with no required run. Its decision would be global, and per-tuple keys would be declared through `or-default-join` alone. That is a second interface rule for one form, beside the rule for `or` and `not`, which goal 3 excludes. The zero-default count of `OR_FALLBACK_SEMANTICS.md` is the case the rule answers:

```clojure
[:find ?category ?count
 :where [?c :category/name ?category]
        (or-default [(q [:find (count ?p)
                         :in $ ?c
                         :where [?p :product/category ?c]]
                        $ ?c) [?count ...]]
                    [(ground 0) ?count])]
```

Both branches bind `?count`, so it is an output. The subquery reads `?c` and the ground does not mention it, so `?c` is required. The header is `[[?c] ?count]`, and each category gets its count or zero.

An `or` whose branches bind different variables is legal (§4.4). In

```clojure
[:find ?q
 :where [?q :t/n _]
        (or [?q ?a ?v] [?q :t/tag :t/none])
        [(ground :t/tag) ?a]]
```

`?q` is shared and every branch binds it, so it is an output. `?a` is shared and the second branch does not bind it, so it is required. The header is `[[?a] ?q]`. The ground binds `?a` before the form in every written order, and the query has one answer.

An output that another clause also binds is joined, never superseded. In

```clojure
[:find ?e
 :where [?w :p ?e]
        [?y :q ?e]
        (or-default [?e :p ?m]
                    (and [?m :p ?e] [?y ?a ?m]))]
```

`?e` is an output and `?y` is required. For each `?y`, the form answers with the `?e` values the first branch with a result produces. The enclosing scope keeps a tuple when its `?e` is among them.

A `not` provides nothing, so every variable it shares is required. In

```clojure
[:find ?e
 :where [?e :p _]
        (not [?e :q ?x])]
```

`?e` is shared and required. `?x` occurs only inside the form, so it is local, and the form holds for an `?e` that has no `:q` at all. The header is `[?e]`.

### 2.4 Strictness

- A written header is read strictly. The alternative is a tolerant header, under which a branch's read of a variable the header does not name is bound from the enclosing scope. Under it a `not-join` body reading an unnamed outer variable would be bound to it, and the same query with that variable renamed would answer differently. That breaks §2.1's rule that renaming a local changes nothing.
- An `or-join` or `or-default-join` output that some branch does not bind is a query error. The error names the form, the branch by its position counted from one, and the variable. The repair is moving the variable into the required run.
- A variable a `not-join` body reads and neither binds nor names in its header is a query error. The error names the form and the variable. The repair is naming the variable in the header or binding it in the body.
- A `not-join` header variable the body neither binds nor reads is a query error. The error names the form and the variable. The repair is dropping the variable from the header or reading it in the body.
- Whether a branch binds a variable is read under the rule set.
- A variable a branch's predicate, expression, subquery input or required position reads, which neither the branch nor the header binds, is a query error. The error names the form and the variable. The repair is binding the variable in the branch or naming it in the header.
- A form whose variables occur nowhere else in its scope is a query error. The error names the form. The repair is naming in the form a variable the scope binds.
- A required run that no written order binds before the form is a query error. The error names the form and the variable. The repair is binding the variable before the form. Two forms that each partially bind one variable nothing else binds are refused by this rule.

A form sharing nothing is checked after `InferHeaders`' other refusals. So a form that also reads a variable neither its branch nor its header binds is refused naming the variable.

### 2.5 Subqueries and invocations

A subquery's inner query is a scope of its own. Its inputs are what its `:in` declares and its call passes, and nothing else of the enclosing scope. Its clauses are ordered like any scope's, never run in written order.

A subquery's binding names the inner query's outputs in the enclosing scope. A binding asserts nothing about cardinality (§5.4). A name written at two places of a binding unifies the two outputs it names, as a variable repeated across an invocation's arguments does: only the tuples whose two outputs agree enter the scope, under the one name. The spellings are Datomic's binding forms (goal 12). The relation form `[[?a ?b]]` is Datomic's `bind-rel`. The collection form `[?a ...]` is a relation of one symbol. The scalar form `?a` is read as the relation over one symbol. `[[?a ?b] ...]` is a spelling of janus's own. It is read as the relation form, and it is the relation form's rendering, on a subquery and on an expression alike, since its trailing `...` says a set of tuples. The tuple form `[?a ?b]` is an expression's destructuring and not a subquery binding. The alternative is reading `[[?a ?b]]` as a single tuple and `[[?a ?b] ...]` as the relation. Under it one text reads as a relation at Datomic's `q` and as a cardinality assertion here. Goal 12 excludes it.

An `:in` entry takes the same forms (goal 12). `?a` is the scalar input. `[?a ...]` is the collection input. `[?a ?b]` is the tuple input, one tuple over its symbols. `[[?a ?b]]` is the relation input, a set of tuples. `[[?a ?b] ...]`, janus's own spelling, is read as the relation input and is the relation input's rendering, since its trailing `...` says a set of tuples where `[[?a ?b]]` alone reads as one. The tuple input renders as `[?a ?b]`. A tuple input and a relation input of one tuple answer the same tuples. The alternative is reading `[[?a ?b]]` as the tuple input and `[[?a ?b] ...]` as the relation input. Under it a query written for Datomic with a tuple input is refused here, and one with a relation input binds one tuple. Goal 12 excludes it.

An invocation inside a scope is a clause at its planned position. An invocation of a predicate with required positions is ordered after the clauses that bind them, inside a branch as at the top level.

An invocation of a predicate of its rule's own stratum may stand inside a subquery whose inner query does not aggregate, as it may stand inside an `or` or `or-join` branch. The subquery joins the inner query's result with the scope over its combinations, and that is monotone, as a branch is. Under an aggregating inner query, at any depth, it is refused, because aggregation inside a recursive component is refused (`RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.2).

## 3. The contracts the tests pin

Each contract below is the subject of one test.

- The four worked examples of §2.3 give one answer on both planner paths in every written order.
- A form sharing nothing is refused naming the form.
- A required run no order binds is refused naming the form and the variable.
- A flat `or-join` header variable some branch does not bind is refused naming the form, the branch and the variable.
- A variable a `not-join` body binds that shares the name of an input its header does not name is the body's own.
- A branch variable that shares an outer variable's name reads nothing of the outer.
- An invocation of a required predicate inside a branch is ordered after its binder on both paths.
- A rule body holding an `or` whose branch holds a `not` with a variable of its own is a safe rule.
- Generated queries over random graphs, in both optimizer modes, match a reference computed in Go from the datoms under §2. Every query §2.4 refuses is refused by the engine. That a refusal names the form and the variable is a contract of its own, beside the differential.

## 4. The fact that decides the architecture

The interface of a headerless form is not a local property of the form. Whether a variable inside it is shared depends on where else the variable occurs. Whether a shared variable is an output depends on what every branch binds, which depends on the rule set.

No reader that holds the form alone can compute its interface. `ScopeOf` holds the clause alone. The executor holds the form and the relation in hand. The algebra compiler holds the form and the symbols the fold has bound. A reader that computes the interface from what it holds substitutes its own context for the scope. Two readers with two contexts give two answers. `ScopeOf` with headerless cases, headerless compilers in the algebra engine and headerless handlers in the executor are three such readers, and every disagreement among them is a defect.

So the interface is computed once, with the whole scope and the rule set in hand, and written into the text as the header. After that it is local, and every reader reads it.

The fact is a consequence of the rule §2.3 states. A different rule would remove it, at a price. The rest of this section proves the fact, bounds it, and states the trade the rule makes.

### 4.1 Two rules

A rule for a headerless form's interface is local when the interface is a function of the form's text alone. It is non-local when the interface is a function of the form and its scope. Two rules are in question for the branch forms, `or` and `or-default`.

- The same-set rule is Datomic's rule for `or`. Every branch mentions one set of variables. Every variable of the set is shared with the enclosing scope. A form whose branches mention different sets is refused.
- The occurrence rule is §2.3's. A variable inside the form is shared when it occurs elsewhere in its scope, and local otherwise.

Under both rules a shared variable every branch binds is an output, and every other shared variable is required.

The same-set rule is local. The set is read from any one branch, since every branch mentions it. Which of its variables every branch binds is read from the branches. Nothing outside the form is read.

The occurrence rule is non-local. One form in two scopes shows it. The form is

```clojure
(or [?q ?a ?v] [?q :t/tag :t/none])
```

Its first scope is the second worked example of §2.3, where a ground binds `?a` after the form. `?a` occurs in the ground, so it is shared. The second branch does not bind it, so it is required. The header is `[[?a] ?q]`. The form holds for a `?q` that carries `:t/tag` with some value, or carries `:t/tag` as `:t/none`.

Its second scope drops the ground:

```clojure
[:find ?q
 :where [?q :t/n _]
        (or [?q ?a ?v] [?q :t/tag :t/none])]
```

`?a` occurs nowhere else, so it is local. The header is `[?q]`. The form holds for a `?q` that carries some attribute with some value, or carries `:t/tag` as `:t/none`.

One text, two headers, two relations. No function of the text gives both.

### 4.2 Where the rules differ

The two rules agree on every variable every branch mentions. When it occurs elsewhere in the scope, both rules share it and split it the same way. When it occurs nowhere else, the occurrence rule makes it local, and nothing changes. A local every branch binds gives the answer the shared variable gives, once the find boundary projects it away. A local some branch reads without binding is refused under either rule, by §2.4 under this one.

The rules differ only on a variable some branch mentions and another does not. The same-set rule refuses every form that holds one. So the occurrence rule is non-local only on forms the same-set rule refuses. Every form the same-set rule accepts gets the same answer under either rule.

Such a variable cannot be an output. A relation has no unbound position, and a branch that does not bind the variable would yield tuples with none. The same-set rule meets the constraint by refusing the form. The occurrence rule meets it by making the variable required. The variable is then bound before the form runs, so every branch's tuple carries it, and §2.4 refuses the query when no written order binds it.

`not` has one body, so no variable of it is asymmetric. A rule under which every variable of a `not` is shared with its scope is local. That is the uniform same-set rule, (e) in §4.4. It refuses the fourth worked example of §2.3, where `?x` is an existential nothing else names. The occurrence rule reads the existential from the scope, by the same non-local reading.

### 4.3 The cost

The cost of the occurrence rule is action at a distance. A clause added elsewhere in the scope that names a variable a branch held as a local makes that variable shared. When the clause reads the variable and nothing binds it before the form, §2.4 refuses the query, and the change is loud. When the clause binds it, the form joins on it, and the change is silent: the form's answer changes with no edit to the form.

Two facts bound the cost.

- It falls only on a variable some branch does not bind. For a variable every branch binds, the same-set rule shares it from the start, and the added clause joins on it under either rule. So the cost falls only on the forms the same-set rule refuses, which are the forms the occurrence rule admits.
- In Datalog a name used twice is a join. Every pattern in a query carries the same hazard for every variable it names. The same-set rule removes one case of it.

### 4.4 The alternatives

Five rules could state a headerless form's interface:

- (a) The same-set rule for `or` and `or-default`, with `not` under the occurrence rule. It is local for the branch forms. An author writes an asymmetric form as `or-join`, which is the design's own principle applied by hand. The language carries two interface rules.
- (b) The intersection rule. A headerless branch form is read as the header form whose outputs are the variables every branch mentions, with no required run. It is local. A branch-only variable that shares an outer name does not unify with it, and nothing in the text says so.
- (c) The occurrence rule for `or`, `or-default` and `not`. It is non-local. It is one rule for the three forms. Every form the same-set rule accepts gets the same answer, and the asymmetric forms are admitted at the cost of §4.3.
- (d) A header on every nested form, with no headerless form in the language. It is local in the text as written. Every headerless form in every query is rewritten, and the inference has nothing to do.
- (e) The uniform same-set rule: the same-set rule for `or` and `or-default`, and for `not` every variable of the body shared with its scope. It is local for every form. The fourth worked example of §2.3 is refused and written as `not-join`.

The design's rule is (c). Goal 3 excludes (a), which carries two interface rules, since consistency outranks the compatibility (a) keeps. §4.3's fact excludes (b): in Datalog a name used twice is a join, and under (b) a branch-only variable sharing an outer name is not joined with it, with nothing in the text to say so. Two rules remain that no goal of §1 excludes, (d) and (e). Goal 10 chooses (c) over them and states their costs. The price of (c) falls on goal 2's clarity: a reader of a headerless form reads its scope to know its interface. Two things answer the price. `Explain` plans the prepared query (§5.1), so its phases carry the header `InferHeaders` wrote. The written forms, `or-join`, `or-default-join` and `not-join`, state the interface in the text wherever the scope would leave it unclear.

## 5. The architecture

Four stages, one job each, with an optional rewrite between the second and the third:

```
parser.ParseQuery(text)          Query      syntax
query.Prepare(q, rules)          Query      shapes checked, every nested form stating its interface, every invocation resolved
algebra.Compile → passes → Decompile        optional: an equivalent, better query
QueryPlanner.PlanQuery(q, bindings, handler)   Plan       ordering
executor                         Relations  running
```

### 5.1 Preparation and the inference

`query.Prepare(q *Query, rules *RuleSet) (*Query, error)` takes a parsed query and the rule set bound to `%`. It returns the query ready to plan. It composes five static steps, each a function of the text and the rule set:

- `ValidateInputShapes` on `q.In`.
- `ValidateStaticClauseShapes` on `q.Where`.
- `ValidateRulesInput` on both: the check that a clause using the query's own `%` has `%` declared in its `:in`. `ParseQuery` and `qb.Build` run that check too.
- `InferHeaders`.
- The check that a rule set was given when the query invokes a predicate, and that every invocation names a predicate the set defines at its arity.

`Prepare` runs once per execution, before planning. Its result is not held on the query. The alternative is each caller composing the five steps itself. That is five copies of one fact the callers must agree on. Under it two boundaries that compose the steps differently refuse different queries.

The executor's entry runs in this order: the cancellation check, the `QueryInvoked` event rendering the query as received, `Prepare`, `PlanQuery`, the vocabulary check against the roster the plan carries and this execution's inputs, and execution. A preparation error completes the query with that error, as a planning error does. `Explain`, `ExplainAlgebra` and `Analyze` call `Prepare` before they plan. `Analyze` prepares the query it plans, and prepares it again in the execution it runs through `Query`'s path. Nothing after preparation sees a headerless form.

`query.InferHeaders(q *Query, rules *RuleSet) (*Query, error)` returns `q` with every `or`, `not` and `or-default` at any depth rewritten as the header form that states its interface, by the rule of §2.3. It refuses every query §2.4 names as a query error, except the one §2.4's last bullet names. That one, a required run no written order binds before the form, is the planner's refusal on both paths: `orderClausesForCompile` on the algebra path and `createPhasesGreedy` on the clause-based path, each naming the form and the variable. `InferHeaders` infers a form's branches under the form's header. It infers a subquery's inner query as a scope of its own. A form on the path to a rewritten form is copied, and every other clause is shared with `q`. A query of header forms is returned as it is. The function is the one place the rule of §2.3 exists. Two alternatives are excluded. Inference at each reader is excluded by §4, since two readers with two contexts give two answers. Inference in the planner is excluded by §5's stage order, under which `algebra.Compile` runs before `QueryPlanner.PlanQuery`, and by §4, since an engine inferring for itself is a second reader.

`NewRuleSet` calls `InferHeaders` on each rule body once the heads are collected. It infers the body as a query that finds the head, with the required run as its inputs. So the safety check, the magic rewrite, the roster and the body planner all read bodies of header forms. A body's shapes and invocations are checked by `NewRuleSet`'s own validation. The alternative is a rule set holding each body as given, with the inference at each of those four readers, which goal 7 excludes. Holding the rules as given guards against a field the text does not render. An inferred body holds header forms, which the text renders, and every phase the planner emits parses back to itself (§5.5), so no such field exists.

Downstream of preparation there are three nested forms. `ScopeOf` panics on a headerless form as it does on an unknown clause, on the planner's path and the fold's alike. `orderClausesForCompile` reads every clause's scope before any compiles, so the fold's path panics before a clause compiles. `DefaultQueryExecutor.Execute`'s dispatch returns its unsupported-clause error naming the clause's type. So a skipped preparation surfaces at once.

### 5.2 The planner

`QueryPlanner.PlanQuery(q, bindings, handler)` takes a prepared query. The planner orders and phases. It computes no interface. Readiness is `ClauseReady` over `ScopeOf`. `ScopeOf` on a nested form reads its header alone: the required run correlates and is mandatory, and the outputs are provided. No gate holds an `or`, `not` or `or-default` back by a closure of pending clauses. `createPhasesGreedy` carries no check of a `not`. The clause scoring has no arm for a headerless form. Each of those would read a headerless form, and none arrives.

The planner's scheduling preferences stand. `patternDependsOnPendingExpression` holds a pattern back while a pending expression that can run before it binds one of the pattern's variables. `subqueryDependsOnPendingProvider` holds a subquery back while a pending clause provides one of its binding variables and waits, through the pending clauses, on no output of the subquery. So a binder is placed before its consumer, and no gate waits on the clause it holds (`REWRITE_ORDER_PROOF.md` §6). `invocationDependsOnPendingProvider` holds an invocation with no available argument back behind any pending selectable clause that provides one of its arguments. It holds an invocation with an available argument back only behind a provider with visible selectivity, because a scan of a whole extent placed before a bound invocation multiplies its magic set by the scan's extent (`REWRITE_ORDER_PROOF.md` T2, T3). The selection within a phase takes a rank before the score. A clause without visible selectivity is selected only when no other clause is selectable. Visible selectivity is a constant beyond a pattern's attribute or an invocation's name, a variable available when the clause is selected, or a seed-restricted predicate. Among the rest, the scores stand as they are (`REWRITE_ORDER_PROOF.md` §6). An expression that waits, through the pending clauses binding what it needs, on a variable the pattern itself binds cannot run before that pattern. The pattern runs first, and the expression unifies with the variable the pattern bound. These preferences order, and they decide no interface.

Both optimizer modes phase a prepared query. With the algebra engine on, the planner phases the clauses the decompiler returns. With it off, the planner phases the prepared clauses.

The plan cache serves a plan under a key of four parts in one fixed order: the prepared text, the positions the rule set requires of its invocations, the options that shape a plan, and the symbols bound when the plan was made. Every plan enters the cache (goal 10 of `SCOPE_EXECUTION_AND_CONTRACTS.md`). The bound symbols reach a plan at two places. `planWithBindings` unions them into the symbols the `:in` seeds. A non-empty set turns `EnableJoinProjectInsertion` off for the algebra bridge. So two plans made under one key are one plan. A scope's query and a rule body are planned under no bound symbol, so their plans are served under the text alone. The alternative is a plan made under a bound symbol kept out of the cache. The executor's entry binds a symbol for a relation input, and for a scalar, tuple or collection input whose relation is non-empty. So a query with a non-empty input would be planned on every execution, which goal 10 excludes.

### 5.3 The algebra engine

`algebra.Compile(q *query.Query, rules *query.RuleSet) (*Node, error)` takes a prepared query and the rule set its invocations are ordered under. `Compile` is `Query → tree`. The passes are `tree → tree`. `Decompile` is `tree → clauses`, and `optimizeViaAlgebra` sets those clauses into a query of the prepared query's parts. The engine is a rewrite from a query to an equivalent query. It infers nothing, and it is optional. Where a header came from is not a fact the engine reads. An or-join the inference wrote and the same or-join a user wrote are one query, and the engine's output is a function of that text alone.

Each header form compiles to the operator `main` compiles it to, in `main`'s shape. The header is where the operator's keys come from:

- `not-join` compiles to an `AntiJoin` keyed on its header. The body compiles under the header alone.
- `or-join` compiles to a `Union`. `Output` is the operator's schema: the required run and the outputs. `RequiredVars` and `OutputVars` are the header's two halves. They are `LateralUnion`'s fields, with the same meanings. The alternative is the halves under names of `Union`'s own, which is two names for one fact across the two operators. `Required` is the required run plus the outputs the current relation binds. `Output` and `Required` keep each operator's own meaning: a `LateralUnion`'s `Output` is the outer schema plus the symbols every branch produces, and its `Required` is the outer schema. The branches compile under the required run alone.
- `or-default-join` compiles to a `LateralUnion` whose outer schema is the current relation's symbols plus the required run. The branches compile under the required run alone.
- The lowering of an `or-default-join` to a `LateralJoin` with defaults fires only when branch 1 is one subquery and branch 2 is ground expressions alone. The condition removes this failure: a lowering that fires with a predicate beside the subquery, or a `not` beside the ground, applies the predicate outside the fallback and does not apply the `not` at all.
- A `LateralJoin` carries the rule set its call's `%` binds. So the decorrelation pass compiles the inner query it rewrites under that rule set and the inner `:in`. The alternative is a trailing rule-set parameter on both pass constructors, a second home for a fact the node's call carries, which goal 5 excludes.

The fold orders a form after the clauses that bind its required run. `orderClausesForCompile` reads `ScopeOf`, which on a header form reports the required run as mandatory correlates. So a form's position in the fold depends on its header, never on where it was written.

The decompiler renders every header: an or-join's required run and outputs, a not-join's header, and an `or-default-join` for every fallback. A fallback's header comes from the operator the fallback compiled or rewrote to:

- From a `LateralUnion`: its `RequiredVars` and `OutputVars`.
- From a `LateralJoin` with defaults, where the lowering fired: its `CorrelationVars` as the required run, and the binding's other symbols as the outputs. When the call passes no variable, the required run is instead the symbols the binding shares with the outer.
- From a `LeftOuterJoin`, whether decorrelation's or the get-else rewrite's: its `JoinSymbols` as the required run and the right child's other symbols as the outputs. The rewrite's default branch is a `get-else` expression.

### 5.4 The executor

The executor runs three nested forms. Each is a scope with a header. One runner, `runScope(ctx, scope, outer)`, serves them and the subquery. A scope is a header and a clause list. Each form's handler builds one scope from each branch. The subquery's handler builds one from its inner query's `:in`, `:find` and `:where`. The alternative is a runner of the subquery's own. Running the inner query in its written order is excluded by §2.5. Running an inner query that holds an invocation once per combination is excluded by the cost goal 2 of `SCOPE_EXECUTION_AND_CONTRACTS.md` names: that invocation would seed one fixpoint per combination. The window an inner query runs over is that design's §4.2, stated for the subquery in its §4.4. A second runner with neither defect would be a second component for one job, which goal 7 excludes.

The runner frames the scope as a query that finds its header. The query's `:in` is `$` and `%`. Its `:where` opens with an invocation of the minted predicate `scope/required` over the required run, followed by the scope's clauses. A scope whose required run is empty reads no group, so its outer relation is the unit relation. It opens with `(scope/required)` over that relation's zero-symbol projection, a verdict that holds, and runs once. An empty group standing beside it is a group the required run does not read. `scope` is the fourth of the engine's namespaces, which `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.1 states, and its §1.3 states the scheduling that leads with `scope/required`. The text carries the binding, and the scope is planned under no bound symbol, so its plan caches under the text alone (§5.2). Two alternatives are excluded. The first is the required run as a `query.RelationInput` in the scope's `:in`. `executeRealizedWithRelationInputIteration` (`datalog/executor/executor.go`) runs a plan once per tuple of such an input. A branch holding an invocation instead runs over the window, with its fixpoint continued across the windows, so one evaluation derives each tuple once (`SCOPE_EXECUTION_AND_CONTRACTS.md` §4.2). The second is the required run as the planner's bound symbols. A bound symbol tells the planner that the symbol is bound, and names no relation the run enters by. The `scope/required` invocation is the clause the run enters through. It is the one the planner leads with, and the one the magic rewrite reads the scope's plan less (`SCOPE_EXECUTION_AND_CONTRACTS.md` §4.1).

The required run enters the scope as a relation:

- The runner projects `outer`, joined with the environment's tuple, onto the required run. It holds that projection in the scope's context through `Context.WithDerived`.
- A required variable the environment alone binds thereby takes its value in every tuple of the projection.
- The map the projection is held in carries every relation the enclosing context holds beside it.
- `runStratum` (`datalog/executor/rules.go`) holds each delta under its delta symbol in the map that `stratumEvaluation.run` enters through the same method.
- `invocationSource` reads the projection at the invocation as it reads a delta.
- The scope's context carries no environment. A query input reaches a branch only through the header.

The two branch forms' handlers construct `OrFallbackRelation` on the primitive of `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.1, as its §4.3 states. The `not-join` handler runs its body once per window, over the window's distinct projection onto its header (that design's §4.4). The alternative is a handler per form, each keying the outer relation its own way: one per tuple on the required run, one on the outputs, and one running its body once per key combination. That is three readers of one fact, which §4 excludes.

The window a scope's plan runs over is `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.2's. Then:

- `or-join` unions the branch results over the header. It joins the outer relation on the required run and the outputs.
- `or-default-join` answers each tuple of the required run from the first branch with a result for it. It joins the outer relation on the required run and the outputs (`SCOPE_EXECUTION_AND_CONTRACTS.md` §4.4).
- `not-join` anti-joins the outer relation on the header with the body's result.
- An output the environment binds is joined with the environment's tuple, as an output another clause binds is joined with that clause's relation (§2.3).
- A relation group the required run does not read stands as it is, unjoined, until the find boundary.
- `Relations.Collapse` joins only groups that share a symbol.
- The find boundary of `DefaultQueryExecutor.Execute` returns the group holding every `:find` symbol, or the product of the groups that together hold them. It asks every group outside that set for one tuple, by one pull of the group's iterator. A group with none empties the result (`SCOPE_EXECUTION_AND_CONTRACTS.md` §3 derives the verdict).
- An iterator filling a group's replay cache is read to its end before it is closed, so the cache is whole for the group's later readers.
- A product ending on an empty relation reads the iterators it has pulled to their ends the same way. It never opens the relations after the empty one.
- A subquery is a scope under the same window, run over the relation of its argument combinations (`SCOPE_EXECUTION_AND_CONTRACTS.md` §4.4).
- An aggregating inner query whose body reads an input carries its inputs as group keys. One whose body reads no input is a pure aggregate. It is aggregated once and paired with every run tuple, since adding group keys to a pure aggregate changes its kind (`SCOPE_EXECUTION_AND_CONTRACTS.md` §4.4).
- The binding names the inner query's outputs in the enclosing scope, and the result enters the scope as the relation over them. A binding asserts nothing about cardinality (`SCOPE_EXECUTION_AND_CONTRACTS.md` §4.4).
- An invocation inside a scope evaluates at its clause, seeded from the scope's bindings. The alternative is the hoist, which derives a scope's invocations before the scope, over the whole outer relation. It fails twice over. An invocation is seeded from the whole outer relation rather than the header. And it is derived before any clause of the form has run, so an argument the form's own clauses bind counts as unbound.

A rule body already runs this way, through `planBody`. The join on the outputs is what makes an output that another clause binds a join and never a supersession (§2.3's third example).

### 5.5 The language package

`query` owns the forms, the header grammar, `ScopeOf`, `InferHeaders` and `Prepare`. `OrJoinClause` carries `RequiredVars` and `OutputVars`. `OrDefaultJoinClause` gives the same halves of its header the same names. `NotJoinClause` carries its header as `RequiredVars`, the name an all-required header has. The parser, `qb.OrJoin` with `Require`, and `Query.String` carry the grammar. So every phase the planner emits parses back to itself.

Each header form has a `Validate` method. It checks the shape the text alone decides: a non-empty header for every form, at least one branch for an `or-join`, at least two for an `or-default-join`, and for the two branch forms a required run disjoint from the outputs. A union of one branch is that branch over the header. It is the form the inference writes from a one-branch `or`. The alternative is a two-branch minimum for `or-join`. Under it the inference would write, from a one-branch `or`, a form the written form's check refuses. That breaks goal 5 of `SCOPE_EXECUTION_AND_CONTRACTS.md`, that every form the inference writes is a form an author may write. `ValidateStaticClauseShapes` calls the three `Validate` methods beside `SubqueryPattern.Validate` and `FunctionPredicate.Validate`. Two checks run at every text boundary: whether every branch binds every output, and whether a body reads a variable it neither binds nor names in its header. The alternative is both checks in `InferHeaders` alone. The reason for that, that an invocation's binding depends on the rule set, covers only a query that declares `%`. Goal 5 of `SCOPE_EXECUTION_AND_CONTRACTS.md`, one grammar whose boundaries accept and refuse the same text, excludes it.

`NewRuleSet` infers each body once the heads are collected, and holds each rule with its body inferred (§5.1). `checkRuleSafety` reads the inferred body. A branch input nothing binds is `InferHeaders`' refusal (§2.4), a typed error carrying the form and the variable. `NewRuleSet` returns that refusal as `UnsafeRuleError`, naming the rule and the variable. So a rule holding one is refused at build.

The magic rewrite reads headers, and its order is the planner's:

- The rewrite reads each scope's order from the planner. So it lives above the planner, in the planner package, and asks the planner for a scope's clauses in its order (`SCOPE_EXECUTION_AND_CONTRACTS.md` §4.1). The alternative is the rewrite in `query`, handed the order as a function by the executor. The planner is the one orderer (that design's §4.1), and that alternative breaks it twice. The executor would choose which passes the planner runs. And the rewrite's tests in `query`, which cannot import the planner, would run under an order of their own that no production path uses.
- The planned clauses are the plan's own, in phase order. Under the algebra mode they are in the decompiler's shape, since the rewritten body is planned again and parses back. The rewrite reads the scope's plan as the planner made it: the phases' clauses, less the `scope/required` invocation that opens them. Which passes a plan runs is the planner's decision.
- What the plan places before an invocation decides its adornment and its magic rule. The rewrite is correct under every order (`REWRITE_ORDER_PROOF.md` T1, T2).
- The planner of §5.2 places no scan of a whole extent before an invocation the bound positions reach. So the closure adorns under the planner's order as it does under the written one (`REWRITE_ORDER_PROOF.md` T3, T4).
- The rewrite walks the clauses the planner answers and computes no order of its own. So the order depends on the optimizer mode.
- At a body's top level, an invocation's bound set is the head's bound positions plus what the planned clauses before it provide. The body is the scope whose header is the head, with the bound positions as its required run.
- Inside a form's branch, an invocation's bound set is the form's required run plus what the branch's planned clauses before it provide. The branch is the scope whose header is the form's. The alternative is the hoist's bound set, excluded in §5.4.
- The magic rule reads the planned clauses before the invocation.
- For an invocation inside a form's branch, the magic rule reads the body's planned clauses before the form and the branch's planned clauses before the invocation. The branch enters the rewrite as a scope, the way a subquery's inner query does. The magic rule reads the branch's clauses through a subquery over the header symbols the enclosing scope binds. That subquery finds the invocation's bound variables the branch's clauses provide, and binds each to the name standing for it in the body, a local under a fresh name. So a local that shares a body variable's name outside the header (§2.1) never unifies with it. A bound variable the header alone binds passes through under its own name, which the body's clauses bind. A branch whose clauses provide none of the invocation's bound variables contributes no read, since a clause left out widens the magic set and loses no tuple. The alternative is the branch's clauses appended to the body's prefix under renamed locals. That is a rename of clauses, an operation the language has no form for, where a subquery scopes its inner variables by construction. A local left under its own name would unify with the body's variable and narrow the magic set below what the adorned rule's guard needs.
- A negation is not read, since it binds nothing. A form is read with each negation at any depth left out.
- A fallback form is read as the union of its branches over its header. The union binds what the form binds and holds every tuple the form yields. The alternative is a rewrite that skips a fallback form. Under it the form's outputs never join the bound set, so a required invocation after an `or-default` that binds its position is refused.
- A subquery in a body is read as a branch is. It is a scope whose required run is the inner query's `:in` symbols, each bound to the argument its call passes, and whose clauses are the inner query's planned clauses. Inside the inner query, an invocation's bound set is that run plus what the inner query's planned clauses before it provide. Its magic rule reads the body's planned clauses before the subquery and the inner query's planned clauses before the invocation. The inner query's variables are renamed apart from the body's, and each `:in` symbol is unified with the argument its call passes. The alternative is a subquery the rewrite does not enter, its invocations evaluating at their own clauses with the subquery's bindings. Under a bound invocation the rewritten set holds the adorned predicate and not the original (`RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.3). So an invocation of the rule's own stratum inside such a subquery would name a predicate the evaluation does not hold.
- An invocation of the rule's own stratum inside a subquery is an occurrence of the semi-naive step. The body copy for that occurrence invokes the delta symbol inside the inner query. The subquery's scope reads the delta from the map that carries every relation the enclosing context holds (§5.4). The alternative counts the occurrences at a body's top level and inside its branches alone. Under it a rule whose only occurrence of its stratum is inside a subquery runs at iteration 0 alone (`RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §1.3) and derives nothing from a later delta.
- An invocation under a non-monotone form, that is a negation, a fallback form or an aggregating inner query, is adorned by the passing sealed at the first clause that invokes a predicate of the rule's own stratum. Its magic rule reads the sealed clauses. The site is left unadorned, with its predicate's rules joining the set unchanged, in two cases: where adorning it would place its adorned predicate in the rule's own stratum, and where the sealed passing leaves a required position free. When the constructor refuses the set, the rewrite rebuilds it with such a site excluded. So every rewrite is accepted and none is declined (`NON_MONOTONE_FORMS_PROOF.md` T6, L4). Two alternatives are excluded. No adornment under a non-monotone form costs the extent of every predicate under one, a number the engine cannot see, and it evaluates a required predicate at its clause once per execution of the form at every iteration of the stratum (that proof's T4, T5). The sealed passing alone costs the decline on the shape of its T3, where the rule's own predicate is derived whole, and it refuses a required predicate whose position is bound after the recursion, which the evaluation at the clause answers (its T5).
- The dependency walk records every form on an invocation's path, at any depth: `not` and `not-join`, `or-default` and `or-default-join`, an aggregating inner query, and a subquery body. The refusal below reads that record. The alternative is a record of the innermost form alone. Under it a fallback inside a subquery body overwrites the subquery body's mark, and a fallback enclosing an admitted subquery reads as the subquery.
- A predicate with required positions under a non-monotone form is adorned when the sealed passing binds every position it requires. Its adorned copy has no required run, so it is derived once in the rewritten set's fixpoint. Otherwise the site is left unadorned, and the executor derives the predicate at its clause, under the positions the branch binds there, once per execution of the form. A fixpoint never holds a predicate with required positions (`NON_MONOTONE_FORMS_PROOF.md` T5). The branch binds the required position before the invocation (§2.4, §2.5), so that evaluation answers. No refusal at `NewRuleSet` and none at the rewrite reads such a site. The alternative is a refusal of the rule. That records the rewrite's limitation as the query's error, which goal 6 of `SCOPE_EXECUTION_AND_CONTRACTS.md` excludes.
- `NewRuleSet` refuses an invocation of the rule's own stratum under `or-default`, `or-default-join` or an aggregating inner query, at any depth. It admits one under a subquery whose inner query does not aggregate (§2.5). The refusal reads every fallback form and aggregating inner query the walk recorded on the invocation's path. It is a typed error naming the rule, the predicate and the form.
- A free invocation of a predicate whose needed strata hold a predicate with required positions is rewritten too, with its head all free. So each required predicate is adorned from the clauses before its invocation. `NewRuleSet` refuses no recursion between a required predicate and a free one. A positive invocation whose planned clauses before it leave a required position free is refused at the rewrite as `RequiredBindingError`.

The roster reads headers: an attribute variable inside a form is the form's own unless the header names it. The plan holds the roster the prepared text names under no rule set. It is collected once at planning, and the vocabulary check reads it there. The planner collects it under an option, `PlannerOptions.CollectRosters`. A database sets the option for the executors it builds, from whether a source the query runs over refuses unknown attributes, whether that source is the database's own at `$` or a named one. So a plan over sources that refuse nothing carries no roster and costs no walk, which goal 7 of `SCOPE_EXECUTION_AND_CONTRACTS.md` asks of an option that is off. The option is part of the cache's key (§5.2). The alternative is the roster collected for every plan, a walk of the prepared text for every plan a database that refuses nothing makes. The alternative to the plan as the roster's home is the roster held on the parsed `query.Query`, collected once per query value. `Prepare`'s result is not held on the query (§5.1), so a roster collected there would read headerless forms, where this roster reads headers. The keywords the query's invocations pass at positions the rule set reads as attributes are read at the check, through `RuleSet.invocationAttributes`. It maps the plan's invocation constants through the rule set's attribute positions on each execution. The rule bodies' own roster is the rule set's, collected once.

## 6. The algebra package

The package's shape is `main`'s, which is the clarity goal 4 names:

- One operator per form, one compiler per operator, and the passes and the decompiler over them.
- `compileOrJoin` has two union routes. One is for branches holding no `not`, `not-join` or `missing?`. The other, `compileOrUnionCorrelated`, compiles the branches over a placeholder for the required run, which is how an anti-join inside a branch reads the header.
- No compiler takes a headerless form, since none arrives (§5.1).
- `Union` carries the header's two halves under the names `LateralUnion` uses (§5.3). No second name stands for `Output`.
- `AntiJoin` carries no flag for an explicit join. `compileNotJoin` is its one compiler, and `decompileAntiJoin` emits a `NotJoinClause`.
- `Compile` takes the prepared query and the rule set it orders invocations under, and nothing else.

## 7. The two oracles

The algebra engine on against off checks whether the optimizer preserves meaning. That covers compile fidelity, decorrelation, the get-else rewrite, project insertion, and any order the decompiler produces, each against the clause-based path on the same prepared query. The optimizer-mode matrix runs every query under both.

The Go reference in the differential tests checks the semantics of §2. It is an interpreter of the generated query over the generated datoms, and the text the engine parses is rendered from that same query. For the rules shapes, the closures `closureOf` and `parityClosureOf` compute the reference. None of it shares code with the inference, the planner or the executor. When the two engine paths disagree, the matrix says that one is wrong, and the reference says which. When both are wrong alike, only the reference sees it.

Each oracle checks one question. The inference and the executor's handling of header forms are shared by both paths, and the reference checks them.

Cost is measured, not claimed. The executor evaluates the two branch forms by running each branch once per window of the subject, through the primitive, and probing per tuple of the window. `SCOPE_EXECUTION_AND_CONTRACTS.md` §4.2 to §4.4 state that, and they state the `not-join` body run once per window over the window's distinct projection onto its header. A window's keys are sought as that design's §4.6 states: per key below its crossover, and above it through the matcher's binding-driven scan, `HashJoinScan` or its merge join in `hash_join_matcher.go`, as a top-level pattern with bound symbols does. `SCOPE_EXECUTION_AND_CONTRACTS.md` §8 names the benchmarks. `BenchmarkPrepare` (`datalog/query/prepare_test.go`) measures preparation, which runs per execution.

## 8. The engine's vocabulary

This section answers goal 11 of §1: the engine's own queries run on every database it opens. The principle of §4, applied to the schema, delivers it. The engine's words are declared once, into every running schema, and every reader reads them there. The engine has words of its own: the commit mark, the entity id a pull pattern names, the snapshot marker's words, the branch record's words, the definition's ref to its declaration, and the seven words of a declaration. They form a schema the engine installs into every running schema (`engineSchema`, `datalog/storage/schema_facts.go`). `RULES_WITNESSES_AND_SPECULATIVE_VALUES.md` §4 states the same schema. It is installed in memory and never declared as facts. Each of its words resolves by its declaration, never by inference. So an attribute entity's ref to its current declaration resolves to one declaration, whatever the ops hold. Two alternatives are excluded. The first is no definitions for the engine's words. Then `:db.attribute/declaration` is undeclared on a database with a given schema, it resolves by inference, and the V-bound path of a pattern on it emits every ref datom. The second is the engine's words declared as facts. Then a user transaction writing `:db.attribute/declaration` commits, and every later open fails. And a many-valued declaration of `:db/cardinality` or `:db/ident` fails every later `SetSchema` and `Import`.

- `validateAttributeStorable` refuses a user's write of an engine attribute. `Transaction.Add`, `Transaction.Set`, `Transaction.Remove` and `Transaction.Retract` run it on every datom's attribute. So the check takes the transaction beside the keyword.
- Import's replay is exempt, as it is from every schema check.
- The engine's own writers, `assertSchemaFacts`, `addBranchRecord` and the marker `Snapshot` writes, pass the check. The transaction the engine opens for itself carries that provenance, and the check reads it.
- A given schema that redeclares an engine word is refused wherever a schema is given: at open and at `SetSchema`. The alternative is accepting it. Then the word resolves by the given definition, and a many-valued `:db/cardinality` or `:db/ident` fails every later `SetSchema` and `Import`, the failure named above.
- `Schema.Add` panics on an ident longer than `datalog.MaxAttributeBytes`, as it panics on a wrong-vocabulary keyword, since a definition is constructed there. `ParseSchema` returns the error instead.
- `SetSchema` and `adoptDeclarations` compose the whole next state, validate it, and then publish it under the lock. So no field records a step that failed.
- `adoptDeclarations` composes the branch's running schema from the definitions its store states after the parent's declarations are asserted, as an open composes its own. So the branch runs with what its store states.
- A cache entry carries the resolution rule it was resolved under: cardinality and uniqueness together. The alternative is an entry keyed on cardinality alone. A change of uniqueness leaves such an entry in place. So a matcher minted before a change from unique to plain would serve, after the change, a value resolved under the unique rule.
