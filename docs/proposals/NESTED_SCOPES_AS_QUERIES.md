# Nested Scopes as Queries

**Status**: Proposed

Every nested scope is a query, and its interface is its header, written in
the text. This document derives that sentence from the engine as it stands,
states the design that makes it true, and records what the design changes
for queries and what it deletes.

## 0. The fact and its owner

Two facts decide how a nested form runs.

- The interface of the form: which of the variables inside it belong to the
  enclosing scope.
- The bound set at a clause: which variables are bound when the clause runs,
  given the interfaces, the query's inputs and the order the planner chose.

`ScopeOf` in `datalog/query/clause_scope.go` owns the first. `ClauseBlockers`
in `datalog/query/clause_ready.go` derives the second from it, and
`ClauseReady` is its emptiness test. Both planner paths consume `ClauseReady`:
the clause-based planner through `clauseSelectable` in
`datalog/planner/clause_utils.go`, and the algebra bridge through
`orderClausesForCompile` in `datalog/algebra/compile_order.go`.

For the three forms with a header, `or-join`, `not-join` and
`or-default-join`, the interface is the header. It is a function of the text,
and `ScopeOf` states it completely: `headerScope`, and the `NotJoinClause`
and `OrDefaultJoinClause` cases. A branch variable outside the header is
local, and renaming it changes nothing.

## 1. Where the definition is missing

For `or`, `or-default` and `not`, the record says the form unifies with the
enclosing scope on every free variable its branches share with it. Sharing is
decided by what the enclosing scope binds when the form runs. That is a fact
about an evaluation, not about the text. No function of the clause and the
program can state it, and `ScopeOf` states what it can:

- `OrClause`: Provides is the intersection of the branches' provides, and
  Correlates is the variables a branch consumes without providing. A
  variable one branch provides and another does not is in neither set.
- `OrDefaultClause`: the same, plus the entity variable of each branch's
  first pattern.
- `NotClause`: Correlates is every free variable of the body.

Every reader that needs the whole set computes it from its own context:

- the clause-based planner's gate, `inferenceFormDependsOnPendingProvider`,
  from the closure of what the unselected clauses can provide
- the executor, through `branchFreeVariables` in
  `datalog/executor/or_fallback_relation.go` and `scopeBindsABranchLocal` in
  `datalog/executor/query_executor.go`, from the groups in hand
- the algebra compiler, in `compileNot` and `compileOrUnionWithJoinVars` in
  `datalog/algebra/compile.go`, from the current node's symbols and
  `boundDomain`
- the magic rewrite, in `rewriteBody` in `datalog/query/magic.go`, from the
  bound set it carries left to right
- the attribute roster, in `scopeFrame` in
  `datalog/query/attribute_names.go`, as every variable

Two more facts about the engine sit under the same defects.

A nested scope is not ordered. On the clause-based path a branch or body runs
in written order through `executeInnerClauses`, and a subquery's inner query
runs in written order through `executeSubquery` calling `Execute`. The
algebra path orders every branch and body through `compileClausesFrom`, but
`compileSubquery` wraps the inner query unordered in a `LateralJoin`, so a
subquery body is the one scope neither path orders. The algebra bridge,
`optimizeViaAlgebra` in `datalog/planner/algebra_bridge.go`, compiles with no
rule set, so on that path an invocation of a predicate with required positions
is read as providing every argument.

A nested scope is evaluated per outer tuple. `OrFallbackIterator` runs each
branch once per outer tuple, `filterWithNotClause` and
`filterWithNotJoinClause` run the body once per key combination, and
`executeSubquery` runs the inner query once per input combination. An
invocation inside a scope would therefore run its fixpoint once per tuple.
`hoistInvocations` avoids that by deriving the invocation once, before the
scope runs, against the whole outer relation. The hoist is two
approximations: it binds the invocation from every outer column, including
columns the scope's interface does not carry, and it binds before the
scope's own earlier clauses have run.

## 2. Datomic's rules

From docs.datomic.com/query/query-data-reference.html:

- `or`: "All clauses used in an or clause must use the same set of
  variables, which will unify with the surrounding query." "Datomic will
  attempt to push the or clause down until all necessary variables are
  bound, and will throw an exception if that is not possible."
- `or-join`: "[variable+] specifies which logic variables should unify." A
  variable outside the list "is only used inside the or clause and doesn't
  need to unify with the outer clause."
- `not`: "All variables used in a not clause will unify with the surrounding
  query." The same push-down sentence, throwing "if that is not possible".
- `not-join`: a variable outside the list "is used only inside the not-join
  clause and will not unify."
- Required bindings exist for rules only: `rule-vars = [variable+ |
  ([variable+] variable*)]`.

Under the same-set rule an `or` has no variable that some branches bind and
others do not, so the text answers which of its variables the surrounding
query sees. `parseOrClause` in `datalog/parser/parser.go` checks the branch
count and nothing else, so janus accepts asymmetric branches. Every
ambiguity about an inference form's interface lives in the variables that
rule forbids: the ones some branches bind and others do not.

## 3. The design

### 3.1 One inference, in the planner, before either path

The planner rewrites every inference form into its header form: `or` to
`or-join`, `not` to `not-join`, `or-default` to `or-default-join`. The header
is computed from the text by one rule:

- A variable inside the form that occurs elsewhere in the enclosing scope is
  shared. Elsewhere is another clause of the same list, the enclosing form's
  own header, the query's `:in`, or the query's `:find`.
- A variable that occurs only inside one branch is local.
- For `or`, the monotone form, a shared variable every branch binds is an
  output, and any other shared variable is required.
- For `not` and `or-default`, the non-monotone forms, every shared variable
  is required.

A required variable is bound before the form runs. The planner pushes the
form down until its required variables are bound, and a query in which no
order binds one is refused naming the form and the variable. That is
Datomic's push-down rule with Datomic's exception.

The inference runs under the rule set, because an invocation's required
positions make a variable consumed. It runs in the planner, where the rule
set is in hand, and its output is a query whose text carries the headers, so
every phase still parses back to itself. Both planner paths start from the
inferred query, and the algebra bridge orders under the same rule set.

Deleted: the gate and `inferenceFormBranches`; the inference inside
`compileNot`, `compileOrUnionWithJoinVars` and `compileOrUnionCorrelated`;
the first-pattern-entity rule in the `OrDefaultClause` case of `ScopeOf`.

### 3.2 One header grammar

`or-join` takes the `[[required...] outputs...]` header that
`or-default-join` has. A flat `or-join` header is all outputs, as now, so
every existing query stands. A `not-join` header is all required. The parser,
the query builder and the formatter carry the grammar.

### 3.3 Every nested scope is planned and run as a query over its interface

One runner serves three callers: a rule body, a form's branch or body, and a
subquery's inner query. A rule body already runs this way: `planBody` in
`datalog/executor/rules.go` plans it as a query under the rule set, and
`executeRealizedPlan` runs it with the relations it reads as groups.

- A branch or body is planned as `[:find required... outputs... :in $ %
  [[required...] ...] :where branch]` through `PlanQuery` with
  `Bindings{Symbols: required, Rules}`. Its plan enters the plan cache under
  its own text.
- It runs once, with one relation group: the outer groups that carry the
  required variables, joined and projected onto them.
- `or-join` unions the branch results over the header and joins the outer on
  the required variables.
- `or-default-join` runs branch 1 over the projection, branch 2 over the
  projection minus the groups branch 1 answered, and so on. That is the
  `LateralUnion` the algebra IR states.
- `not-join` anti-joins the outer on the header with the body's projection.
- A subquery's inner query runs once over the relation of its input
  combinations. An aggregating inner query carries its inputs as group keys,
  so an aggregate is per combination, and a tuple or scalar binding keeps
  its one-tuple-per-combination check per group. That is the transformation
  the algebra decorrelation pass applies on its path, moved to the one
  place every scope passes through.
- An invocation inside a scope is a clause at its planned position.
  `boundPositions` reads the groups, `seedRelation` projects them, and one
  fixpoint runs.

Deleted: `OrFallbackRelation`, `OrFallbackIterator` and `projectionPlan`;
the written-order loop of `executeInnerClauses`; `executeOrClauseUnion`,
`branchesNeedCorrelatedExecution`, `clausesNeedCorrelation` and
`scopeBindsABranchLocal`; the combination loops of `filterWithNotClause`,
`filterWithNotJoinClause` and `executeSubquery`; `hoistInvocations`,
`hoistBranches`, `hoistWalk` and the union of two invocations adorned
alike; `computeOrBranchOutputSymbols`, `collectBranchOutputSymbols` and
their siblings.

### 3.4 The magic rewrite reads headers

`passing.within(header)` applies to every form, since rule bodies are
inferred before they are rewritten. The rewrite keeps two sets it conflates
today: the variables bound before an invocation, which decide
`CheckRequired`, and the variables a magic rule can derive, which decide the
adornment. A required position that is bound but not derivable is left
unadorned and evaluated at its clause with the body's bindings, which
`deriveInvocation` does for every predicate with required positions.

### 3.5 The roster reads headers

`scopeFrame.sharesAll` goes, and `interfaceVariables` is the header.
`attributePositionsOf` in `datalog/query/rules.go` stops at a header. The
roster is a property of the inferred query, which is a function of the text
and the rule set, so it is collected with the plan, and `under` and
`widenedBy` go with the per-execution re-resolution they served.

## 4. The engine's own vocabulary

The same principle, applied to the schema. The engine writes and reads
attributes it never declares: `:db/ident` and the other six words of a
declaration, `:db.attribute/declaration`, `:db/txInstant`, and the snapshot
and branch words. Under the design they are a schema the engine installs into
every running schema. `Transaction.Add`, the one validation every write
passes, refuses a user write of an engine attribute and a given schema that
redeclares one. `Schema.Add` checks the ident length, since a definition is
constructed there. `SetSchema` and `adoptDeclarations` compose the whole next
state, validate it, and then publish it under the lock, so no field records a
step that failed. A cache entry carries the resolution rule it was resolved
under, cardinality and uniqueness together.

## 5. What changes for queries

- Asymmetric `or` branches stay legal, which is more permissive than
  Datomic. A variable they share with the enclosing scope is bound before the
  form, or the query is refused naming the form and the variable. Today the
  same variable unifies or is local depending on the planner path and the
  written order.
- Two forms that each partially bind one variable nothing else binds are
  refused, since no order binds the variable before either form.
- `or-default`'s correlation keys are inferred from the text by the rule in
  §3.1. `OR_FALLBACK_SEMANTICS.md` says they are never inferred; it changes
  to "inferred from the text for or-default, declared for or-default-join".
- A subquery body is ordered by the planner like every other scope. The
  sentence "A subquery body runs in its written order" leaves `RULES.md`, and
  with it the rule that an invocation in a subquery body stands after the
  clause binding its required position.
- Renaming a local variable never changes a result, in every form.

## 6. Measurement

Correctness is measured by differential tests: generated queries, with rules
and without, in both optimizer modes, each result compared to a reference
computed outside the engine and to the result at the commit the change
starts from. The reference states the semantics of §5, so a query §5 refuses
is expected to be refused.

Cost is measured, not claimed. The per-tuple path narrows a branch through
the entity cache, in `buildBranchFromEACache` and `outerJoinKeys`. The
set-at-a-time path seeks through the matcher's bound-group path, the way a
top-level pattern does. The or, or-default, not and subquery benchmarks run
before and after, and the result decides whether the cache narrowing is
carried into the runner.

## 7. Order of work

1. The differential tests, with their baselines.
2. The inference and the header grammar.
3. The runner, in three steps: rule bodies are already on it; form branches
   and bodies; subqueries.
4. The rewrite's two sets.
5. The roster on headers.
6. The engine's vocabulary.

## 8. Rulings

Ruled 2026-10-14:

- Asymmetric `or` branches stay legal, with their shared variables bound
  before the form. The alternative, Datomic's refusal of asymmetric
  branches, is not taken.
- `or-default`'s keys are inferred from the text by the rule in §3.1.
- Subqueries take the runner in the same change, landed last of the three
  scopes.

Derived, with the alternative each excludes:

- The inference runs in the planner, not the parser, because the header
  depends on the rule set. A parse-time inference would read every
  invocation as providing its arguments.
- The header is computed from the text's occurrences, not from what an
  execution binds. The alternative is the state this document starts from.
- For the non-monotone forms every shared variable is required. Treating a
  shared variable every branch binds as an output would make the fallback
  decision's grouping depend on the planner's order.
