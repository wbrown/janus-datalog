# Evaluation failure as a third truth value

The failure contract decides when an evaluation failure fails a query. It is stated for a conjunction with one failing clause. This document proposes a contract that decides every form the engine accepts. Each clause, form and scope is true, false or failed at an assignment, and the forms combine those values by Kleene's strong three-valued connectives.

## Sources

- The contract today: `docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md`, §1 (the failure contract, the evaluating mark, the failed tuple), P10, P13, T11, T13 and §7.
- The goal it serves: the Optimizer Mode Matrix header, `datalog/storage/optimizer_modes_test.go`.
- The departures recorded against it: `docs/bugs/BUG_OPTIMIZER_PATHS_ANSWER_DIFFERENTLY_WHEN_A_CORRELATED_SUBQUERY_FAILS_ON_A_VALUE.md` and `docs/bugs/BUG_A_FAILED_TUPLE_RAISES_AT_AN_OPERATOR_THAT_DOES_NOT_CARRY_IT.md`.
- The rewrites whose proofs it reaches: `docs/ALGEBRA.md`, Rules 1, 4 and 5.
- The audit entry on failures: `docs/reviews/ANTIPATTERN_AUDIT_2026_07.md`, A6.
- The code:
  - `datalog/executor/windowed_form.go`: `canFailOnAValue`, `windowedIterator.Next`, `windowedIterator.buildBranch`
  - `datalog/executor/window_reader.go`: `windowReader.carriedFailure`
  - `datalog/executor/iterator_composition.go`: `failedTupleIterator`
  - `datalog/executor/aggregation.go`: `resolveAggregate`, `updateSum`
  - `datalog/executor/rules.go`: `ruleEvaluation.runStratum`, `stratumEvaluation.run`
  - `datalog.ValidateDatomValue`

## Goals

1. Every plan of a query answers it alike. The two optimizer paths are two plans of one query. So are the orders the planner may choose, and an invocation's free and bound evaluations.
2. Every reader answers the query's outcome. A reader that stops early answers what a read to the end answers.
3. A failed evaluation fails the query where the forms around it leave the answer undecided, judged form by form. A false conjunct decides its conjunction. A true branch decides its disjunction, and a true witness decides the tuple it projects to.
   - The alternative is to fail the query wherever no false clause excludes the failure's assignment, whatever a true branch or witness decides.
   - It fails queries whose answer is decided.
   - Its disjunction is not the dual of its conjunction under negation, so two spellings of one condition answer differently (§3).
4. No failure becomes a value. A failed evaluation is the query's error, or its assignment is excluded. It never appears in an answer as NaN, nil or a default. Arithmetic over a non-number is an error (`ANTIPATTERN_AUDIT_2026_07.md`, A6).
5. One rule decides every form the engine accepts: a conjunction, a disjunction, a negation, the fallback, a nested scope, an aggregate and a rule.

## 1. The contract today

- The failure contract: "an evaluation failure fails the query only at an assignment where every other clause holds" (`PULL_BASED_HASH_JOIN_PROOF.md` §1).
- T11 makes such a failure the query's error through every reader, for a relation carrying the evaluating mark.
- T13 has a windowed form decide a failed tuple of its subject at the tuple's key.
- The contract is stated for a conjunction with one failing clause, which leaves two gaps.
- The first gap is an assignment where two clauses fail.
  - Neither failure meets "every other clause holds", so the text fails no query there.
  - The assignment then drops out of the answer with no error, against goal 4.
  - The engine raises the failure its plan meets first. A filter and an evaluator answer no failed tuple for their source's failure, so the next one ends with it (`PULL_BASED_HASH_JOIN_PROOF.md` §5, P13).
- The second gap is every form beside the conjunction. The text says nothing of a failure inside a disjunction, a negation, the fallback, a nested scope, an aggregate or a rule body. The engine decides each by the iterator the failure meets:
  - A windowed form ends with a failed tuple's failure at the first output it answers for the tuple (`windowedIterator.Next`).
  - A branch whose build fails ends the whole form, whatever another branch holds (`windowedIterator.buildBranch`).
  - A fold that refuses a value fails the aggregation (`updateSum`, `resolveAggregate`).
  - A rule body's failure ends the rule's evaluation, at any tuple the evaluation derives. `ruleEvaluation.runStratum` absorbs each body's result and returns the first failure.
- The engine departs from the contract's own text in three shapes the bug records state: a fold and a body that fail at a key no outer tuple reaches, and a failed tuple that passes an operator that does not carry it.

## 2. The contract proposed

### 2.1 Values

- At an assignment of the variables it reads, each clause, form and scope is true, false or failed.
- A data pattern is true where its datom resolves and false elsewhere. It never fails.
- A predicate is true or false as its function answers. It is failed where the function fails on the assignment's values.
- An expression is true where its binding's values are among the tuples the binding reads from the function's value, and false where they are not. It is failed where the function fails, or where the value's shape is not the binding's (`bindingTuples`).
- A database function is failed where its entity value is no identity (`toIdentity`, `datalog/query/database_function.go`).
- A fault of the store, a scan or a decode that fails, is no evaluation failure. This contract does not decide it, as the hash-join proof's §7 does not.

### 2.2 Connectives

The connectives are Kleene's strong ones. In the order false < failed < true, a conjunction takes the minimum of its parts and a disjunction the maximum.

- A conjunction is false where any part is false, failed where no part is false and some part is failed, and true otherwise. A query's body, a branch's body and a rule's body are conjunctions.
- A disjunction is true where any branch is true, failed where no branch is true and some branch is failed, and false otherwise. `or` and `or-join` are disjunctions.
- A negation is true where its body is false, false where its body is true, and failed where its body is failed. `not` and `not-join` are negations.
- A form reads its local variables existentially. A branch's own variables, a negation body's and an inner query's are its locals. The form's value at an outer assignment is the maximum, in that order, over the extensions of its locals.

### 2.3 The fallback

- `or-default` and `or-default-join` answer, at a key of the required run, the first branch's results where that branch has any, and the next branch's otherwise.
- Read in these connectives, the form at a key `k` and outputs `o` is `(has₁(k) ∧ b₁(k, o)) ∨ (¬has₁(k) ∧ rest(k, o))`. `has₁(k)` is the maximum of `b₁` over every output at `k`.
- So where the first branch is failed at `k` and true at no output there, the choice of branch is undecided.
- The form at `(k, o)` is then false only where both `b₁(k, o)` and `rest(k, o)` are false. It is failed elsewhere.

### 2.4 Nested scopes

- A subquery at an outer assignment reads its inner query's answer at the inputs the call passes.
- It is true where the binding's tuple is a true tuple of that answer, failed where it is a failed one, and false otherwise.
- The inner query's answer is its `:find` relation in these values (2.6).

### 2.5 Aggregates

- An aggregate at a group is failed where a member of the group is failed, or where its fold fails on a member's value. Otherwise its value is the fold of the members.
- A fold fails on a value `updateSum` refuses, one that is no int64 or float64. It fails on a result `resultSum` or `resultAvg` refuses, a NaN (`resolveAggregate`).
- An aggregating inner query whose group is failed at an input answers a failed tuple at every value of the aggregate's position.
- So a subquery is failed at an outer assignment that reaches a failed group, whatever value its binding names.

### 2.6 The query's answer

- The answer is the query's `:find` relation in these values.
- A projected tuple is true where some assignment projecting onto it is true. It is failed where none is true and some is failed.
- The query fails where that relation holds a failed tuple, or where an aggregate of the `:find` is failed at a group. Otherwise it answers its true tuples.
- Every reader answers that outcome.

### 2.7 Rules

- A predicate's tuples are true, failed or absent.
- A tuple's value is the maximum of its rules' bodies over their extensions.
- The predicate is the least fixpoint of its rules in the order false < failed < true. The connectives are the minimum and the maximum in that order, both monotone, so the least fixpoint exists over a finite set of tuples.
- During the fixpoint a tuple's value only rises. A tuple failed under one derivation becomes true where a later derivation is true.
- A negation reads a lower stratum's predicate once that stratum's fixpoint is reached, as stratified negation does today.

## 3. Why this contract

Each alternative fails a goal.

- Raising a failure wherever a plan evaluates the function. The outcome then depends on which clause binds the function's inputs first, against goal 1. The hash-join proof's §1 makes this argument.
- The contract as written. It decides one failing clause in a conjunction and nothing else, against goal 5. It drops an assignment with two failing clauses with no error, against goal 4.
- Masking a failure by a false clause and never by a true one.
  - It fails queries whose answer is decided, against goal 3.
  - Its disjunction is not the dual of its conjunction under negation.
  - Where `P` fails and `Q` holds, `(not (or P Q))` is failed: `P` or `Q` is failed under this alternative, and so is its negation.
  - `(not P) (not Q)` is false: the negation of `Q` is false, and a false conjunct decides.
  - So two spellings of one condition answer differently, against goal 1.
- Failing exactly where some completion of the failed values would change the answer.
  - It is not computed form by form.
  - Where `?v` is a failed expression's output, `(or [(> ?v 5)] [(<= ?v 5)])` holds at every value of `?v`, while each branch alone is failed.
  - Deciding it needs every completion of every failed value, which no evaluation of the forms one at a time reads.
- Answering a failure as a value, a NaN, a nil or a default. The answer then holds a value no completion supplies, against goal 4.

## 4. What it decides

Each shape below states the contract's answer, then what the engine does today.

- A conjunction with one failing clause, at an assignment where every other clause holds:
  - The assignment is failed, and the query fails.
  - The contract today says the same, and the engine raises.
- A conjunction with two failing clauses at one assignment:
  - The assignment is failed, and the query fails where no other clause is false.
  - The engine raises (§1).
- A projected tuple that a true assignment and a failed one both project to:
  - The tuple is true, and the failure fails nothing.
  - The engine raises, as T11 states.
- An or-join whose branch is true at a key while another branch is failed there:
  - The form is true at that key.
  - The failing branch's build ends the whole form (`windowedIterator.buildBranch`), so the engine raises.
- A not-join whose body is true at one extension and failed at another:
  - The negation is false, and the assignment is excluded.
  - The body's build ends the form, so the engine raises.
- A fallback whose first branch is failed at a key and true at no output there:
  - The form is failed at that key wherever its branches are not both false (2.3).
  - The engine raises.
- A correlated subquery whose `:find` sum or avg fails at a group:
  - The subquery is failed at an outer assignment that reaches the group, and at no other.
  - The optimizer-off path's early readers then read on, and the decorrelated aggregate decides the failure at the group's key.
  - The first bug record states what the engine does.
- A correlated subquery whose body fails at a key:
  - The subquery is failed at an outer assignment that reaches the key, and at no other.
  - The decorrelated subquery decides the failure at the key.
  - The first bug record states what the engine does.
- A windowed form whose subject is another windowed form's result:
  - The second form is failed only where it holds at an extension of the failed tuple.
  - The second bug record states what the engine does.
- An invocation's free and bound evaluations:
  - They answer alike at the tuples the query reads. A failure at a tuple only the free evaluation derives fails nothing.
  - `runStratum` ends the evaluation at any body failure. So an invocation evaluated free raises at a failure that the same invocation, evaluated bound, never derives.
- "Through every reader" (T11) follows from goal 2. A reader that stops early reads on wherever the relation can still hold a failed tuple.

## 5. Tests

The tests come before the change.

- The reference is an evaluator over assignments.
  - For a small database and a query, it enumerates the assignments of every scope.
  - It gives each clause, form and scope its value by §2.
  - It answers the query's outcome: its true tuples, or the failure.
- The differential checks generated databases and queries against the reference, on both optimizer paths and through every reader.
  - The readers are a read to the end, `:limit`, the existence verdict, `QueryOneInto` and a product.
  - Each run answers the reference's true tuples, or fails where the reference fails.
  - The failing clauses are an arithmetic expression over an attribute holding a string beside numbers, a sum or an avg over that attribute, and a database function over a value naming no entity.
  - The shapes are each form alone and each form nested in each other form, with the failure at a key the outer query reaches and at one it does not.
  - For rules, a predicate whose body fails at a tuple, invoked free and bound.
  - It extends the generated differentials `TestQueryShapesOnRandomGraphsMatchTheReference` and `TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation` (`datalog/storage`).
- The two bug records' reproducers, as they stand.
- One test per shape §4 lists, each beside its control.

## 6. The change

- The documents:
  - The hash-join proof's §1 states this contract, and T11, P13, L12 and §7 are derived from it.
  - Each rewrite rule's proof in `docs/ALGEBRA.md` shows that the rule keeps every failure at the keys it holds at.
  - Rules 1 and 4 evaluate the rewritten subquery over every key, so their proofs need the rewritten form to decide a failure at its key.
- Where the engine raises and the contract answers:
  - Every iterator a failed tuple passes between an evaluation and a reader answers the failed tuple, with the positions it holds no value at. Among them are the windowed forms, the hash join, the caching stream and the pattern join.
  - A branch's build records a failure against the key it holds at, and the form decides it there.
  - The aggregation records a failing group's failure against the group's key, and a probe of that key raises it.
  - The final projection holds each failed projected tuple until a true witness of it arrives or the relation ends.
- Where the engine answers and the contract fails: the evaluating mark reads the aggregates of a subquery's `:find` that can fail.
- Rules:
  - A predicate's set holds failed tuples beside true ones.
  - The fixpoint runs over the three values.
  - `runStratum` records a body's failed tuples where it now returns the first failure.

## 7. Cost

- A failed tuple arises only where an evaluation fails. So where none fails, each reader reads as it does now, except for the read-on below.
- A reader that stops early reads to the end of any relation that can hold a failed tuple, since no prefix shows that the rest holds none.
  - Every contract that meets goals 1 and 2 and raises failures needs this.
  - The contract today requires it of a relation carrying the evaluating mark (P10, T11).
- The read-on is confined to relations that can hold a failed tuple.
  - An uncorrelated subquery runs whole at its form's first window (`docs/ALGEBRA.md`, OR-Fallback Branch Caching), so no reader can stop before its failure.
  - A declared `:db.type/long` or `:db.type/double` attribute refuses a non-number at the write (`Transaction.Add`, `schema.ValidateDatom`).
  - A declared double admits every float64 but NaN (`datalog.ValidateDatomValue`), so a sum over it fails only where opposite infinities meet in one group.
- Where a failure occurs, a failed tuple travels with the positions it holds no value at. The reader of a projection holds each failed projected tuple until a true witness of it arrives or the relation ends.

## 8. What this does not decide

- A fault of the store (2.1).
- Which fold results are failures beyond the ones `resolveAggregate` binds. `updateSum` adds int64 values with no check of overflow, so a sum past int64's range wraps. Whether such a sum is a failure is a fact of the fold, which this contract reads and does not set.
