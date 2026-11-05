# The optimizer paths answer differently when a correlated subquery fails on a value

**Status**: Open. At a failing key an outer tuple reaches, both optimizer paths fail the query: a fold's failure through a `:limit 0` or `:limit 1`, the existence verdict, `QueryOneInto`, a not-join body and an or-join branch, and a body's failure read whole or through a `:limit 0`. At a failing key no outer tuple reaches, the optimizer-off path answers the reached keys, and the optimizer-on path fails the query.

A correlated subquery whose `:find` sums or averages a value the fold refuses, or whose body evaluates a function on a value it fails on, is to fail the query where an outer tuple reaches the failing key, and only there (the failure contract, `docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md` §1). The two optimizer paths are to be observationally equivalent for every query the engine accepts (the Optimizer Mode Matrix, `datalog/storage/optimizer_modes_test.go`).

## The two paths

### With the optimizer off: the evaluating mark

1. The subquery runs as a windowed form over the keys its subject's windows carry (`executeSubquery`, `datalog/executor/query_executor.go`; `PULL_BASED_HASH_JOIN_PROOF.md`, P5). Its form evaluates the body and folds the groups at those keys alone.
2. `executeSubquery` marks its result as evaluating where `canFailOnAValue` finds that the subquery clause can fail on a value (P10).
3. `canFailOnAValue`'s subquery arm reads the body and `subqueryFoldCanFail` (`datalog/executor/windowed_form.go`).
   - The body can fail where it holds a clause `canFailOnAValue` names, such as an expression whose function reads a variable.
   - `subqueryFoldCanFail` holds for a subquery whose `:find` holds an aggregate whose fold can fail on a value (`aggregateOps.failsOnAValue`, set for sum and avg in `resolveAggregate`, `datalog/executor/aggregation.go`).
4. `sum` and `avg` fold through `updateSum`, which refuses a value that is no int64 or float64. `resultSum` and `resultAvg` refuse NaN. `count`'s fold refuses nothing, and `min`'s and `max`'s refuse nil alone, which no resolved value is.
5. So the result carries the mark. `:limit`, the existence verdict and a product's first pull read it to its end (P10, T11), and so does a `:limit 0` (`LimitRelation.take`).

### With the optimizer on: Rules 1 and 4

1. Rule 1 rewrites a correlated subquery whose `:find` aggregates: "The right side executes S once for ALL values of x, and aggregates per value of x and G" (`docs/ALGEBRA.md`, Rule 1).
2. Rule 4 rewrites a correlated subquery whose body holds a filtering clause, an expression among them: "The right side executes S once for ALL values of x" (`docs/ALGEBRA.md`, Rule 4).
3. Each rewritten call passes no variable (`docs/ALGEBRA.md`, the decompilation of Rules 1 and 4). Its `:find` still sums or averages, or its body still holds the clause that can fail, so `canFailOnAValue` holds, and the rewritten result carries the mark.
4. It reads nothing of the outer relation, so its form runs it once, over the unit relation, at the first window (`docs/ALGEBRA.md`, OR-Fallback Branch Caching). That run evaluates the body and folds the groups at every value of the correlation variable.
5. So a `:limit 0` reads the rewritten result to its end (`LimitRelation.take`), as it reads the correlated one.

## The shape that answers differently

The reproducer below asserts the contract at a failing key no outer tuple reaches, on memory-trees, in two shapes:

- A correlated subquery whose `:find` sums or averages. Sixteen groups hold numbers, and one holds a string beside a number.
- A correlated subquery whose body adds one to a value. Sixteen items hold numbers, and one holds a string.

In each, the outer pattern reaches the numeric groups or items alone. Its `algebra_off` cases pass, and its `algebra_on` cases fail:

| Shape | The contract | `algebra_on` answers |
|---|---|---|
| a failing group no outer tuple reaches (Rule 1) | each reached group's aggregate, no error | the error "sum/avg input string (x) is not int64/float64" |
| a failing body at a key no outer tuple reaches (Rule 4) | each reached key's value, no error | the error "+: operand x (string) is not a number" |

## Contracts it contradicts

- The Optimizer Mode Matrix (`datalog/storage/optimizer_modes_test.go`): "a query that fails without the algebra optimizer must fail with it, and vice versa — the two paths are observationally equivalent for every query the engine accepts."
- The failure contract (`PULL_BASED_HASH_JOIN_PROOF.md` §1): an evaluation failure fails the query only at an assignment where every other clause holds. A key no outer tuple reaches is at no such assignment, so its failure fails no query. The optimizer-on path fails the query there.
- The proofs of Rules 1 and 4 show that the two sides answer the same tuples for each outer tuple, not that they fail alike. The rewritten side evaluates keys the correlated side never evaluates (`docs/ALGEBRA.md`).

## Reach

- A correlated subquery Rule 1 rewrites, whose `:find` holds `sum` or `avg` over values the fold refuses or values whose sum is NaN, with the algebra optimizer on.
  - A value of no number type reaches the fold only from an attribute with no declared value type. A declared `:db.type/long` or `:db.type/double` refuses it at the write (`Transaction.Add`, `schema.ValidateDatom`).
  - A declared `:db.type/double` admits every float64 but NaN (`datalog.ValidateDatomValue`). So +Inf and -Inf in one group sum to NaN, which `resultSum` and `resultAvg` refuse.
- A correlated subquery Rule 4 rewrites, whose body holds a clause that fails on a value, with the algebra optimizer on.
  - The clauses that can fail on a value are those `canFailOnAValue` names (`PULL_BASED_HASH_JOIN_PROOF.md` §1).
  - The reproducer's clause is `+` over an operand that is no number.
- Every reader of such a subquery's result, a `:limit 0` among them. `LimitRelation.take` (`datalog/executor/limit_relation.go`) reads a relation carrying the evaluating mark to its end at a limit of zero too (`PULL_BASED_HASH_JOIN_PROOF.md`, T11), so the rewritten subquery runs at its first window and fails there.
  - At 69bfc3b a `:limit 0` opened no iterator of its source, so both paths answered the empty relation.

## What was not established

- That each case's plan holds the rewrite its shape names, Rule 1's or Rule 4's. Explaining each case's plan settles it.

## The fix, if taken

- The rewritten subquery's build records a failure against the key it arose at, and the build goes on.
  - A fold's failure is recorded against its group's key.
  - A body's failure is recorded against the correlation variable's value in the failed tuple its iterator answers (`PULL_BASED_HASH_JOIN_PROOF.md`, P13).
- A probe of that key raises the failure.
- A key no outer tuple probes raises nothing.
- Where nothing fails, nothing is recorded.

## Reproducers

These tests, in `datalog/storage/failure_contract_reach_test.go`, pin the shapes where an outer tuple reaches the failing key, and pass on both optimizer paths:

- `TestAnAggregatingSubqueryWhoseAggregateFailsOnAValueFailsTheQueryThroughEveryReader`: under sum and avg, a `:limit 1`, the existence verdict, a not-join body, an or-join branch and `QueryOneInto` over the failing group fail the query.
- `TestACorrelatedAggregateFailingAtOneGroupAnswersByTheFailureContractOnBothOptimizerPaths`: under sum and avg, a `:limit 0` over the failing group fails the query.
- `TestACorrelatedSubqueryWhoseBodyFailsAtOneKeyAnswersByTheFailureContractOnBothOptimizerPaths`: the failing body at a reached key fails the query, read whole and through a `:limit 0`.

`TestCanFailOnAValueReadsASubquerysFoldingAggregates` (`datalog/executor/can_fail_on_a_value_test.go`) asserts that a subquery whose `:find` sums or averages carries the mark whether or not its call passes a variable, and that one whose `:find` counts or takes a min or a max carries none.

The code below reproduces the shape that answers differently. Its assertions state the contract's expectation in each shape: no error, and each reached group's aggregate or each reached key's value. The table's `algebra_on` errors contradict that expectation, so under `algebra_on` each `require.NoError` fails.

```go
package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A correlated subquery whose aggregate cannot fold one group's value is
// expected to fail no query where no outer tuple reaches that group, and to
// answer each reached group's aggregate, on both optimizer paths. The outer
// pattern binds :grp/kind, which the failing group does not carry.
func TestACorrelatedAggregateFailingAtAGroupNoOuterTupleReachesFailsNoQuery(t *testing.T) {
	groupName := datalog.NewKeyword(":grp/name")
	groupKind := datalog.NewKeyword(":grp/kind")
	memberOf := datalog.NewKeyword(":mem/grp")
	value := datalog.NewKeyword(":mem/v")
	subquery := `[(q [:find (%s ?v) :in $ ?g :where [?m :mem/grp ?g] [?m :mem/v ?v]] $ ?g) [[?s]]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			for i := 0; i < 16; i++ {
				g := datalog.NewIdentity(fmt.Sprintf("aggregate-contract:g%d", i))
				require.NoError(t, tx.Add(g, groupName, fmt.Sprintf("g%d", i)))
				require.NoError(t, tx.Add(g, groupKind, "num"))
				for j := 0; j < 2; j++ {
					m := datalog.NewIdentity(fmt.Sprintf("aggregate-contract:g%d-m%d", i, j))
					require.NoError(t, tx.Add(m, memberOf, g))
					require.NoError(t, tx.Add(m, value, int64(i+j)))
				}
			}
			stringy := datalog.NewIdentity("aggregate-contract:stringy")
			require.NoError(t, tx.Add(stringy, groupName, "stringy"))
			for j, v := range []interface{}{"x", int64(1)} {
				m := datalog.NewIdentity(fmt.Sprintf("aggregate-contract:stringy-m%d", j))
				require.NoError(t, tx.Add(m, memberOf, stringy))
				require.NoError(t, tx.Add(m, value, v))
			}
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, aggregate := range []string{"sum", "avg"} {
				t.Run(aggregate, func(t *testing.T) {
					text := fmt.Sprintf(`[:find ?g ?s :where [?g :grp/kind "num"] `+subquery+`]`, aggregate)
					got, err := executor.CollectTuples(d.Query(text))
					// The contract's expectation. Under algebra_on the query fails
					// with "sum/avg input string (x) is not int64/float64", so this
					// assertion fails there: that failure is the reproduction.
					require.NoError(t, err, "no outer tuple reaches the failing group, so its failure fails no query")
					require.Len(t, got, 16, "each reached group's aggregate is answered")
				})
			}
		})
	}
}

// A correlated subquery whose body cannot evaluate at one key is expected to
// fail no query where no outer tuple reaches that key, and to answer each
// reached key's value, on both optimizer paths. The outer pattern binds
// :item/kind, which the failing item does not carry.
func TestACorrelatedSubqueryWhoseBodyFailsAtAKeyNoOuterTupleReachesFailsNoQuery(t *testing.T) {
	v := datalog.NewKeyword(":item/v")
	kind := datalog.NewKeyword(":item/kind")
	subquery := `[(q [:find ?b :in $ ?q :where [?q :item/v ?a] [(+ ?a 1) ?b]] $ ?q) [[?b]]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			for i := 0; i < 16; i++ {
				e := datalog.NewIdentity(fmt.Sprintf("body-contract:n%d", i))
				require.NoError(t, tx.Add(e, v, int64(i)))
				require.NoError(t, tx.Add(e, kind, "num"))
			}
			require.NoError(t, tx.Add(datalog.NewIdentity("body-contract:stringy"), v, "x"))
			_, err := tx.Commit()
			require.NoError(t, err)
			got, err := executor.CollectTuples(d.Query(`[:find ?q ?b :where [?q :item/kind "num"] ` + subquery + `]`))
			// The contract's expectation. Under algebra_on the query fails with
			// "+: operand x (string) is not a number", so this assertion fails
			// there: that failure is the reproduction.
			require.NoError(t, err, "no outer tuple reaches the failing key, so its failure fails no query")
			require.Len(t, got, 16, "each reached key's value is answered")
		})
	}
}
```

## Related

- `PULL_BASED_HASH_JOIN_PROOF.md` §7 lists the open shape.
- `BUG_A_FAILED_TUPLE_RAISES_AT_AN_OPERATOR_THAT_DOES_NOT_CARRY_IT.md` is the failure contract's other recorded departure.
