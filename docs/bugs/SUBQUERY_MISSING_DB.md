# Bug: qb.Subquery Does Not Pass Database to Correlated Subqueries

## Summary

When using `qb.Subquery()` with a subquery that declares `qb.DB` in its `In()` clause, the generated EDN omits the database `$` from the subquery invocation. This causes the subquery to receive incorrect inputs, breaking correlated subqueries.

## Expected Behavior

When a subquery is defined with `:in $ ?s` (database + scalar input), and invoked with a correlated variable, the generated EDN should pass both the database and the variable:

```edn
(q [:find (count ?t)
    :in $ ?s
    :where [?t :task/scenario ?s]
           [?t :task/status :status/complete]]
   $ ?scenario) [[?taskCount]]]
;;  ^ database must be passed here
```

## Actual Behavior

The query builder generates:

```edn
(q [:find (count ?t)
    :in $ ?s
    :where [?t :task/scenario ?s]
           [?t :task/status :status/complete]]
   ?scenario) [[?taskCount]]]
;; ^ missing $ - only variable passed
```

This causes the subquery to misinterpret `?scenario` as the database, breaking correlation entirely. The subquery runs uncorrelated, returning global aggregates instead of per-tuple values.

## Reproduction

```go
package main

import (
	"fmt"
	"github.com/wbrown/janus-datalog/datalog/qb"
)

func main() {
	// Outer query variable
	scenario := qb.NewVar("scenario")
	taskCount := qb.NewVar("taskCount")

	// Subquery with DB + scalar input
	t := qb.NewVar("t")
	s := qb.NewVar("s")
	subquery := qb.Query().
		Find(qb.Count(t)).
		In(qb.DB, qb.Scalar(s)).  // Expects: database, then scalar
		Where(
			qb.Pat(t, qb.Kw(":task/scenario"), s),
			qb.Pat(t, qb.Kw(":task/status"), qb.Kw(":status/complete")),
		)

	// Main query using correlated subquery
	q := qb.Query().
		Find(scenario, taskCount).
		Where(
			qb.Pat(scenario, qb.Kw(":scenario/id"), qb.NewVar("_")),
			qb.Subquery(subquery, scenario).BindTuple(taskCount),
		).
		MustBuild()

	fmt.Println("Generated:")
	fmt.Println(q.String())
	fmt.Println()
	fmt.Println("Expected the subquery call to be:")
	fmt.Println("  (q [...] $ ?scenario) [[?taskCount]]")
	fmt.Println()
	fmt.Println("But got:")
	fmt.Println("  (q [...] ?scenario) [[?taskCount]]")
	fmt.Println()
	fmt.Println("The $ is missing!")
}
```

## Impact

This bug makes correlated subqueries unusable via the query builder. The workaround is to use raw EDN strings.

## Suggested Fix

In `qb.Subquery()`, when generating the subquery invocation, check if the subquery's `In()` clause includes `qb.DB`. If so, automatically prepend `$` to the inputs list in the generated EDN.

Alternatively, require explicit `qb.DB` in the `Subquery()` call:
```go
qb.Subquery(subquery, qb.DB, scenario).BindTuple(taskCount)
```

The first option (automatic) is cleaner since the subquery already declares it needs the database.
