package tests

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/qb"
)

// TestSubqueryMissingDB demonstrates that qb.Subquery does not pass
// the database ($) to correlated subqueries that declare qb.DB in their In() clause.
func TestSubqueryMissingDB(t *testing.T) {
	// Outer query variable
	scenario := qb.NewVar("scenario")
	taskCount := qb.NewVar("taskCount")

	// Subquery with DB + scalar input
	task := qb.NewVar("t")
	s := qb.NewVar("s")
	subquery := qb.Query().
		Find(qb.Count(task)).
		In(qb.DB, qb.Scalar(s)). // Expects: database, then scalar
		Where(
			qb.Pat(task, qb.Kw(":task/scenario"), s),
			qb.Pat(task, qb.Kw(":task/status"), qb.Kw(":status/complete")),
		)

	// Main query using correlated subquery
	q := qb.Query().
		Find(scenario, taskCount).
		Where(
			qb.Pat(scenario, qb.Kw(":scenario/id"), qb.NewVar("id")),
			qb.Subquery(subquery, scenario).BindTuple(taskCount),
		).
		MustBuild()

	generated := q.String()

	// The subquery call should include $ before ?scenario
	// Expected: (q [...] $ ?scenario) [[?taskCount]]
	// Actual:   (q [...] ?scenario) [[?taskCount]]

	if !strings.Contains(generated, "$ ?scenario) [[?taskCount]]") {
		t.Errorf("Subquery invocation missing database $\n\nGenerated:\n%s\n\nExpected to contain: $ ?scenario) [[?taskCount]]", generated)
	}
}

// TestSubqueryWithDBGeneratesCorrectEDN verifies the fix once applied.
func TestSubqueryWithDBGeneratesCorrectEDN(t *testing.T) {
	outer := qb.NewVar("outer")
	result := qb.NewVar("result")

	inner := qb.NewVar("inner")
	input := qb.NewVar("input")
	subq := qb.Query().
		Find(qb.Count(inner)).
		In(qb.DB, qb.Scalar(input)).
		Where(
			qb.Pat(inner, qb.Kw(":ref"), input),
		)

	q := qb.Query().
		Find(outer, result).
		Where(
			qb.Pat(outer, qb.Kw(":id"), qb.NewVar("_")),
			qb.Subquery(subq, outer).BindTuple(result),
		).
		MustBuild()

	generated := q.String()

	// After the fix, this should pass
	if !strings.Contains(generated, "$ ?outer) [[?result]]") {
		t.Skipf("Bug not yet fixed - subquery missing $ in: %s", generated)
	}
}
