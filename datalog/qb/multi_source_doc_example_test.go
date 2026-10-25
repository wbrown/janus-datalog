package qb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// MULTI_SOURCE.md's subquery example builds: an inner query over a named
// source and a scalar input, bound into the enclosing query as the relation
// over the headcount variable (docs/reference/MULTI_SOURCE.md).
func TestMultiSourceDocExample_Builds(t *testing.T) {
	users := Source("$users")
	dept := NewVar("dept")

	innerQ := Query().
		Find(Count(NewVar("e"))).
		In(users, Scalar(dept)).
		Where(
			PatFrom(users, NewVar("e"), Kw(":user/dept"), dept),
		)

	q, err := Query().
		Find(dept, NewVar("headcount")).
		Where(
			Pat(NewVar("d"), Kw(":dept/name"), dept),
			Subquery(innerQ, dept).BindRelation(NewVar("headcount")),
		).
		Build()
	require.NoError(t, err)
	require.Contains(t, q.String(), "[[?headcount] ...]", "the relation binding renders with its trailing dots")
}
