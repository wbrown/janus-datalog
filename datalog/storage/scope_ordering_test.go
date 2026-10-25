package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A branch's clauses are ordered by the planner, never run in written order:
// an expression written before the pattern binding its input runs after it,
// and a pattern reading a variable an expression of the same scope binds runs
// before the expression when the expression waits on the pattern, where the
// expression unifies with the variable the pattern bound. The same holds of
// the query's own clauses.
func TestOrBranchExpressionIsOrderedAfterItsBinder(t *testing.T) {
	e0, e1, e2, e3, e4, e5 := named("e0"), named("e1"), named("e2"), named("e3"), named("e4"), named("e5")
	s0, a1, a2, b1, b2 := named("s0"), named("a1"), named("a2"), named("b1"), named("b2")
	cases := []struct {
		name  string
		facts []fact
		text  string
		want  []string
	}{
		{"an expression written before its binder, the A12 query",
			[]fact{
				{e0, ":p", e2}, {e0, ":q", e4}, {e0, ":r", int64(1)},
				{e1, ":q", e3}, {e1, ":r", int64(1)},
				{e2, ":k", kw(":r")}, {e2, ":q", e5}, {e2, ":r", int64(0)},
				{e3, ":k", kw(":p")},
				{e4, ":k", kw(":q")}, {e4, ":p", e2}, {e4, ":q", e3},
				{e5, ":q", e3},
			},
			`[:find ?e :where [?e :q _] [?e ?a _] [(ground :q) ?a] (or-default (and [(identity ?l) ?x] [?l :k :q]) (and [?m ?a _] [?x :q ?m]))]`,
			[]string{"e0", "e1", "e2", "e4", "e5"}},
		{"an expression waiting on the pattern of its branch",
			[]fact{{s0, ":self", s0}, {e1, ":k", kw(":p")}},
			`[:find ?a ?y :where (or-join [?a ?y] [?y :k ?a] (and [?e ?a ?y] [(identity ?e) ?y]))]`,
			[]string{":p e1", ":self s0"}},
		{"an expression waiting on a pattern of the query",
			[]fact{{s0, ":p", s0}, {e1, ":p", s0}},
			`[:find ?y :where [?e :p ?y] [(identity ?e) ?y]]`,
			[]string{"s0"}},
		{"two patterns and two expressions in a cycle",
			[]fact{{a1, ":p", b1}, {b1, ":q", a1}, {a2, ":p", b2}, {b2, ":q", a1}},
			`[:find ?a :where [?a :p ?y] [(identity ?b) ?y] [?b :q ?z] [(identity ?a) ?z]]`,
			[]string{"a1"}},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				names := commitFacts(t, d, c.facts)
				rel, err := d.Query(c.text)
				require.NoError(t, err, c.text)
				require.Equal(t, c.want, renderedTuples(t, rel, names), c.text)
			})
		}
	}
}
