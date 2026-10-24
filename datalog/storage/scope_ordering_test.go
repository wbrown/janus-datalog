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
	cases := []struct {
		name     string
		entities map[string][]fact
		text     string
		want     []string
	}{
		{"an expression written before its binder, the A12 query",
			map[string][]fact{
				"e0": {{":p", ref("e2")}, {":q", ref("e4")}, {":r", int64(1)}},
				"e1": {{":q", ref("e3")}, {":r", int64(1)}},
				"e2": {{":k", kw(":r")}, {":q", ref("e5")}, {":r", int64(0)}},
				"e3": {{":k", kw(":p")}},
				"e4": {{":k", kw(":q")}, {":p", ref("e2")}, {":q", ref("e3")}},
				"e5": {{":q", ref("e3")}},
			},
			`[:find ?e :where [?e :q _] [?e ?a _] [(ground :q) ?a] (or-default (and [(identity ?l) ?x] [?l :k :q]) (and [?m ?a _] [?x :q ?m]))]`,
			[]string{"e0", "e1", "e2", "e4", "e5"}},
		{"an expression waiting on the pattern of its branch",
			map[string][]fact{
				"s0": {{":self", ref("s0")}},
				"e1": {{":k", kw(":p")}},
			},
			`[:find ?a ?y :where (or-join [?a ?y] [?y :k ?a] (and [?e ?a ?y] [(identity ?e) ?y]))]`,
			[]string{":p e1", ":self s0"}},
		{"an expression waiting on a pattern of the query",
			map[string][]fact{
				"s0": {{":p", ref("s0")}},
				"e1": {{":p", ref("s0")}},
			},
			`[:find ?y :where [?e :p ?y] [(identity ?e) ?y]]`,
			[]string{"s0"}},
		{"two patterns and two expressions in a cycle",
			map[string][]fact{
				"a1": {{":p", ref("b1")}},
				"b1": {{":q", ref("a1")}},
				"a2": {{":p", ref("b2")}},
				"b2": {{":q", ref("a1")}},
			},
			`[:find ?a :where [?a :p ?y] [(identity ?b) ?y] [?b :q ?z] [(identity ?a) ?z]]`,
			[]string{"a1"}},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				names := namedEntities(t, d, c.entities)
				rel, err := d.Query(c.text)
				require.NoError(t, err, c.text)
				require.Equal(t, c.want, renderedRows(t, rel, names), c.text)
			})
		}
	}
}
