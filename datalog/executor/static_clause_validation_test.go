package executor

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Static clause-shape rules are user-boundary contracts enforced at three
// doors with one message: the parser (EDN text), the qb builder (Go
// construction), and the executor entry (hand-built ASTs). This pins the
// executor door: a hand-built statically-invalid query is rejected before
// planning, identically under both planner modes. See
// docs/bugs/BUG_SUBQUERY_BINDING_ARITY_VALIDATED_AT_DIFFERENT_LAYERS.md and
// docs/bugs/BUG_NOTJOIN_HEADER_VALIDATION_ONLY_ON_ALGEBRA_PATH.md.
func TestExecutorEntryRejectsStaticallyInvalidClauses(t *testing.T) {
	valAttr := datalog.NewKeyword(":item/val")
	matcher := NewMemoryPatternMatcher([]datalog.Datom{
		{E: datalog.NewIdentity("item:1"), A: valAttr, V: int64(1), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	})
	e := datalog.NewSymbol("?e")
	v := datalog.NewSymbol("?v")
	other := datalog.NewSymbol("?other")

	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: e},
		query.Constant{Value: valAttr},
		query.Variable{Name: v},
	}}

	cases := map[string]struct {
		clause  query.Clause
		wantErr string
	}{
		"subquery binding arity": {
			clause: &query.SubqueryPattern{
				Query: &query.Query{
					Find:  []query.FindElement{query.FindVariable{Symbol: v}},
					Where: []query.Clause{pattern},
				},
				Inputs:  []query.PatternElement{query.Variable{Name: e}},
				Binding: query.TupleBinding{Variables: []query.Symbol{v, other}},
			},
			wantErr: "subquery tuple binding declares 2 symbol(s), but the inner :find has 1 element(s)",
		},
		"not-join header completeness": {
			clause: &query.NotJoinClause{
				JoinVars: []query.Symbol{e},
				Clauses: []query.Clause{
					&query.Comparison{
						Op:    datalog.SymEQ,
						Left:  query.VariableTerm{Symbol: e},
						Right: query.VariableTerm{Symbol: other}, // consumed, unbound, undeclared
					},
				},
			},
			wantErr: "not-join header",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			q := &query.Query{
				Find:  []query.FindElement{query.FindVariable{Symbol: v}},
				Where: []query.Clause{pattern, tc.clause},
			}
			for _, mode := range optimizerModes {
				t.Run(mode.name, func(t *testing.T) {
					exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
					_, err := exec.Execute(q)
					if err == nil {
						t.Fatalf("expected the executor entry to reject the query with %q, got no error", tc.wantErr)
					}
					if !strings.Contains(err.Error(), tc.wantErr) {
						t.Fatalf("expected error containing %q, got %q", tc.wantErr, err.Error())
					}
				})
			}
		})
	}
}
