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
// planning, identically under both planner modes. A not-join body reading a
// variable outside its header is the inference's refusal, which the executor
// reaches on both modes too. See
// BUG_SUBQUERY_BINDING_ARITY_VALIDATED_AT_DIFFERENT_LAYERS.md.
func TestExecutorEntryRejectsStaticallyInvalidClauses(t *testing.T) {
	valAttr := datalog.NewKeyword(":item/val")
	var matcher PatternMatcher

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
				Binding: query.RelationBinding{Variables: []query.Symbol{v, other}},
			},
			wantErr: "subquery relation binding declares 2 symbol(s), but the inner :find has 1 element(s)",
		},
		"not-join body reads outside its header": {
			clause: &query.NotJoinClause{
				RequiredVars: []query.Symbol{e},
				Clauses: []query.Clause{
					&query.Comparison{
						Op:    datalog.SymEQ,
						Left:  query.VariableTerm{Symbol: e},
						Right: query.VariableTerm{Symbol: other}, // read, bound by neither the body nor the header
					},
				},
			},
			wantErr: "branch 1 reads ?other, which neither the branch nor the header binds; bind ?other in the branch or name it in the header",
		},
		"unregistered predicate function": {
			// Hand-built AST bypassing the parser: the boundary walk must
			// reject the unregistered name here too — the per-tuple Eval
			// guard alone never fires when upstream clauses match nothing.
			clause: &query.FunctionPredicate{
				Fn:   "test/never-registered-executor-door?",
				Args: []query.PatternElement{query.Variable{Name: v}},
			},
			wantErr: "unknown predicate function: test/never-registered-executor-door?",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			q := &query.Query{
				Find:  []query.FindElement{query.FindVariable{Symbol: v}},
				Where: []query.Clause{pattern, tc.clause},
			}
			for _, mode := range legsFor(t) {
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
