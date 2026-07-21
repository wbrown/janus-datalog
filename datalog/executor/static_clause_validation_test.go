package executor

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestNotJoinAcceptsNestedExistentialNot pins that header completeness
// demands only MANDATORY outer requirements. A plain (not ...) nested inside
// a not-join body carries optional correlates: its free variables unify when
// the environment binds them and are existential otherwise (Datomic's
// unification rule) — they are not requirements the header must declare.
// This exact shape was wrongly rejected when validation flattened correlates
// through branchInterface and lost the CorrelatesOptional flag.
func TestNotJoinAcceptsNestedExistentialNot(t *testing.T) {
	goalAttr := datalog.NewKeyword(":event/goal")
	nameAttr := datalog.NewKeyword(":goal/name")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	goalA := datalog.NewIdentity("goal:a")
	goalB := datalog.NewIdentity("goal:b")
	matcher := NewMemoryPatternMatcher([]datalog.Datom{
		{E: goalA, A: nameAttr, V: "a", Tx: tx},
		{E: goalB, A: nameAttr, V: "b", Tx: tx},
		// goalA has an event, and that event's goal is flagged by some
		// entity — so the nested (not ...) fails for goalA, the not-join
		// body matches nothing, and goalA SURVIVES the outer not-join...
		// unless the flag entity exists, in which case the body match
		// stands and goalA is excluded. goalB has no event at all and
		// always survives.
		{E: datalog.NewIdentity("event:1"), A: goalAttr, V: goalA, Tx: tx},
	})

	q, err := parser.ParseQuery(`[:find ?name
	  :where
	  [?goal :goal/name ?name]
	  (not-join [?goal]
	    [?ev :event/goal ?goal]
	    (not [?flagger :other/flag ?goal]))]`)
	if err != nil {
		t.Fatalf("parser rejected a legal nested-existential not-join: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("execution rejected a legal nested-existential not-join: %v", err)
			}
			// goalA has an event and no flagger exists, so the body's
			// nested NOT holds, the body matches, and goalA is excluded.
			// goalB has no event and survives.
			if result.Size() != 1 {
				for i := 0; i < result.Size(); i++ {
					t.Logf("tuple %d: %v", i, result.Get(i))
				}
				t.Fatalf("expected only the event-less goal to survive, got %d rows", result.Size())
			}
			if !datalog.ValuesEqual(result.Get(0)[0], "b") {
				t.Fatalf("expected goal b to survive, got %v", result.Get(0))
			}
		})
	}
}

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
