package executor_test

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// An expression whose binding variable is already bound is a unification
// constraint (Datomic semantics): the tuple survives when the computed value
// equals the existing value and is dropped otherwise; the existing value is
// never replaced. The executor's ground-constants path already implements
// exactly this ("filter (unify) instead of extending", executeExpression);
// these tests pin the same contract for the per-tuple path, which must not
// overwrite the bound value. Overwriting silently corrupts tuples:
// the OHLC decorrelation failure produced tuples whose ?datetime came from the
// outer bar while the aggregates belonged to a different group, because the
// hour expressions overwrote the join keys instead of filtering.
//
// These pins involve no subquery, so the planner's subquery scheduling
// cannot affect them: the expression-provided query case and every
// function-level case are the direct reproducers of the executor defect.
// (The pattern-provided query case is enforced by pattern deferral today and
// pins that adjacent behavior.) See
// BUG_EXPRESSION_BINDING_OVERWRITES_BOUND_VARIABLE.md and its
// planner sibling
// BUG_UNCORRELATED_SUBQUERY_SCHEDULES_BEFORE_BINDING_PROVIDERS.md.

// TestExpressionOntoBoundVariableUnifies pins the query-level contract under
// both planner modes: an expression targeting a bound ?y keeps exactly the
// entities where ?y equals the computed value.
//
// The provider of ?y matters for WHERE the contract is enforced today.
// Pattern-provided: patternDependsOnPendingExpression defers the ?y pattern
// behind the expression, so the join itself unifies — correct results by
// scheduling. Expression-provided: nothing reorders two expressions, so the
// second one evaluates with ?y bound and takes the per-tuple path — the
// direct query-level reproducer of the overwrite defect.
func TestExpressionOntoBoundVariableUnifies(t *testing.T) {
	xAttr := datalog.NewKeyword(":test/x")
	yAttr := datalog.NewKeyword(":test/y")
	zAttr := datalog.NewKeyword(":test/z")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	e1 := datalog.NewIdentity("test:e1")
	e2 := datalog.NewIdentity("test:e2")

	cases := map[string]struct {
		datoms    []datalog.Datom
		queryText string
	}{
		"bound by pattern": {
			datoms: []datalog.Datom{
				{E: e1, A: xAttr, V: int64(1), Tx: tx},
				{E: e1, A: yAttr, V: int64(2), Tx: tx}, // 1+1 = 2 — unifies
				{E: e2, A: xAttr, V: int64(3), Tx: tx},
				{E: e2, A: yAttr, V: int64(7), Tx: tx}, // 3+1 ≠ 7 — dropped
			},
			queryText: `[:find ?x ?y
			  :where [?e :test/x ?x]
			         [?e :test/y ?y]
			         [(+ ?x 1) ?y]]`,
		},
		"bound by expression": {
			datoms: []datalog.Datom{
				{E: e1, A: xAttr, V: int64(1), Tx: tx},
				{E: e1, A: zAttr, V: int64(2), Tx: tx}, // ?y = 2*1; 1+1 = 2 — unifies
				{E: e2, A: xAttr, V: int64(3), Tx: tx},
				{E: e2, A: zAttr, V: int64(7), Tx: tx}, // ?y = 7*1; 3+1 ≠ 7 — dropped
			},
			queryText: `[:find ?x ?y
			  :where [?e :test/x ?x]
			         [?e :test/z ?z]
			         [(* ?z 1) ?y]
			         [(+ ?x 1) ?y]]`,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			matcher := factsDatabase(t, tc.datoms)
			q, err := parser.ParseQuery(tc.queryText)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}

			for _, mode := range OptimizerModes {
				t.Run(mode.Name(), func(t *testing.T) {
					exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
					result, err := exec.Execute(q)
					if err != nil {
						t.Fatalf("execute failed: %v", err)
					}
					tuples, err := CollectTuples(result, nil)
					if err != nil {
						t.Fatalf("collecting the results: %v", err)
					}
					if len(tuples) != 1 {
						for i, tuple := range tuples {
							t.Logf("tuple %d: %v", i, tuple)
						}
						t.Fatalf("expected exactly the unifying entity (1 tuple), got %d", len(tuples))
					}
					tuple := tuples[0]
					if !datalog.ValuesEqual(tuple[0], int64(1)) || !datalog.ValuesEqual(tuple[1], int64(2)) {
						t.Fatalf("expected [1 2], got %v", tuple)
					}
				})
			}
		})
	}
}
