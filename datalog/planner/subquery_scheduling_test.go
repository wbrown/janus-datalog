package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func schedulingPattern(e, attr, v query.PatternElement) *query.DataPattern {
	return &query.DataPattern{Elements: []query.PatternElement{e, attr, v}}
}

// TestCorrelatedSubqueryDefersBehindSimultaneouslyReadySiblings pins the
// greedy tie-break: once a pattern binds ?e, a NOT filter and a correlated
// subquery on ?e are simultaneously executable, and dependency ordering
// cannot separate them. A correlated subquery executes once per input
// combination, so it must schedule after every clause that can narrow its
// input — behind the NOT. Derived 2026-07 on
// BenchmarkSubqueryDeferralScheduling: deferral is ~8× on this shape.
func TestCorrelatedSubqueryDefersBehindSimultaneouslyReadySiblings(t *testing.T) {
	e := datalog.NewSymbol("?e")
	total := datalog.NewSymbol("?total")

	pattern := schedulingPattern(
		query.Variable{Name: e},
		query.Constant{Value: datalog.NewKeyword(":user/active")},
		query.Constant{Value: true},
	)
	notClause := &query.NotClause{Clauses: []query.Clause{
		schedulingPattern(
			query.Variable{Name: e},
			query.Constant{Value: datalog.NewKeyword(":user/blocked")},
			query.Constant{Value: true},
		),
	}}
	correlated := &query.SubqueryPattern{
		Query: &query.Query{},
		Inputs: []query.PatternElement{
			query.Constant{Value: datalog.SymDollar},
			query.Variable{Name: e},
		},
		Binding: query.TupleBinding{Variables: []query.Symbol{total}},
	}

	phases, err := createPhasesGreedy(
		[]query.Clause{correlated, notClause, pattern},
		[]query.Symbol{e, total},
		map[query.Symbol]bool{},
	)
	if err != nil {
		t.Fatalf("phasing failed: %v", err)
	}

	assertScheduledAfter(t, phases, correlated, notClause,
		"correlated subquery must schedule after the simultaneously-ready NOT filter")
}

// TestUncorrelatedSubqueryKeepsDataSourceScheduling pins the exemption: an
// uncorrelated subquery executes exactly once wherever it is placed, so
// deferring it cannot reduce its cost and would forfeit its bindings as
// early join input. It keeps data-source scoring and schedules ahead of the
// NOT filter.
func TestUncorrelatedSubqueryKeepsDataSourceScheduling(t *testing.T) {
	e := datalog.NewSymbol("?e")
	threshold := datalog.NewSymbol("?threshold")

	pattern := schedulingPattern(
		query.Variable{Name: e},
		query.Constant{Value: datalog.NewKeyword(":user/active")},
		query.Constant{Value: true},
	)
	notClause := &query.NotClause{Clauses: []query.Clause{
		schedulingPattern(
			query.Variable{Name: e},
			query.Constant{Value: datalog.NewKeyword(":user/blocked")},
			query.Constant{Value: true},
		),
	}}
	uncorrelated := &query.SubqueryPattern{
		Query: &query.Query{},
		Inputs: []query.PatternElement{
			query.Constant{Value: datalog.SymDollar},
		},
		Binding: query.TupleBinding{Variables: []query.Symbol{threshold}},
	}

	phases, err := createPhasesGreedy(
		[]query.Clause{notClause, uncorrelated, pattern},
		[]query.Symbol{e, threshold},
		map[query.Symbol]bool{},
	)
	if err != nil {
		t.Fatalf("phasing failed: %v", err)
	}

	assertScheduledAfter(t, phases, notClause, uncorrelated,
		"uncorrelated subquery keeps data-source scoring and precedes the NOT filter")
}

// assertScheduledAfter fails unless later appears after earlier in the
// flattened phase clause order.
func assertScheduledAfter(t *testing.T, phases []ClausePhase, later, earlier query.Clause, msg string) {
	t.Helper()
	laterIdx, earlierIdx := -1, -1
	position := 0
	for _, phase := range phases {
		for _, clause := range phase.Clauses {
			if clause == later {
				laterIdx = position
			}
			if clause == earlier {
				earlierIdx = position
			}
			position++
		}
	}
	if laterIdx < 0 || earlierIdx < 0 {
		t.Fatalf("clauses missing from phases: later=%d earlier=%d", laterIdx, earlierIdx)
	}
	if laterIdx < earlierIdx {
		t.Errorf("%s (got position %d before %d)", msg, laterIdx, earlierIdx)
	}
}
