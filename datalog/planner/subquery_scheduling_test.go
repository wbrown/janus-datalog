package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
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

// TestUncorrelatedSubqueryDefersBehindPendingBindingProviders pins the limit
// of the uncorrelated-subquery exemption above: "executes exactly once
// wherever placed" is true of its cost, not of its join. A subquery relation
// joins the accumulated relation on whichever of its binding variables are
// bound at selection time; scheduling it while another pending clause still
// provides one of those variables joins on a subset of the keys, and the
// under-keyed join admits row combinations no later clause is entitled to
// remove. The subquery must defer until the pending providers of its binding
// variables have run. The exemption pin above still holds: a binding
// variable nothing else provides never defers the subquery.
//
// This is the planner half of the OHLC decorrelation failure — structural,
// independent of executor expression semantics. See
// docs/bugs/BUG_UNCORRELATED_SUBQUERY_SCHEDULES_BEFORE_BINDING_PROVIDERS.md
// and its executor sibling BUG_EXPRESSION_BINDING_OVERWRITES_BOUND_VARIABLE.md.
func TestUncorrelatedSubqueryDefersBehindPendingBindingProviders(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?year ?high
	  :where
	  [?e :price/time ?t]
	  [(year ?t) ?year]
	  [(q [:find ?py (max ?h)
	       :in $
	       :where [?b :price/time ?time]
	              [(year ?time) ?py]
	              [?b :price/high ?h]]
	      $) [[?year ?high] ...]]]`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	yearExpr := q.Where[1].(*query.Expression)
	grouped := q.Where[2].(*query.SubqueryPattern)

	phases, err := createPhasesGreedy(
		q.Where,
		[]query.Symbol{datalog.NewSymbol("?year"), datalog.NewSymbol("?high")},
		map[query.Symbol]bool{},
	)
	if err != nil {
		t.Fatalf("phasing failed: %v", err)
	}

	assertScheduledAfter(t, phases, grouped, yearExpr,
		"uncorrelated subquery binding ?year must schedule after the pending expression that provides ?year")
}

// TestSubqueryDeferralNeverStallsOnGatedProvider pins the deferral gate's
// progress guarantee against the composition that broke it: a subquery whose
// outputs feed an expression that feeds a pattern. The pattern is a
// clauseReady provider of the subquery's ?x — but the selection loop skips
// it (it uses ?y from the pending expression), the expression is not ready
// (it needs ?z from the subquery), and a gate that defers on merely-READY
// providers deadlocks the phase: "cannot create phase" for a valid query.
// Deferral may only wait on providers that are actually selectable this
// iteration — ready AND unskipped by every sibling gate.
func TestSubqueryDeferralNeverStallsOnGatedProvider(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?x ?y
	  :where
	  [(q [:find ?x ?z
	       :in $
	       :where [?x :bar ?z]]
	      $) [[?x ?z] ...]]
	  [(+ ?z 1) ?y]
	  [?x :rel ?y]]`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	grouped := q.Where[0].(*query.SubqueryPattern)
	pattern := q.Where[2]

	phases, err := createPhasesGreedy(
		q.Where,
		[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
		map[query.Symbol]bool{},
	)
	if err != nil {
		t.Fatalf("phasing failed on a valid query: %v", err)
	}

	// The subquery is the only selectable clause at the outset and must be
	// scheduled, unblocking the expression and then the pattern.
	assertScheduledAfter(t, phases, pattern, grouped,
		"the pattern consumes the expression output derived from the subquery, so it schedules after the subquery")
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
