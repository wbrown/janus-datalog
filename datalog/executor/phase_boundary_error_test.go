package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// failingScanMatcher returns a streaming relation that defers an error (like a
// real storage scan whose value fails to decode) for one attribute, and ordinary
// data for any other. It lets a synthetic multi-phase RealizedPlan drive a
// failing scan into a specific phase.
type failingScanMatcher struct {
	failAttr datalog.Keyword
	dataRel  Relation
}

func (m *failingScanMatcher) Match(q *query.Query, _ Relations) (Relation, error) {
	pattern, err := q.SingleDataPattern()
	if err != nil {
		return nil, err
	}
	var syms []query.Symbol
	for _, el := range pattern.Elements {
		if v, ok := el.(query.Variable); ok {
			syms = append(syms, v.Name)
		}
	}
	if c, ok := pattern.GetA().(query.Constant); ok {
		if attr, ok := c.Value.(datalog.Keyword); ok && attr == m.failAttr {
			// A real scan surfaces a decode failure as a StreamingRelation whose
			// iterator defers the error to Error(); mirror that here so Project and
			// the phase pipeline see a realistic failing source.
			inner := NewMaterializedRelation(syms, nil).Iterator()
			return NewStreamingRelation(syms, &failingIterator{inner: inner, failAfter: 0}), nil
		}
	}
	return m.dataRel, nil
}

// TestExecuteRealized_NonLastPhaseKeep_SurfacesScanError feeds a hand-built
// two-phase RealizedPlan to the executor. Phase 0's scan fails (deferred error)
// and is a non-last phase with Keep symbols, so its result flows through the Keep
// materialization in ExecuteRealized and then across the phase boundary into phase 1.
// The failure must surface, not be laundered into an empty result.
//
// The planner currently never emits multi-phase plans (the greedy phaser
// collapses every query into one phase — unintentionally), so this laundering
// site is only reachable via a synthetic plan today. That does not make the
// laundering acceptable: when the phaser is fixed to emit real multi-phase plans,
// a failing scan in a non-last phase must already propagate.
func TestExecuteRealized_NonLastPhaseKeep_SurfacesScanError(t *testing.T) {
	symE := datalog.NewSymbol("?e")
	symV := datalog.NewSymbol("?v")
	symW := datalog.NewSymbol("?w")
	aAttr := datalog.NewKeyword(":doc/a")
	bAttr := datalog.NewKeyword(":doc/b")

	varE := query.Variable{Name: symE}
	varV := query.Variable{Name: symV}
	varW := query.Variable{Name: symW}

	// Phase 0 scans :doc/a (fails); Phase 1 scans :doc/b joining on ?e.
	p0 := &query.DataPattern{Elements: []query.PatternElement{varE, query.Constant{Value: aAttr}, varV}}
	p1 := &query.DataPattern{Elements: []query.PatternElement{varE, query.Constant{Value: bAttr}, varW}}

	plan := &planner.RealizedPlan{
		Query: &query.Query{
			Find:  []query.FindElement{query.FindVariable{Symbol: symW}},
			In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
			Where: []query.Clause{p0, p1},
		},
		Phases: []planner.RealizedPhase{
			{
				// Non-last phase projects its exact ?e boundary for phase 1.
				Query: &query.Query{
					Find:  []query.FindElement{query.FindVariable{Symbol: symE}},
					In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
					Where: []query.Clause{p0},
				},
				Provides: []query.Symbol{symE},
				Keep:     []query.Symbol{symE},
			},
			{
				// Last phase: joins phase 0's ?e with :doc/b.
				Query: &query.Query{
					Find:  []query.FindElement{query.FindVariable{Symbol: symW}},
					In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RelationInput{Symbols: []query.Symbol{symE}}},
					Where: []query.Clause{p1},
				},
				Available: []query.Symbol{symE},
				Provides:  []query.Symbol{symW},
			},
		},
	}

	dataRel := NewMaterializedRelation(
		[]query.Symbol{symE, symW},
		[]Tuple{{datalog.NewIdentity("e1"), "w1"}},
	)
	matcher := &failingScanMatcher{failAttr: aAttr, dataRel: dataRel}
	exec := NewExecutor(matcher, nil)

	result, err := exec.ExecuteRealized(NewContext(), plan, nil)
	if err == nil && result != nil {
		err = driveErr(result)
	}
	require.ErrorIs(t, err, errInjectedIterator,
		"a failing scan in a non-last phase must surface, not be laundered by boundary materialization")
}
