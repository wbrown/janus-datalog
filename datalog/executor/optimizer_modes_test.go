package executor

import (
	"reflect"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/planner"
)

// The optimizer mode matrix for the executor package: every test that plans a
// query (Execute/ExecuteWithContext/ExecuteWithRelations on a query, as
// opposed to hand-built RealizedPlans fed to ExecuteRealized) runs under both
// planner modes. The algebra optimizer must never change results and must
// never make a failing query work — the two paths are observationally
// equivalent for every accepted query. Tests that pin one mode's plan
// structure declare EnableAlgebraOptimizer explicitly instead of looping.
// Plan of record: docs/wip/OPTIMIZER_MODE_MATRIX.md.
//
// This package cannot import storage (import cycle), so it carries its own
// copy of the axis. Two base-option profiles are in live use here and the
// migration preserves each test's existing profile, adding only the algebra
// axis: NewExecutor's default profile via plannerOptions(), and the bare
// planner.PlannerOptions{} zero-value profile via zeroPlannerOptions().
type optimizerMode struct {
	name    string
	algebra bool
}

var optimizerModes = []optimizerMode{
	{"algebra_on", true},
	{"algebra_off", false},
}

// plannerOptions is NewExecutor's default profile with the mode's algebra
// setting — for tests that construct via bare NewExecutor today.
func (m optimizerMode) plannerOptions() planner.PlannerOptions {
	opts := defaultPlannerOptions()
	opts.EnableAlgebraOptimizer = m.algebra
	return opts
}

// zeroPlannerOptions is the bare zero-value profile with the mode's algebra
// setting — for tests that construct via NewExecutorWithOptions with a
// planner.PlannerOptions{} literal today.
func (m optimizerMode) zeroPlannerOptions() planner.PlannerOptions {
	return planner.PlannerOptions{EnableAlgebraOptimizer: m.algebra}
}

// TestOptimizerModeAxisProfiles pins the axis to its base profiles: the only
// field a mode changes is EnableAlgebraOptimizer, and plannerOptions derives
// from the same defaultPlannerOptions NewExecutor delegates to — the axis can
// never drift from the constructor's real default.
func TestOptimizerModeAxisProfiles(t *testing.T) {
	for _, mode := range optimizerModes {
		fromDefault := mode.plannerOptions()
		if fromDefault.EnableAlgebraOptimizer != mode.algebra {
			t.Fatalf("mode %s: plannerOptions algebra = %v", mode.name, fromDefault.EnableAlgebraOptimizer)
		}
		fromDefault.EnableAlgebraOptimizer = false
		// DeepEqual rather than ==: PlannerOptions carries Handler, a func, and a
		// struct holding one is not comparable. Neither profile registers a
		// handler, and DeepEqual holds two nil funcs equal, so this is exact. If a
		// profile ever did register one, DeepEqual would report the two as
		// unequal and this fails loudly — the right direction.
		if !reflect.DeepEqual(fromDefault, defaultPlannerOptions()) {
			t.Fatalf("mode %s: plannerOptions diverges from defaultPlannerOptions beyond the algebra flag", mode.name)
		}

		fromZero := mode.zeroPlannerOptions()
		if fromZero.EnableAlgebraOptimizer != mode.algebra {
			t.Fatalf("mode %s: zeroPlannerOptions algebra = %v", mode.name, fromZero.EnableAlgebraOptimizer)
		}
		fromZero.EnableAlgebraOptimizer = false
		if !reflect.DeepEqual(fromZero, planner.PlannerOptions{}) {
			t.Fatalf("mode %s: zeroPlannerOptions diverges from the zero value beyond the algebra flag", mode.name)
		}
	}
}
