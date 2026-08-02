package qb_test

import (
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// =============================================================================
// Optimizer Mode Matrix
// =============================================================================
//
// Every test that executes a query runs under both the non-optimizer and
// optimizer paths. The optimizer must never change results, and must never
// make things work: a query that fails without the algebra optimizer must
// fail with it, and vice versa — the two paths are observationally
// equivalent for every query the engine accepts.
//
// A test that passes in only one mode has found either a real divergence
// (a bug — ledger it) or optimizer-specific structure it means to pin — and
// a test pinning one mode sets EnableAlgebraOptimizer explicitly instead of
// looping the axis.
//
// This is qb_test's copy of the axis defined in
// datalog/storage/optimizer_modes_test.go: cross-package test code cannot
// share unexported test helpers, so each package that cannot import
// storage's test files carries its own six-line copy.
//
// Plan of record: docs/wip/OPTIMIZER_MODE_MATRIX.md
// =============================================================================

// optimizerMode is one leg of the matrix: a storage backend crossed with an
// optimizer path. Backends come from storage.AvailableBackends, so a backend
// this build has is one every executing test in this package runs against.
type optimizerMode struct {
	name    string
	algebra bool
	backend storage.Backend
}

var optimizerModes = buildOptimizerModes()

func buildOptimizerModes() []optimizerMode {
	algebraLegs := []struct {
		name string
		on   bool
	}{
		{"algebra_on", true},
		{"algebra_off", false},
	}
	backends := storage.AvailableBackends()
	modes := make([]optimizerMode, 0, len(backends)*len(algebraLegs))
	for _, backend := range backends {
		for _, algebra := range algebraLegs {
			modes = append(modes, optimizerMode{
				name:    backend.Name + "/" + algebra.name,
				algebra: algebra.on,
				backend: backend,
			})
		}
	}
	return modes
}

// plannerOptions returns the default planner options with this mode applied.
func (m optimizerMode) plannerOptions() planner.PlannerOptions {
	opts := storage.DefaultPlannerOptions()
	opts.EnableAlgebraOptimizer = m.algebra
	return opts
}
