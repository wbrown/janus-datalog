package tests

import (
	"testing"

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
// Plan of record: docs/wip/OPTIMIZER_MODE_MATRIX.md
//
// This is the tests-package copy of datalog/storage/optimizer_modes_test.go.
// Cross-package test code (tests/, db, qb) cannot import storage's test
// files, so each package carries its own copy of this six-line axis.
// =============================================================================

// optimizerMode is one leg of the optimizer matrix.
type optimizerMode struct {
	name    string
	algebra bool
}

var optimizerModes = []optimizerMode{
	{"algebra_on", true},
	{"algebra_off", false},
}

// plannerOptions returns the default planner options with this mode applied.
func (m optimizerMode) plannerOptions() planner.PlannerOptions {
	opts := storage.DefaultPlannerOptions()
	opts.EnableAlgebraOptimizer = m.algebra
	return opts
}

// createOptimizerModeDB creates a test database whose default planner
// options carry this mode; queries through db.Query and the pull APIs run
// on the mode's path without per-call option plumbing.
func createOptimizerModeDB(t testing.TB, mode optimizerMode) *storage.Database {
	t.Helper()
	opts := mode.plannerOptions()
	db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
		Path:           t.TempDir(),
		PlannerOptions: &opts,
	})
	if err != nil {
		t.Fatalf("failed to create %s database: %v", mode.name, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
