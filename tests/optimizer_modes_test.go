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

// createBackendModeDB creates a test database on a named backend whose default
// planner options carry this mode, so queries through db.Query and the pull
// APIs run on the mode's path without per-call option plumbing. Callers set
// schema, cache and the rest on opts; BackendName, Path and PlannerOptions come
// from the backend and the mode.
func createBackendModeDB(t testing.TB, backend storage.Backend, mode optimizerMode, opts storage.DatabaseOptions) *storage.Database {
	t.Helper()
	planner := mode.plannerOptions()

	opts.BackendName = backend.Name
	opts.Path = ""
	if backend.Persistent {
		opts.Path = t.TempDir()
	}
	if opts.CompressionThreshold == 0 {
		// This harness has always handed its tests a bare BinaryKeyEncoder, so
		// compression is off unless a test asks for it. The constructor reads an
		// unset threshold as the 512-byte production default; -1 is how it spells
		// the encoder these tests have been running on.
		opts.CompressionThreshold = -1
	}
	opts.PlannerOptions = &planner
	db, err := storage.NewDatabaseWithOptions(opts)
	if err != nil {
		t.Fatalf("failed to create %s/%s database: %v", backend.Name, mode.name, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// eachBackendAndMode runs body over the storage axis crossed with the optimizer
// axis. Backends come from storage.AvailableBackends, so a backend the build
// has is a backend this package's integration tests execute against.
func eachBackendAndMode(t *testing.T, body func(t *testing.T, db *storage.Database)) {
	t.Helper()
	eachBackendAndModeOpts(t, storage.DatabaseOptions{}, body)
}

// eachBackendAndModeWith hands the mode to the body as well as the database,
// for a test that builds its own executor or matcher options instead of taking
// the database's — the mode has to reach those too, or the leg runs under a
// name whose optimizer path it never used.
func eachBackendAndModeWith(t *testing.T, body func(t *testing.T, db *storage.Database, mode optimizerMode)) {
	t.Helper()
	for _, backend := range storage.AvailableBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			for _, mode := range optimizerModes {
				t.Run(mode.name, func(t *testing.T) {
					body(t, createBackendModeDB(t, backend, mode, storage.DatabaseOptions{}), mode)
				})
			}
		})
	}
}

// eachBackendAndModeOpts is eachBackendAndMode for a test that needs a schema,
// the cache disabled, or anything else on the database.
func eachBackendAndModeOpts(t *testing.T, opts storage.DatabaseOptions, body func(t *testing.T, db *storage.Database)) {
	t.Helper()
	for _, backend := range storage.AvailableBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			for _, mode := range optimizerModes {
				t.Run(mode.name, func(t *testing.T) {
					body(t, createBackendModeDB(t, backend, mode, opts))
				})
			}
		})
	}
}
