package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/planner"
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
// The optimizer paths cross with a backend, and a test's subject decides
// which. A test of the engine, which is planning, execution, resolution, the
// cache and the language, runs on optimizerModes: the tree store alone,
// since the backend supplies a scan bound's datoms in index order and nothing
// else, and TestStoreBackendContract pins that at the seam for every backend.
// A test of the store, what it persists or how it reads, runs on
// backendModes: every backend the build has.
// TestBadgerAnswersWhatTheTreeStoreAnswersAndAgainAfterReopen runs the
// engine's query shapes on Badger once.
//
// Plan of record: docs/wip/OPTIMIZER_MODE_MATRIX.md
// =============================================================================

// optimizerMode is one leg of the matrix: a storage backend crossed with an
// optimizer path.
type optimizerMode struct {
	name    string
	algebra bool
	backend Backend
}

// optimizerModes is the engine axis: the tree store crossed with the two
// optimizer paths.
var optimizerModes = buildOptimizerModes(engineBackends())

// backendModes is the store axis: every backend this build has crossed with
// the two optimizer paths, for a test of persistence and reopen, dumps,
// snapshots, truncation, forks, read sessions, scan intake, index order, the
// key layout or the blob tier.
var backendModes = buildOptimizerModes(AvailableBackends())

// engineBackends is the backend the engine axis runs on: the tree store,
// which every build has.
func engineBackends() []Backend {
	backend, err := BackendNamed("memory-trees")
	if err != nil {
		panic(err)
	}
	return []Backend{backend}
}

func buildOptimizerModes(backends []Backend) []optimizerMode {
	algebraLegs := []struct {
		name string
		on   bool
	}{
		{"algebra_on", true},
		{"algebra_off", false},
	}
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

// pinned is an axis with one optimizer path fixed — still one leg per
// backend, because pinning a plan shape does not pin a store.
func pinned(modes []optimizerMode, algebra bool) []optimizerMode {
	var legs []optimizerMode
	for _, mode := range modes {
		if mode.algebra == algebra {
			legs = append(legs, mode)
		}
	}
	return legs
}

// byteKeyBackends is the axis for a test whose subject is the binary key
// encoding rather than the engine. Badger and BadgerEmulatorStore both hold encoded
// index keys and decode them into an iterator-owned workspace; MemoryTreeStore
// hands out the datom it already holds, so a property of that workspace — the
// key layout, or anything downstream of it such as the out-of-line value tier
// — is not a property it has.
//
// The switch has no silent default: a new backend fails here until someone says
// which side of that line it falls on.
func byteKeyBackends(t *testing.T) []optimizerMode {
	t.Helper()
	var modes []optimizerMode
	for _, mode := range backendModes {
		switch mode.backend.Name {
		case "badger", "badger-emulator":
			modes = append(modes, mode)
		case "memory-trees":
		default:
			t.Fatalf("byteKeyBackends: backend %q is unclassified — "+
				"does it hold encoded index keys?", mode.backend.Name)
		}
	}
	return modes
}

// plannerOptions returns the default planner options with this mode applied.
func (m optimizerMode) plannerOptions() planner.PlannerOptions {
	opts := DefaultPlannerOptions()
	opts.EnableAlgebraOptimizer = m.algebra
	return opts
}

// createOptimizerModeDB creates a test database on this mode's backend whose
// default planner options carry this mode, so queries through db.Query and the
// pull APIs run on the mode's path without per-call option plumbing. Callers
// set schema, cache, compression, the annotation handler and the rest on opts;
// BackendName, Path and PlannerOptions come from the mode.
//
// An annotation handler must arrive at open: every executor, matcher and
// relation the database builds is constructed with it.
func createOptimizerModeDB(t testing.TB, mode optimizerMode, opts DatabaseOptions) *Database {
	t.Helper()
	plannerOpts := mode.plannerOptions()
	opts.PlannerOptions = &plannerOpts
	return openBackendDB(t, mode.backend, opts)
}

// openBackendDB creates a test database on the named backend, leaving opts as
// the caller wrote it — including PlannerOptions. It is the constructor for a
// test whose own planner options are its subject: the mode axis has nothing
// left to vary there but the backend, so it takes a Backend rather than a mode.
//
// Every other test wants createOptimizerModeDB above, which runs both optimizer
// paths. Reaching for this one drops that half of the axis.
func openBackendDB(t testing.TB, backend Backend, opts DatabaseOptions) *Database {
	t.Helper()
	db, err := NewDatabaseWithOptions(backendOptions(t, backend, opts))
	if err != nil {
		t.Fatalf("failed to create %s database: %v", backend.Name, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// backendOptions returns opts placed on the named backend: its name, a
// temporary path when it persists, and compression off unless opts asks for
// it. A test asserting that an open fails builds its options here and calls
// NewDatabaseWithOptions itself.
func backendOptions(t testing.TB, backend Backend, opts DatabaseOptions) DatabaseOptions {
	t.Helper()
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
	return opts
}
