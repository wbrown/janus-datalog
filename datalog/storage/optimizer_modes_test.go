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
// Plan of record: docs/wip/OPTIMIZER_MODE_MATRIX.md
// =============================================================================

// optimizerMode is one leg of the matrix: a storage backend crossed with an
// optimizer path. Backends come from AvailableBackends, so a backend this build
// has is one every executing test on this axis runs against.
type optimizerMode struct {
	name    string
	algebra bool
	backend Backend
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
	backends := AvailableBackends()
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

// pinnedOptimizerModes is the axis with one optimizer path fixed — still one
// leg per backend, because pinning a plan shape does not pin a store.
func pinnedOptimizerModes(algebra bool) []optimizerMode {
	var modes []optimizerMode
	for _, mode := range optimizerModes {
		if mode.algebra == algebra {
			modes = append(modes, mode)
		}
	}
	return modes
}

// byteKeyBackends is the axis for a test whose subject is the binary key
// encoding rather than the engine. Badger and MemoryStore both hold encoded
// index keys and decode them into an iterator-owned workspace; MemoryTreeStore
// hands out the datom it already holds, so a property of that workspace is not
// a property it has.
//
// The switch has no silent default: a new backend fails here until someone says
// which side of that line it falls on.
func byteKeyBackends(t *testing.T) []optimizerMode {
	t.Helper()
	var modes []optimizerMode
	for _, mode := range optimizerModes {
		switch mode.backend.Name {
		case "badger", "memory":
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
// Store and PlannerOptions come from the mode.
//
// An annotation handler must arrive at open: every executor, matcher and
// relation the database builds is constructed with it.
func createOptimizerModeDB(t testing.TB, mode optimizerMode, opts DatabaseOptions) *Database {
	t.Helper()
	plannerOpts := mode.plannerOptions()

	// An injected store owns its encoder, so a compression threshold has to
	// reach the encoder here rather than through DatabaseOptions.
	encoder := &BinaryKeyEncoder{}
	if opts.CompressionThreshold != 0 {
		encoder.CompressionThreshold = opts.CompressionThreshold
	}
	store, err := mode.backend.Open(t.TempDir(), encoder)
	if err != nil {
		t.Fatalf("failed to open %s store: %v", mode.backend.Name, err)
	}
	// Registered before the database is built: NewDatabaseWithOptions can fail,
	// and an open store abandoned by t.Fatalf holds its directory lock for the
	// rest of the binary.
	t.Cleanup(func() { _ = store.Close() })

	opts.Path = ""
	opts.Store = store
	opts.PlannerOptions = &plannerOpts
	db, err := NewDatabaseWithOptions(opts)
	if err != nil {
		t.Fatalf("failed to create %s database: %v", mode.name, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
