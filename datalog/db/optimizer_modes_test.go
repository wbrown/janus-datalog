package db_test

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
// The optimizer paths cross with a backend, and a test's subject decides
// which. A test of the engine runs on optimizerModes: the tree store alone,
// since the backend supplies a scan bound's datoms in index order and nothing
// else. A test of the open API, which backend a path, a dump or an injected
// store lands on, runs on backendModes: every backend the build has.
//
// This package cannot import datalog/storage's test files, so it carries its
// own copy of the axis — the Go-testing reality for cross-package test code.
// Mirrors datalog/storage/optimizer_modes_test.go.
//
// Plan of record: docs/wip/OPTIMIZER_MODE_MATRIX.md
// =============================================================================

// optimizerMode is one leg of the matrix: a storage backend crossed with an
// optimizer path.
type optimizerMode struct {
	name    string
	algebra bool
	backend storage.Backend
}

// optimizerModes is the engine axis: the tree store crossed with the two
// optimizer paths.
var optimizerModes = buildOptimizerModes(engineBackends())

// backendModes is the store axis: every backend this build has crossed with
// the two optimizer paths.
var backendModes = buildOptimizerModes(storage.AvailableBackends())

// engineBackends is the backend the engine axis runs on: the tree store,
// which every build has.
func engineBackends() []storage.Backend {
	backend, err := storage.BackendNamed("memory-trees")
	if err != nil {
		panic(err)
	}
	return []storage.Backend{backend}
}

func buildOptimizerModes(backends []storage.Backend) []optimizerMode {
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

// plannerOptions returns the default planner options with this mode applied.
func (m optimizerMode) plannerOptions() planner.PlannerOptions {
	opts := storage.DefaultPlannerOptions()
	opts.EnableAlgebraOptimizer = m.algebra
	return opts
}
