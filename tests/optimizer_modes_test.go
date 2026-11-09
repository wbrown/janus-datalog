package tests

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// =============================================================================
// Optimizer Mode Matrix
// =============================================================================
//
// Every test that executes a query runs under every leg of the matrix: a
// setting of each planner option a test can turn on, crossed with a backend
// by the selectors below. An option decides how a query runs, never what it
// answers, so every leg answers alike, and an option must never make things
// work: a query that fails with an option off must fail with it on, and vice
// versa. The legs are observationally equivalent for every query the engine
// accepts.
//
// The legs are a pairwise covering design over seven options: the algebra
// optimizer, scan sharing, entity prefetch, the symmetric hash join,
// streaming joins, attribute-fetch fusion and streaming aggregation. Every
// pair of options takes all four settings on some leg. One leg is the
// reference, every option off, and one is DefaultPlannerOptions' profile.
//
// A test that passes on only some legs has found either a real divergence
// (a bug — ledger it) or a mechanism of one option it means to pin. A test
// pinning a mechanism selects the legs that carry the option, through
// legsWhere with the option's predicate, instead of setting the option
// itself: an option set inside a test is absent from the matrix.
//
// Plan of record: docs/wip/OPTIMIZER_MODE_MATRIX.md
//
// This is the tests-package copy of the table in
// datalog/storage/optimizer_modes_test.go. Cross-package test code cannot
// import storage's test files, so each package carries its own copy, and the
// tests at the end pin the copy to the same design. The mode carries no
// backend: the selectors below cross it with one.
// =============================================================================

// optimizerMode is one leg of the optimizer matrix: a setting of the seven
// options, named by the options on, in the order the fields are declared,
// joined with "+"; "none" when every option is off.
type optimizerMode struct {
	name        string
	algebra     bool
	sharing     bool
	prefetch    bool
	symmetric   bool
	streaming   bool
	fusion      bool
	aggregation bool
}

// optimizerModes is the pairwise covering design: the strength-2 orthogonal
// array on three bits (a, b, c), with the algebra optimizer at a, scan
// sharing at c, prefetch at a⊕c, the symmetric hash join at a⊕b⊕c, streaming
// joins at b, fusion at b⊕c and streaming aggregation at a⊕b. Each leg names
// the options it turns on and sets those alone.
var optimizerModes = []optimizerMode{
	{name: "none"},
	{name: "sharing+prefetch+symmetric+fusion", sharing: true, prefetch: true, symmetric: true, fusion: true},
	{name: "symmetric+streaming+fusion+aggregation", symmetric: true, streaming: true, fusion: true, aggregation: true},
	{name: "sharing+prefetch+streaming+aggregation", sharing: true, prefetch: true, streaming: true, aggregation: true},
	{name: "algebra+prefetch+symmetric+aggregation", algebra: true, prefetch: true, symmetric: true, aggregation: true},
	{name: "algebra+sharing+fusion+aggregation", algebra: true, sharing: true, fusion: true, aggregation: true},
	{name: "algebra+prefetch+streaming+fusion", algebra: true, prefetch: true, streaming: true, fusion: true},
	{name: "algebra+sharing+symmetric+streaming", algebra: true, sharing: true, symmetric: true, streaming: true},
}

// legsWhere is the legs keep admits, in the table's order: the axis of a
// test pinning one option's mechanism, still a leg per setting of the other
// options, because pinning one option pins no other.
func legsWhere(keep func(optimizerMode) bool) []optimizerMode {
	var legs []optimizerMode
	for _, mode := range optimizerModes {
		if keep(mode) {
			legs = append(legs, mode)
		}
	}
	return legs
}

// plannerOptions returns the default planner options with this leg's setting
// of every option applied.
func (m optimizerMode) plannerOptions() planner.PlannerOptions {
	opts := storage.DefaultPlannerOptions()
	opts.EnableAlgebraOptimizer = m.algebra
	opts.EnableScanSharing = m.sharing
	opts.EnableEntityPrefetch = m.prefetch
	opts.EnableSymmetricHashJoin = m.symmetric
	opts.EnableStreamingJoins = m.streaming
	opts.EnableAttributeFetchFusion = m.fusion
	opts.EnableStreamingAggregation = m.aggregation
	return opts
}

// legOption is one option of a leg: its name as a leg's name lists it, and
// whether the leg turns it on.
type legOption struct {
	name string
	on   bool
}

// legOptions is each option of a leg, in the order a leg's name lists them.
func legOptions(mode optimizerMode) []legOption {
	return []legOption{
		{"algebra", mode.algebra},
		{"sharing", mode.sharing},
		{"prefetch", mode.prefetch},
		{"symmetric", mode.symmetric},
		{"streaming", mode.streaming},
		{"fusion", mode.fusion},
		{"aggregation", mode.aggregation},
	}
}

// The modes are a pairwise covering design: every pair of the seven options
// takes all four settings on some mode.
func TestTheOptimizerModesCoverEveryPairOfOptions(t *testing.T) {
	options := legOptions(optimizerMode{})
	for x := range options {
		for y := x + 1; y < len(options); y++ {
			for _, xOn := range []bool{false, true} {
				for _, yOn := range []bool{false, true} {
					covered := false
					for _, mode := range optimizerModes {
						settings := legOptions(mode)
						if settings[x].on == xOn && settings[y].on == yOn {
							covered = true
							break
						}
					}
					require.True(t, covered, "%s=%v with %s=%v is on some mode", options[x].name, xOn, options[y].name, yOn)
				}
			}
		}
	}
}

// Each mode is named by the options it turns on; the first mode is the
// reference, every option off; and one mode is DefaultPlannerOptions'
// profile.
func TestTheOptimizerModesCarryTheReferenceAndTheDefaultProfile(t *testing.T) {
	for _, mode := range optimizerModes {
		var on []string
		for _, option := range legOptions(mode) {
			if option.on {
				on = append(on, option.name)
			}
		}
		name := strings.Join(on, "+")
		if name == "" {
			name = "none"
		}
		require.Equal(t, name, mode.name, "a mode is named by the options it turns on")
	}
	require.Equal(t, "none", optimizerModes[0].name, "the first mode is the reference, every option off")

	defaults := storage.DefaultPlannerOptions()
	profile := optimizerMode{
		algebra:     defaults.EnableAlgebraOptimizer,
		sharing:     defaults.EnableScanSharing,
		prefetch:    defaults.EnableEntityPrefetch,
		symmetric:   defaults.EnableSymmetricHashJoin,
		streaming:   defaults.EnableStreamingJoins,
		fusion:      defaults.EnableAttributeFetchFusion,
		aggregation: defaults.EnableStreamingAggregation,
	}
	carried := false
	for _, mode := range optimizerModes {
		mode.name = ""
		if mode == profile {
			carried = true
		}
	}
	require.True(t, carried, "a mode is DefaultPlannerOptions' profile: %+v", profile)
}

// A mode's planner options carry the mode's setting of every option, and
// differ from DefaultPlannerOptions in nothing else.
func TestAModesPlannerOptionsCarryItsSettings(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			opts := mode.plannerOptions()
			require.Equal(t, mode.algebra, opts.EnableAlgebraOptimizer, "the algebra optimizer")
			require.Equal(t, mode.sharing, opts.EnableScanSharing, "scan sharing")
			require.Equal(t, mode.prefetch, opts.EnableEntityPrefetch, "entity prefetch")
			require.Equal(t, mode.symmetric, opts.EnableSymmetricHashJoin, "the symmetric hash join")
			require.Equal(t, mode.streaming, opts.EnableStreamingJoins, "streaming joins")
			require.Equal(t, mode.fusion, opts.EnableAttributeFetchFusion, "attribute-fetch fusion")
			require.Equal(t, mode.aggregation, opts.EnableStreamingAggregation, "streaming aggregation")
			rest := storage.DefaultPlannerOptions()
			rest.EnableAlgebraOptimizer = mode.algebra
			rest.EnableScanSharing = mode.sharing
			rest.EnableEntityPrefetch = mode.prefetch
			rest.EnableSymmetricHashJoin = mode.symmetric
			rest.EnableStreamingJoins = mode.streaming
			rest.EnableAttributeFetchFusion = mode.fusion
			rest.EnableStreamingAggregation = mode.aggregation
			require.Equal(t, rest, opts, "the options differ from the defaults in the seven options alone")
		})
	}
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

// engineBackend is the backend the engine axis runs on: the tree store, which
// every build has. A test of the engine runs there alone, since the backend
// supplies a scan bound's datoms in index order and nothing else.
func engineBackend(t *testing.T) storage.Backend {
	t.Helper()
	backend, err := storage.BackendNamed("memory-trees")
	if err != nil {
		t.Fatal(err)
	}
	return backend
}

// eachMode runs body on the engine axis: the tree store crossed with the
// optimizer axis.
func eachMode(t *testing.T, body func(t *testing.T, db *storage.Database)) {
	t.Helper()
	eachModeOpts(t, storage.DatabaseOptions{}, body)
}

// eachModeWith hands the mode to the body as well as the database,
// for a test that builds its own executor or matcher options instead of taking
// the database's — the mode has to reach those too, or the leg runs under a
// name whose optimizer path it never used.
func eachModeWith(t *testing.T, body func(t *testing.T, db *storage.Database, mode optimizerMode)) {
	t.Helper()
	backend := engineBackend(t)
	t.Run(backend.Name, func(t *testing.T) {
		for _, mode := range optimizerModes {
			t.Run(mode.name, func(t *testing.T) {
				body(t, createBackendModeDB(t, backend, mode, storage.DatabaseOptions{}), mode)
			})
		}
	})
}

// eachModeOpts is eachMode for a test that needs a schema, the cache
// disabled, or anything else on the database.
func eachModeOpts(t *testing.T, opts storage.DatabaseOptions, body func(t *testing.T, db *storage.Database)) {
	t.Helper()
	backend := engineBackend(t)
	t.Run(backend.Name, func(t *testing.T) {
		for _, mode := range optimizerModes {
			t.Run(mode.name, func(t *testing.T) {
				body(t, createBackendModeDB(t, backend, mode, opts))
			})
		}
	})
}

// eachStoreBackendAndModeOpts runs body on the store axis: every backend the
// build has, crossed with the optimizer axis, for a test whose subject is
// what the store holds, such as the compressed value tiers.
func eachStoreBackendAndModeOpts(t *testing.T, opts storage.DatabaseOptions, body func(t *testing.T, db *storage.Database)) {
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
