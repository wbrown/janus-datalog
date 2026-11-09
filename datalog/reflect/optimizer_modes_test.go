package reflect_test

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
// setting of each planner option a test can turn on, crossed with a backend.
// An option decides how a query runs, never what it answers, so every leg
// answers alike, and an option must never make things work: a query that
// fails with an option off must fail with it on, and vice versa. The legs are
// observationally equivalent for every query the engine accepts.
//
// The legs are a pairwise covering design over seven options: the algebra
// optimizer, scan sharing, entity prefetch, the symmetric hash join,
// streaming joins, attribute-fetch fusion and streaming aggregation
// (optionLegs). Every pair of options takes all four settings on some leg.
// One leg is the reference, every option off, and one is
// DefaultPlannerOptions' profile.
//
// A test that passes on only some legs has found either a real divergence
// (a bug — ledger it) or a mechanism of one option it means to pin. A test
// pinning a mechanism selects the legs that carry the option instead of
// setting the option itself: an option set inside a test is absent from the
// matrix.
//
// This package cannot import datalog/storage's test files, so it carries its
// own copy of the table — the Go-testing reality for cross-package test code.
// Mirrors datalog/storage/optimizer_modes_test.go, and the tests at the end
// pin the copy to the same design.
//
// Plan of record: docs/wip/OPTIMIZER_MODE_MATRIX.md
// =============================================================================

// optimizerMode is one leg of the matrix: a storage backend crossed with a
// setting of every planner option a test can turn on, named backend/leg. The
// leg's fields and its plannerOptions are the mode's by promotion. Every test
// in this package is a test of the engine, so the backend is the tree store
// alone, which every build has: the backend supplies a scan bound's datoms in
// index order and nothing else.
type optimizerMode struct {
	name string
	optionLeg
	backend storage.Backend
}

// optionLeg is one setting of the seven options, named by the options on, in
// the order the fields are declared, joined with "+"; "none" when every
// option is off.
type optionLeg struct {
	name        string
	algebra     bool
	sharing     bool
	prefetch    bool
	symmetric   bool
	streaming   bool
	fusion      bool
	aggregation bool
}

// optionLegs is the pairwise covering design: the strength-2 orthogonal array
// on three bits (a, b, c), with the algebra optimizer at a, scan sharing at
// c, prefetch at a⊕c, the symmetric hash join at a⊕b⊕c, streaming joins at
// b, fusion at b⊕c and streaming aggregation at a⊕b. Each leg names the
// options it turns on and sets those alone.
var optionLegs = []optionLeg{
	{name: "none"},
	{name: "sharing+prefetch+symmetric+fusion", sharing: true, prefetch: true, symmetric: true, fusion: true},
	{name: "symmetric+streaming+fusion+aggregation", symmetric: true, streaming: true, fusion: true, aggregation: true},
	{name: "sharing+prefetch+streaming+aggregation", sharing: true, prefetch: true, streaming: true, aggregation: true},
	{name: "algebra+prefetch+symmetric+aggregation", algebra: true, prefetch: true, symmetric: true, aggregation: true},
	{name: "algebra+sharing+fusion+aggregation", algebra: true, sharing: true, fusion: true, aggregation: true},
	{name: "algebra+prefetch+streaming+fusion", algebra: true, prefetch: true, streaming: true, fusion: true},
	{name: "algebra+sharing+symmetric+streaming", algebra: true, sharing: true, symmetric: true, streaming: true},
}

// optimizerModes is the engine axis: the tree store crossed with the legs.
var optimizerModes = buildOptimizerModes()

func buildOptimizerModes() []optimizerMode {
	trees, err := storage.BackendNamed("memory-trees")
	if err != nil {
		panic(err)
	}
	modes := make([]optimizerMode, 0, len(optionLegs))
	for _, leg := range optionLegs {
		modes = append(modes, optimizerMode{name: trees.Name + "/" + leg.name, optionLeg: leg, backend: trees})
	}
	return modes
}

// mustOpenStore opens this mode's backend for one test. The store closes with
// the test, so a DatabaseOptions carrying it needs no Path.
func mustOpenStore(t *testing.T, backend storage.Backend) storage.Store {
	t.Helper()
	store, err := backend.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("failed to open %s store: %v", backend.Name, err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// plannerOptions returns the default planner options with this leg's setting
// of every option applied.
func (leg optionLeg) plannerOptions() planner.PlannerOptions {
	opts := storage.DefaultPlannerOptions()
	opts.EnableAlgebraOptimizer = leg.algebra
	opts.EnableScanSharing = leg.sharing
	opts.EnableEntityPrefetch = leg.prefetch
	opts.EnableSymmetricHashJoin = leg.symmetric
	opts.EnableStreamingJoins = leg.streaming
	opts.EnableAttributeFetchFusion = leg.fusion
	opts.EnableStreamingAggregation = leg.aggregation
	return opts
}

// legOption is one option of a leg: its name as a leg's name lists it, and
// whether the leg turns it on.
type legOption struct {
	name string
	on   bool
}

// legOptions is each option of a leg, in the order a leg's name lists them.
func legOptions(leg optionLeg) []legOption {
	return []legOption{
		{"algebra", leg.algebra},
		{"sharing", leg.sharing},
		{"prefetch", leg.prefetch},
		{"symmetric", leg.symmetric},
		{"streaming", leg.streaming},
		{"fusion", leg.fusion},
		{"aggregation", leg.aggregation},
	}
}

// The option legs are a pairwise covering design: every pair of the seven
// options takes all four settings on some leg.
func TestTheOptionLegsCoverEveryPairOfOptions(t *testing.T) {
	options := legOptions(optionLeg{})
	for x := range options {
		for y := x + 1; y < len(options); y++ {
			for _, xOn := range []bool{false, true} {
				for _, yOn := range []bool{false, true} {
					covered := false
					for _, leg := range optionLegs {
						settings := legOptions(leg)
						if settings[x].on == xOn && settings[y].on == yOn {
							covered = true
							break
						}
					}
					require.True(t, covered, "%s=%v with %s=%v is on some leg", options[x].name, xOn, options[y].name, yOn)
				}
			}
		}
	}
}

// Each leg is named by the options it turns on; the first leg is the
// reference, every option off; and one leg is DefaultPlannerOptions' profile.
func TestTheOptionLegsCarryTheReferenceAndTheDefaultProfile(t *testing.T) {
	for _, leg := range optionLegs {
		var on []string
		for _, option := range legOptions(leg) {
			if option.on {
				on = append(on, option.name)
			}
		}
		name := strings.Join(on, "+")
		if name == "" {
			name = "none"
		}
		require.Equal(t, name, leg.name, "a leg is named by the options it turns on")
	}
	require.Equal(t, "none", optionLegs[0].name, "the first leg is the reference, every option off")

	defaults := storage.DefaultPlannerOptions()
	profile := optionLeg{
		algebra:     defaults.EnableAlgebraOptimizer,
		sharing:     defaults.EnableScanSharing,
		prefetch:    defaults.EnableEntityPrefetch,
		symmetric:   defaults.EnableSymmetricHashJoin,
		streaming:   defaults.EnableStreamingJoins,
		fusion:      defaults.EnableAttributeFetchFusion,
		aggregation: defaults.EnableStreamingAggregation,
	}
	carried := false
	for _, leg := range optionLegs {
		leg.name = ""
		if leg == profile {
			carried = true
		}
	}
	require.True(t, carried, "a leg is DefaultPlannerOptions' profile: %+v", profile)
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
