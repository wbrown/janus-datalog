package main

import (
	"fmt"
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
// Every test that executes a query in-process runs under every leg of the
// matrix: a setting of each planner option a test can turn on. An option
// decides how a query runs, never what it answers, so every leg answers
// alike, and an option must never make things work: a query that fails with
// an option off must fail with it on, and vice versa. The legs are
// observationally equivalent for every query the engine accepts.
//
// The legs are a pairwise covering design over seven options: the algebra
// optimizer, scan sharing, entity prefetch, the symmetric hash join,
// streaming joins, attribute-fetch fusion and streaming aggregation. Every
// pair of options takes all four settings on some leg. One leg is the
// reference, every option off, and one is DefaultPlannerOptions' profile.
//
// A test that passes on only some legs has found either a real divergence
// (a bug — ledger it) or a mechanism of one option it means to pin. A test
// pinning a mechanism selects the legs that carry the option instead of
// setting the option itself: an option set inside a test is absent from the
// matrix.
//
// A test that runs a query through the built binary loops cliOptimizeFlags
// instead: -optimize is the one planner option the CLI exposes, and the
// other options run at the binary's defaults, so a leg of optimizerModes
// would name options the binary never set.
//
// This package cannot import datalog/storage's test files, so it carries its
// own copy of the table — the Go-testing reality for cross-package test code.
// Mirrors datalog/storage/optimizer_modes_test.go, and the tests at the end
// pin the copy to the same design.
//
// Plan of record: docs/wip/OPTIMIZER_MODE_MATRIX.md
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

// cliOptimize is one setting of the built CLI's -optimize flag, the one
// planner option the binary exposes, named by the setting.
type cliOptimize struct {
	name string
	on   bool
}

// cliOptimizeFlags is the axis of a test that runs a query through the built
// binary: the flag on, the binary's default, and off.
var cliOptimizeFlags = []cliOptimize{
	{"optimize", true},
	{"no-optimize", false},
}

// cliFlag renders this setting as the built CLI's -optimize flag.
func (c cliOptimize) cliFlag() string {
	return fmt.Sprintf("-optimize=%v", c.on)
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
