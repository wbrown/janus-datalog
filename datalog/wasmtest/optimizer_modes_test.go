package wasmtest

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
// Every test that executes a query runs the legs legsFor answers for the
// options its queries reach, one leg per setting of that reach the design
// needs: two legs for a reach of one option, four for two or three, every
// leg for four or more, and the default profile alone for a reach of none.
// An option decides how a query runs, never what it answers, so every leg
// answers alike, and an option must never make things work: a query that
// fails with an option off must fail with it on, and vice versa. The legs are
// observationally equivalent for every query the engine accepts.
//
// The legs are a pairwise covering design over six options: the algebra
// optimizer, scan sharing, entity prefetch, the symmetric hash join,
// attribute-fetch fusion and streaming aggregation. Every pair of options
// takes all four settings on some leg. One leg is the reference, every
// option off, and one is DefaultPlannerOptions' profile. The reference is
// always among the legs legsFor answers, and the default profile wherever a
// sub-design of that size holds both.
//
// The reach is measured, never read off the test: under -reach every leg
// runs under a recording handler, and reach_report_test.go writes per test
// the options whose events fired. A test absent from the report keeps every
// option.
//
// A test that passes on only some legs has found either a real divergence
// (a bug — ledger it) or a mechanism of one option it means to pin. A test
// pinning a mechanism names the option in its reach and runs the legs of
// legsFor's answer that carry it, instead of setting the option itself: an
// option set inside a test is absent from the matrix.
//
// This package cannot import datalog/storage's test files, so it carries its
// own copy of the table — the Go-testing reality for cross-package test code.
// Mirrors datalog/storage/optimizer_modes_test.go, and the tests at the end
// pin the copy to the same design. This package also builds and runs under
// GOOS=js GOARCH=wasm (make test-wasm); nothing here is platform-specific.
//
// Plan of record: docs/wip/OPTIMIZER_MODE_MATRIX.md
// =============================================================================

// optimizerMode is one leg of the optimizer matrix: a setting of the six
// options, named by the options on, in the order the fields are declared,
// joined with "+"; "none" when every option is off. Under -reach legsFor
// hands a test modes that record its reach, recordsReach naming the test,
// and the mode's plannerOptions carry the recorder as the handler.
type optimizerMode struct {
	name         string
	algebra      bool
	sharing      bool
	prefetch     bool
	symmetric    bool
	fusion       bool
	aggregation  bool
	recordsReach string
}

// optimizerModes is the pairwise covering design: the strength-2 orthogonal
// array on three bits (a, b, c), with the algebra optimizer at a, scan
// sharing at c, prefetch at a⊕c, the symmetric hash join at a⊕b⊕c, fusion at
// b⊕c and streaming aggregation at a⊕b. Each leg names the options it turns
// on and sets those alone.
var optimizerModes = []optimizerMode{
	{name: "none"},
	{name: "sharing+prefetch+symmetric+fusion", sharing: true, prefetch: true, symmetric: true, fusion: true},
	{name: "symmetric+fusion+aggregation", symmetric: true, fusion: true, aggregation: true},
	{name: "sharing+prefetch+aggregation", sharing: true, prefetch: true, aggregation: true},
	{name: "algebra+prefetch+symmetric+aggregation", algebra: true, prefetch: true, symmetric: true, aggregation: true},
	{name: "algebra+sharing+fusion+aggregation", algebra: true, sharing: true, fusion: true, aggregation: true},
	{name: "algebra+prefetch+fusion", algebra: true, prefetch: true, fusion: true},
	{name: "algebra+sharing+symmetric", algebra: true, sharing: true, symmetric: true},
}

// plannerOptions returns the default planner options with this leg's setting
// of every option applied, with the handler recording the named test's reach
// where the mode records one.
func (m optimizerMode) plannerOptions() planner.PlannerOptions {
	opts := storage.DefaultPlannerOptions()
	opts.EnableAlgebraOptimizer = m.algebra
	opts.EnableScanSharing = m.sharing
	opts.EnableEntityPrefetch = m.prefetch
	opts.EnableSymmetricHashJoin = m.symmetric
	opts.EnableAttributeFetchFusion = m.fusion
	opts.EnableStreamingAggregation = m.aggregation
	if m.recordsReach != "" {
		opts.Handler = reachRecorder(m.recordsReach)
	}
	return opts
}

// option is one of the six options a leg sets, in the order a leg's name
// lists them: the value a test names its reach with, and the reader
// legOptions takes a leg's settings through.
type option int

const (
	algebraOption option = iota
	sharingOption
	prefetchOption
	symmetricOption
	fusionOption
	aggregationOption
)

// allOptions is the six options in the order a leg's name lists them.
var allOptions = []option{algebraOption, sharingOption, prefetchOption, symmetricOption, fusionOption, aggregationOption}

// String is the option's name as a leg's name lists it.
func (o option) String() string {
	switch o {
	case algebraOption:
		return "algebra"
	case sharingOption:
		return "sharing"
	case prefetchOption:
		return "prefetch"
	case symmetricOption:
		return "symmetric"
	case fusionOption:
		return "fusion"
	case aggregationOption:
		return "aggregation"
	}
	panic("option is none of the six")
}

// on is the mode's setting of the option.
func (o option) on(mode optimizerMode) bool {
	switch o {
	case algebraOption:
		return mode.algebra
	case sharingOption:
		return mode.sharing
	case prefetchOption:
		return mode.prefetch
	case symmetricOption:
		return mode.symmetric
	case fusionOption:
		return mode.fusion
	case aggregationOption:
		return mode.aggregation
	}
	panic("option is none of the six")
}

// legOption is one option of a leg: its name as a leg's name lists it, and
// whether the leg turns it on.
type legOption struct {
	name string
	on   bool
}

// legOptions is each option of a leg, in the order a leg's name lists them.
func legOptions(mode optimizerMode) []legOption {
	settings := make([]legOption, 0, len(allOptions))
	for _, o := range allOptions {
		settings = append(settings, legOption{o.String(), o.on(mode)})
	}
	return settings
}

// defaultProfile is DefaultPlannerOptions' setting of the six options, with
// no name.
func defaultProfile() optimizerMode {
	defaults := storage.DefaultPlannerOptions()
	return optimizerMode{
		algebra:     defaults.EnableAlgebraOptimizer,
		sharing:     defaults.EnableScanSharing,
		prefetch:    defaults.EnableEntityPrefetch,
		symmetric:   defaults.EnableSymmetricHashJoin,
		fusion:      defaults.EnableAttributeFetchFusion,
		aggregation: defaults.EnableStreamingAggregation,
	}
}

// defaultProfileMode is the mode of optimizerModes whose settings are
// DefaultPlannerOptions' profile.
func defaultProfileMode() optimizerMode {
	profile := defaultProfile()
	for _, mode := range optimizerModes {
		settings := mode
		settings.name = ""
		if settings == profile {
			return mode
		}
	}
	panic("no mode of optimizerModes is DefaultPlannerOptions' profile")
}

// legsFor is the legs a test runs on when its query shapes reach the options
// named and no other. Two legs agreeing on every reached option are one leg
// to such a test, so it runs one leg per setting of the reach the pairwise
// design needs: for one option, both settings on two legs; for two or three,
// all four settings of every pair on four legs; for four or more, every leg.
// The reference leg is among them, the default profile is where a sub-design
// of that size holds both, and the rest are the first legs of optimizerModes
// with a setting not yet held. A reach of no option, a test the options
// cannot change, runs the default profile alone. Under -reach every mode
// runs, each recording t's reach, so the record covers them all.
func legsFor(t testing.TB, reach ...option) []optimizerMode {
	if *reachReport != "" {
		test, _, _ := strings.Cut(t.Name(), "/")
		recording := make([]optimizerMode, 0, len(optimizerModes))
		for _, mode := range optimizerModes {
			mode.recordsReach = test
			recording = append(recording, mode)
		}
		return recording
	}
	var distinct []option
	for _, o := range reach {
		held := false
		for _, d := range distinct {
			if d == o {
				held = true
				break
			}
		}
		if !held {
			distinct = append(distinct, o)
		}
	}
	if len(distinct) == 0 {
		return []optimizerMode{defaultProfileMode()}
	}
	if len(distinct) >= 4 {
		return optimizerModes
	}
	// A mode's setting of the reach, one bit per reached option.
	setting := func(mode optimizerMode) (s uint8) {
		for i, o := range distinct {
			if o.on(mode) {
				s |= 1 << i
			}
		}
		return s
	}
	settings := map[uint8]bool{}
	for _, mode := range optimizerModes {
		settings[setting(mode)] = true
	}
	// Three options the legs set independently take all eight settings, and
	// the four of even weight are the half holding the reference on which
	// every pair takes all four settings. Fewer settings are all taken.
	taken := func(s uint8) bool {
		if len(settings) < 8 {
			return true
		}
		ones := 0
		for v := s; v != 0; v >>= 1 {
			ones += int(v & 1)
		}
		return ones%2 == 0
	}
	held := map[uint8]bool{}
	chosen := map[string]bool{}
	take := func(mode optimizerMode) {
		s := setting(mode)
		if taken(s) && !held[s] {
			held[s] = true
			chosen[mode.name] = true
		}
	}
	take(optimizerModes[0])
	take(defaultProfileMode())
	for _, mode := range optimizerModes {
		take(mode)
	}
	var legs []optimizerMode
	for _, mode := range optimizerModes {
		if chosen[mode.name] {
			legs = append(legs, mode)
		}
	}
	return legs
}

// reachCovered reports whether legs take every reached option at both
// settings and, for a reach of two or more, every pair of them at all four.
func reachCovered(reach []option, legs []optimizerMode) bool {
	for x, o := range reach {
		for _, on := range []bool{false, true} {
			found := false
			for _, leg := range legs {
				if o.on(leg) == on {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		for y := x + 1; y < len(reach); y++ {
			for _, xOn := range []bool{false, true} {
				for _, yOn := range []bool{false, true} {
					found := false
					for _, leg := range legs {
						if o.on(leg) == xOn && reach[y].on(leg) == yOn {
							found = true
							break
						}
					}
					if !found {
						return false
					}
				}
			}
		}
	}
	return true
}

// aSubDesignHoldsBoth reports whether some size legs of optimizerModes
// holding the reference and the default profile cover the reach.
func aSubDesignHoldsBoth(reach []option, size int) bool {
	profile := defaultProfileMode()
	for mask := 0; mask < 1<<len(optimizerModes); mask++ {
		var legs []optimizerMode
		for i, mode := range optimizerModes {
			if mask>>i&1 == 1 {
				legs = append(legs, mode)
			}
		}
		if len(legs) != size {
			continue
		}
		holdsBoth := false
		for _, leg := range legs {
			if leg.name == profile.name {
				holdsBoth = true
			}
		}
		if mask&1 == 1 && holdsBoth && reachCovered(reach, legs) {
			return true
		}
	}
	return false
}

// The modes are a pairwise covering design: every pair of the six options
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

	profile := defaultProfile()
	carried := false
	for _, mode := range optimizerModes {
		mode.name = ""
		if mode == profile {
			carried = true
		}
	}
	require.True(t, carried, "a mode is DefaultPlannerOptions' profile: %+v", profile)
}

// A test runs on one leg per setting of its reach the pairwise design needs:
// two legs for one option, four for two or three, every leg for four or
// more, each reached option at both settings and each pair at all four, no
// two legs alike on the reach, the reference among them and the default
// profile where a sub-design of that size holds both. A reach of none runs
// the default profile alone. Under -reach every leg runs, each recording
// the test's reach.
func TestLegsForRunsOneLegPerSettingOfTheReach(t *testing.T) {
	reference := optimizerModes[0]
	profile := defaultProfileMode()
	for mask := 0; mask < 1<<len(allOptions); mask++ {
		var reach []option
		var names []string
		for i, o := range allOptions {
			if mask>>i&1 == 1 {
				reach = append(reach, o)
				names = append(names, o.String())
			}
		}
		name := strings.Join(names, "+")
		if name == "" {
			name = "none"
		}
		t.Run(name, func(t *testing.T) {
			legs := legsFor(t, reach...)
			if *reachReport != "" {
				require.Len(t, legs, len(optimizerModes), "under -reach every leg runs")
				for _, leg := range legs {
					require.Equal(t, "TestLegsForRunsOneLegPerSettingOfTheReach", leg.recordsReach, "each leg records this test's reach")
				}
				return
			}
			want := len(optimizerModes)
			switch len(reach) {
			case 0:
				want = 1
			case 1:
				want = 2
			case 2, 3:
				want = 4
			}
			require.Len(t, legs, want, "the legs a reach of %d options runs", len(reach))
			taken := map[string]bool{}
			for _, leg := range legs {
				taken[leg.name] = true
			}
			if len(reach) == 0 {
				require.True(t, taken[profile.name], "a reach of none runs the default profile")
				return
			}
			require.True(t, taken[reference.name], "the reference is among the legs")
			require.True(t, reachCovered(reach, legs), "every reached option takes both settings and every pair all four")
			for i := range legs {
				for j := i + 1; j < len(legs); j++ {
					alike := true
					for _, o := range reach {
						if o.on(legs[i]) != o.on(legs[j]) {
							alike = false
						}
					}
					require.False(t, alike, "%s and %s agree on every reached option", legs[i].name, legs[j].name)
				}
			}
			if aSubDesignHoldsBoth(reach, want) {
				require.True(t, taken[profile.name], "the default profile is among the legs where a sub-design of this size holds it beside the reference")
			}
		})
	}
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
			require.Equal(t, mode.fusion, opts.EnableAttributeFetchFusion, "attribute-fetch fusion")
			require.Equal(t, mode.aggregation, opts.EnableStreamingAggregation, "streaming aggregation")
			rest := storage.DefaultPlannerOptions()
			rest.EnableAlgebraOptimizer = mode.algebra
			rest.EnableScanSharing = mode.sharing
			rest.EnableEntityPrefetch = mode.prefetch
			rest.EnableSymmetricHashJoin = mode.symmetric
			rest.EnableAttributeFetchFusion = mode.fusion
			rest.EnableStreamingAggregation = mode.aggregation
			require.Equal(t, rest, opts, "the options differ from the defaults in the six options alone")
		})
	}
}
