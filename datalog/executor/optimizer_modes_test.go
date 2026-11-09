package executor

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// The optimizer mode matrix for the executor package: every test that plans a
// query (Execute/ExecuteWithContext/ExecuteWithRelations on a query, as
// opposed to hand-built RealizedPlans fed to ExecuteRealized) runs the legs
// legsFor answers for the options its queries reach, one leg per setting of
// that reach the design needs. The reach is measured: under -reach every leg
// runs, and reach_report_test.go writes per test the options whose events
// fired; a test absent from the report keeps every option. An option must
// never change results and must never make a failing query work — the legs
// are observationally equivalent for every accepted query. A test pinning one
// option's mechanism names the option in its reach and runs the legs of
// legsFor's answer that carry it. Plan of record:
// docs/wip/OPTIMIZER_MODE_MATRIX.md.
//
// The legs are the pairwise covering design datalog/storage's
// optimizer_modes_test.go states: the strength-2 orthogonal array on three
// bits (a, b, c), with the algebra optimizer at a, scan sharing at c,
// prefetch at a⊕c, the symmetric hash join at a⊕b⊕c, fusion at b⊕c and
// streaming aggregation at a⊕b. A ninth leg, fusion with streaming
// aggregation, is the profile NewExecutor constructs with
// (defaultPlannerOptions). Each leg names the options it turns on and sets
// those alone. This package cannot import storage (import cycle), so it
// states the design itself. Two base-option profiles are in live use here,
// each varying only the options from its base: NewExecutor's default profile
// via plannerOptions(), and the bare planner.PlannerOptions{} zero-value
// profile via zeroPlannerOptions(). Under -reach legsFor hands a test modes
// that record its reach, recordsReach naming the test, and the options a
// mode applies carry the recorder as the handler.
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

var optimizerModes = []optimizerMode{
	{name: "none"},
	{name: "sharing+prefetch+symmetric+fusion", sharing: true, prefetch: true, symmetric: true, fusion: true},
	{name: "symmetric+fusion+aggregation", symmetric: true, fusion: true, aggregation: true},
	{name: "sharing+prefetch+aggregation", sharing: true, prefetch: true, aggregation: true},
	{name: "algebra+prefetch+symmetric+aggregation", algebra: true, prefetch: true, symmetric: true, aggregation: true},
	{name: "algebra+sharing+fusion+aggregation", algebra: true, sharing: true, fusion: true, aggregation: true},
	{name: "algebra+prefetch+fusion", algebra: true, prefetch: true, fusion: true},
	{name: "algebra+sharing+symmetric", algebra: true, sharing: true, symmetric: true},
	{name: "fusion+aggregation", fusion: true, aggregation: true},
}

// optimizerModesWhere is the modes keep admits, in the table's order: the legs
// a test of one option's mechanism runs on.
func optimizerModesWhere(keep func(optimizerMode) bool) []optimizerMode {
	var modes []optimizerMode
	for _, mode := range optimizerModes {
		if keep(mode) {
			modes = append(modes, mode)
		}
	}
	return modes
}

// apply sets the mode's setting of every option on opts, with the handler
// recording the named test's reach where the mode records one.
func (m optimizerMode) apply(opts planner.PlannerOptions) planner.PlannerOptions {
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

// plannerOptions is NewExecutor's default profile with the mode's setting of
// every option — for tests that construct via bare NewExecutor today.
func (m optimizerMode) plannerOptions() planner.PlannerOptions {
	return m.apply(defaultPlannerOptions())
}

// zeroPlannerOptions is the bare zero-value profile with the mode's setting
// of every option — for tests that construct via NewExecutorWithOptions with
// a planner.PlannerOptions{} literal today.
func (m optimizerMode) zeroPlannerOptions() planner.PlannerOptions {
	return m.apply(planner.PlannerOptions{})
}

// settingsOf is the mode whose six options are those opts carries, named as
// opts is.
func settingsOf(name string, opts planner.PlannerOptions) optimizerMode {
	return optimizerMode{
		name:        name,
		algebra:     opts.EnableAlgebraOptimizer,
		sharing:     opts.EnableScanSharing,
		prefetch:    opts.EnableEntityPrefetch,
		symmetric:   opts.EnableSymmetricHashJoin,
		fusion:      opts.EnableAttributeFetchFusion,
		aggregation: opts.EnableStreamingAggregation,
	}
}

// TestOptimizerModeAxisProfiles pins the axis to its base profiles: the only
// fields a mode changes are the six options, and plannerOptions derives
// from the same defaultPlannerOptions NewExecutor delegates to — the axis can
// never drift from the constructor's real default.
func TestOptimizerModeAxisProfiles(t *testing.T) {
	for _, mode := range optimizerModes {
		for _, profile := range []struct {
			name string
			base planner.PlannerOptions
			with planner.PlannerOptions
		}{
			{"plannerOptions", defaultPlannerOptions(), mode.plannerOptions()},
			{"zeroPlannerOptions", planner.PlannerOptions{}, mode.zeroPlannerOptions()},
		} {
			settings := settingsOf(mode.name, profile.with)
			if settings != mode {
				t.Fatalf("mode %s: %s carries the settings %+v", mode.name, profile.name, settings)
			}
			// DeepEqual rather than ==: PlannerOptions carries Handler, a func,
			// and a struct holding one is not comparable. Neither profile
			// registers a handler, and DeepEqual holds two nil funcs equal, so
			// this is exact. If a profile ever did register one, DeepEqual would
			// report the two as unequal and this fails loudly — the right
			// direction.
			restored := settingsOf(mode.name, profile.base).apply(profile.with)
			if !reflect.DeepEqual(restored, profile.base) {
				t.Fatalf("mode %s: %s diverges from its base beyond the six options", mode.name, profile.name)
			}
		}
	}
}

// option is one of the six options a mode sets, in the order a mode's name
// lists them: the value a test names its reach with, and the reader
// legOptions takes a mode's settings through.
type option int

const (
	algebraOption option = iota
	sharingOption
	prefetchOption
	symmetricOption
	fusionOption
	aggregationOption
)

// allOptions is the six options in the order a mode's name lists them.
var allOptions = []option{algebraOption, sharingOption, prefetchOption, symmetricOption, fusionOption, aggregationOption}

// String is the option's name as a mode's name lists it.
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

// legOption is one option of a mode: its name as a mode's name lists it, and
// whether the mode turns it on.
type legOption struct {
	name string
	on   bool
}

// legOptions is each option of a mode, in the order a mode's name lists them.
func legOptions(mode optimizerMode) []legOption {
	settings := make([]legOption, 0, len(allOptions))
	for _, o := range allOptions {
		settings = append(settings, legOption{o.String(), o.on(mode)})
	}
	return settings
}

// defaultProfileMode is the mode of optimizerModes whose settings are
// defaultPlannerOptions' profile, the one NewExecutor constructs with.
func defaultProfileMode() optimizerMode {
	profile := settingsOf("", defaultPlannerOptions())
	for _, mode := range optimizerModes {
		settings := mode
		settings.name = ""
		if settings == profile {
			return mode
		}
	}
	panic("no mode of optimizerModes is defaultPlannerOptions' profile")
}

// legsFor is the legs a test runs on when its query shapes reach the options
// named and no other. Two legs agreeing on every reached option are one leg
// to such a test, so it runs one leg per setting of the reach the pairwise
// design needs: for one option, both settings; for two or three, all four
// settings of every pair; for four or more, every setting the legs hold. The
// reference leg is among them, the default profile is wherever its setting
// of the reach is one the legs taken before it do not hold, and the rest are
// the first legs of optimizerModes with a setting not yet held. A reach of
// no option, a test the options cannot change, runs the default profile
// alone. Under -reach every mode runs, each recording t's reach, so the
// record covers them all.
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
	// every pair takes all four settings. Every other reach's settings are
	// all taken.
	halve := len(distinct) == 3 && len(settings) == 8
	taken := func(s uint8) bool {
		if !halve {
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

// A test runs on one leg per setting of its reach the pairwise design needs:
// two legs for one option, four for two or three, the array's eight for four
// or more, each reached option at both settings and each pair at all four,
// no two legs alike on the reach, the reference among them, and the default
// profile, the ninth leg, wherever its setting of the reach is one no leg of
// the array holds, one leg more. A reach of none runs the default profile
// alone. Under -reach every leg runs, each recording the test's reach.
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
			want := 8
			switch len(reach) {
			case 0:
				want = 1
			case 1:
				want = 2
			case 2, 3:
				want = 4
			}
			profileApart := len(reach) > 0
			for _, mode := range optimizerModes[:8] {
				alike := true
				for _, o := range reach {
					if o.on(mode) != o.on(profile) {
						alike = false
					}
				}
				if alike {
					profileApart = false
				}
			}
			if profileApart {
				want++
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
			if profileApart {
				require.True(t, taken[profile.name], "the default profile is among the legs where the array's legs do not hold its setting of the reach")
			}
		})
	}
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
// reference, every option off; and one mode is defaultPlannerOptions'
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

	profile := settingsOf("", defaultPlannerOptions())
	carried := false
	for _, mode := range optimizerModes {
		mode.name = ""
		if mode == profile {
			carried = true
		}
	}
	require.True(t, carried, "a mode is defaultPlannerOptions' profile: %+v", profile)
}
