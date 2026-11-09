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
// opposed to hand-built RealizedPlans fed to ExecuteRealized) runs under every
// leg: a setting of each planner option a test can turn on. An option must
// never change results and must never make a failing query work — the legs
// are observationally equivalent for every accepted query. A test pinning one
// option's mechanism selects the legs that carry the option. Plan of record:
// docs/wip/OPTIMIZER_MODE_MATRIX.md.
//
// The legs are the pairwise covering design datalog/storage's
// optimizer_modes_test.go states: the strength-2 orthogonal array on three
// bits (a, b, c), with the algebra optimizer at a, scan sharing at c,
// prefetch at a⊕c, the symmetric hash join at a⊕b⊕c, streaming joins at b,
// fusion at b⊕c and streaming aggregation at a⊕b. A ninth leg, fusion with
// streaming aggregation, is the profile NewExecutor constructs with
// (defaultPlannerOptions). Each leg names the options it turns on and sets
// those alone. This package cannot import storage (import cycle), so it
// states the design itself. Two base-option profiles are in live use here,
// each varying only the options from its base: NewExecutor's default profile
// via plannerOptions(), and the bare planner.PlannerOptions{} zero-value
// profile via zeroPlannerOptions().
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

var optimizerModes = []optimizerMode{
	{name: "none"},
	{name: "sharing+prefetch+symmetric+fusion", sharing: true, prefetch: true, symmetric: true, fusion: true},
	{name: "symmetric+streaming+fusion+aggregation", symmetric: true, streaming: true, fusion: true, aggregation: true},
	{name: "sharing+prefetch+streaming+aggregation", sharing: true, prefetch: true, streaming: true, aggregation: true},
	{name: "algebra+prefetch+symmetric+aggregation", algebra: true, prefetch: true, symmetric: true, aggregation: true},
	{name: "algebra+sharing+fusion+aggregation", algebra: true, sharing: true, fusion: true, aggregation: true},
	{name: "algebra+prefetch+streaming+fusion", algebra: true, prefetch: true, streaming: true, fusion: true},
	{name: "algebra+sharing+symmetric+streaming", algebra: true, sharing: true, symmetric: true, streaming: true},
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

// apply sets the mode's setting of every option on opts.
func (m optimizerMode) apply(opts planner.PlannerOptions) planner.PlannerOptions {
	opts.EnableAlgebraOptimizer = m.algebra
	opts.EnableScanSharing = m.sharing
	opts.EnableEntityPrefetch = m.prefetch
	opts.EnableSymmetricHashJoin = m.symmetric
	opts.EnableStreamingJoins = m.streaming
	opts.EnableAttributeFetchFusion = m.fusion
	opts.EnableStreamingAggregation = m.aggregation
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

// settingsOf is the mode whose seven options are those opts carries, named
// as opts is.
func settingsOf(name string, opts planner.PlannerOptions) optimizerMode {
	return optimizerMode{
		name:        name,
		algebra:     opts.EnableAlgebraOptimizer,
		sharing:     opts.EnableScanSharing,
		prefetch:    opts.EnableEntityPrefetch,
		symmetric:   opts.EnableSymmetricHashJoin,
		streaming:   opts.EnableStreamingJoins,
		fusion:      opts.EnableAttributeFetchFusion,
		aggregation: opts.EnableStreamingAggregation,
	}
}

// TestOptimizerModeAxisProfiles pins the axis to its base profiles: the only
// fields a mode changes are the seven options, and plannerOptions derives
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
				t.Fatalf("mode %s: %s diverges from its base beyond the seven options", mode.name, profile.name)
			}
		}
	}
}

// legOption is one option of a mode: its name as a mode's name lists it, and
// whether the mode turns it on.
type legOption struct {
	name string
	on   bool
}

// legOptions is each option of a mode, in the order a mode's name lists them.
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
