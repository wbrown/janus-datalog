package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A branch's frame, resolved against the subject and the environment, is
// what the window's run is read through. Each contract below is the subject
// of one test. The subject carries ?n and ?e, and the environment binds ?z
// to 7.

func frameFixture() (e, n, z query.Symbol, subject, env Relation) {
	e, n, z = datalog.NewSymbol("?e"), datalog.NewSymbol("?n"), datalog.NewSymbol("?z")
	return e, n, z, NewMaterializedRelation([]query.Symbol{n, e}, nil), NewMaterializedRelation([]query.Symbol{z}, []Tuple{{int64(7)}})
}

// branchFraming is a branch form's framing over subject with env as the
// frames' environment relation: its given symbols stand for themselves.
func branchFraming(subject, env Relation, given ...query.Symbol) framing {
	return framing{subject: subject, environment: env, given: given, standsFor: given}
}

// Branches framed over one symbol list share one frame, and each branch
// names its frame by index.
func TestWindowFrames_SharesAFrameAmongBranchesFramedAlike(t *testing.T) {
	e, _, z, subject, env := frameFixture()
	plans := []branchRun{
		{frame: planner.Scope{Required: []query.Symbol{e}}},
		{frame: planner.Scope{Required: []query.Symbol{e}}},
		{frame: planner.Scope{Required: []query.Symbol{z, e}}},
		{frame: planner.Scope{}},
	}
	frames, frameOf := windowFrames(plans, branchFraming(subject, env, z, e))
	require.Len(t, frames, 3, "three distinct frames among four branches")
	assert.Equal(t, []int{0, 0, 1, 2}, frameOf)
	assert.Equal(t, []query.Symbol{e}, frames[0].required)
	assert.Equal(t, []query.Symbol{z, e}, frames[1].required)
	assert.Empty(t, frames[2].required)
}

// Each required symbol resolves to its position in the subject tuple, or to
// the environment's value when the environment alone binds it.
func TestWindowFrames_ResolvesEachRequiredSymbolToTheSubjectOrTheEnvironment(t *testing.T) {
	e, _, z, subject, env := frameFixture()
	frames, _ := windowFrames([]branchRun{{frame: planner.Scope{Required: []query.Symbol{z, e}}}}, branchFraming(subject, env, z, e))
	frame := frames[0]
	assert.Equal(t, []int{1}, frame.subjectPos, "?e is the subject's second symbol")
	assert.Equal(t, []int{1}, frame.framePos, "?e is the frame's second symbol")
	assert.Equal(t, Tuple{int64(7), nil}, frame.envValues, "?z takes the environment's value, ?e the subject's")
	assert.NotNil(t, frame.ran, "a frame carrying a subject symbol keeps the keys its windows ran")
}

// A frame carrying no subject symbol keeps no ran keys, and its run is the
// environment's tuple over its symbols, or the unit relation when it has
// none.
func TestWindowFrame_AFrameOfNoSubjectSymbolRunsOverTheEnvironmentsTuple(t *testing.T) {
	e, _, z, subject, env := frameFixture()
	frames, _ := windowFrames([]branchRun{
		{frame: planner.Scope{Required: []query.Symbol{z}}},
		{frame: planner.Scope{}},
	}, branchFraming(subject, env, z, e))
	environmentOnly, empty := frames[0], frames[1]
	assert.Nil(t, environmentOnly.ran)
	assert.Nil(t, empty.ran)

	run := environmentOnly.environmentRun(ExecutorOptions{})
	require.NotNil(t, run)
	assert.Equal(t, []query.Symbol{z}, run.Symbols())
	tuples, err := CollectTuples(run, nil)
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{int64(7)}}, tuples)

	assert.Nil(t, empty.environmentRun(ExecutorOptions{}), "a frame of no symbol runs over the unit relation")
}

// A window's run is the window's distinct keys in the frame's order, each
// decorated with the environment's values, less the keys an earlier window
// ran; asked again for the same window it is the same relation.
func TestWindowFrame_WindowRunIsTheWindowsDistinctKeysLessThoseRunBefore(t *testing.T) {
	e, _, z, subject, env := frameFixture()
	e1, e2, e3 := datalog.NewIdentity("wf:e1"), datalog.NewIdentity("wf:e2"), datalog.NewIdentity("wf:e3")
	frames, _ := windowFrames([]branchRun{{frame: planner.Scope{Required: []query.Symbol{z, e}}}}, branchFraming(subject, env, z, e))
	frame := &frames[0]
	opts := ExecutorOptions{}

	first := frame.windowRun([]Tuple{{int64(1), e1}, {int64(2), e2}, {int64(3), e1}}, 1, opts)
	tuples, err := CollectTuples(first, nil)
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{int64(7), e1}, {int64(7), e2}}, tuples, "the window's distinct keys, in frame order, with the environment's value")
	assert.Same(t, first, frame.windowRun(nil, 1, opts), "the same window's run is computed once")

	second := frame.windowRun([]Tuple{{int64(4), e2}, {int64(5), e3}}, 2, opts)
	tuples, err = CollectTuples(second, nil)
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{int64(7), e3}}, tuples, "a key the first window ran is left out")

	third := frame.windowRun([]Tuple{{int64(6), e1}}, 3, opts)
	assert.Zero(t, third.Size(), "a window of keys run before has an empty run")
}

// A frame symbol is found in the subject by the symbol it stands for, a
// subquery's inner :in symbol by the argument its call passes: two frame
// symbols standing for one argument read one subject position, and a frame
// symbol standing for nothing, a constant argument's, takes the frame
// environment's value under its own name, whether the subject lacks a symbol
// of that name or carries one. So the window's run carries the subject's
// value at the two and the environment's at the third.
func TestWindowFrames_ResolvesAFrameSymbolByTheSymbolItStandsFor(t *testing.T) {
	e, n, _, lacking, _ := frameFixture()
	x, y, k := datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?k")
	e1 := datalog.NewIdentity("wf:standsfor1")
	frameEnv := NewMaterializedRelation([]query.Symbol{k}, []Tuple{{int64(9)}})
	carrying := NewMaterializedRelation([]query.Symbol{n, e, k}, nil)
	for _, subject := range []Relation{lacking, carrying} {
		f := framing{subject: subject, environment: frameEnv, given: []query.Symbol{x, k, y}, standsFor: []query.Symbol{e, nil, e}}
		frames, _ := windowFrames([]branchRun{{frame: planner.Scope{Required: []query.Symbol{x, k, y}}}}, f)
		frame := &frames[0]
		assert.Equal(t, []int{1, 1}, frame.subjectPos, "?x and ?y both stand for ?e, the subject's second symbol")
		assert.Equal(t, []int{0, 2}, frame.framePos)
		assert.Equal(t, Tuple{nil, int64(9), nil}, frame.envValues, "?k takes the frame environment's value under its own name")

		run := frame.windowRun([]Tuple{{int64(1), e1, int64(4)}}, 1, ExecutorOptions{})
		tuples, err := CollectTuples(run, nil)
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{{e1, int64(9), e1}}, tuples, "the subject's value at both symbols standing for it, the environment's at the third")
	}
}

// A window's keys are distinct under typed value identity: two tuples whose
// values render alike, a delimiter split two ways, an integer beside its
// digits, a boolean beside its name, a float beside its digits, and two byte
// slices of different content, are two keys.
func TestWindowFrame_WindowRunDistinguishesKeysByTypedValue(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	subject := NewMaterializedRelation([]query.Symbol{a, b}, nil)
	cases := []struct {
		name   string
		window []Tuple
	}{
		{"delimiter", []Tuple{{"a|b", "c"}, {"a", "b|c"}}},
		{"int64-vs-string", []Tuple{{int64(5), "x"}, {"5", "x"}}},
		{"bool-vs-string", []Tuple{{true, "x"}, {"true", "x"}}},
		{"float-vs-string", []Tuple{{float64(3.14), "x"}, {"3.14", "x"}}},
		{"bytes-distinct", []Tuple{{[]byte("ab"), "x"}, {[]byte("cd"), "x"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frames, _ := windowFrames([]branchRun{{frame: planner.Scope{Required: []query.Symbol{a, b}}}}, branchFraming(subject, nil, a, b))
			run := frames[0].windowRun(tc.window, 1, ExecutorOptions{})
			require.Equal(t, 2, run.Size(), "two keys distinct under typed value identity, got %s", run.Table())
		})
	}
}
