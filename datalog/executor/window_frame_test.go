package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A branch's frame, resolved against the subject and the environment, is
// what the window's run is read through. Each contract below is the subject
// of one test. The subject carries ?n and ?e, and the environment binds ?z
// to 7.

func frameFixture() (e, n, z query.Symbol, subjectSyms []query.Symbol, env Relation) {
	e, n, z = datalog.NewSymbol("?e"), datalog.NewSymbol("?n"), datalog.NewSymbol("?z")
	return e, n, z, []query.Symbol{n, e}, NewMaterializedRelation([]query.Symbol{z}, []Tuple{{int64(7)}})
}

// Branches framed over one symbol list share one frame, and each branch
// names its frame by index.
func TestWindowFrames_SharesAFrameAmongBranchesFramedAlike(t *testing.T) {
	e, _, z, subjectSyms, env := frameFixture()
	plans := []branchRun{
		{frame: scope{required: []query.Symbol{e}}},
		{frame: scope{required: []query.Symbol{e}}},
		{frame: scope{required: []query.Symbol{z, e}}},
		{frame: scope{}},
	}
	frames, frameOf := windowFrames(plans, subjectSyms, env)
	require.Len(t, frames, 3, "three distinct frames among four branches")
	assert.Equal(t, []int{0, 0, 1, 2}, frameOf)
	assert.Equal(t, []query.Symbol{e}, frames[0].required)
	assert.Equal(t, []query.Symbol{z, e}, frames[1].required)
	assert.Empty(t, frames[2].required)
}

// Each required symbol resolves to its position in the subject tuple, or to
// the environment's value when the environment alone binds it.
func TestWindowFrames_ResolvesEachRequiredSymbolToTheSubjectOrTheEnvironment(t *testing.T) {
	e, _, z, subjectSyms, env := frameFixture()
	frames, _ := windowFrames([]branchRun{{frame: scope{required: []query.Symbol{z, e}}}}, subjectSyms, env)
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
	_, _, z, subjectSyms, env := frameFixture()
	frames, _ := windowFrames([]branchRun{
		{frame: scope{required: []query.Symbol{z}}},
		{frame: scope{}},
	}, subjectSyms, env)
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
	e, _, z, subjectSyms, env := frameFixture()
	e1, e2, e3 := datalog.NewIdentity("wf:e1"), datalog.NewIdentity("wf:e2"), datalog.NewIdentity("wf:e3")
	frames, _ := windowFrames([]branchRun{{frame: scope{required: []query.Symbol{z, e}}}}, subjectSyms, env)
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
