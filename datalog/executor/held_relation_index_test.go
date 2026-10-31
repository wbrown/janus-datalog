package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A derived set's index at a key answers, through a view of the set, the
// view's tuples holding the key and none the set admitted after the view,
// and through a later view the later tuples too; a tuple enters the index
// once, whatever views ask through it
// (docs/proposals/A_HELD_RELATION_KEEPS_ITS_JOIN_INDEX_PROOF.md, T2, T4, L1,
// L5).
func TestDerivedSet_AnIndexAnswersTheViewsTuplesUnderAKey(t *testing.T) {
	e, r := datalog.NewSymbol("?e"), datalog.NewSymbol("?r")
	symbols := []query.Symbol{e, r}
	set := newDerivedSet(symbols)
	absorb := func(tuples ...Tuple) {
		_, err := set.absorb(NewMaterializedRelation(symbols, tuples))
		require.NoError(t, err)
	}
	// Three steps, a view taken after each. The ordinals are the order of
	// admission, and a tuple the set holds is admitted once.
	absorb(Tuple{int64(1), "a"}, Tuple{int64(2), "b"}, Tuple{int64(1), "c"})
	first := set.relation(ExecutorOptions{})
	absorb(Tuple{int64(1), "d"}, Tuple{int64(2), "b"}, Tuple{int64(3), "e"})
	second := set.relation(ExecutorOptions{})
	absorb(Tuple{int64(1), "f"})
	third := set.relation(ExecutorOptions{})
	require.Same(t, set, first.set, "a view carries the set it views")
	require.Equal(t, []int{3, 5, 6}, []int{first.Size(), second.Size(), third.Size()})

	// The set is keyed at its position 0, and the probing tuple carries the
	// key at its own position 1.
	key := []int{0}
	probe := func(view *MaterializedRelation, k int64) []Tuple {
		ix := view.set.index(key, view.Size())
		var tuples []Tuple
		for _, ordinal := range ix.probe(Tuple{"probe", k}, []int{1}, view.Size()) {
			tuples = append(tuples, set.tuples[ordinal])
		}
		return tuples
	}
	require.Equal(t, []Tuple{{int64(1), "a"}, {int64(1), "c"}}, probe(first, 1),
		"the first view's tuples under the key, and none admitted after it")
	require.Equal(t, []Tuple{{int64(1), "a"}, {int64(1), "c"}, {int64(1), "d"}}, probe(second, 1),
		"a later view answers the later tuples too")
	require.Equal(t, []Tuple{{int64(1), "a"}, {int64(1), "c"}, {int64(1), "d"}, {int64(1), "f"}}, probe(third, 1))
	require.Equal(t, []Tuple{{int64(1), "a"}, {int64(1), "c"}}, probe(first, 1),
		"the first view's answer stands after the index was extended past it")
	require.Equal(t, []Tuple{{int64(2), "b"}}, probe(third, 2),
		"a tuple the set held is admitted once, so it is indexed once")
	require.Empty(t, probe(first, 3), "a tuple admitted after the view is not in the view's answer")
	require.Equal(t, []Tuple{{int64(3), "e"}}, probe(second, 3))
	require.Empty(t, probe(third, 4), "a key no tuple holds answers nothing")
	require.Len(t, set.indexes, 1, "one index per key asked for")
	require.Equal(t, len(set.tuples), set.indexes[0].held,
		"asked for through the latest view, the index holds every tuple of the set")
}

// A join into a view of a derived set probes the index the set keeps at the
// join key with the other side's tuples and drains nothing of the view
// (docs/proposals/A_HELD_RELATION_KEEPS_ITS_JOIN_INDEX_PROOF.md, T1, T3, L2,
// L4): it answers the pairs a nested loop computes, each once, with the view
// on either side, the group materialized or streaming, and under a rename of
// the view, under each planner profile's executor options; the strategy
// event names the kept index as the build side, no build.copy event fires,
// and the probe event's tuple count is the group's.
func TestHashJoin_AViewIsProbedAndNeverDrained(t *testing.T) {
	e, l, r, s := datalog.NewSymbol("?e"), datalog.NewSymbol("?l"), datalog.NewSymbol("?r"), datalog.NewSymbol("?s")
	// side is a set over the keys from first on, each repeated fanout times
	// with a payload of its own.
	side := func(tag string, first, keys, fanout int) []Tuple {
		tuples := make([]Tuple, 0, keys*fanout)
		for k := first; k < first+keys; k++ {
			for j := 0; j < fanout; j++ {
				tuples = append(tuples, Tuple{int64(k), fmt.Sprintf("%s-%d-%d", tag, k, j)})
			}
		}
		return tuples
	}
	// The view holds the keys 0 to 3 three times each and the key 9 once;
	// the group holds the keys 2 to 5 twice each. The pairs are under the
	// keys 2 and 3, three view tuples by two group tuples each.
	viewTuples := append(side("V", 0, 4, 3), Tuple{int64(9), "V-9-0"})
	groupTuples := side("G", 2, 4, 2)
	// pairs is the join as a nested loop computes it: the left tuple whole,
	// then the right tuple's payload, for each pair agreeing at the key.
	pairs := func(left, right []Tuple) []Tuple {
		var out []Tuple
		for _, lt := range left {
			for _, rt := range right {
				if datalog.ValuesEqual(lt[0], rt[0]) {
					out = append(out, Tuple{lt[0], lt[1], rt[1]})
				}
			}
		}
		return out
	}
	set := newDerivedSet([]query.Symbol{e, r})
	_, err := set.absorb(NewMaterializedRelation([]query.Symbol{e, r}, viewTuples))
	require.NoError(t, err)

	cases := []struct {
		name     string
		viewLeft bool
		stream   bool
		renamed  bool
	}{
		{"the view on the right of a materialized group", false, false, false},
		{"the view on the left of a materialized group", true, false, false},
		{"the view on the right of a streaming group", false, true, false},
		{"the view on the left of a streaming group", true, true, false},
		{"the view renamed, on the right of a materialized group", false, false, true},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				var events []annotations.Event
				opts := ExecutorOptionsFromPlanner(mode.plannerOptions())
				opts.Handler = func(event annotations.Event) { events = append(events, event) }
				var view Relation = set.relation(opts)
				if c.renamed {
					subst, err := query.SubstitutionOf([]query.Symbol{e, r}, []query.Symbol{e, s})
					require.NoError(t, err)
					view, err = view.Rename(subst)
					require.NoError(t, err)
				}
				var group Relation = NewMaterializedRelationWithOptions([]query.Symbol{e, l}, groupTuples, opts)
				if c.stream {
					group = NewStreamingRelationWithOptions([]query.Symbol{e, l}, &sliceIterator{tuples: groupTuples, pos: -1}, opts)
				}
				left, right := group, view
				expected := pairs(groupTuples, viewTuples)
				buildSide := "right"
				if c.viewLeft {
					left, right = view, group
					expected = pairs(viewTuples, groupTuples)
					buildSide = "left"
				}
				result := HashJoinWithOptions(left, right, []query.Symbol{e}, opts)

				// The answers, each a pair of the nested loop and each once.
				positions := allPositions(3)
				want := NewTupleSet(positions, positions, len(expected))
				for _, tuple := range expected {
					_, hash, held := want.Find(tuple)
					require.False(t, held, "the reference holds each pair once")
					want.Add(tuple, hash)
				}
				got := NewTupleSet(positions, positions, len(expected))
				copies := result.RequiresCopy()
				count := 0
				require.NoError(t, ForEach(result, func(tuple Tuple) error {
					if copies {
						tuple = copyTuple(tuple)
					}
					require.True(t, want.Probe(tuple), "the join answered %v, which is no pair of the join", tuple)
					_, hash, held := got.Find(tuple)
					require.False(t, held, "the join answered %v twice", tuple)
					got.Add(tuple, hash)
					count++
					return nil
				}))
				require.Equal(t, len(expected), count, "the join answers each pair once")

				var strategy, build, probe []annotations.Event
				drained := 0
				for _, event := range events {
					switch event.Name {
					case annotations.JoinStrategy:
						strategy = append(strategy, event)
					case annotations.JoinBuild:
						build = append(build, event)
					case annotations.JoinProbe:
						probe = append(probe, event)
					case annotations.JoinBuildCopy:
						drained++
					}
				}
				require.Len(t, strategy, 1)
				require.Equal(t, buildSide, strategy[0].Data["build_side"], "the view is the build side")
				require.Equal(t, true, strategy[0].Data["build_kept"], "the strategy names the kept index as the build")
				require.Len(t, build, 1)
				require.Equal(t, true, build[0].Data["build_kept"])
				require.Equal(t, 0, build[0].Data["copied"], "nothing of the view is copied")
				require.Equal(t, 0, build[0].Data["passthru"], "nothing of the view is drained")
				require.Zero(t, drained, "no build.copy event fires: the view is not drained")
				require.Len(t, probe, 1)
				require.Equal(t, len(groupTuples), probe[0].Data["tuple_count"], "the probe reads the group's tuples")
				require.Equal(t, len(expected), probe[0].Data["result_count"])
			})
		}
	}
}
