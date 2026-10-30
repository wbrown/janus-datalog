package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The join of two sets answers each matching pair once and no tuple twice
// (docs/proposals/JOIN_OUTPUT_IS_A_SET_PROOF.md, T1 and T2), through the
// streaming arm in either setting of EnableStreamingJoins and through the
// symmetric arm. The streaming arm hands back one workspace it overwrites on
// the next pull, so it answers RequiresCopy true, and an empty build answers
// the empty relation without opening its probe side
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, T1, T4, T5 and T6).
func TestAJoinOfTwoSetsAnswersEachPairOnce(t *testing.T) {
	e := datalog.NewSymbol("?e")
	l := datalog.NewSymbol("?l")
	r := datalog.NewSymbol("?r")
	leftSymbols := []query.Symbol{e, l}
	rightSymbols := []query.Symbol{e, r}
	join := []query.Symbol{e}

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
	// pairs is the join as a nested loop computes it: the combined tuple of
	// each left tuple with each right tuple E at the join position.
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

	cases := []struct {
		name        string
		left, right []Tuple
	}{
		{"an empty side", nil, side("R", 0, 3, 2)},
		{"one tuple on each side", side("L", 0, 1, 1), side("R", 0, 1, 1)},
		{"keys that partly overlap", side("L", 0, 4, 1), side("R", 2, 4, 1)},
		{"a key repeated on one side", side("L", 0, 4, 1), side("R", 0, 4, 3)},
		{"a key repeated on both sides", side("L", 0, 4, 3), side("R", 0, 4, 3)},
		{"a large input", side("L", 0, 300, 5), side("R", 0, 300, 5)},
	}

	// answers drains the join and asserts its tuples are the expected pairs,
	// each once, under the symbols of the join. A hash arm hands back one
	// workspace: a slice kept past the next pull holds the next pair, and the
	// relation answers RequiresCopy true whenever it handed back a tuple.
	answers := func(t *testing.T, arm string, result Relation, expected []Tuple, workspace bool) int {
		require.Equal(t, []query.Symbol{e, l, r}, result.Symbols(), arm)
		positions := allPositions(3)
		want := NewTupleSet(positions, positions, len(expected))
		for _, tuple := range expected {
			require.False(t, want.Probe(tuple), "the reference holds each pair once")
			want.Add(tuple)
		}
		got := NewTupleSet(positions, positions, len(expected))
		count := 0
		copies := result.RequiresCopy()
		var kept Tuple
		require.NoError(t, ForEach(result, func(tuple Tuple) error {
			if workspace && kept != nil {
				require.Equal(t, tuple, kept,
					"%s hands back one workspace, so a slice kept past the next pull holds the next pair", arm)
			}
			kept = tuple
			if copies {
				tuple = copyTuple(tuple)
			}
			require.True(t, want.Probe(tuple),
				"%s answered %v, which is no pair of the join", arm, tuple)
			require.False(t, got.Probe(tuple), "%s answered %v twice", arm, tuple)
			got.Add(tuple)
			count++
			return nil
		}))
		require.Equal(t, len(expected), count, "%s answers each pair once", arm)
		if workspace && count > 0 {
			require.True(t, copies,
				"%s hands back a workspace it overwrites, so it answers RequiresCopy true", arm)
		}
		return count
	}
	// probeEvent is the one JoinProbe event the hash arms emit.
	probeEvent := func(t *testing.T, events []annotations.Event) annotations.Event {
		var probes []annotations.Event
		for _, event := range events {
			if event.Name == annotations.JoinProbe {
				probes = append(probes, event)
			}
		}
		require.Len(t, probes, 1, "the probe reports once")
		return probes[0]
	}
	strategyEvent := func(t *testing.T, events []annotations.Event) annotations.Event {
		for _, event := range events {
			if event.Name == annotations.JoinStrategy {
				return event
			}
		}
		require.Fail(t, "the join reports its strategy")
		return annotations.Event{}
	}
	// unopened asserts a stream's iterator was never called: an empty build
	// answers without pulling its probe side.
	unopened := func(t *testing.T, arm string, stream *StreamingRelation) {
		stream.mu.Lock()
		opened := stream.iteratorCalled
		stream.mu.Unlock()
		require.False(t, opened, "%s opened its probe side under an empty build", arm)
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			expected := pairs(c.left, c.right)
			// The left side is the build in every hash arm below: the smaller
			// of two materialized sides, or the materialized side beside a
			// stream. An empty left is an empty build.
			emptyBuild := len(c.left) == 0

			// hashArm asserts what every hash arm reports: the streaming mode,
			// every pair emitted, and under an empty build no probe tuple read.
			hashArm := func(t *testing.T, arm string, events []annotations.Event, count int) {
				require.Equal(t, "streaming", strategyEvent(t, events).Data["mode"], arm)
				probe := probeEvent(t, events)
				require.Equal(t, "streaming", probe.Data["mode"], arm)
				require.Equal(t, count, probe.Data["result_count"], "%s emitted every pair", arm)
				if emptyBuild {
					require.Equal(t, 0, probe.Data["tuple_count"],
						"%s read a probe tuple under an empty build", arm)
				}
			}

			t.Run("with streaming joins off", func(t *testing.T) {
				arm := "the hash join with streaming joins off"
				var events []annotations.Event
				opts := ExecutorOptions{Handler: func(event annotations.Event) { events = append(events, event) }}
				result := HashJoinWithOptions(
					NewMaterializedRelation(leftSymbols, c.left),
					NewMaterializedRelation(rightSymbols, c.right),
					join, opts)
				count := answers(t, arm, result, expected, true)
				hashArm(t, arm, events, count)
			})

			t.Run("with streaming joins on", func(t *testing.T) {
				arm := "the hash join with streaming joins on"
				var events []annotations.Event
				opts := ExecutorOptions{
					EnableStreamingJoins: true,
					Handler:              func(event annotations.Event) { events = append(events, event) },
				}
				right := NewStreamingRelationWithOptions(rightSymbols, &sliceIterator{tuples: c.right, pos: -1}, opts)
				result := HashJoinWithOptions(NewMaterializedRelation(leftSymbols, c.left), right, join, opts)
				count := answers(t, arm, result, expected, true)
				hashArm(t, arm, events, count)
				if emptyBuild {
					unopened(t, arm, right)
				}
			})

			t.Run("over a probe that reuses its workspace", func(t *testing.T) {
				arm := "the hash join over a probe that reuses its workspace"
				var events []annotations.Event
				opts := ExecutorOptions{
					EnableStreamingJoins: true,
					Handler:              func(event annotations.Event) { events = append(events, event) },
				}
				right := newReusingWorkspaceStream(rightSymbols, c.right)
				result := HashJoinWithOptions(NewMaterializedRelation(leftSymbols, c.left), right, join, opts)
				count := answers(t, arm, result, expected, true)
				hashArm(t, arm, events, count)
				if emptyBuild {
					unopened(t, arm, right)
				}
			})

			t.Run("symmetric", func(t *testing.T) {
				var events []annotations.Event
				opts := ExecutorOptions{
					EnableStreamingJoins:    true,
					EnableSymmetricHashJoin: true,
					Handler:                 func(event annotations.Event) { events = append(events, event) },
				}
				result := HashJoinWithOptions(
					NewStreamingRelationWithOptions(leftSymbols, &sliceIterator{tuples: c.left, pos: -1}, opts),
					NewStreamingRelationWithOptions(rightSymbols, &sliceIterator{tuples: c.right, pos: -1}, opts),
					join, opts)
				answers(t, "the symmetric arm", result, expected, false)
				require.Equal(t, "symmetric", strategyEvent(t, events).Data["mode"])
			})
		})
	}
}
