package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The join of two sets answers each matching pair once and no tuple twice,
// in the materialized arm, the streaming arm and the symmetric arm
// (docs/proposals/JOIN_OUTPUT_IS_A_SET_PROOF.md, T1 and T2).
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
	// each once, under the symbols of the join.
	answers := func(t *testing.T, arm string, result Relation, expected []Tuple) int {
		require.Equal(t, []query.Symbol{e, l, r}, result.Symbols(), arm)
		want := NewTupleKeyMapWithCapacity(len(expected))
		for _, tuple := range expected {
			require.False(t, want.PutIfAbsent(NewTupleKeyFull(tuple), true),
				"the reference holds each pair once")
		}
		got := NewTupleKeyMapWithCapacity(len(expected))
		count := 0
		copies := result.RequiresCopy()
		require.NoError(t, ForEach(result, func(tuple Tuple) error {
			if copies {
				tuple = copyTuple(tuple)
			}
			require.True(t, want.Exists(NewTupleKeyFull(tuple)),
				"%s answered %v, which is no pair of the join", arm, tuple)
			require.False(t, got.PutIfAbsent(NewTupleKeyFull(tuple), true),
				"%s answered %v twice", arm, tuple)
			count++
			return nil
		}))
		require.Equal(t, len(expected), count, "%s answers each pair once", arm)
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

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			expected := pairs(c.left, c.right)

			t.Run("materialized", func(t *testing.T) {
				var events []annotations.Event
				opts := ExecutorOptions{Handler: func(event annotations.Event) { events = append(events, event) }}
				result := HashJoinWithOptions(
					NewMaterializedRelation(leftSymbols, c.left),
					NewMaterializedRelation(rightSymbols, c.right),
					join, opts)
				count := answers(t, "the materialized arm", result, expected)
				probe := probeEvent(t, events)
				require.Equal(t, "materialized", probe.Data["mode"])
				require.Equal(t, count, probe.Data["result_count"], "the probe emitted every pair")
			})

			t.Run("streaming", func(t *testing.T) {
				var events []annotations.Event
				opts := ExecutorOptions{
					EnableStreamingJoins: true,
					Handler:              func(event annotations.Event) { events = append(events, event) },
				}
				result := HashJoinWithOptions(
					NewMaterializedRelation(leftSymbols, c.left),
					NewStreamingRelationWithOptions(rightSymbols, &sliceIterator{tuples: c.right, pos: -1}, opts),
					join, opts)
				count := answers(t, "the streaming arm", result, expected)
				probe := probeEvent(t, events)
				require.Equal(t, "streaming", probe.Data["mode"])
				require.Equal(t, count, probe.Data["result_count"], "the probe emitted every pair")
			})

			t.Run("streaming over a probe that reuses its workspace", func(t *testing.T) {
				var events []annotations.Event
				opts := ExecutorOptions{
					EnableStreamingJoins: true,
					Handler:              func(event annotations.Event) { events = append(events, event) },
				}
				result := HashJoinWithOptions(
					NewMaterializedRelation(leftSymbols, c.left),
					newReusingWorkspaceStream(rightSymbols, c.right),
					join, opts)
				count := answers(t, "the streaming arm", result, expected)
				probe := probeEvent(t, events)
				require.Equal(t, "streaming", probe.Data["mode"])
				require.Equal(t, count, probe.Data["result_count"], "the probe emitted every pair")
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
				answers(t, "the symmetric arm", result, expected)
				require.Equal(t, "symmetric", strategyEvent(t, events).Data["mode"])
			})
		})
	}
}
