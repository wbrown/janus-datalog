package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A derived set's index at a key, asked for at the count of a view of the
// set, answers the view's tuples holding the key and none the set admitted
// after the view, and asked for at a later view's count the later tuples
// too; a tuple enters the index once, whatever counts ask for it
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
	require.Equal(t, []int{3, 5, 6}, []int{first.Size(), second.Size(), third.Size()})

	// The set is keyed at its position 0, and the probing tuple carries the
	// key at its own position 1.
	key := []int{0}
	probe := func(view *MaterializedRelation, k int64) []Tuple {
		ix := set.index(key, view.Size())
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

// A join into a relation a derived set answers leaves the set as it was: the
// relation is the set's tuples so far, and joining it writes nothing the set
// holds, so a relation the evaluation hands out does not change when it is
// joined (docs/proposals/CONCURRENCY_PROOF.md, P2). The join answers the
// pairs a nested loop computes, under each planner profile.
func TestDerivedSet_AJoinIntoItsRelationLeavesTheSetAsItWas(t *testing.T) {
	e, l, r := datalog.NewSymbol("?e"), datalog.NewSymbol("?l"), datalog.NewSymbol("?r")
	setTuples := []Tuple{{int64(1), "a"}, {int64(2), "b"}, {int64(2), "c"}}
	groupTuples := []Tuple{{int64(2), "x"}, {int64(3), "y"}}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			set := newDerivedSet([]query.Symbol{e, r})
			_, err := set.absorb(NewMaterializedRelation([]query.Symbol{e, r}, setTuples))
			require.NoError(t, err)
			opts := ExecutorOptionsFromPlanner(mode.plannerOptions())
			group := NewMaterializedRelationWithOptions([]query.Symbol{e, l}, groupTuples, opts)
			var got []Tuple
			require.NoError(t, ForEach(HashJoinWithOptions(group, set.relation(opts), []query.Symbol{e}, opts), func(tuple Tuple) error {
				got = append(got, copyTuple(tuple))
				return nil
			}))
			require.ElementsMatch(t, []Tuple{{int64(2), "x", "b"}, {int64(2), "x", "c"}}, got, "the join answers the nested loop's pairs")
			require.Empty(t, set.indexes, "a join into the set's relation wrote an index into the set")
			require.Equal(t, setTuples, set.tuples, "the set holds the tuples it absorbed and nothing else")
		})
	}
}

// An invocation of a predicate the fixpoint holds, whose arguments are
// distinct free variables, answers its relation renamed with the set the
// running evaluation keeps for the predicate, so a join at the invocation
// probes the set's index. Every other shape answers no set: a constant, a
// repeated variable, the delta symbol, and a predicate the context keeps no
// set for (docs/proposals/A_HELD_RELATION_KEEPS_ITS_JOIN_INDEX_PROOF.md, P3).
func TestExecuteRuleInvocation_ARenameOfAHeldPredicateAnswersItsSet(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	x, y := datalog.NewSymbol("?x"), datalog.NewSymbol("?y")
	follows := datalog.NewSymbol("follows")
	rs := mustRules(t, followsRulesText)
	set := newDerivedSet([]query.Symbol{a, b})
	_, err := set.absorb(NewMaterializedRelation([]query.Symbol{a, b}, []Tuple{{page(1), page(2)}, {page(2), page(3)}, {page(3), page(3)}}))
	require.NoError(t, err)
	delta := NewMaterializedRelation([]query.Symbol{a, b}, []Tuple{{page(2), page(3)}})
	held := map[query.Symbol]Relation{follows: set.relation(ExecutorOptions{}), query.DeltaSymbol(follows): delta}
	keeping := NewContext().WithRules(rs).WithDerived(held).WithSets(map[query.Symbol]*derivedSet{follows: set})
	exec := newQueryExecutor(nil, nil, ExecutorOptions{})
	invoke := func(name query.Symbol, args ...query.PatternElement) *query.RuleInvocation {
		return &query.RuleInvocation{Name: name, Args: args}
	}
	vx, vy := query.Variable{Name: x}, query.Variable{Name: y}

	cases := []struct {
		name string
		ctx  *Context
		inv  *query.RuleInvocation
		set  *derivedSet
	}{
		{"distinct free variables", keeping, invoke(follows, vx, vy), set},
		{"a constant", keeping, invoke(follows, vx, query.Constant{Value: page(3)}), nil},
		{"a repeated variable", keeping, invoke(follows, vx, vx), nil},
		{"the delta symbol", keeping, invoke(query.DeltaSymbol(follows), vx, vy), nil},
		{"a held relation with no set kept", NewContext().WithRules(rs).WithDerived(held), invoke(follows, vx, vy), nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rel, got, err := exec.executeRuleInvocation(c.ctx, c.inv, nil)
			require.NoError(t, err)
			require.NotNil(t, rel)
			if c.set == nil {
				require.Nil(t, got, "the invocation answered a set where its relation is no rename of a kept set")
				return
			}
			require.Same(t, c.set, got, "the rename of a held predicate answers the set the evaluation keeps for it")
			require.Equal(t, c.set.tuples, rel.(*MaterializedRelation).tuples, "the rename is the set's tuples so far")
		})
	}
}

// A join of a group with a held relation whose set the caller names probes
// the index the set keeps at the join key with the group's tuples and drains
// nothing of the relation
// (docs/proposals/A_HELD_RELATION_KEEPS_ITS_JOIN_INDEX_PROOF.md, T1, T3, L2,
// L4): it answers the pairs a nested loop computes, each once, the group on
// the left, materialized or streaming, the relation under the set's symbols
// and renamed, under each planner profile's executor options; the strategy
// event names the kept index as the build side, no build.copy event fires,
// and the probe event's tuple count is the group's.
func TestHashJoinWithSet_AHeldRelationIsProbedAndNeverDrained(t *testing.T) {
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
		name    string
		stream  bool
		renamed bool
	}{
		{"a materialized group", false, false},
		{"a streaming group", true, false},
		{"the relation renamed, a materialized group", false, true},
		{"the relation renamed, a streaming group", true, true},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				var events []annotations.Event
				opts := ExecutorOptionsFromPlanner(mode.plannerOptions())
				opts.Handler = func(event annotations.Event) { events = append(events, event) }
				var held Relation = set.relation(opts)
				if c.renamed {
					subst, err := query.SubstitutionOf([]query.Symbol{e, r}, []query.Symbol{e, s})
					require.NoError(t, err)
					held, err = held.Rename(subst)
					require.NoError(t, err)
				}
				var group Relation = NewMaterializedRelationWithOptions([]query.Symbol{e, l}, groupTuples, opts)
				if c.stream {
					group = NewStreamingRelationWithOptions([]query.Symbol{e, l}, &sliceIterator{tuples: groupTuples, pos: -1}, opts)
				}
				expected := pairs(groupTuples, viewTuples)
				buildSide := "right"
				result := hashJoinWithSet(group, held, []query.Symbol{e}, opts, set)

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
