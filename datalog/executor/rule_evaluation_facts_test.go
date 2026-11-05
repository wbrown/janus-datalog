package executor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// An invocation's evaluation is continued across the windows of the scope
// it stands in (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.2): the
// sets of every stratum and the derived-tuple count are carried from window
// to window, a window's seed enters the seed predicate's stratum as a delta,
// the strata run again from the lowest one that changed, and a stratum
// above a changed one reads that predicate's new tuples through the copies
// of its bodies marked on it. Each contract below is the subject of one
// test over the closure of a chain of six pages, page1 to page6, each page
// linked to the next, rewritten for a bound first position.

// followsRecursionFirstText is the closure with its recursive rule invoking
// the predicate before its pattern, so the rewrite's magic predicate has no
// rule of its own and the base rule's edge pattern is the one pattern whose
// entity is the rule's first variable.
const followsRecursionFirstText = `[[(follows ?a ?b) [?a :page/next ?b]]
                                   [(follows ?a ?c) (follows ?a ?b) [?b :page/next ?c]]]`

// boundClosure is the closure in text rewritten for a bound first position:
// the executor over matcher under opts, the evaluation planned for the
// invocation with its first position bound, whose set is the rewrite, and a
// context carrying the original set.
func boundClosure(t *testing.T, text string, matcher PatternMatcher, opts planner.PlannerOptions) (*Executor, *planner.InvocationEvaluation, *Context) {
	t.Helper()
	rs := MustRules(t, text)
	planned, err := planner.PlanEvaluation(planner.CreatePlanner(nil, opts), rs, datalog.NewSymbol("follows"), []bool{true, false}, nil)
	require.NoError(t, err)
	return NewExecutorWithOptions(matcher, nil, opts), planned, NewContext().WithRules(rs)
}

// seedOf is a seed relation of the pages given, over the seed predicate's
// head under the rule set.
func seedOf(rs *query.RuleSet, seed query.Symbol, pages ...int) Relation {
	head, _ := rs.Head(seed)
	tuples := make([]Tuple, len(pages))
	for i, p := range pages {
		tuples[i] = Tuple{Page(p)}
	}
	return NewMaterializedRelation(head, tuples)
}

// pairsFrom is every pair (i, j) with from ≤ i < j ≤ n: the closure of the
// chain from page from.
func pairsFrom(from, n int) map[[2]datalog.Identity]bool {
	pairs := map[[2]datalog.Identity]bool{}
	for i := from; i <= n; i++ {
		for j := i + 1; j <= n; j++ {
			pairs[[2]datalog.Identity{Page(i), Page(j)}] = true
		}
	}
	return pairs
}

// A later window's seed derives the new keys alone: the magic facts and the
// pairs its keys reach that the sets do not hold enter a delta once, and a
// window whose keys the sets already hold runs no iteration.
func TestRuleEvaluation_ALaterSeedDerivesTheNewKeysAlone(t *testing.T) {
	for _, mode := range OptimizerModes {
		t.Run(mode.Name(), func(t *testing.T) {
			events := &RuleEvents{}
			opts := mode.PlannerOptions()
			opts.Handler = events.Handle
			exec, planned, ctx := boundClosure(t, FollowsRulesText, factsDatabase(t, ChainDatoms(6)), opts)
			rules, seed := planned.Magic.Rules, planned.Magic.Seed
			ev := NewRuleEvaluation(exec, planned)

			rel, err := ev.Extend(ctx, seedOf(rules, seed, 4))
			require.NoError(t, err)
			RequireSamePairs(t, IdentityPairs(t, rel), pairsFrom(4, 6))
			require.Equal(t, 3+3, events.Entered(), "the magic facts of pages four to six and the three pairs from them")

			events.Reset()
			rel, err = ev.Extend(ctx, seedOf(rules, seed, 2))
			require.NoError(t, err)
			RequireSamePairs(t, IdentityPairs(t, rel), pairsFrom(2, 6))
			require.Equal(t, 2+7, events.Entered(), "the magic facts of pages two and three and the seven pairs from them; the held facts enter no delta again")

			events.Reset()
			rel, err = ev.Extend(ctx, seedOf(rules, seed, 4))
			require.NoError(t, err)
			RequireSamePairs(t, IdentityPairs(t, rel), pairsFrom(2, 6))
			require.Zero(t, events.Count(annotations.RulesIteration), "a window of keys the sets hold runs no iteration")
		})
	}
}

// edgeKeysMatcher counts, per variable of a :page/next pattern, the keys the
// pattern is matched against through a binding carrying that variable: the
// pages whose edges a body seeks, at the entity position or the value
// position.
type edgeKeysMatcher struct {
	*storage.Database
	keys map[query.Symbol]int
}

func (m *edgeKeysMatcher) Match(q *query.Query, bindings Relations) (Relation, error) {
	for _, clause := range q.Where {
		pattern, ok := clause.(*query.DataPattern)
		if !ok {
			continue
		}
		attr, ok := pattern.GetA().(query.Constant)
		if !ok || attr.Value != PageNext {
			continue
		}
		for _, elem := range pattern.Elements {
			v, ok := elem.(query.Variable)
			if !ok {
				continue
			}
			for _, binding := range bindings {
				if query.SymbolIndex(binding.Symbols(), v.Name) >= 0 {
					m.keys[v.Name] += binding.Size()
				}
			}
		}
	}
	return m.Database.Match(q, bindings)
}

// A delta copy seeks its pattern on the delta's keys: over the closure no
// magic predicate guards, the recursive copy matches the edge pattern under
// the binding its delta provides, so the keys the pattern is matched against
// through a binding carrying ?b total the closure's pairs, one per delta
// tuple across the iterations (docs/proposals/DELTA_LEADS_ITS_COPY_PROOF.md,
// T3).
func TestRunStratum_ADeltaCopySeeksItsPatternOnTheDeltasKeys(t *testing.T) {
	b := datalog.NewSymbol("?b")
	rs := MustRules(t, FollowsRulesText)
	q := MustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`)
	for _, mode := range OptimizerModes {
		t.Run(mode.Name(), func(t *testing.T) {
			matcher := &edgeKeysMatcher{Database: factsDatabase(t, ChainDatoms(6)), keys: map[query.Symbol]int{}}
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
			result, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			require.NoError(t, err)
			RequireSamePairs(t, IdentityPairs(t, result), ChainClosure(6))
			require.Equal(t, len(ChainClosure(6)), matcher.keys[b], "the recursive copy seeks the edge pattern on the delta's ?b, one key per delta tuple")
		})
	}
}

// A copy's pattern over its materialized delta binds through the matcher
// under the whole delta and runs under no window
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.4, §7): over the
// closure no magic predicate guards, under a window cap of two, no window
// event fires, and the keys the edge pattern is matched against total the
// deltas' tuples, so the matcher's boundary reads each delta's size whole.
func TestRunStratum_ACopyMatchesItsPatternUnderTheWholeDelta(t *testing.T) {
	b := datalog.NewSymbol("?b")
	rs := MustRules(t, FollowsRulesText)
	q := MustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`)
	for _, mode := range OptimizerModes {
		t.Run(mode.Name(), func(t *testing.T) {
			events := &RuleEvents{}
			opts := mode.PlannerOptions()
			opts.ScopeWindowCap = 2
			opts.Handler = events.Handle
			matcher := &edgeKeysMatcher{Database: factsDatabase(t, ChainDatoms(6)), keys: map[query.Symbol]int{}}
			exec := NewExecutorWithOptions(matcher, nil, opts)
			result, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			require.NoError(t, err)
			RequireSamePairs(t, IdentityPairs(t, result), ChainClosure(6))
			require.Equal(t, len(ChainClosure(6)), matcher.keys[b], "the edge pattern is matched under each delta whole, one key per delta tuple")
			require.Zero(t, events.Count(annotations.ScopeWindow), "a pattern over a group held in memory binds through the matcher and reads no window")
			require.Equal(t, 6, events.Count(annotations.RulesIteration), "the chain's five deltas and the empty one")
		})
	}
}

// leafwardRulesText is the closure with a rule reaching the leaves a page
// follows: its invocation of follows stands beside a pattern with a value
// constant on the invocation's second argument.
const leafwardRulesText = `[[(follows ?a ?b) [?a :page/next ?b]]
                           [(follows ?a ?c) [?a :page/next ?b] (follows ?b ?c)]
                           [(leafward ?a ?x) (follows ?a ?x) [?x :page/kind "leaf"]]]`

// A body's invocation with an available argument beside a pattern with a
// value constant on its other argument, a clause outside the seed component,
// is adorned for the available argument alone, so the fixpoint derives what
// the seed reaches and the pattern filters it after
// (A_CLAUSE_OUTSIDE_THE_SEED_COMPONENT_BINDS_NOTHING_PROOF.md T1, T3): over
// the chain, leafward bound to the first page runs follows adorned for its
// first position and under no adornment of both, and answers the five pages
// after the first.
func TestRuleEvaluation_ABodysInvocationBesideAValueConstantPatternDerivesWhatTheSeedReaches(t *testing.T) {
	follows := datalog.NewSymbol("follows")
	rs := MustRules(t, leafwardRulesText)
	q := MustParse(t, `[:find ?x :in $ % :where (leafward #id "page1" ?x)]`)
	for _, mode := range OptimizerModes {
		t.Run(mode.Name(), func(t *testing.T) {
			events := &RuleEvents{}
			opts := mode.PlannerOptions()
			opts.Handler = events.Handle
			exec := NewExecutorWithOptions(factsDatabase(t, ChainDatoms(6)), nil, opts)
			result, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			require.NoError(t, err)
			var got []datalog.Identity
			require.NoError(t, ForEach(result, func(tuple Tuple) error {
				got = append(got, tuple[0].(datalog.Identity))
				return nil
			}))
			require.ElementsMatch(t, []datalog.Identity{Page(2), Page(3), Page(4), Page(5), Page(6)}, got, "the leaves the first page follows")
			derived := events.Predicates()
			require.Contains(t, derived, query.AdornedSymbol(follows, []bool{true, false}), "follows runs adorned for the available first argument")
			require.NotContains(t, derived, query.AdornedSymbol(follows, []bool{true, true}), "the pattern with a value constant binds no position of follows")
		})
	}
}

// A copy's join into a held predicate probes the index the predicate's set
// keeps with the delta's tuples and drains nothing of the predicate
// (docs/proposals/A_HELD_RELATION_KEEPS_ITS_JOIN_INDEX_PROOF.md, T1, T3, L4,
// L6): over the closure of step through reach, whose recursive copy joins
// reach's delta into step, the probe event of each iteration after the
// first reports as many probes as the iteration before added to reach, no
// build.copy event drains step, and the answer is the chain's closure.
func TestRunStratum_ACopysJoinIntoAHeldPredicateProbesTheDeltaAlone(t *testing.T) {
	reach := datalog.NewSymbol("reach")
	rs := MustRules(t, `[[(step ?a ?b) [?a :page/next ?b]]
	                     [(reach ?x ?y) (step ?x ?y)]
	                     [(reach ?x ?z) (reach ?x ?y) (step ?y ?z)]]`)
	q := MustParse(t, `[:find ?x ?y :in $ % :where (reach ?x ?y)]`)
	for _, mode := range OptimizerModes {
		t.Run(mode.Name(), func(t *testing.T) {
			events := &RuleEvents{}
			opts := mode.PlannerOptions()
			opts.Handler = events.Handle
			exec := NewExecutorWithOptions(factsDatabase(t, ChainDatoms(6)), nil, opts)
			result, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			require.NoError(t, err)
			RequireSamePairs(t, IdentityPairs(t, result), ChainClosure(6))
			require.Zero(t, events.Count(annotations.JoinBuildCopy), "no join drains a held predicate")

			// The probes of each iteration's join are read between the
			// iteration events of reach's stratum: the delta the iteration
			// before added to reach is the probe side of the copy's join into
			// step, and no other join runs in the iteration.
			var probes []int
			previous, compared := -1, 0
			for _, e := range events.Events() {
				switch e.Name {
				case annotations.JoinProbe:
					probes = append(probes, e.Data["tuple_count"].(int))
				case annotations.RulesIteration:
					if !query.ContainsSymbol(e.Data["predicates"].([]query.Symbol), reach) {
						probes = nil
						continue
					}
					if previous >= 0 {
						require.Equal(t, []int{previous}, probes, "iteration %v: the copy's join probes the delta of reach the iteration before added", e.Data["iteration"])
						compared++
					}
					previous = e.Data["delta"].(map[query.Symbol]int)[reach]
					probes = nil
				}
			}
			require.Equal(t, 5, compared, "the chain's five deltas, each the probe side of one join")
		})
	}
}

// A pass carries a chain of derivations along every edge of the stratum's
// dependency graph but its back edges
// (docs/proposals/A_COPY_READS_THE_TUPLES_ADDED_SINCE_IT_RAN_PROOF.md, T1,
// T4, L2, L6): over the ring a, b, c, where b reads a, c reads b and a's
// recursive body reads c, one hop of a's closure runs through b and c back
// to a inside one pass, so the iteration events number one more than the
// chain's longest path's edge count, every pass but the last adds to a, b
// and c alike, and the answer is the chain's closure.
func TestRunStratum_APassCarriesAChainOfDerivations(t *testing.T) {
	a, b, c := datalog.NewSymbol("a"), datalog.NewSymbol("b"), datalog.NewSymbol("c")
	rs := MustRules(t, `[[(a ?x ?y) [?x :page/next ?y]]
	                     [(b ?x ?y) (a ?x ?y)]
	                     [(c ?x ?y) (b ?x ?y)]
	                     [(a ?x ?z) (c ?x ?y) [?y :page/next ?z]]]`)
	q := MustParse(t, `[:find ?x ?y :in $ % :where (a ?x ?y)]`)
	for _, mode := range OptimizerModes {
		t.Run(mode.Name(), func(t *testing.T) {
			events := &RuleEvents{}
			opts := mode.PlannerOptions()
			opts.Handler = events.Handle
			exec := NewExecutorWithOptions(factsDatabase(t, ChainDatoms(6)), nil, opts)
			result, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			require.NoError(t, err)
			RequireSamePairs(t, IdentityPairs(t, result), ChainClosure(6))

			var passes []map[query.Symbol]int
			for _, e := range events.Events() {
				if e.Name == annotations.RulesIteration {
					passes = append(passes, e.Data["delta"].(map[query.Symbol]int))
				}
			}
			require.Len(t, passes, 6, "the seeding run and the chain's five hops, one pass each: %v", passes)
			for i, delta := range passes[1 : len(passes)-1] {
				for _, p := range []query.Symbol{a, b, c} {
					require.Positive(t, delta[p], "pass %d adds to %s: %v", i+1, p, delta)
				}
			}
		})
	}
}

// A stratum above a changed one runs, at a later window, the copies of its
// bodies marked on the changed predicate over that predicate's new tuples
// alone: the base rule seeks the edge of the page the window's seed added
// to the magic set, and not of the page held since the first window.
func TestRuleEvaluation_ALaterWindowReadsALowerStratumsNewTuplesAlone(t *testing.T) {
	a := datalog.NewSymbol("?a")
	for _, mode := range OptimizerModes {
		t.Run(mode.Name(), func(t *testing.T) {
			matcher := &edgeKeysMatcher{Database: factsDatabase(t, ChainDatoms(6)), keys: map[query.Symbol]int{}}
			// Attribute-fetch fusion is off, so the base rule's edge pattern is
			// matched over the window's seeds, the keys the matcher counts.
			opts := mode.PlannerOptions()
			opts.EnableAttributeFetchFusion = false
			exec, planned, ctx := boundClosure(t, followsRecursionFirstText, matcher, opts)
			rules, seed := planned.Magic.Rules, planned.Magic.Seed
			ev := NewRuleEvaluation(exec, planned)
			// A bound invocation answers the whole invocation's tuples carrying
			// the bound value at the bound position, the rule
			// TestExecuteWithRelations_SeededEqualsWhole pins: for the first
			// position bound to the seeded pages, the closure's pairs whose
			// first page is one of them.
			closureFrom := func(seeded ...int) map[[2]datalog.Identity]bool {
				want := map[[2]datalog.Identity]bool{}
				for pair := range ChainClosure(6) {
					for _, from := range seeded {
						if pair[0] == Page(from) {
							want[pair] = true
						}
					}
				}
				return want
			}

			rel, err := ev.Extend(ctx, seedOf(rules, seed, 4))
			require.NoError(t, err)
			pairs := IdentityPairs(t, rel)
			RequireSamePairs(t, pairs, closureFrom(4))
			require.True(t, pairs[[2]datalog.Identity{Page(4), Page(6)}], "page six, two edges from page four, is reached")
			require.Equal(t, 1, matcher.keys[a], "the first window seeks the edge of page four")

			matcher.keys = map[query.Symbol]int{}
			rel, err = ev.Extend(ctx, seedOf(rules, seed, 2))
			require.NoError(t, err)
			RequireSamePairs(t, IdentityPairs(t, rel), closureFrom(4, 2))
			require.Equal(t, 1, matcher.keys[a], "the second window seeks the edge of page two alone, the magic fact it added, and not of page four")
		})
	}
}

// The strata begin at the first window and complete once, when the
// evaluation completes, each with its iterations summed over the windows and
// its sets' tuples; completing again reports nothing.
func TestRuleEvaluation_CompleteReportsEachStratumOnce(t *testing.T) {
	for _, mode := range OptimizerModes {
		t.Run(mode.Name(), func(t *testing.T) {
			events := &RuleEvents{}
			opts := mode.PlannerOptions()
			opts.Handler = events.Handle
			exec, planned, ctx := boundClosure(t, FollowsRulesText, factsDatabase(t, ChainDatoms(6)), opts)
			rules, seed := planned.Magic.Rules, planned.Magic.Seed
			ev := NewRuleEvaluation(exec, planned)
			for _, s := range []Relation{seedOf(rules, seed, 4), seedOf(rules, seed, 2)} {
				_, err := ev.Extend(ctx, s)
				require.NoError(t, err)
			}
			require.Equal(t, 2, events.Count(annotations.RulesStratumBegin), "the magic stratum and the adorned stratum begin at the first window")
			require.Zero(t, events.Count(annotations.RulesStratumComplete), "no stratum completes before the evaluation does")

			ev.Complete()
			iterations, tuples := 0, 0
			for _, e := range events.Events() {
				if e.Name == annotations.RulesStratumComplete {
					iterations += e.Data["iterations"].(int)
					tuples += e.Data["tuples"].(int)
				}
			}
			require.Equal(t, 2, events.Count(annotations.RulesStratumComplete), "each stratum completes once")
			require.Equal(t, 5+4, iterations, "the magic stratum's three and two iterations and the adorned stratum's two and two, summed over the two windows")
			require.Equal(t, 5+10, tuples, "the five magic facts and the ten pairs the sets hold")

			ev.Complete()
			require.Equal(t, 2, events.Count(annotations.RulesStratumComplete), "a stratum completes once")
		})
	}
}

// A whole evaluation, one with no seed predicate, derives its predicate at
// the first window, and a later window runs no stratum.
func TestRuleEvaluation_AWholeEvaluationRunsOnce(t *testing.T) {
	follows := datalog.NewSymbol("follows")
	for _, mode := range OptimizerModes {
		t.Run(mode.Name(), func(t *testing.T) {
			events := &RuleEvents{}
			opts := mode.PlannerOptions()
			opts.Handler = events.Handle
			exec := NewExecutorWithOptions(factsDatabase(t, ChainDatoms(6)), nil, opts)
			rs := MustRules(t, FollowsRulesText)
			ctx := NewContext().WithRules(rs)
			planned, err := planner.PlanEvaluation(exec.GetPlanner(), rs, follows, []bool{false, false}, nil)
			require.NoError(t, err)
			ev := NewRuleEvaluation(exec, planned)

			rel, err := ev.Extend(ctx, nil)
			require.NoError(t, err)
			RequireSamePairs(t, IdentityPairs(t, rel), ChainClosure(6))
			require.Equal(t, 1, events.Count(annotations.RulesStratumBegin))

			events.Reset()
			rel, err = ev.Extend(ctx, nil)
			require.NoError(t, err)
			RequireSamePairs(t, IdentityPairs(t, rel), ChainClosure(6))
			require.Zero(t, events.Count(annotations.RulesIteration), "a later window of a whole evaluation runs nothing")
			require.Zero(t, events.Count(annotations.RulesStratumBegin))
		})
	}
}

// A stratum's window run absorbs the seed into its set as the window's first
// delta and returns the tuples the window added, nothing for a seed the set
// holds.
func TestRuleEvaluation_RunStratumReturnsWhatTheWindowAdded(t *testing.T) {
	for _, mode := range OptimizerModes {
		t.Run(mode.Name(), func(t *testing.T) {
			exec, planned, ctx := boundClosure(t, FollowsRulesText, factsDatabase(t, ChainDatoms(6)), mode.PlannerOptions())
			rules, seed := planned.Magic.Rules, planned.Magic.Seed
			ev := NewRuleEvaluation(exec, planned)
			_, err := ev.Extend(ctx, seedOf(rules, seed, 4))
			require.NoError(t, err)
			magic := ev.Strata()[0]
			require.Equal(t, []query.Symbol{seed}, magic.Stratum().Predicates, "the magic stratum is the lowest")

			added, err := ev.RunStratum(ctx, magic, seedOf(rules, seed, 2), nil)
			require.NoError(t, err)
			require.Len(t, added[seed], 2, "page two, the seed, and page three, the fact it reaches before the held ones")

			added, err = ev.RunStratum(ctx, magic, seedOf(rules, seed, 5), nil)
			require.NoError(t, err)
			require.Empty(t, added, "a seed the set holds adds nothing")
		})
	}
}

// Under continuations an invocation clause's evaluation is created at its
// first run, with the one rewrite, and extended by every later run with that
// run's seed; without them each run is an evaluation of its own.
func TestDeriveInvocation_ContinuesTheClausesEvaluationUnderContinuations(t *testing.T) {
	follows := datalog.NewSymbol("follows")
	x, y := datalog.NewSymbol("?x"), datalog.NewSymbol("?y")
	inv := &query.RuleInvocation{Name: follows, Args: []query.PatternElement{query.Variable{Name: x}, query.Variable{Name: y}}}
	groupsOf := func(pages ...int) []Relation {
		tuples := make([]Tuple, len(pages))
		for i, p := range pages {
			tuples[i] = Tuple{Page(p)}
		}
		return []Relation{NewMaterializedRelation([]query.Symbol{x}, tuples)}
	}
	for _, mode := range OptimizerModes {
		t.Run(mode.Name(), func(t *testing.T) {
			events := &RuleEvents{}
			opts := mode.PlannerOptions()
			opts.Handler = events.Handle
			ex := NewExecutorWithOptions(factsDatabase(t, ChainDatoms(6)), nil, opts)
			rs := MustRules(t, FollowsRulesText)
			planned, err := planner.PlanEvaluation(ex.GetPlanner(), rs, follows, []bool{true, false}, nil)
			require.NoError(t, err)
			exec := ex.QueryExecutor(&planner.RealizedPlan{Evaluations: map[*query.RuleInvocation]*planner.InvocationEvaluation{inv: planned}})

			continued := NewRuleContinuations()
			ctx := NewContext().WithRules(rs).WithContinuations(continued)
			rel, _, err := exec.DeriveInvocation(ctx, inv, groupsOf(4))
			require.NoError(t, err)
			RequireSamePairs(t, IdentityPairs(t, rel), pairsFrom(4, 6))
			rel, _, err = exec.DeriveInvocation(ctx, inv, groupsOf(2))
			require.NoError(t, err)
			RequireSamePairs(t, IdentityPairs(t, rel), pairsFrom(2, 6))
			require.Equal(t, 1, events.Count(annotations.RulesMagicSets), "one rewrite serves every run of the clause")
			require.Equal(t, 2, events.Count(annotations.RulesStratumBegin), "the two strata begin once")
			require.Zero(t, events.Count(annotations.RulesStratumComplete), "the strata complete when the form execution does")
			require.Len(t, continued.ByClause(), 1, "the clause's one evaluation")
			continued.Complete()
			require.Equal(t, 2, events.Count(annotations.RulesStratumComplete))

			events.Reset()
			alone := NewContext().WithRules(rs)
			for _, groups := range [][]Relation{groupsOf(4), groupsOf(2)} {
				_, _, err := exec.DeriveInvocation(alone, inv, groups)
				require.NoError(t, err)
			}
			require.Equal(t, 2, events.Count(annotations.RulesMagicSets), "an invocation outside a form execution evaluates the rewrite anew at each run")
			require.Equal(t, 4, events.Count(annotations.RulesStratumBegin))
			require.Equal(t, 4, events.Count(annotations.RulesStratumComplete), "each run's evaluation completes at once")
		})
	}
}

// A form inside a form's branch shares the outer execution's continuations:
// its invocation's evaluation is continued across the outer windows as well
// as the inner, and completes with the outer execution.
func TestFormInsideAFormContinuesOneEvaluationAcrossTheOuterWindows(t *testing.T) {
	const pages = 6
	matcher := factsDatabase(t, ChainDatoms(pages))
	rs := MustRules(t, FollowsRulesText)
	q := MustParse(t, `[:find ?x ?y :in $ % :where [?x :page/kind "leaf"] (or-join [[?x] ?y] (or-default-join [[?x] ?y] (follows ?x ?y) [(ground :end) ?y]) [(ground :none) ?y])]`)
	for _, mode := range OptimizerModes {
		t.Run(mode.Name(), func(t *testing.T) {
			events := &RuleEvents{}
			opts := mode.PlannerOptions()
			opts.ScopeWindowCap = 2
			opts.Handler = events.Handle
			exec := NewExecutorWithOptions(matcher, nil, opts)
			inputs, err := CreateInputRelationsFromValuesWithOptions(q, []interface{}{datalog.SymDollar, datalog.NewSymbol("%")}, exec.Options())
			require.NoError(t, err)
			rel, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, inputs)
			require.NoError(t, err)
			collected, err := CollectTuples(rel, nil)
			require.NoError(t, err)
			require.Len(t, collected, pages*(pages-1)/2+1+pages, "every reachable pair, the sink's end, and each page's none")
			require.Greater(t, events.Count(annotations.ScopeWindow), 1, "the subject spans several windows")
			require.Equal(t, 2, events.Count(annotations.RulesStratumBegin), "the magic stratum and the adorned stratum each begin once across the outer and the inner windows")
			require.Equal(t, 2, events.Count(annotations.RulesStratumComplete), "each stratum completes once, with the outer execution")
			require.Equal(t, pages+pages*(pages-1)/2, events.Entered(), "six magic facts and fifteen pairs, each entering a delta once")
		})
	}
}

// A form's result closed before its subject is exhausted completes the
// evaluations its execution continued.
func TestOrFallbackIteratorClosedBeforeExhaustionCompletesItsEvaluations(t *testing.T) {
	const pages = 6
	matcher := factsDatabase(t, ChainDatoms(pages))
	rs := MustRules(t, FollowsRulesText)
	form := MustParse(t, `[:find ?x ?y :in $ % :where (or-default-join [[?x] ?y] (follows ?x ?y) [(ground :end) ?y])]`).Where
	x := datalog.NewSymbol("?x")
	var subjects []Tuple
	for i := 1; i <= pages; i++ {
		subjects = append(subjects, Tuple{Page(i)})
	}
	for _, mode := range OptimizerModes {
		t.Run(mode.Name(), func(t *testing.T) {
			events := &RuleEvents{}
			opts := mode.PlannerOptions()
			opts.ScopeWindowCap = 2
			opts.Handler = events.Handle
			exec := NewExecutorWithOptions(matcher, nil, opts).QueryExecutor(nil)
			subject := NewStreamingRelation([]query.Symbol{x}, NewMaterializedRelation([]query.Symbol{x}, subjects).Iterator())
			groups, err := exec.Execute(NewContext().WithRules(rs), &query.Query{Where: form}, []Relation{subject})
			require.NoError(t, err)
			require.Len(t, groups, 1)
			it := groups[0].Iterator()
			require.True(t, it.Next(), "the first window answers a tuple")
			require.Equal(t, 2, events.Count(annotations.RulesStratumBegin))
			require.Zero(t, events.Count(annotations.RulesStratumComplete))
			require.NoError(t, it.Close())
			require.Equal(t, 2, events.Count(annotations.RulesStratumComplete), "the close completes the strata")
		})
	}
}
