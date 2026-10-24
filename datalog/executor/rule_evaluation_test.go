package executor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
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

// ruleEvents holds the events a handler saw.
type ruleEvents struct{ events []annotations.Event }

func (r *ruleEvents) handle(e annotations.Event) { r.events = append(r.events, e) }

// count is how many events of one name were seen.
func (r *ruleEvents) count(name string) int {
	n := 0
	for _, e := range r.events {
		if e.Name == name {
			n++
		}
	}
	return n
}

// entered is the tuples that entered a delta: the delta sizes of every
// iteration event, summed.
func (r *ruleEvents) entered() int {
	n := 0
	for _, e := range r.events {
		if e.Name != annotations.RulesIteration {
			continue
		}
		for _, size := range e.Data["delta"].(map[query.Symbol]int) {
			n += size
		}
	}
	return n
}

// boundClosure is the closure in text rewritten for a bound first position:
// the executor over matcher under opts, the rewritten set, the predicate the
// invocation reads, its seed predicate, and a context carrying the original
// set.
func boundClosure(t *testing.T, text string, matcher PatternMatcher, opts planner.PlannerOptions) (*Executor, *query.RuleSet, query.Symbol, query.Symbol, *Context) {
	t.Helper()
	rs := mustRules(t, text)
	magic, err := planner.Rewrite(planner.CreatePlanner(nil, opts), rs, datalog.NewSymbol("follows"), []bool{true, false}, nil)
	require.NoError(t, err)
	return NewExecutorWithOptions(matcher, nil, opts), magic.Rules, magic.Predicate, magic.Seed, NewContext().WithRules(rs)
}

// seedOf is a seed relation of the pages given, over the seed predicate's
// head under the rule set.
func seedOf(rs *query.RuleSet, seed query.Symbol, pages ...int) Relation {
	head, _ := rs.Head(seed)
	tuples := make([]Tuple, len(pages))
	for i, p := range pages {
		tuples[i] = Tuple{page(p)}
	}
	return NewMaterializedRelation(head, tuples)
}

// pairsFrom is every pair (i, j) with from ≤ i < j ≤ n: the closure of the
// chain from page from.
func pairsFrom(from, n int) map[[2]datalog.Identity]bool {
	pairs := map[[2]datalog.Identity]bool{}
	for i := from; i <= n; i++ {
		for j := i + 1; j <= n; j++ {
			pairs[[2]datalog.Identity{page(i), page(j)}] = true
		}
	}
	return pairs
}

// A later window's seed derives the new keys alone: the magic facts and the
// pairs its keys reach that the sets do not hold enter a delta once, and a
// window whose keys the sets already hold runs no iteration.
func TestRuleEvaluation_ALaterSeedDerivesTheNewKeysAlone(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			events := &ruleEvents{}
			opts := mode.plannerOptions()
			opts.Handler = events.handle
			exec, rules, predicate, seed, ctx := boundClosure(t, followsRulesText, NewMemoryPatternMatcher(chainDatoms(6)), opts)
			ev := newRuleEvaluation(exec, rules, predicate, seed)

			rel, err := ev.extend(ctx, seedOf(rules, seed, 4))
			require.NoError(t, err)
			requireSamePairs(t, identityPairs(t, rel), pairsFrom(4, 6))
			require.Equal(t, 3+3, events.entered(), "the magic facts of pages four to six and the three pairs from them")

			events.events = nil
			rel, err = ev.extend(ctx, seedOf(rules, seed, 2))
			require.NoError(t, err)
			requireSamePairs(t, identityPairs(t, rel), pairsFrom(2, 6))
			require.Equal(t, 2+7, events.entered(), "the magic facts of pages two and three and the seven pairs from them; the held facts enter no delta again")

			events.events = nil
			rel, err = ev.extend(ctx, seedOf(rules, seed, 4))
			require.NoError(t, err)
			requireSamePairs(t, identityPairs(t, rel), pairsFrom(2, 6))
			require.Zero(t, events.count(annotations.RulesIteration), "a window of keys the sets hold runs no iteration")
		})
	}
}

// edgeKeysMatcher counts, per entity symbol, the keys a :page/next pattern
// is matched against through a binding carrying that symbol: the pages
// whose edges a body seeks.
type edgeKeysMatcher struct {
	PatternMatcher
	keys map[query.Symbol]int
}

func (m *edgeKeysMatcher) Match(q *query.Query, bindings Relations) (Relation, error) {
	for _, clause := range q.Where {
		pattern, ok := clause.(*query.DataPattern)
		if !ok {
			continue
		}
		attr, ok := pattern.GetA().(query.Constant)
		if !ok || attr.Value != pageNext {
			continue
		}
		entity, ok := pattern.GetE().(query.Variable)
		if !ok {
			continue
		}
		for _, binding := range bindings {
			if query.SymbolIndex(binding.Symbols(), entity.Name) >= 0 {
				m.keys[entity.Name] += binding.Size()
			}
		}
	}
	return m.PatternMatcher.Match(q, bindings)
}

// A stratum above a changed one runs, at a later window, the copies of its
// bodies marked on the changed predicate over that predicate's new tuples
// alone: the base rule seeks the edge of the page the window's seed added
// to the magic set, and not of the page held since the first window.
func TestRuleEvaluation_ALaterWindowReadsALowerStratumsNewTuplesAlone(t *testing.T) {
	a := datalog.NewSymbol("?a")
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			matcher := &edgeKeysMatcher{PatternMatcher: NewMemoryPatternMatcher(chainDatoms(6)), keys: map[query.Symbol]int{}}
			exec, rules, predicate, seed, ctx := boundClosure(t, followsRecursionFirstText, matcher, mode.plannerOptions())
			ev := newRuleEvaluation(exec, rules, predicate, seed)
			// A bound invocation answers the whole invocation's tuples carrying
			// the bound value at the bound position, the rule
			// TestExecuteWithRelations_SeededEqualsWhole pins: for the first
			// position bound to the seeded pages, the closure's pairs whose
			// first page is one of them.
			closureFrom := func(seeded ...int) map[[2]datalog.Identity]bool {
				want := map[[2]datalog.Identity]bool{}
				for pair := range chainClosure(6) {
					for _, from := range seeded {
						if pair[0] == page(from) {
							want[pair] = true
						}
					}
				}
				return want
			}

			rel, err := ev.extend(ctx, seedOf(rules, seed, 4))
			require.NoError(t, err)
			pairs := identityPairs(t, rel)
			requireSamePairs(t, pairs, closureFrom(4))
			require.True(t, pairs[[2]datalog.Identity{page(4), page(6)}], "page six, two edges from page four, is reached")
			require.Equal(t, 1, matcher.keys[a], "the first window seeks the edge of page four")

			matcher.keys = map[query.Symbol]int{}
			rel, err = ev.extend(ctx, seedOf(rules, seed, 2))
			require.NoError(t, err)
			requireSamePairs(t, identityPairs(t, rel), closureFrom(4, 2))
			require.Equal(t, 1, matcher.keys[a], "the second window seeks the edge of page two alone, the magic fact it added, and not of page four")
		})
	}
}

// The strata begin at the first window and complete once, when the
// evaluation completes, each with its iterations summed over the windows and
// its sets' tuples; completing again reports nothing.
func TestRuleEvaluation_CompleteReportsEachStratumOnce(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			events := &ruleEvents{}
			opts := mode.plannerOptions()
			opts.Handler = events.handle
			exec, rules, predicate, seed, ctx := boundClosure(t, followsRulesText, NewMemoryPatternMatcher(chainDatoms(6)), opts)
			ev := newRuleEvaluation(exec, rules, predicate, seed)
			for _, s := range []Relation{seedOf(rules, seed, 4), seedOf(rules, seed, 2)} {
				_, err := ev.extend(ctx, s)
				require.NoError(t, err)
			}
			require.Equal(t, 2, events.count(annotations.RulesStratumBegin), "the magic stratum and the adorned stratum begin at the first window")
			require.Zero(t, events.count(annotations.RulesStratumComplete), "no stratum completes before the evaluation does")

			ev.complete()
			iterations, tuples := 0, 0
			for _, e := range events.events {
				if e.Name == annotations.RulesStratumComplete {
					iterations += e.Data["iterations"].(int)
					tuples += e.Data["tuples"].(int)
				}
			}
			require.Equal(t, 2, events.count(annotations.RulesStratumComplete), "each stratum completes once")
			require.Equal(t, 5+4, iterations, "the magic stratum's three and two iterations and the adorned stratum's two and two, summed over the two windows")
			require.Equal(t, 5+10, tuples, "the five magic facts and the ten pairs the sets hold")

			ev.complete()
			require.Equal(t, 2, events.count(annotations.RulesStratumComplete), "a stratum completes once")
		})
	}
}

// A whole evaluation, one with no seed predicate, derives its predicate at
// the first window, and a later window runs no stratum.
func TestRuleEvaluation_AWholeEvaluationRunsOnce(t *testing.T) {
	follows := datalog.NewSymbol("follows")
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			events := &ruleEvents{}
			opts := mode.plannerOptions()
			opts.Handler = events.handle
			exec := NewExecutorWithOptions(NewMemoryPatternMatcher(chainDatoms(6)), nil, opts)
			rs := mustRules(t, followsRulesText)
			ctx := NewContext().WithRules(rs)
			ev := newRuleEvaluation(exec, rs, follows, nil)

			rel, err := ev.extend(ctx, nil)
			require.NoError(t, err)
			requireSamePairs(t, identityPairs(t, rel), chainClosure(6))
			require.Equal(t, 1, events.count(annotations.RulesStratumBegin))

			events.events = nil
			rel, err = ev.extend(ctx, nil)
			require.NoError(t, err)
			requireSamePairs(t, identityPairs(t, rel), chainClosure(6))
			require.Zero(t, events.count(annotations.RulesIteration), "a later window of a whole evaluation runs nothing")
			require.Zero(t, events.count(annotations.RulesStratumBegin))
		})
	}
}

// A stratum's window run absorbs the seed into its set as the window's first
// delta and returns the tuples the window added, nothing for a seed the set
// holds.
func TestRuleEvaluation_RunStratumReturnsWhatTheWindowAdded(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec, rules, predicate, seed, ctx := boundClosure(t, followsRulesText, NewMemoryPatternMatcher(chainDatoms(6)), mode.plannerOptions())
			ev := newRuleEvaluation(exec, rules, predicate, seed)
			_, err := ev.extend(ctx, seedOf(rules, seed, 4))
			require.NoError(t, err)
			magic := ev.strata[0]
			require.Equal(t, []query.Symbol{seed}, magic.stratum.Predicates, "the magic stratum is the lowest")

			added, err := ev.runStratum(ctx, magic, seedOf(rules, seed, 2), nil)
			require.NoError(t, err)
			require.Len(t, added[seed], 2, "page two, the seed, and page three, the fact it reaches before the held ones")

			added, err = ev.runStratum(ctx, magic, seedOf(rules, seed, 5), nil)
			require.NoError(t, err)
			require.Empty(t, added, "a seed the set holds adds nothing")
		})
	}
}

// The continuations a context carries reach every context derived from it.
func TestContext_ContinuationsAreCarriedByEveryDerivedContext(t *testing.T) {
	continued := newRuleContinuations()
	ctx := NewContext().WithContinuations(continued)
	require.Same(t, continued, ctx.continued)
	for name, derived := range map[string]*Context{
		"WithEnvironment":  ctx.WithEnvironment(nil),
		"WithDerived":      ctx.WithDerived(nil),
		"WithRules":        ctx.WithRules(nil),
		"WithCancellation": ctx.WithCancellation(context.Background()),
		"forkContext":      forkContext(ctx),
	} {
		require.Same(t, continued, derived.continued, name)
	}
	require.Nil(t, NewContext().continued, "a context carries no continuations until a form execution gives it some")
}

// An invocation's evaluation is whole when nothing is bound, and rewritten
// for the bound positions, with the seed predicate the windows fill,
// otherwise.
func TestInvocationEvaluation_IsWholeWithNothingBoundAndRewrittenOtherwise(t *testing.T) {
	follows := datalog.NewSymbol("follows")
	x, y := datalog.NewSymbol("?x"), datalog.NewSymbol("?y")
	inv := &query.RuleInvocation{Name: follows, Args: []query.PatternElement{query.Variable{Name: x}, query.Variable{Name: y}}}
	events := &ruleEvents{}
	opts := defaultPlannerOptions()
	opts.Handler = events.handle
	exec := NewExecutorWithOptions(NewMemoryPatternMatcher(chainDatoms(6)), nil, opts).queryExecutor()
	rs := mustRules(t, followsRulesText)

	whole, err := exec.invocationEvaluation(rs, inv, []bool{false, false})
	require.NoError(t, err)
	require.Same(t, rs, whole.rs)
	require.Equal(t, follows, whole.predicate)
	require.Nil(t, whole.seed, "nothing bound: the predicate whole")
	require.Zero(t, events.count(annotations.RulesMagicSets))

	bound, err := exec.invocationEvaluation(rs, inv, []bool{true, false})
	require.NoError(t, err)
	require.NotSame(t, rs, bound.rs, "something bound: the rewritten set")
	require.NotEqual(t, follows, bound.predicate, "the adorned predicate")
	require.NotNil(t, bound.seed, "the seed predicate the windows fill")
	require.Equal(t, 1, events.count(annotations.RulesMagicSets), "the rewrite is recorded once")
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
			tuples[i] = Tuple{page(p)}
		}
		return []Relation{NewMaterializedRelation([]query.Symbol{x}, tuples)}
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			events := &ruleEvents{}
			opts := mode.plannerOptions()
			opts.Handler = events.handle
			exec := NewExecutorWithOptions(NewMemoryPatternMatcher(chainDatoms(6)), nil, opts).queryExecutor()
			rs := mustRules(t, followsRulesText)

			continued := newRuleContinuations()
			ctx := NewContext().WithRules(rs).WithContinuations(continued)
			rel, _, err := exec.deriveInvocation(ctx, inv, groupsOf(4))
			require.NoError(t, err)
			requireSamePairs(t, identityPairs(t, rel), pairsFrom(4, 6))
			rel, _, err = exec.deriveInvocation(ctx, inv, groupsOf(2))
			require.NoError(t, err)
			requireSamePairs(t, identityPairs(t, rel), pairsFrom(2, 6))
			require.Equal(t, 1, events.count(annotations.RulesMagicSets), "one rewrite serves every run of the clause")
			require.Equal(t, 2, events.count(annotations.RulesStratumBegin), "the two strata begin once")
			require.Zero(t, events.count(annotations.RulesStratumComplete), "the strata complete when the form execution does")
			require.Len(t, continued.byClause, 1, "the clause's one evaluation")
			continued.complete()
			require.Equal(t, 2, events.count(annotations.RulesStratumComplete))

			events.events = nil
			alone := NewContext().WithRules(rs)
			for _, groups := range [][]Relation{groupsOf(4), groupsOf(2)} {
				_, _, err := exec.deriveInvocation(alone, inv, groups)
				require.NoError(t, err)
			}
			require.Equal(t, 2, events.count(annotations.RulesMagicSets), "an invocation outside a form execution is rewritten at each run")
			require.Equal(t, 4, events.count(annotations.RulesStratumBegin))
			require.Equal(t, 4, events.count(annotations.RulesStratumComplete), "each run's evaluation completes at once")
		})
	}
}

// A form inside a form's branch shares the outer execution's continuations:
// its invocation's evaluation is continued across the outer windows as well
// as the inner, and completes with the outer execution.
func TestFormInsideAFormContinuesOneEvaluationAcrossTheOuterWindows(t *testing.T) {
	const pages = 6
	matcher := NewMemoryPatternMatcher(chainDatoms(pages))
	rs := mustRules(t, followsRulesText)
	q := mustParse(t, `[:find ?x ?y :in $ % :where [?x :page/kind "leaf"] (or-join [[?x] ?y] (or-default-join [[?x] ?y] (follows ?x ?y) [(ground :end) ?y]) [(ground :none) ?y])]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			events := &ruleEvents{}
			opts := mode.plannerOptions()
			opts.ScopeWindowCap = 2
			opts.Handler = events.handle
			exec := NewExecutorWithOptions(matcher, nil, opts)
			inputs, err := createInputRelationsFromValuesWithOptions(q, []interface{}{datalog.SymDollar, datalog.NewSymbol("%")}, exec.options)
			require.NoError(t, err)
			rel, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, inputs)
			require.NoError(t, err)
			collected, err := CollectTuples(rel, nil)
			require.NoError(t, err)
			require.Len(t, collected, pages*(pages-1)/2+1+pages, "every reachable pair, the sink's end, and each page's none")
			require.Greater(t, events.count(annotations.ScopeWindow), 1, "the subject spans several windows")
			require.Equal(t, 2, events.count(annotations.RulesStratumBegin), "the magic stratum and the adorned stratum each begin once across the outer and the inner windows")
			require.Equal(t, 2, events.count(annotations.RulesStratumComplete), "each stratum completes once, with the outer execution")
			require.Equal(t, pages+pages*(pages-1)/2, events.entered(), "six magic facts and fifteen pairs, each entering a delta once")
		})
	}
}

// A form's result closed before its subject is exhausted completes the
// evaluations its execution continued.
func TestOrFallbackIteratorClosedBeforeExhaustionCompletesItsEvaluations(t *testing.T) {
	const pages = 6
	matcher := NewMemoryPatternMatcher(chainDatoms(pages))
	rs := mustRules(t, followsRulesText)
	form := mustParse(t, `[:find ?x ?y :in $ % :where (or-default-join [[?x] ?y] (follows ?x ?y) [(ground :end) ?y])]`).Where
	x := datalog.NewSymbol("?x")
	var subjects []Tuple
	for i := 1; i <= pages; i++ {
		subjects = append(subjects, Tuple{page(i)})
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			events := &ruleEvents{}
			opts := mode.plannerOptions()
			opts.ScopeWindowCap = 2
			opts.Handler = events.handle
			exec := NewExecutorWithOptions(matcher, nil, opts).queryExecutor()
			subject := NewStreamingRelation([]query.Symbol{x}, NewMaterializedRelation([]query.Symbol{x}, subjects).Iterator())
			groups, err := exec.Execute(NewContext().WithRules(rs), &query.Query{Where: form}, []Relation{subject})
			require.NoError(t, err)
			require.Len(t, groups, 1)
			it := groups[0].Iterator()
			require.True(t, it.Next(), "the first window answers a tuple")
			require.Equal(t, 2, events.count(annotations.RulesStratumBegin))
			require.Zero(t, events.count(annotations.RulesStratumComplete))
			require.NoError(t, it.Close())
			require.Equal(t, 2, events.count(annotations.RulesStratumComplete), "the close completes the strata")
		})
	}
}
