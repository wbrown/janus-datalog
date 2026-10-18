package storage

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The contracts of docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §7 that a
// query through the database pins, each the subject of one test.

// A branch sees the required run and nothing else of the enclosing scope: a
// branch variable sharing an outer variable's name is the branch's own, so
// the branch over :t/a holds for an entity whatever its :t/n, and the
// entity's :t/n is answered beside it.
func TestOrJoinBranchSeesOnlyItsHeader(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(2)}},
		"e2": {{":t/n", int64(2)}, {":t/a", int64(1)}},
		"e3": {{":t/n", int64(3)}},
	}
	clauses := []string{`[?e :t/n ?x]`, `(or-join [?e] [?e :t/a ?x] [?e :t/b ?x])`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?x", clauses, []string{"e1 1", "e2 2"})
		})
	}
}

// An invocation of a predicate with required positions inside a branch is
// bound by the branch's own clauses before it: the first branch binds ?n from
// the entity's :t/n and invokes succ on it, and the second answers the
// entity's :t/fixed.
func TestInvocationInABranchSeesTheBranchBindingsBeforeIt(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}},
		"e2": {{":t/n", int64(5)}, {":t/fixed", int64(9)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e ?m :in $ % :where [?e :t/n _] (or-join [[?e] ?m] (and [?e :t/n ?n] (succ ?n ?m)) [?e :t/fixed ?m])]`,
				mustRuleSet(t, `[[(succ [?n] ?m) [(+ ?n 1) ?m]]]`))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"e1 2", "e2 6", "e2 9"}, renderedRows(t, rel, names))
		})
	}
}

// An output the subject carries is joined with the branch's value, never
// superseded by it: an entity whose :t/a disagrees with its :t/q is dropped,
// and one with no branch value is dropped too.
func TestOrJoinOutputIsJoinedWithTheOuter(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/q", int64(1)}, {":t/a", int64(1)}},
		"e2": {{":t/q", int64(2)}, {":t/a", int64(3)}},
		"e3": {{":t/q", int64(4)}},
	}
	clauses := []string{`[?e :t/q ?q]`, `(or-join [?e ?q] [?e :t/a ?q] [?e :t/b ?q])`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?q", clauses, []string{"e1 1"})
		})
	}
}

// An output the environment binds is joined with the environment's tuple:
// with ?q given as 1, the entity whose :t/a is 1 is kept, and the one whose
// :t/a is 3 is dropped.
func TestOutputTheEnvironmentBindsIsJoinedWithItsTuple(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(1)}},
		"e2": {{":t/n", int64(2)}, {":t/a", int64(3)}},
		"e3": {{":t/n", int64(3)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e :in $ ?q :where [?e :t/n _] (or-join [?e ?q] [?e :t/a ?q] [?e :t/b ?q])]`, int64(1))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"e1"}, renderedRows(t, rel, names))
		})
	}
}

// A fallback branch providing a required variable the environment alone
// binds, without reading it, answers as the per-tuple path does: the run
// carries the input's value in every tuple, so the branch grounding ?z to 2
// meets the given ?z of 1 and keeps no tuple, and the default answers.
func TestFallbackBranchProvidingAnEnvironmentBoundRequiredVariableMeetsItsValue(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			rel, err := d.Query(`[:find ?v :in $ ?z :where (or-default-join [[?z] ?v] (and [(ground 2) ?z] [(ground 7) ?v]) [(ground 0) ?v])]`, int64(1))
			require.NoError(t, err)
			require.Equal(t, []string{"0"}, renderedRows(t, rel, nil))
		})
	}
}

// A union branch mentioning no header symbol under a header of required
// variables alone is a verdict: built once over the unit relation, its body
// having a match answers every tuple of the required run, and none answers
// none.
func TestUnionBranchReadingNothingUnderARequiredOnlyHeaderIsAVerdict(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(1)}},
		"e2": {{":t/n", int64(2)}},
		"e3": {{":t/b", int64(3)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e", []string{`[?e :t/n _]`, `(or-join [[?e]] [?e :t/a _] [?m :t/b _])`}, []string{"e1", "e2"})
			everyOrder(t, d, names, "?e", []string{`[?e :t/n _]`, `(or-join [[?e]] [?e :t/a _] [?m :t/c _])`}, []string{"e1"})
		})
	}
}

// An or-default-join decides its fallback per tuple of the required run: an
// entity with :t/a answers it, and one without takes the default.
func TestOrDefaultJoinFallsBackPerRequiredGroup(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(10)}},
		"e2": {{":t/n", int64(2)}},
		"e3": {{":t/n", int64(3)}, {":t/a", int64(30)}},
	}
	clauses := []string{`[?e :t/n _]`, `(or-default-join [[?e] ?v] [?e :t/a ?v] [(ground 0) ?v])`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?v", clauses, []string{"e1 10", "e2 0", "e3 30"})
		})
	}
}

// A not-join anti-joins the subject with its body's result on the header: on
// the entity alone, every entity carrying :t/a is dropped; on the entity and
// its :t/n, only the entity whose :t/a equals its :t/n is.
func TestNotJoinAntiJoinsOnItsHeader(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(1)}},
		"e2": {{":t/n", int64(2)}},
		"e3": {{":t/n", int64(3)}, {":t/a", int64(4)}},
	}
	headers := []struct {
		form string
		want []string
	}{
		{`(not-join [?e] [?e :t/a _])`, []string{"e2"}},
		{`(not-join [?e ?x] [?e :t/a ?x])`, []string{"e2", "e3"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			for _, h := range headers {
				everyOrder(t, d, names, "?e", []string{`[?e :t/n ?x]`, h.form}, h.want)
			}
		})
	}
}

// A scope's plan is served from the plan cache: a form's second execution
// plans no branch anew, and each branch's plan is a hit.
func TestScopePlanIsServedFromTheCache(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(1)}},
		"e2": {{":t/n", int64(2)}, {":t/b", int64(2)}},
	}
	const text = `[:find ?e :where [?e :t/n _] (or-join [?e] [?e :t/a _] [?e :t/b _])]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			require.NotNil(t, d.PlanCache(), "the database plans through a cache")
			rel, err := d.Query(text)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"e1", "e2"}, renderedRows(t, rel, names))
			hitsBefore, missesBefore, _ := d.PlanCache().Stats()

			rel, err = d.Query(text)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"e1", "e2"}, renderedRows(t, rel, names))
			hitsAfter, missesAfter, _ := d.PlanCache().Stats()
			require.Equal(t, missesBefore, missesAfter, "the second execution plans nothing anew")
			require.GreaterOrEqual(t, hitsAfter-hitsBefore, int64(2), "each branch's plan is a hit on the second execution; hits %d before, %d after", hitsBefore, hitsAfter)
		})
	}
}

// A scope inside a rule body reads the stratum's relations: a rule whose
// body binds its node and then holds an or-join one branch of which invokes
// the rule's own predicate derives the closure of the chain.
func TestScopeInsideABodySeesTheStratum(t *testing.T) {
	const rules = `[[(reach ?x ?z) [?x :type :node] (or-join [[?x] ?z] [?x :edge ?z] (and [?x :edge ?y] (reach ?y ?z)))]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, chain(4))
			rel, err := d.Query(`[:find ?x ?z :in $ % :where (reach ?x ?z)]`, mustRuleSet(t, rules))
			require.NoError(t, err)
			require.ElementsMatch(t, chainClosure(4), g.tuples(t, rel))
		})
	}
}

// The fallback decision for a run tuple reads the branch's result before the
// filter on the carried output: an entity whose :t/want disagrees with its
// :t/a is answered by the first branch and dropped, never passed to the
// default; one with no :t/a takes the default, kept when it equals its :t/want.
func TestFallbackDecisionReadsTheBranchBeforeTheCarriedOutputFilter(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/want", int64(0)}, {":t/a", int64(5)}},
		"e2": {{":t/n", int64(2)}, {":t/want", int64(0)}},
		"e3": {{":t/n", int64(3)}, {":t/want", int64(7)}, {":t/a", int64(7)}},
	}
	clauses := []string{`[?e :t/n _]`, `[?e :t/want ?v]`, `(or-default-join [[?e] ?v] [?e :t/a ?v] [(ground 0) ?v])`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?v", clauses, []string{"e2 0", "e3 7"})
		})
	}
}

// windowCapNames names the window caps from one tuple to the subject's size
// for the subjects of four tuples the window tests read, indexed by the cap.
var windowCapNames = [...]string{"", "cap of one tuple", "cap of two", "cap of three", "cap of the subject's size"}

// Every window size answers the same tuples: under caps from one tuple to the
// subject's size, the fallback whose keyed branch holds an invocation answers
// each node's reachable nodes and the sink's dead end, and the union of that
// branch with a ground answers the reachable nodes and every node's :self,
// the union taking every branch's answer. A later fallback branch never runs
// for a key an earlier branch answered: with a ground first, which reads
// nothing and answers every key, the branch holding the invocation is built
// for no window and no stratum begins.
func TestEveryWindowSizeAnswersTheSameTuples(t *testing.T) {
	const nodes = 4
	underFallback := append(chainClosure(nodes), nodeName(nodes)+">:dead-end")
	underUnion := chainClosure(nodes)
	var everyNodeOnce []string
	for i := 1; i <= nodes; i++ {
		underUnion = append(underUnion, nodeName(i)+">:self")
		everyNodeOnce = append(everyNodeOnce, nodeName(i)+">1")
	}
	forms := []struct {
		name, text string
		want       []string
	}{
		{"fallback", `[:find ?n ?m :in $ % :where [?n :type :node] (or-default-join [[?n] ?m] (reach ?n ?m) [(ground :dead-end) ?m])]`, underFallback},
		{"union", `[:find ?n ?m :in $ % :where [?n :type :node] (or-join [[?n] ?m] (reach ?n ?m) [(ground :self) ?m])]`, underUnion},
	}
	for _, mode := range optimizerModes {
		for windowCap := 1; windowCap <= nodes; windowCap++ {
			for _, form := range forms {
				t.Run(mode.name+"/"+form.name+"/"+windowCapNames[windowCap], func(t *testing.T) {
					opts := mode.plannerOptions()
					opts.ScopeWindowCap = windowCap
					d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts})
					g := writeGraph(t, d, chain(nodes))
					rel, err := d.Query(form.text, mustRuleSet(t, closureRules))
					require.NoError(t, err)
					require.ElementsMatch(t, form.want, g.tuples(t, rel))
				})
			}
			t.Run(mode.name+"/ground first/"+windowCapNames[windowCap], func(t *testing.T) {
				var invocationBuilds, strata atomic.Int64
				opts := mode.plannerOptions()
				opts.ScopeWindowCap = windowCap
				d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts, AnnotationHandler: func(e annotations.Event) {
					switch e.Name {
					case annotations.OrFallbackCacheBuild:
						if e.Data["branch"].(int) == 1 {
							invocationBuilds.Add(1)
						}
					case annotations.RulesStratumBegin:
						strata.Add(1)
					}
				}})
				g := writeGraph(t, d, chain(nodes))
				rel, err := d.Query(`[:find ?x ?k :in $ % :where [?x :type :node] (or-default-join [[?x] ?k] [(ground 1) ?k] (and (reach ?x ?z) [(ground 2) ?k]))]`, mustRuleSet(t, closureRules))
				require.NoError(t, err)
				require.ElementsMatch(t, everyNodeOnce, g.tuples(t, rel))
				require.Zero(t, invocationBuilds.Load(), "the branch holding the invocation is built for no window, the ground before it having answered every key")
				require.Zero(t, strata.Load(), "no stratum begins, the invocation never evaluated")
			})
		}
	}
}

// A branch is evaluated once per window and probed per tuple of the window:
// the branch holding the invocation is built once per window of the subject,
// the ground, reading nothing, is built once over the unit relation, and the
// subject's tuples are reported once each. The chain of four is read in more
// than one window under the default cap, so a build per window is told from
// a build once.
func TestBranchIsEvaluatedOncePerWindowAndProbedPerTuple(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var invocationBuilds, groundBuilds, outerTuples, windows atomic.Int64
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: func(e annotations.Event) {
				switch e.Name {
				case annotations.OrFallbackCacheBuild:
					switch e.Data["branch"].(int) {
					case 0:
						invocationBuilds.Add(1)
					case 1:
						groundBuilds.Add(1)
					}
				case annotations.OrFallbackOuterTuple:
					outerTuples.Add(1)
				case annotations.OrFallbackWindow:
					windows.Add(1)
				}
			}})
			g := writeGraph(t, d, chain(4))
			rel, err := d.Query(`[:find ?n ?m :in $ % :where [?n :type :node] (or-default-join [[?n] ?m] (reach ?n ?m) [(ground :dead-end) ?m])]`, mustRuleSet(t, closureRules))
			require.NoError(t, err)
			require.ElementsMatch(t, append(chainClosure(4), "n4>:dead-end"), g.tuples(t, rel))
			require.Greater(t, windows.Load(), int64(1), "the subject is read in more than one window")
			require.Equal(t, windows.Load(), invocationBuilds.Load(), "the branch holding the invocation is built once per window")
			require.Equal(t, int64(1), groundBuilds.Load(), "the ground, reading nothing, is built once over the unit relation")
			require.Equal(t, int64(4), outerTuples.Load(), "the built branches are probed once per subject tuple")
		})
	}
}

// The outer-tuple event fires once per subject tuple whatever the window: an
// or-join of data-pattern branches over four entities reports four subject
// tuples under each cap, and each branch is built once per window, never
// per tuple.
func TestOuterTupleEventFiresPerSubjectTuple(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(1)}},
		"e2": {{":t/n", int64(2)}, {":t/b", int64(2)}},
		"e3": {{":t/n", int64(3)}},
		"e4": {{":t/n", int64(4)}, {":t/a", int64(4)}},
	}
	for _, mode := range optimizerModes {
		for windowCap := 1; windowCap <= len(entities); windowCap++ {
			t.Run(mode.name+"/"+windowCapNames[windowCap], func(t *testing.T) {
				var outerTuples, cacheBuilds, windows atomic.Int64
				opts := mode.plannerOptions()
				opts.ScopeWindowCap = windowCap
				d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts, AnnotationHandler: func(e annotations.Event) {
					switch e.Name {
					case annotations.OrFallbackOuterTuple:
						outerTuples.Add(1)
					case annotations.OrFallbackCacheBuild:
						cacheBuilds.Add(1)
					case annotations.OrFallbackWindow:
						windows.Add(1)
					}
				}})
				names := namedEntities(t, d, entities)
				rel, err := d.Query(`[:find ?e :where [?e :t/n _] (or-join [?e] [?e :t/a _] [?e :t/b _])]`)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"e1", "e2", "e4"}, renderedRows(t, rel, names))
				require.Equal(t, int64(4), outerTuples.Load(), "the subject's tuples are reported once each")
				require.Equal(t, 2*windows.Load(), cacheBuilds.Load(), "each of the two branches is built once per window")
			})
		}
	}
}

// A written flat or-join keeps its subject's keys through branches that
// answer at most one tuple per subject tuple, replaces the groups it
// consumed, and seeks on the subject's values.
func TestFlatOrJoinKeepsItsSubjectsPropertiesAndReplacesItsGroups(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(1)}},
		"e2": {{":t/n", int64(2)}, {":t/b", int64(2)}},
		"e3": {{":t/n", int64(3)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var replaced atomic.Int64
			var events []annotations.Event
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: func(e annotations.Event) {
				if e.Name == annotations.OrOuterReplaced {
					replaced.Add(1)
				}
				events = append(events, e)
			}})
			names := namedEntities(t, d, entities)
			subject, err := d.Query(`[:find ?e ?v :where [?e :t/n ?v]]`)
			require.NoError(t, err)
			subjectKeys := subject.Properties().Keys
			require.NotEmpty(t, subjectKeys, "the subject carries a key for the contract to be testable")

			rel, err := d.Query(`[:find ?e ?v :where [?e :t/n ?v] (or-join [?e] [?e :t/a _] [?e :t/b _])]`)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"e1 1", "e2 2"}, renderedRows(t, rel, names))
			for _, key := range subjectKeys {
				require.True(t, hasKey(rel.Properties().Keys, key), "the result keeps the subject's key %v; keys=%v", key, rel.Properties().Keys)
			}
			require.GreaterOrEqual(t, replaced.Load(), int64(1), "the form's result replaces the consumed groups")
			// A branch that seeks on the subject's entity reaches no
			// attribute-primary index selection; a scan of the attribute's
			// whole extent selects one.
			attrPrimary := map[string]bool{"AETV": true, "AEVT": true, "AVET": true, "ATEV": true}
			for _, attr := range []string{":t/a", ":t/b"} {
				index, bound := indexForAttrPattern(events, attr)
				require.False(t, attrPrimary[index], "the branch over %s seeks on the subject's values; index=%s bound=%s", attr, index, bound)
			}
		})
	}
}

// hasKey reports whether keys holds a key of the same symbols as key.
func hasKey(keys [][]query.Symbol, key []query.Symbol) bool {
	for _, candidate := range keys {
		if len(candidate) != len(key) {
			continue
		}
		same := true
		for _, sym := range key {
			if !query.ContainsSymbol(candidate, sym) {
				same = false
			}
		}
		if same {
			return true
		}
	}
	return false
}

// A flat-header or-default-join decides its fallback once: when the first
// branch answers for any tuple, the default fires for none, and every entity
// is paired with the first branch's values.
func TestFlatHeaderOrDefaultJoinDecidesOnce(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(5)}},
		"e2": {{":t/n", int64(2)}},
	}
	clauses := []string{`[?e :t/n _]`, `(or-default-join [?v] [?x :t/a ?v] [(ground 0) ?v])`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?v", clauses, []string{"e1 5", "e2 5"})
		})
	}
}

// A :where of one form whose required run nothing binds is refused on both
// planner paths, naming the form and the variable.
func TestOneFormWhereWithNothingBindingItsRequiredRunIsRefused(t *testing.T) {
	forms := []struct{ head, text string }{
		{"(or-join", `[:find ?e :where (or-join [[?x] ?e] [?e :t/a ?x] [?e :t/b ?x])]`},
		{"(or-default-join", `[:find ?e :where (or-default-join [[?x] ?e] [?e :t/a ?x] [(ground :none) ?e])]`},
		{"(not-join", `[:find ?x :where (not-join [?x] [?x :t/a _])]`},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			for _, form := range forms {
				_, err := executor.CollectTuples(d.Query(form.text))
				require.Error(t, err, form.text)
				require.Contains(t, err.Error(), form.head, form.text)
				require.Contains(t, err.Error(), "?x", form.text)
			}
		})
	}
}

// With EnableEntityPrefetch off no prefetch runs: a query of two patterns
// over the same entity triggers no prefetch and populates no cache entry on
// anyone's behalf.
func TestEntityPrefetchOffRunsNoPrefetch(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(1)}},
		"e2": {{":t/n", int64(2)}, {":t/a", int64(2)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var triggers, populated atomic.Int64
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: func(e annotations.Event) {
				switch e.Name {
				case annotations.PrefetchTrigger:
					triggers.Add(1)
				case annotations.StorageResolveComplete:
					if _, ok := e.Data[annotations.KeyEntriesPopulated]; ok {
						populated.Add(1)
					}
				}
			}})
			require.False(t, mode.plannerOptions().EnableEntityPrefetch, "the option is off by default")
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e ?v :where [?e :t/n _] [?e :t/a ?v]]`)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"e1 1", "e2 2"}, renderedRows(t, rel, names))
			require.Zero(t, triggers.Load(), "no prefetch triggers with the option off")
			require.Zero(t, populated.Load(), "no read populates the cache on another's behalf with the option off")
		})
	}
}

// A subquery reads a form's result once: the planner orders a subquery after
// the form that provides its input, the form's result stands in its
// streaming subject's place, and the subquery's binding enters the scope as
// a relation over its symbols, joined on them. Each entity carrying :t/p is
// answered beside its count of :t/s referrers, in every written order on
// both planner paths, and the entity with no referrer takes no tuple, since a
// count over nothing binds none.
func TestSubqueryAfterAFormAnswersOverItsStreamingResult(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/p", ref("e4")}},
		"e2": {{":t/p", ref("e5")}},
		"e3": {{":t/p", ref("e4")}},
		"r1": {{":t/s", ref("e1")}},
		"r2": {{":t/s", ref("e2")}},
		"r3": {{":t/s", ref("e2")}},
	}
	clauses := []string{
		`[?e :t/p ?y]`,
		`[(q [:find (count ?s) :in $ ?e :where [?s :t/s ?e]] $ ?e) [[?n] ...]]`,
		`(or-join [[?y] ?e] [?e :t/p ?m] [?y :t/q ?e])`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?n", clauses, []string{"e1 1", "e2 2"})
		})
	}
}

// A subquery whose inner query holds an invocation is built once over the
// run: the invocation seeds one rewrite and one fixpoint from every node at
// once, whatever the number of nodes, and each node is answered beside the
// count of nodes it reaches, grouped by the input; the sink, reaching none,
// takes no tuple.
func TestSubqueryHoldingAnInvocationIsBuiltOnceOverTheRun(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			counts := &ruleEventCounts{}
			var cacheBuilds atomic.Int64
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: func(e annotations.Event) {
				counts.handle(e)
				if e.Name == annotations.OrFallbackCacheBuild {
					cacheBuilds.Add(1)
				}
			}})
			g := writeGraph(t, d, chain(5))
			rel, err := d.Query(`[:find ?n ?k :in $ % :where [?n :type :node] [(q [:find (count ?m) :in $ % ?n :where (reach ?n ?m)] $ % ?n) [[?k] ...]]]`, mustRuleSet(t, closureRules))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"n1>4", "n2>3", "n3>2", "n4>1"}, g.tuples(t, rel))
			require.Equal(t, 1, counts.snapshot()[0], "the invocation inside the inner query rewrites once, seeded from every node; the events:\n%s", counts.events())
			require.Equal(t, int64(1), cacheBuilds.Load(), "the inner query holding the invocation is built once over the run")
		})
	}
}

// A subquery's inner query is ordered by the planner, never run in its
// written order: an expression written before the pattern that binds its
// input is evaluated after it, and each entity is answered beside its :t/n
// plus one.
func TestSubqueryBodyIsOrderedByThePlanner(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}},
		"e2": {{":t/n", int64(5)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e ?v :where [?e :t/n _] [(q [:find ?v :in $ ?e :where [(+ ?n 1) ?v] [?e :t/n ?n]] $ ?e) [[?v] ...]]]`)
			require.NoError(t, err)
			require.Equal(t, []string{"e1 2", "e2 6"}, renderedRows(t, rel, names))
		})
	}
}

// A subquery over its combination relation gives the per-combination
// tuples: two entities sharing a :t/g each take the inner tuples of that
// group, and an entity whose group has none takes none.
func TestSubqueryOverItsCombinationsGivesThePerCombinationTuples(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/g", int64(1)}},
		"e2": {{":t/g", int64(1)}},
		"e3": {{":t/g", int64(2)}},
		"m1": {{":t/in", int64(1)}, {":t/v", "a"}},
		"m2": {{":t/in", int64(1)}, {":t/v", "b"}},
	}
	clauses := []string{`[?e :t/g ?g]`, `[(q [:find ?v :in $ ?g :where [?m :t/in ?g] [?m :t/v ?v]] $ ?g) [[?v] ...]]`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?v", clauses, []string{"e1 a", "e1 b", "e2 a", "e2 b"})
		})
	}
}

// A pure aggregate, whose inner query reads no input, over a body matching
// nothing binds no tuple: with no :t/absent to count, no entity is answered.
func TestPureAggregateOverACombinationMatchingNothingBindsNoTuple(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}},
		"e2": {{":t/n", int64(2)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e ?k :where [?e :t/n _] [(q [:find (count ?s) :in $ ?e :where [?s :t/absent _]] $ ?e) [[?k] ...]]]`)
			require.NoError(t, err)
			require.Empty(t, renderedRows(t, rel, names), "a count over nothing binds no tuple")
		})
	}
}

// An aggregate whose body does not read the input is a pure aggregate,
// aggregated once and paired with every run tuple: each entity is answered
// beside the count of every :t/s in the store.
func TestAggregateNotReadingTheInputIsPairedWithEveryRunTuple(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}},
		"e2": {{":t/n", int64(2)}},
		"s1": {{":t/s", int64(1)}},
		"s2": {{":t/s", int64(2)}},
		"s3": {{":t/s", int64(3)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e ?k :where [?e :t/n _] [(q [:find (count ?s) :in $ ?e :where [?s :t/s _]] $ ?e) [[?k] ...]]]`)
			require.NoError(t, err)
			require.Equal(t, []string{"e1 3", "e2 3"}, renderedRows(t, rel, names))
		})
	}
}

// A branch reading a required variable after an uncorrelated subquery runs
// per tuple, never as a constant: the predicate on the subquery's result
// reads the required ?e, so the branch is answered per entity, and an
// entity with a :t/q referrer or a :t/flag is kept, in every written order
// on both planner paths.
func TestBranchReadingARequiredVariableAfterAnUncorrelatedSubqueryRunsPerTuple(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}},
		"e2": {{":t/n", int64(2)}, {":t/flag", true}},
		"e3": {{":t/n", int64(3)}},
		"r1": {{":t/q", ref("e1")}},
	}
	clauses := []string{
		`[?e :t/n _]`,
		`(or-join [[?e]] (and [(q [:find ?y (count ?s) :in $ :where [?s :t/q ?y]] $) [[?y ?c] ...]] [(= ?y ?e)]) [?e :t/flag true])`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e", clauses, []string{"e1", "e2"})
		})
	}
}

// A collection input of a subquery yields one run tuple per member: an
// entity's ordered set, passed to an inner query declaring a collection
// input, pairs the entity with the holder of each member and no other.
func TestCollectionInputYieldsOneRunTuplePerMember(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/ordered", "a"}, {":t/ordered", "b"}},
		"h1": {{":t/string", "a"}},
		"h2": {{":t/string", "b"}},
		"h3": {{":t/string", "c"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: schemaFactsFixture(t)})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e ?h :where [?e :t/ordered ?members] [(q [:find ?h :in $ [?m ...] :where [?h :t/string ?m]] $ ?members) [[?h] ...]]]`)
			require.NoError(t, err)
			require.Equal(t, []string{"e1 h1", "e1 h2"}, renderedRows(t, rel, names))
		})
	}
}

// A union branch is planned over the required run alone before the carried
// outputs are bound into it: a branch no order runs over the required run
// is the form's refusal on both planner paths, naming the branch counted
// from one, never an answer over the carried output. The cycle of two
// identities under a header binding none of it reads nothing of what it is
// given and is built once over the required run. The same cycle beside a
// read of a required variable runs per tuple, where the seek would bind the
// carried output and close the cycle.
func TestBranchNoOrderRunsUnderTheRequiredRunIsRefused(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/p", ref("e2")}},
		"e2": {{":t/n", int64(2)}},
	}
	queries := []struct {
		name  string
		query string
	}{
		{"a branch reading nothing", `[:find ?e :where [?e :t/n _] (or-join [?e] (and [(identity ?m) ?e] [(identity ?e) ?m]) [?e :t/p ?m])]`},
		{"a branch run per tuple", `[:find ?e :where [?e :t/n ?r] (or-join [[?r] ?e] (and [(identity ?m) ?e] [(identity ?e) ?m] [(> ?r 0)]) [?e :t/p ?m])]`},
		{"the headerless spelling", `[:find ?e :where [?e :t/n ?r] (or (and [(identity ?m) ?e] [(identity ?e) ?m] [(> ?r 0)]) [?e :t/p ?m])]`},
	}
	for _, mode := range optimizerModes {
		for _, q := range queries {
			t.Run(mode.name+"/"+q.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				namedEntities(t, d, entities)
				_, err := executor.CollectTuples(d.Query(q.query))
				require.Error(t, err, "no order runs the first branch over the required run alone")
				require.Contains(t, err.Error(), "branch 1")
			})
		}
	}
}
