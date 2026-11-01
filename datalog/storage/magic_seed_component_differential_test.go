package storage

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// seedComponentRules holds, beside the closure, one predicate per shape a
// bound invocation reaches a clause outside its seed component through: a
// rule body, an or-join branch, an or branch, a not, a not-join, an
// or-default fallback, an aggregating and a non-aggregating subquery, a
// required position bound from an extent chain at the body and inside a
// branch, two invocations, a mutually recursive pair, a recursive body
// holding a value constant, an entity constant at the body and inside a
// branch, a value constant joined into the seed component, and a ground
// value. Each predicate with a required position has a twin whose positions
// are all free, its reference.
const seedComponentRules = `[[(reach ?a ?b) [?a :edge ?b]]
  [(reach ?a ?c) [?a :edge ?b] (reach ?b ?c)]
  [(leafward ?a ?x) [?x :kind "leaf"] (reach ?a ?x)]
  [(hubward ?a ?x) [#id "magic/hub" :edge ?x] (reach ?a ?x)]
  [(kindward ?a ?k ?x) [?x :kind ?k] (reach ?a ?x)]
  [(orleaf ?a ?x) [?a :type :node] (or-join [?a ?x] (and [?x :kind "leaf"] (reach ?a ?x)) (and [?x :kind "root"] [?x :edge ?a]))]
  [(orany ?a ?x) [?a :type :node] [?x :kind "leaf"] (or (reach ?a ?x) [?x :alt ?a])]
  [(unreached ?a ?x) [?a :type :node] [?x :kind "leaf"] (not (reach ?a ?x))]
  [(unreachednode ?a ?x) [?a :type :node] [?x :type :node] (not (reach ?a ?x))]
  [(unreachedj ?a ?x) [?a :type :node] [?x :type :node] (not-join [?a ?x] [?x :kind "leaf"] (reach ?a ?x))]
  [(fallback ?a ?x ?k) [?a :type :node] [?x :kind "leaf"] (or-default (and (reach ?a ?x) [(ground 1) ?k]) [(ground 0) ?k])]
  [(between ?a ?x ?n) [?a :type :node] [?x :kind "leaf"] [(q [:find (count ?y) :in $ % ?a ?x :where (reach ?a ?y) (reach ?y ?x)] $ % ?a ?x) [[?n]]]]
  [(reached ?a ?x) [?a :type :node] [?x :kind "leaf"] [(q [:find ?w :in $ % ?a :where (reach ?a ?w)] $ % ?a) [[?x] ...]]]
  [(down [?p] ?q) [?p :edge ?q]]
  [(down [?p] ?q) [?p :edge ?m] (down ?m ?q)]
  [(downfree ?p ?q) [?p :edge ?q]]
  [(downfree ?p ?q) [?p :edge ?m] (downfree ?m ?q)]
  [(fromroots ?a ?q) [?a :type :node] [?r :kind "root"] (down ?r ?q) (reach ?a ?q)]
  [(fromrootsfree ?a ?q) [?a :type :node] [?r :kind "root"] (downfree ?r ?q) (reach ?a ?q)]
  [(branchroots ?a ?q) [?a :type :node] [?r :kind "root"] (or-join [[?r] ?q] (and [?r :edge ?m] (down ?m ?q)) [?r :alt ?q]) (reach ?a ?q)]
  [(branchrootsfree ?a ?q) [?a :type :node] [?r :kind "root"] (or-join [[?r] ?q] (and [?r :edge ?m] (downfree ?m ?q)) [?r :alt ?q]) (reach ?a ?q)]
  [(both ?a ?b ?x) [?x :kind "leaf"] (reach ?a ?x) (reach ?b ?x)]
  [(odd ?a ?b) [?a :edge ?b]]
  [(even ?a ?c) (odd ?a ?b) [?b :edge ?c]]
  [(odd ?a ?c) (even ?a ?b) [?b :edge ?c]]
  [(oddleaf ?a ?x) [?x :kind "leaf"] (odd ?a ?x)]
  [(leafpath ?a ?x) [?a :edge ?x] [?x :kind "leaf"]]
  [(leafpath ?a ?x) [?x :kind "leaf"] [?a :edge ?b] (leafpath ?b ?x)]
  [(hubbranch ?a ?x) [?a :type :node] (or-join [?a ?x] (and [#id "magic/hub" :edge ?x] (reach ?a ?x)) (and [?x :kind "root"] [?x :edge ?a]))]
  [(midward ?a ?x) [?m :kind "mid"] [?a :edge ?m] (reach ?m ?x)]
  [(grounded ?a ?x) [(ground "n4") ?s] [?x :name ?s] (reach ?a ?x)]]`

// A bound invocation derives exactly what the free evaluation of the same
// predicate derives, restricted to the binding (REWRITE_ORDER_PROOF.md T1,
// A_CLAUSE_OUTSIDE_THE_SEED_COMPONENT_BINDS_NOTHING_PROOF.md T1), for every
// shape seedComponentRules holds and for the top-level shapes beside it, at
// every binding the fixture admits. The reference is a top-level invocation
// with no clause before it and no bound argument, which derives its
// predicate whole.
func TestRules_ABoundInvocationBesideAClauseOutsideTheSeedComponentDerivesWhatTheFreeEvaluationDerives(t *testing.T) {
	hub := datalog.NewIdentity("magic/hub")
	r1 := datalog.NewIdentity("magic/r1")
	n := make([]datalog.Identity, 9)
	for i := 1; i <= 8; i++ {
		n[i] = datalog.NewIdentity(fmt.Sprintf("magic/n%d", i))
	}
	nodes := []datalog.Identity{hub, r1, n[1], n[2], n[3], n[4], n[5], n[6], n[7], n[8]}
	names := []string{"hub", "r1", "n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8"}
	type fact struct {
		e datalog.Identity
		a string
		v interface{}
	}
	var facts []fact
	for i, node := range nodes {
		facts = append(facts, fact{node, ":type", kw(":node")}, fact{node, ":name", names[i]})
	}
	facts = append(facts,
		fact{r1, ":edge", n[2]}, fact{r1, ":edge", n[7]},
		fact{n[1], ":edge", n[2]}, fact{n[2], ":edge", n[3]}, fact{n[3], ":edge", n[4]},
		fact{n[4], ":edge", n[2]}, fact{n[2], ":edge", n[5]}, fact{n[5], ":edge", n[6]},
		fact{hub, ":edge", n[3]}, fact{hub, ":edge", n[6]}, fact{n[7], ":edge", n[8]},
		fact{n[4], ":kind", "leaf"}, fact{n[6], ":kind", "leaf"}, fact{n[8], ":kind", "leaf"},
		fact{r1, ":kind", "root"}, fact{n[5], ":kind", "mid"},
		fact{r1, ":alt", n[8]},
	)

	single := make([][]interface{}, len(nodes))
	for i, node := range nodes {
		single[i] = []interface{}{node}
	}
	var withKind, pairs [][]interface{}
	for _, node := range nodes {
		for _, k := range []string{"leaf", "mid", "root"} {
			withKind = append(withKind, []interface{}{node, k})
		}
		for _, other := range nodes {
			pairs = append(pairs, []interface{}{node, other})
		}
	}

	// Each case's bound query takes one tuple of bindings as its inputs;
	// its reference finds the bound symbols first, in input order, then the
	// bound query's find symbols in their order. A case with a constant in
	// its bound query has one binding, the constant, and no inputs.
	cases := []struct {
		name, bound, reference string
		inputs                 bool
		bindings               [][]interface{}
	}{
		{"a rule body beside a value constant", `[:find ?x :in $ % ?a :where (leafward ?a ?x)]`, `(leafward ?a ?x)`, true, single},
		{"a rule body beside an entity constant", `[:find ?x :in $ % ?a :where (hubward ?a ?x)]`, `(hubward ?a ?x)`, true, single},
		{"a rule body beside a pattern on a bound value", `[:find ?x :in $ % ?a ?k :where (kindward ?a ?k ?x)]`, `(kindward ?a ?k ?x)`, true, withKind},
		{"an or-join branch", `[:find ?x :in $ % ?a :where (orleaf ?a ?x)]`, `(orleaf ?a ?x)`, true, single},
		{"an or branch", `[:find ?x :in $ % ?a :where (orany ?a ?x)]`, `(orany ?a ?x)`, true, single},
		{"a not", `[:find ?x :in $ % ?a :where (unreached ?a ?x)]`, `(unreached ?a ?x)`, true, single},
		{"a not over a scan", `[:find ?x :in $ % ?a :where (unreachednode ?a ?x)]`, `(unreachednode ?a ?x)`, true, single},
		{"a not-join", `[:find ?x :in $ % ?a :where (unreachedj ?a ?x)]`, `(unreachedj ?a ?x)`, true, single},
		{"an or-default fallback branch", `[:find ?x ?k :in $ % ?a :where (fallback ?a ?x ?k)]`, `(fallback ?a ?x ?k)`, true, single},
		{"an aggregating subquery given an extent argument", `[:find ?x ?c :in $ % ?a :where (between ?a ?x ?c)]`, `(between ?a ?x ?c)`, true, single},
		{"a non-aggregating subquery", `[:find ?x :in $ % ?a :where (reached ?a ?x)]`, `(reached ?a ?x)`, true, single},
		{"a required position bound from an extent chain", `[:find ?q :in $ % ?a :where (fromroots ?a ?q)]`, `(fromrootsfree ?a ?q)`, true, single},
		{"a required position inside a branch given an extent symbol", `[:find ?q :in $ % ?a :where (branchroots ?a ?q)]`, `(branchrootsfree ?a ?q)`, true, single},
		{"two invocations", `[:find ?x :in $ % ?a ?b :where (both ?a ?b ?x)]`, `(both ?a ?b ?x)`, true, pairs},
		{"mutually recursive predicates", `[:find ?x :in $ % ?a :where (oddleaf ?a ?x)]`, `(oddleaf ?a ?x)`, true, single},
		{"a recursive body holding a value constant", `[:find ?x :in $ % ?a :where (leafpath ?a ?x)]`, `(leafpath ?a ?x)`, true, single},
		{"an entity constant inside a branch", `[:find ?x :in $ % ?a :where (hubbranch ?a ?x)]`, `(hubbranch ?a ?x)`, true, single},
		{"a value constant joined into the seed component", `[:find ?x :in $ % ?a :where (midward ?a ?x)]`, `(midward ?a ?x)`, true, single},
		{"a ground value", `[:find ?x :in $ % ?a :where (grounded ?a ?x)]`, `(grounded ?a ?x)`, true, single},
		{"the top level beside a value constant", `[:find ?x :in $ % ?a :where [?x :kind "leaf"] (reach ?a ?x)]`, `(leafward ?a ?x)`, true, single},
		{"the top level under a not over a scan", `[:find ?x :in $ % ?a :where [?a :type :node] [?x :type :node] (not (reach ?a ?x))]`, `(unreachednode ?a ?x)`, true, single},
		{"the top level under an or-default", `[:find ?x ?k :in $ % ?a :where [?a :type :node] [?x :kind "leaf"] (or-default (and (reach ?a ?x) [(ground 1) ?k]) [(ground 0) ?k])]`, `(fallback ?a ?x ?k)`, true, single},
		{"the top level under an aggregating subquery", `[:find ?x ?c :in $ % ?a :where [?a :type :node] [?x :kind "leaf"] [(q [:find (count ?y) :in $ % ?a ?x :where (reach ?a ?y) (reach ?y ?x)] $ % ?a ?x) [[?c]]]]`, `(between ?a ?x ?c)`, true, single},
		{"the top level bound by an entity constant", `[:find ?x :in $ % :where (reach #id "magic/n1" ?x)]`, `(reach ?a ?x)`, false, [][]interface{}{{n[1]}}},
		{"the top level bound by an entity constant beside a value constant", `[:find ?x :in $ % :where [?x :kind "leaf"] (reach #id "magic/r1" ?x)]`, `(leafward ?a ?x)`, false, [][]interface{}{{r1}}},
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			adorned := &rewriteAdornments{}
			var strata []string
			handle := func(e annotations.Event) {
				adorned.handle(e)
				if e.Name == annotations.RulesStratumBegin {
					strata = append(strata, fmt.Sprint(e.Data["predicates"]))
				}
			}
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: handle})
			tx := d.NewTransaction()
			for _, f := range facts {
				require.NoError(t, tx.Add(f.e, kw(f.a), f.v))
			}
			_, err := tx.Commit()
			require.NoError(t, err)
			rules := mustRuleSet(t, seedComponentRules)

			named, err := d.Query(`[:find ?e ?n :where [?e :name ?n]]`)
			require.NoError(t, err)
			display := map[datalog.Identity]string{}
			require.NoError(t, executor.ForEach(named, func(tuple executor.Tuple) error {
				display[tuple[0].(datalog.Identity)] = tuple[1].(string)
				return nil
			}))
			render := func(values []interface{}) string {
				parts := make([]string, len(values))
				for i, v := range values {
					if id, ok := v.(datalog.Identity); ok {
						parts[i] = display[id]
					} else {
						parts[i] = fmt.Sprint(v)
					}
				}
				return strings.Join(parts, ">")
			}

			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					k := len(c.bindings[0])
					refVars := referenceFind(c.reference, c.bound, k, c.inputs)
					refText := fmt.Sprintf(`[:find %s :in $ %% :where %s]`, strings.Join(refVars, " "), c.reference)
					before := len(adorned.made())
					ref, err := d.Query(refText, rules)
					require.NoError(t, err, "reference %s", refText)
					for _, made := range adorned.made()[before:] {
						for _, b := range made {
							require.False(t, b, "the reference %s binds no position of any rewrite it makes", refText)
						}
					}
					var whole [][]interface{}
					require.NoError(t, executor.ForEach(ref, func(tuple executor.Tuple) error {
						whole = append(whole, append([]interface{}(nil), tuple...))
						return nil
					}))

					derived := 0
					for b, binding := range c.bindings {
						strata = nil
						var want [][]interface{}
						for _, tuple := range whole {
							agrees := true
							for i := 0; i < k; i++ {
								if !datalog.ValuesEqual(tuple[i], binding[i]) {
									agrees = false
									break
								}
							}
							if agrees {
								want = append(want, tuple[k:])
							}
						}
						args := []interface{}{rules}
						if c.inputs {
							args = append(args, binding...)
						}
						rel, err := d.Query(c.bound, args...)
						require.NoError(t, err, "bound at %s", render(binding))
						var got [][]interface{}
						require.NoError(t, executor.ForEach(rel, func(tuple executor.Tuple) error {
							got = append(got, append([]interface{}(nil), tuple...))
							return nil
						}))
						derived += len(got)
						if b == 2 || len(c.bindings) == 1 {
							t.Logf("strata at %s: %v", render(binding), strata)
						}
						require.True(t, sameTuples(got, want), "bound at %s: got %v, the free evaluation restricted to the binding %v", render(binding), renderAll(got, render), renderAll(want, render))
					}
					t.Logf("%d tuples across %d bindings", derived, len(c.bindings))
				})
			}
		})
	}
}

// referenceFind is the reference's find: the bound query's :in symbols
// after $ and % when it takes inputs, or the invocation's first argument
// when a constant binds it, then the bound query's find symbols.
func referenceFind(reference, bound string, k int, inputs bool) []string {
	var vars []string
	if inputs {
		in := bound[strings.Index(bound, ":in $ %")+len(":in $ %") : strings.Index(bound, ":where")]
		vars = append(vars, strings.Fields(in)...)
	} else {
		args := strings.Fields(strings.Trim(reference, "()"))[1:]
		vars = append(vars, args[:k]...)
	}
	find := bound[len("[:find ") : strings.Index(bound, " :in")]
	return append(vars, strings.Fields(find)...)
}

// sameTuples reports whether two tuple lists hold the same set of tuples.
func sameTuples(a, b [][]interface{}) bool {
	held := func(tuples [][]interface{}, tuple []interface{}) bool {
		for _, other := range tuples {
			if len(other) != len(tuple) {
				continue
			}
			equal := true
			for i := range tuple {
				if !datalog.ValuesEqual(other[i], tuple[i]) {
					equal = false
					break
				}
			}
			if equal {
				return true
			}
		}
		return false
	}
	for _, tuple := range a {
		if !held(b, tuple) {
			return false
		}
	}
	for _, tuple := range b {
		if !held(a, tuple) {
			return false
		}
	}
	return true
}

// renderAll renders each tuple and sorts them.
func renderAll(tuples [][]interface{}, render func([]interface{}) string) []string {
	out := make([]string, len(tuples))
	for i, tuple := range tuples {
		out[i] = render(tuple)
	}
	sort.Strings(out)
	return out
}
