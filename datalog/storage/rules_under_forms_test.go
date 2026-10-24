package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A rule invoked inside a nested form reads the form's header and nothing
// else of the enclosing scope (docs/proposals/NESTED_SCOPES_AS_QUERIES.md
// §2.1, §2.2): a branch local is the branch's own whatever name it carries,
// an invocation's seed is what the branch binds for the header's run, and two
// invocations in one branch each keep their own seed.

// A local of a branch that shares an outer symbol's name is the branch's own,
// so renaming it to a name that occurs nowhere else in the query changes
// nothing: under not-join, or-join and or-default-join, the invocation's free
// position written as the outer ?y and as a fresh ?q answers the same tuples.
func TestRules_RenamingABranchLocalChangesNothing(t *testing.T) {
	g := coloredGraph{
		spec:   graphSpec{nodes: []string{"n1", "n2", "n3"}, edges: [][2]string{{"n1", "n2"}}},
		colors: map[string]int64{"n1": 0, "n2": 1, "n3": 0},
		vals:   map[string]int64{"n1": 1, "n2": 2, "n3": 3},
	}
	forms := []struct {
		name, shadowed, renamed string
		want                    []string
	}{
		{"not-join",
			`[:find ?x ?y :in $ % :where [?x :color ?c] [?y :color ?c] (not-join [?x] (r ?x ?y))]`,
			`[:find ?x ?y :in $ % :where [?x :color ?c] [?y :color ?c] (not-join [?x] (r ?x ?q))]`,
			[]string{"n2>n2", "n3>n1", "n3>n3"}},
		{"or-join",
			`[:find ?x ?y :in $ % :where [?x :color ?c] [?y :color ?c] (or-join [?x] (r ?x ?y) [?x :name "zz"])]`,
			`[:find ?x ?y :in $ % :where [?x :color ?c] [?y :color ?c] (or-join [?x] (r ?x ?q) [?x :name "zz"])]`,
			[]string{"n1>n1", "n1>n3"}},
		{"or-default-join",
			`[:find ?x ?y ?k :in $ % :where [?x :color ?c] [?y :color ?c] (or-default-join [[?x] ?k] (and (r ?x ?y) [(ground 1) ?k]) [(ground 0) ?k])]`,
			`[:find ?x ?y ?k :in $ % :where [?x :color ?c] [?y :color ?c] (or-default-join [[?x] ?k] (and (r ?x ?q) [(ground 1) ?k]) [(ground 0) ?k])]`,
			[]string{"n1>n1>1", "n1>n3>1", "n2>n2>0", "n3>n1>0", "n3>n3>0"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: coloredGraphSchema()})
			names := writeColoredGraph(t, d, g)
			rs := mustRuleSet(t, "["+rClosure+"]")
			for _, f := range forms {
				t.Run(f.name, func(t *testing.T) {
					shadowed, err := d.Query(f.shadowed, rs)
					require.NoError(t, err)
					require.ElementsMatch(t, f.want, names.tuples(t, shadowed), "the local written as the outer symbol's name")
					renamed, err := d.Query(f.renamed, rs)
					require.NoError(t, err)
					require.ElementsMatch(t, f.want, names.tuples(t, renamed), "the local renamed to a name occurring nowhere else")
				})
			}
		})
	}
}

// An invocation whose argument a clause of its own branch binds is seeded by
// that clause's values for the header's run, so it derives what those values
// reach and never the closure whole: on a chain of two hundred with the head
// as the required run, the branch's edge binds the second node, whose closure
// is 198 tuples, and the query answers under a bound of two thousand derived
// tuples with one rewrite for the bound position. A flat or-join header makes
// ?x an output the branch binds, so the branch sees nothing of the head, its
// edge pattern binds every node, and the invocation derives the closure whole:
// the same bound refuses it, and without the bound it answers the same tuples.
func TestRules_AnInvocationSeededByItsBranchDerivesWhatTheSeedReaches(t *testing.T) {
	const rules = `[[(r ?a ?b) [?a :edge ?b]] [(r ?a ?c) (r ?a ?b) [?b :edge ?c]]]`
	spec := chain(200)
	var reached []string
	for i := 3; i <= 200; i++ {
		reached = append(reached, "n1>"+nodeName(i))
	}
	forms := []struct {
		name, text string
		want       []string
	}{
		{"not",
			`[:find ?x :in $ % :where [?x :name "n1"] (not [?x :edge ?m] (r ?m ?y) [?y :name ?nm] [(= ?nm "zz")])]`,
			[]string{"n1"}},
		{"not-join",
			`[:find ?x :in $ % :where [?x :name "n1"] (not-join [?x] [?x :edge ?m] (r ?m ?y) [?y :name ?nm] [(= ?nm "zz")])]`,
			[]string{"n1"}},
		{"or-join with the head required",
			`[:find ?x ?y :in $ % :where [?x :name "n1"] (or-join [[?x] ?y] (and [?x :edge ?m] (r ?m ?y)) (and [?x :name "zz"] [(ground 0) ?y]))]`,
			reached},
	}
	const flat = `[:find ?x ?y :in $ % :where [?x :name "n1"] (or-join [?x ?y] (and [?x :edge ?m] (r ?m ?y)) (and [?x :name "zz"] [(ground 0) ?y]))]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			adorned := &rewriteAdornments{}
			opts := mode.plannerOptions()
			opts.MaxDerivedTuples = 2000
			d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts, AnnotationHandler: adorned.handle})
			g := writeGraph(t, d, spec)
			writeNames(t, d, spec)
			rs := mustRuleSet(t, rules)
			for _, f := range forms {
				t.Run(f.name, func(t *testing.T) {
					before := len(adorned.made())
					rel, err := d.Query(f.text, rs)
					require.NoError(t, err)
					require.ElementsMatch(t, f.want, g.tuples(t, rel), "the invocation derives what the branch's binding reaches, under the bound")
					require.Equal(t, [][]bool{{true, false}}, adorned.made()[before:], "one rewrite, for the position the branch binds")
				})
			}
			t.Run("or-join with the head as an output", func(t *testing.T) {
				_, err := executor.CollectTuples(d.Query(flat, rs))
				require.Error(t, err, "the branch sees nothing of the head, so the invocation derives the closure whole and the bound refuses it")
				unbounded := createOptimizerModeDB(t, mode, DatabaseOptions{})
				g := writeGraph(t, unbounded, spec)
				writeNames(t, unbounded, spec)
				rel, err := unbounded.Query(flat, rs)
				require.NoError(t, err)
				require.ElementsMatch(t, reached, g.tuples(t, rel), "without the bound the flat header answers the same tuples")
			})
		})
	}
}

// Two invocations of one predicate inside one branch, adorned alike at their
// first position and seeded from different symbols of the run, each derive
// what their own seed reaches, in either order of the pair: on the chain of
// six, n2 and n5 both reach n6, so a not over the two invocations whose free
// positions a predicate equates excludes the pair, and an or-join carrying
// the shared node as its output answers it, with one rewrite per invocation.
func TestRules_TwoAlikeInvocationsUnderAFormAnswerInEitherOrder(t *testing.T) {
	forms := []struct {
		name, text string
		want       func(x, z string) []string
	}{
		{"not",
			`[:find ?xn ?zn :in $ % [[?xn ?zn]] :where [?x :name ?xn] [?z :name ?zn] (not (reach ?x ?w) (reach ?z ?u) [(= ?w ?u)])]`,
			func(x, z string) []string { return nil }},
		{"or-join",
			`[:find ?xn ?zn ?w :in $ % [[?xn ?zn]] :where [?x :name ?xn] [?z :name ?zn] (or-join [[?x ?z] ?w] (and (reach ?x ?w) (reach ?z ?u) [(= ?w ?u)]) (and [?x :name "zz"] [(ground 0) ?w]))]`,
			func(x, z string) []string { return []string{x + ">" + z + ">n6"} }},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			adorned := &rewriteAdornments{}
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: adorned.handle})
			spec := chain(6)
			g := writeGraph(t, d, spec)
			writeNames(t, d, spec)
			rs := mustRuleSet(t, closureRules)
			for _, f := range forms {
				for _, pair := range [][2]string{{"n2", "n5"}, {"n5", "n2"}} {
					t.Run(f.name+" "+pair[0]+" "+pair[1], func(t *testing.T) {
						before := len(adorned.made())
						rel, err := d.Query(f.text, rs, [][]interface{}{{pair[0], pair[1]}})
						require.NoError(t, err)
						require.ElementsMatch(t, f.want(pair[0], pair[1]), g.tuples(t, rel))
						require.Equal(t, [][]bool{{true, false}, {true, false}}, adorned.made()[before:], "one rewrite per invocation, each for its own bound position")
					})
				}
			}
		})
	}
}
