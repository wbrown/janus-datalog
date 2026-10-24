package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A bound invocation derives what the free one derives for its binding,
// whatever form the rules hold beside the recursion: a magic rule reads no
// negation and no aggregating subquery, so the rewritten set is accepted
// (docs/proposals/NON_MONOTONE_FORMS_PROOF.md §1), and a required predicate
// below a free invocation is adorned inside the rewritten set, so it is
// derived once there (docs/reference/RULES.md, the evaluation section).

// writeRanks records :rank i on the i-th node of spec, counted from one.
func writeRanks(t testing.TB, d *Database, spec graphSpec) {
	t.Helper()
	tx := d.NewTransaction()
	for i, name := range spec.nodes {
		require.NoError(t, tx.Add(nodeID(name), kw(":rank"), int64(i+1)))
	}
	_, err := tx.Commit()
	require.NoError(t, err)
}

// A bound query answers what the free one answers for its binding when an
// aggregating subquery stands before the recursion, when a negation stands
// inside an or-join branch the recursion reads, and when a free invocation
// under a fallback, of a predicate reaching a required one, would lie in the
// invoking rule's stratum named all free and so keeps its original rules.
func TestRules_ABoundQueryAnswersWhereTheFreeOneDoes(t *testing.T) {
	cases := []struct{ name, rules string }{
		{"an aggregating subquery before the recursion",
			`[[(low ?a ?b) [?a :edge ?b]]
			  [(p ?a ?b) [?a :edge ?b]]
			  [(p ?a ?c) [?a :edge _] [(q [:find ?w (count ?z) :in $ % ?x :where (low ?x ?w) (low ?w ?z)] $ % ?a) [[?v ?n]]] [(identity ?v) ?m] (p ?m ?c)]]`},
		{"a negation inside a branch the recursion reads",
			`[[(low ?a ?b) [?a :edge ?b]]
			  [(p ?a ?b) [?a :edge ?b]]
			  [(p ?a ?c) [?a :edge ?m] (or-join [[?m] ?k] (and (not-join [?m] (low ?m ?z)) [(identity ?m) ?k]) (and [?m :type :node] [(identity ?m) ?k])) (p ?k ?c)]]`},
		{"a free site under a fallback whose all-free name would not stratify",
			`[[(nb [?x] ?y) [?x :edge ?y]]
			  [(low ?a ?b) [?a :edge ?b]]
			  [(low ?a ?b) [?a :edge ?m] (low ?m ?b)]
			  [(low ?a ?b) [?a :type :node] (or-join [[?a] ?b] (nb ?a ?b) [?a :edge ?b])]
			  [(mid ?a ?b) [?a :edge ?b]]
			  [(mid ?a ?b) [?a :edge ?m] (low ?m ?b)]
			  [(mid ?a ?b) (mid ?a ?b) (or-default-join [[?b] ?k] (and (low ?b ?z) [(ground 1) ?k]) [(ground 0) ?k]) [(= ?k 1)]]
			  [(p ?a ?b) (mid ?a ?m) [?m :edge ?b] [(q [:find (count ?z) :in $ % ?x :where [?x :edge ?w] (low ?w ?z)] $ % ?a) [[?n]]] [(> ?n 0)]]]`},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: graphSchema()})
			g := writeGraph(t, d, chain(6))
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					rs := mustRuleSet(t, c.rules)
					free, err := d.Query(`[:find ?x ?y :in $ % :where (p ?x ?y)]`, rs)
					require.NoError(t, err)
					var want []string
					for _, pair := range g.tuples(t, free) {
						if strings.HasPrefix(pair, "n1>") {
							want = append(want, strings.TrimPrefix(pair, "n1>"))
						}
					}
					require.NotEmpty(t, want, "the free query answers from n1, so the bound one has something to answer")
					bound, err := d.Query(`[:find ?y :in $ % ?s :where (p ?s ?y)]`, rs, nodeID("n1"))
					require.NoError(t, err, "the bound query is accepted where the free one answers")
					require.ElementsMatch(t, want, g.tuples(t, bound))
				})
			}
		})
	}
}

// A free invocation of a predicate whose rules reach a required predicate two
// levels down costs one rewrite: the required predicate is adorned inside the
// rewritten set and derived once there, never at its clause on every run of
// the body invoking it.
func TestRules_ARequiredPredicateTwoLevelsDownIsDerivedOnce(t *testing.T) {
	const rules = `[[(succ [?n] ?m) [(+ ?n 1) ?m]]
	                [(walk ?a ?b) [?a :edge ?b]]
	                [(walk ?a ?c) (walk ?a ?b) [?b :rank ?k] (succ ?k ?j) [(< ?j 100)] [?b :edge ?c]]
	                [(top ?a ?c) (walk ?a ?c)]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			adorned := &rewriteAdornments{}
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: adorned.handle})
			spec := chain(6)
			g := writeGraph(t, d, spec)
			writeRanks(t, d, spec)
			rel, err := d.Query(`[:find ?a ?r :in $ % :where (top ?a ?r)]`, mustRuleSet(t, rules))
			require.NoError(t, err)
			require.ElementsMatch(t, chainClosure(6), g.tuples(t, rel))
			require.Equal(t, [][]bool{{false, false}}, adorned.made(),
				"one rewrite, the top-level invocation all free; succ is adorned inside it and never derived at its clause per run of walk's body")
		})
	}
}

// A free root whose own recursion reaches the required predicate is rewritten
// all free once: its recursive invocations name the adorned predicate, not
// the original rules, and the required predicate is adorned inside the set.
func TestRules_AFreeRootsOwnRecursionIsRewrittenOnce(t *testing.T) {
	const rules = `[[(succ [?n] ?m) [(+ ?n 1) ?m]]
	                [(walk ?a ?b) [?a :edge ?b] [?b :rank ?k] (succ ?k ?j) [(< ?j 100)]]
	                [(walk ?a ?c) (walk ?a ?b) (walk ?b ?c)]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			adorned := &rewriteAdornments{}
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: adorned.handle})
			spec := chain(6)
			g := writeGraph(t, d, spec)
			writeRanks(t, d, spec)
			rel, err := d.Query(`[:find ?x ?y :in $ % :where (walk ?x ?y)]`, mustRuleSet(t, rules))
			require.NoError(t, err)
			require.ElementsMatch(t, chainClosure(6), g.tuples(t, rel))
			require.Equal(t, [][]bool{{false, false}}, adorned.made(),
				"one rewrite, the root all free; its own recursion and succ are adorned inside it")
		})
	}
}
