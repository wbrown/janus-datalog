package storage

// A fallback form's branch is given the required run alone, under the magic
// rewrite as under the executor: an output of the form the enclosing scope
// binds before it, by any binder and in any order, binds nothing inside a
// branch and adorns no invocation inside one at its position. So a bound
// invocation of a rule whose body holds the fallback answers the tuples the
// free one answers.

import "testing"

// fallbackGraph is the fixture's entities: x1 :link m1; x2 :link z1; m1
// :edge k9 and :tag "c"; k9 :name "k9" and :edge z1; z1 :name "z1"; z
// :marker "c". low(m1, ?k) answers k9, so a fallback over low answers k9 for
// m1 and never the default m1, and k9 carries no :tag. z1 has no edge, so
// low(z1, ?k) answers nothing while low holds tuples.
type fallbackGraph struct {
	x1, x2, m1, k9, z1, z namedEntity
}

func newFallbackGraph() fallbackGraph {
	return fallbackGraph{x1: named("x1"), x2: named("x2"), m1: named("m1"), k9: named("k9"), z1: named("z1"), z: named("z")}
}

func (g fallbackGraph) facts() []fact {
	return []fact{
		{g.x1, ":link", g.m1}, {g.x2, ":link", g.z1},
		{g.m1, ":edge", g.k9}, {g.m1, ":tag", "c"},
		{g.k9, ":name", "k9"}, {g.k9, ":edge", g.z1},
		{g.z1, ":name", "z1"},
		{g.z, ":marker", "c"},
	}
}

const lowRule = `[(low ?a ?b) [?a :edge ?b]]`

// p holds of x1 and the :tag of what the fallback answers for m1. The branch
// answers k9, which carries no :tag, so p holds no tuple, and binding its
// second position before the fallback changes nothing: every case answers no
// tuple. The free evaluation is the control.
func TestFallbackBoundOutput_AnOutputBoundBeforeTheFormNeverNarrowsABranch(t *testing.T) {
	const p = `[(p ?a ?b) [?a :link ?m] (or-default-join [[?m] ?k] (low ?m ?k) [(identity ?m) ?k]) [?k :tag ?b]]`
	const pInOr = `[(p ?a ?b) [?a :link ?m] (or-join [[?m] ?k] (or-default-join [[?m] ?k] (low ?m ?k) [(identity ?m) ?k]) [?m :none ?k]) [?k :tag ?b]]`
	const pHeaderless = `[(p ?a ?b) [?a :link ?m] (or-default (low ?m ?k) [(identity ?m) ?k]) [?k :tag ?b]]`
	cases := []queryCase{
		{"free, the whole evaluation", "[" + lowRule + p + "]",
			`[:find ?x ?y :in $ % :where (p ?x ?y)]`, nil, nil},
		{"the second position bound by an :in scalar", "[" + lowRule + p + "]",
			`[:find ?x :in $ % ?y :where (p ?x ?y)]`, []interface{}{"c"}, nil},
		{"the second position bound by a pattern before the invocation", "[" + lowRule + p + "]",
			`[:find ?x :in $ % :where [?z :marker ?y] (p ?x ?y)]`, nil, nil},
		{"the second position a constant", "[" + lowRule + p + "]",
			`[:find ?x :in $ % :where (p ?x "c")]`, nil, nil},
		{"the fallback inside an or-join branch, bound by an :in scalar", "[" + lowRule + pInOr + "]",
			`[:find ?x :in $ % ?y :where (p ?x ?y)]`, []interface{}{"c"}, nil},
		{"the headerless or-default, bound by an :in scalar", "[" + lowRule + pHeaderless + "]",
			`[:find ?x :in $ % ?y :where (p ?x ?y)]`, []interface{}{"c"}, nil},
	}
	runQueryCases(t, newFallbackGraph().facts(), cases)
}

// p's second position is the fallback's output. Bound at m1, the default's
// value, the invocation answers nothing, since the branch answers k9 for m1;
// bound at k9 it answers x1. An output the branch's own clause provides
// before an invocation is the branch's own as well: the branch reaches k9
// and low(k9, ?z) answers z1, so the fallback answers k9 for m1, and bound
// at m1 the invocation answers nothing. Under a union the carried output is
// given to the branch, and bound at k9 the invocation answers x1.
func TestFallbackBoundOutput_AnOutputAtTheHeadsBoundPositionNeverNarrowsABranch(t *testing.T) {
	const pHead = `[(p ?a ?k) [?a :link ?m] (or-default-join [[?m] ?k] (low ?m ?k) [(identity ?m) ?k])]`
	const pBranchProvided = `[(p ?a ?k) [?a :link ?m] (or-default-join [[?m] ?k] (and [?m :edge ?k] (low ?k ?z)) [(identity ?m) ?k])]`
	const pUnion = `[(p ?a ?k) [?a :link ?m] (or-join [[?m] ?k] (low ?m ?k) [?m :none ?k])]`
	g := newFallbackGraph()
	cases := []queryCase{
		{"the head's bound position at the default's value", "[" + lowRule + pHead + "]",
			`[:find ?x :in $ % ?k :where (p ?x ?k)]`, []interface{}{g.m1.id}, nil},
		{"the head's bound position at the branch's answer", "[" + lowRule + pHead + "]",
			`[:find ?x :in $ % ?k :where (p ?x ?k)]`, []interface{}{g.k9.id}, []string{"x1"}},
		{"an output the branch's own clause provides before an invocation, at the default's value", "[" + lowRule + pBranchProvided + "]",
			`[:find ?x :in $ % ?k :where (p ?x ?k)]`, []interface{}{g.m1.id}, nil},
		{"the same output under a union, at the branch's answer", "[" + lowRule + pUnion + "]",
			`[:find ?x :in $ % ?k :where (p ?x ?k)]`, []interface{}{g.k9.id}, []string{"x1"}},
	}
	runQueryCases(t, g.facts(), cases)
}

// A flat-header fallback decides once over its whole first branch, so the
// branch is given nothing and an invocation in it derives its predicate
// whole (FALLBACK_GIVEN_PROOF.md T4). Both branches bind ?m and ?k, so the
// header infers to [?m ?k]. low holds tuples, so the default never fires: p
// answers "k9" for x1, whose link's edge the branch answers, and nothing for
// x2, whose link z1 has no edge. Bound at x2 the invocation answers nothing,
// as the free evaluation does; a branch narrowed to z1 would be empty, and
// the default would answer "z1".
func TestFallbackBoundOutput_AFlatHeaderFallbacksBranchIsGivenNothing(t *testing.T) {
	const p = `[(p ?a ?c) [?a :link ?m] (or-default (low ?m ?k) (and [?m :name _] [(identity ?m) ?k])) [?k :name ?c]]`
	g := newFallbackGraph()
	cases := []queryCase{
		{"free, the whole evaluation", "[" + lowRule + p + "]",
			`[:find ?x ?c :in $ % :where (p ?x ?c)]`, nil, []string{"x1 k9"}},
		{"the first position bound at x1, whose link's edge the branch answers", "[" + lowRule + p + "]",
			`[:find ?c :in $ % ?x :where (p ?x ?c)]`, []interface{}{g.x1.id}, []string{"k9"}},
		{"the first position bound at x2, whose link has no edge", "[" + lowRule + p + "]",
			`[:find ?c :in $ % ?x :where (p ?x ?c)]`, []interface{}{g.x2.id}, nil},
	}
	runQueryCases(t, g.facts(), cases)
}
