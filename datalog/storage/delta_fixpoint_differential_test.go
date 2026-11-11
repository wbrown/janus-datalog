package storage

import (
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// Shapes past the fixpoint pin's: three predicates recursive through one
// another around back edges, a recursive occurrence inside a non-aggregating
// subquery holding a union, a root whose own free site in an inner query
// makes it a whole predicate, a lower predicate read whole beside its bound
// sites under not, a subquery and or-default-join, and a mutual recursion
// whose free inner site makes the root whole. Each is answered through Query
// free, bound at each position to each node, under a windowed form, under a
// not, and from eight goroutines sharing the database's plan cache, against
// the naive fixpoint computed in Go
// (docs/proposals/A_COPY_READS_THE_TUPLES_ADDED_SINCE_IT_RAN_PROOF.md T1,
// A_PREDICATE_DERIVED_WHOLE_IS_READ_WHOLE_PROOF.md T1,
// A_HELD_RELATION_KEEPS_ITS_JOIN_INDEX_PROOF.md T2).

func domainOf(s pairSet) map[datalog.Identity]bool {
	out := map[datalog.Identity]bool{}
	for p := range s {
		out[p[0]] = true
	}
	return out
}

func deltaFixpointCases() []fixpointCase {
	return []fixpointCase{
		{
			name: "three predicates recursive around back edges",
			rules: `[[(a ?x ?y) [?x :edge ?y]]
			         [(a ?x ?z) (b ?x ?y) (c ?y ?z)]
			         [(b ?x ?y) [?x :jump ?y]]
			         [(b ?x ?z) (c ?x ?y) (a ?y ?z)]
			         [(c ?x ?y) [?x :child ?y]]
			         [(c ?x ?z) (a ?x ?y) (b ?y ?z)]]`,
			predicate: "a",
			want: func(fx fixpointFixture) pairSet {
				e, j, ch := fx.pairs(":edge"), fx.pairs(":jump"), fx.pairs(":child")
				a, b, c := pairSet{}, pairSet{}, pairSet{}
				for {
					na := union(e, compose(b, c))
					nb := union(j, compose(c, a))
					nc := union(ch, compose(a, b))
					if len(na) == len(a) && len(nb) == len(b) && len(nc) == len(c) {
						return a
					}
					a, b, c = na, nb, nc
				}
			},
		},
		{
			name: "recursion inside a non-aggregating subquery holding a union",
			rules: `[[(reach ?a ?b) [?a :edge ?b]]
			         [(reach ?a ?c) [?a :jump ?b] [(q [:find ?w :in $ % ?b :where (or (reach ?b ?w) [?b :child ?w])] $ % ?b) [[?c] ...]]]]`,
			predicate: "reach",
			want: func(fx fixpointFixture) pairSet {
				e, j, ch := fx.pairs(":edge"), fx.pairs(":jump"), fx.pairs(":child")
				return naive(func(r pairSet) pairSet { return union(e, compose(j, union(r, ch))) })
			},
		},
		{
			name: "a root its own free inner site reads whole",
			rules: `[[(reach ?a ?b) [?a :edge ?b]]
			         [(reach ?a ?c) [?a :edge ?b] [(q [:find ?u ?v :in $ % :where (reach ?u ?v)] $ %) [[?b ?c] ...]]]]`,
			predicate: "reach",
			want: func(fx fixpointFixture) pairSet {
				e := fx.pairs(":edge")
				return naive(func(r pairSet) pairSet { return union(e, compose(e, r)) })
			},
		},
		{
			name: "a lower predicate read whole beside its bound sites",
			rules: `[[(reach ?a ?b) [?a :edge ?b]]
			         [(reach ?a ?c) (reach ?a ?b) [?b :edge ?c]]
			         [(top ?x ?y) (reach ?x ?y) (not (reach ?y ?x))]
			         [(top ?x ?y) [?x :jump ?y] [(q [:find ?u :in $ % :where (reach ?u ?v) [?v :child _]] $ %) [[?y] ...]]]
			         [(top ?x ?y) [?x :child ?y] (or-default-join [[?y] ?k] (and (reach ?y ?n) [?n :bad true] [(ground 1) ?k]) [(ground 0) ?k]) [(= ?k 0)]]]`,
			predicate: "top",
			want: func(fx fixpointFixture) pairSet {
				e, j, ch := fx.pairs(":edge"), fx.pairs(":jump"), fx.pairs(":child")
				bad := fx.marked(":bad")
				r := naive(func(r pairSet) pairSet { return union(e, compose(r, e)) })
				parents := domainOf(ch)
				out := pairSet{}
				for p := range r {
					if !r[[2]datalog.Identity{p[1], p[0]}] {
						out[p] = true
					}
				}
				for p := range j {
					for q := range r {
						if q[0] == p[1] && parents[q[1]] {
							out[p] = true
						}
					}
				}
				for p := range ch {
					reachesBad := false
					for q := range r {
						if q[0] == p[1] && bad[q[1]] {
							reachesBad = true
						}
					}
					if !reachesBad {
						out[p] = true
					}
				}
				return out
			},
		},
		{
			name: "a mutual recursion whose free inner site makes the root whole",
			rules: `[[(odd ?a ?b) [?a :edge ?b]]
			         [(odd ?a ?c) [?a :edge ?b] (even ?b ?c)]
			         [(even ?a ?c) [?a :edge ?b] (odd ?b ?c)]
			         [(even ?a ?c) [?a :jump ?c] [(q [:find ?u :in $ % :where (odd ?u ?w)] $ %) [[?a] ...]]]]`,
			predicate: "odd",
			want: func(fx fixpointFixture) pairSet {
				e, j := fx.pairs(":edge"), fx.pairs(":jump")
				odd, even := pairSet{}, pairSet{}
				for {
					starts := domainOf(odd)
					jumps := pairSet{}
					for p := range j {
						if starts[p[0]] {
							jumps[p] = true
						}
					}
					nOdd := union(e, compose(e, even))
					nEven := union(compose(e, odd), jumps)
					if len(nOdd) == len(odd) && len(nEven) == len(even) {
						return odd
					}
					odd, even = nOdd, nEven
				}
			},
		},
		{
			name: "a required predicate a not leaves short of its binding",
			rules: `[[(nb [?x] ?y) [?x :edge ?y]]
			         [(walk ?a ?b) [?a :name _] (nb ?a ?b)]
			         [(walk ?a ?c) (walk ?a ?b) (nb ?b ?c) (not (nb ?c ?w) [?w :bad true])]]`,
			predicate: "walk",
			want: func(fx fixpointFixture) pairSet {
				e := fx.pairs(":edge")
				bad := fx.marked(":bad")
				clear := pairSet{}
				for p := range e {
					intoBad := false
					for q := range e {
						if q[0] == p[1] && bad[q[1]] {
							intoBad = true
						}
					}
					if !intoBad {
						clear[p] = true
					}
				}
				return naive(func(w pairSet) pairSet { return union(e, compose(w, clear)) })
			},
		},
		{
			name: "a seed-restricted lower predicate under a mutual recursion",
			rules: `[[(step ?a ?b) [?a :edge ?b]]
			         [(step ?a ?c) (step ?a ?b) [?b :jump ?c]]
			         [(p ?a ?b) (step ?a ?b)]
			         [(p ?a ?c) (step ?a ?b) (q ?b ?c)]
			         [(q ?a ?c) (step ?a ?b) (p ?b ?c)]
			         [(q ?a ?b) [?a :child ?b]]]`,
			predicate: "p",
			want: func(fx fixpointFixture) pairSet {
				e, j, ch := fx.pairs(":edge"), fx.pairs(":jump"), fx.pairs(":child")
				step := naive(func(s pairSet) pairSet { return union(e, compose(s, j)) })
				p, q := pairSet{}, pairSet{}
				for {
					np := union(step, compose(step, q))
					nq := union(ch, compose(step, p))
					if len(np) == len(p) && len(nq) == len(q) {
						return p
					}
					p, q = np, nq
				}
			},
		},
	}
}

func TestDeltaFixpoint_EveryShapeAnswersTheNaiveFixpoint(t *testing.T) {
	fx := newFixpointFixture()
	for _, tc := range deltaFixpointCases() {
		for _, mode := range legsFor(optimizerModes, sharingOption, symmetricOption, fusionOption) {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: fixpointSchema()})
				tx := d.NewTransaction()
				for _, f := range fx.facts {
					require.NoError(t, tx.Add(f.e, kw(f.a), f.v))
				}
				_, err := tx.Commit()
				require.NoError(t, err)
				names := readNames(t, d)
				rules := mustRuleSet(t, tc.rules)
				want := tc.want(fx)
				label := func(n datalog.Identity) string { return names[n] }

				free := fmt.Sprintf(`[:find ?x ?y :in $ %% :where (%s ?x ?y)]`, tc.predicate)
				boundFirst := fmt.Sprintf(`[:find ?y :in $ %% ?x :where (%s ?x ?y)]`, tc.predicate)
				boundSecond := fmt.Sprintf(`[:find ?x :in $ %% ?y :where (%s ?x ?y)]`, tc.predicate)
				windowed := fmt.Sprintf(`[:find ?x ?y :in $ %% :where [?x :name _] (or-join [[?x] ?y] (%s ?x ?y) (and [?x :never ?y]))]`, tc.predicate)
				negated := fmt.Sprintf(`[:find ?x ?y :in $ %% :where [?x :name _] [?y :name _] (not (%s ?x ?y))]`, tc.predicate)
				complement := pairSet{}
				for _, x := range fx.nodes {
					for _, y := range fx.nodes {
						if !want[[2]datalog.Identity{x, y}] {
							complement[[2]datalog.Identity{x, y}] = true
						}
					}
				}

				// run answers every query once, reporting the first
				// disagreement; it reads nothing but d and the inputs.
				run := func() error {
					rel, err := d.Query(free, rules)
					if err != nil {
						return fmt.Errorf("free: %w", err)
					}
					got, err := identityPairsOfErr(rel, 0, 1, nil)
					if err != nil {
						return fmt.Errorf("free: %w", err)
					}
					if !pairSetsEqual(got, want) {
						return fmt.Errorf("free: got %d pairs, want %d", len(got), len(want))
					}
					for _, n := range fx.nodes {
						n := n
						rel, err := d.Query(boundFirst, rules, n)
						if err != nil {
							return fmt.Errorf("bound first at %s: %w", label(n), err)
						}
						got, err := identityPairsOfErr(rel, -1, 0, &n)
						if err != nil {
							return fmt.Errorf("bound first at %s: %w", label(n), err)
						}
						if w := restrict(want, &n, nil); !pairSetsEqual(got, w) {
							return fmt.Errorf("bound first at %s: got %v, want %v", label(n), renderPairs(names, got), renderPairs(names, w))
						}
						rel, err = d.Query(boundSecond, rules, n)
						if err != nil {
							return fmt.Errorf("bound second at %s: %w", label(n), err)
						}
						got, err = identityPairsOfErr(rel, 0, -1, &n)
						if err != nil {
							return fmt.Errorf("bound second at %s: %w", label(n), err)
						}
						if w := restrict(want, nil, &n); !pairSetsEqual(got, w) {
							return fmt.Errorf("bound second at %s: got %v, want %v", label(n), renderPairs(names, got), renderPairs(names, w))
						}
					}
					rel, err = d.Query(windowed, rules)
					if err != nil {
						return fmt.Errorf("windowed: %w", err)
					}
					got, err = identityPairsOfErr(rel, 0, 1, nil)
					if err != nil {
						return fmt.Errorf("windowed: %w", err)
					}
					if !pairSetsEqual(got, want) {
						return fmt.Errorf("windowed: got %v, want %v", renderPairs(names, got), renderPairs(names, want))
					}
					rel, err = d.Query(negated, rules)
					if err != nil {
						return fmt.Errorf("negated: %w", err)
					}
					got, err = identityPairsOfErr(rel, 0, 1, nil)
					if err != nil {
						return fmt.Errorf("negated: %w", err)
					}
					if !pairSetsEqual(got, complement) {
						return fmt.Errorf("negated: got %v, want %v", renderPairs(names, got), renderPairs(names, complement))
					}
					return nil
				}

				require.NoError(t, run(), "one goroutine")

				const goroutines = 8
				errs := make([]error, goroutines)
				var wg sync.WaitGroup
				for g := 0; g < goroutines; g++ {
					wg.Add(1)
					go func(g int) {
						defer wg.Done()
						for round := 0; round < 3 && errs[g] == nil; round++ {
							errs[g] = run()
						}
					}(g)
				}
				wg.Wait()
				for g, err := range errs {
					require.NoError(t, err, "goroutine %d", g)
				}
			})
		}
	}
}

// identityPairsOfErr is identityPairsOf for a goroutine other than the
// test's: it returns a failure to read as its error.
func identityPairsOfErr(rel executor.Relation, x, y int, fixed *datalog.Identity) (pairSet, error) {
	out := pairSet{}
	err := executor.ForEach(rel, func(tuple executor.Tuple) error {
		var p [2]datalog.Identity
		if x < 0 {
			p[0] = *fixed
		} else {
			p[0] = tuple[x].(datalog.Identity)
		}
		if y < 0 {
			p[1] = *fixed
		} else {
			p[1] = tuple[y].(datalog.Identity)
		}
		out[p] = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func pairSetsEqual(a, b pairSet) bool {
	if len(a) != len(b) {
		return false
	}
	for p := range a {
		if !b[p] {
			return false
		}
	}
	return true
}

func renderPairs(names map[datalog.Identity]string, s pairSet) []string {
	var out []string
	for p := range s {
		out = append(out, names[p[0]]+">"+names[p[1]])
	}
	sort.Strings(out)
	return out
}
