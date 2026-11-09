package storage

import (
	"fmt"
	"sync/atomic"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// joinsIntoViewsRules derive the reach of a graph forward, through edge, and
// backward, through rev, whose head lists its symbols in the other order, so
// a join of the two predicates reads one key at different positions on each
// side; mutual and agree join two views of derived sets inside a body, and
// none derives nothing.
const joinsIntoViewsRules = `[
 [(edge ?x ?y) [?x :g/to ?y]]
 [(rev ?y ?x) [?x :g/to ?y]]
 [(reach ?x ?y) (edge ?x ?y)]
 [(reach ?x ?z) (reach ?x ?y) (edge ?y ?z)]
 [(reachb ?y ?x) (rev ?y ?x)]
 [(reachb ?z ?x) (rev ?z ?y) (reachb ?y ?x)]
 [(mutual ?x ?y) (reach ?x ?y) (reach ?y ?x)]
 [(agree ?x ?y) (reach ?x ?y) (reachb ?y ?x)]
 [(none ?x ?y) (edge ?x ?y) [?y :g/missing ?x]]
]`

// A join into a view of a derived set, inside a body or in the invoking
// query, answers the join of sets: each pair once, the key read at each
// side's own positions, on every backend, over a fixed graph with a cycle
// and a self-loop and over a generated graph of many passes.
func TestJoinsIntoDerivedViewsAnswerTheJoinOfSets(t *testing.T) {
	type graph struct {
		name  string
		nodes []string
		edges [][2]int
	}
	fixed := graph{name: "fixed", nodes: []string{"a", "b", "c", "d", "e"},
		edges: [][2]int{{0, 1}, {1, 2}, {2, 0}, {2, 3}, {3, 4}, {4, 4}}}
	generated := graph{name: "generated"}
	for i := 0; i < 40; i++ {
		generated.nodes = append(generated.nodes, fmt.Sprintf("n%d", i))
	}
	state := uint32(7)
	seen := map[[2]int]bool{}
	for len(generated.edges) < 70 {
		state = state*1664525 + 1013904223
		x := int(state>>8) % 40
		state = state*1664525 + 1013904223
		y := int(state>>8) % 40
		if seen[[2]int{x, y}] {
			continue
		}
		seen[[2]int{x, y}] = true
		generated.edges = append(generated.edges, [2]int{x, y})
	}

	to := datalog.NewKeyword(":g/to")
	nameAttr := datalog.NewKeyword(":g/name")

	for _, g := range []graph{fixed, generated} {
		ids := make([]datalog.Identity, len(g.nodes))
		byID := map[datalog.Identity]string{}
		for i, n := range g.nodes {
			ids[i] = datalog.NewIdentity("r2-views:" + g.name + ":" + n)
			byID[ids[i]] = n
		}
		// The reference: reach as the transitive closure of the edges.
		n := len(g.nodes)
		reach := make([][]bool, n)
		for i := range reach {
			reach[i] = make([]bool, n)
		}
		for _, e := range g.edges {
			reach[e[0]][e[1]] = true
		}
		for k := 0; k < n; k++ {
			for i := 0; i < n; i++ {
				for j := 0; j < n; j++ {
					if reach[i][k] && reach[k][j] {
						reach[i][j] = true
					}
				}
			}
		}
		pairs := func(keep func(x, y int) bool) [][]interface{} {
			out := [][]interface{}{}
			for x := 0; x < n; x++ {
				for y := 0; y < n; y++ {
					if keep(x, y) {
						out = append(out, []interface{}{ids[x], ids[y]})
					}
				}
			}
			return out
		}
		reachPairs := pairs(func(x, y int) bool { return reach[x][y] })
		mutualPairs := pairs(func(x, y int) bool { return reach[x][y] && reach[y][x] })
		notMutual := pairs(func(x, y int) bool { return reach[x][y] && !reach[y][x] })
		cycles := pairs(func(x, z int) bool {
			for y := 0; y < n; y++ {
				if reach[x][y] && reach[y][z] && reach[z][x] {
					return true
				}
			}
			return false
		})
		var counts [][]interface{}
		for x := 0; x < n; x++ {
			c := 0
			for y := 0; y < n; y++ {
				if reach[x][y] {
					c++
				}
			}
			if c > 0 {
				counts = append(counts, []interface{}{ids[x], int64(c)})
			}
		}
		var fromFirst, toFourth [][]interface{}
		for y := 0; y < n; y++ {
			if reach[0][y] {
				fromFirst = append(fromFirst, []interface{}{ids[y]})
			}
		}
		for x := 0; x < n; x++ {
			if reach[x][3] {
				toFourth = append(toFourth, []interface{}{ids[x]})
			}
		}
		var mutualNames [][]interface{}
		named := map[string]bool{}
		for x := 0; x < n; x++ {
			for y := 0; y < n; y++ {
				if reach[x][y] && reach[y][x] && !named[g.nodes[y]] {
					named[g.nodes[y]] = true
					mutualNames = append(mutualNames, []interface{}{g.nodes[y]})
				}
			}
		}

		cases := []struct {
			name   string
			query  string
			inputs []interface{}
			want   [][]interface{}
		}{
			{"the closure", `[:find ?x ?y :in $ % :where (reach ?x ?y)]`, nil, reachPairs},
			{"a body joining two views of one set at permuted keys", `[:find ?x ?y :in $ % :where (mutual ?x ?y)]`, nil, mutualPairs},
			{"a body joining views whose layouts list the key in other orders", `[:find ?x ?y :in $ % :where (agree ?x ?y)]`, nil, reachPairs},
			{"the query joining views whose layouts list the key in other orders", `[:find ?x ?y :in $ % :where (reach ?x ?y) (reachb ?y ?x)]`, nil, reachPairs},
			{"the query joining three views on three keys", `[:find ?x ?z :in $ % :where (reach ?x ?y) (reach ?y ?z) (reach ?z ?x)]`, nil, cycles},
			{"a count over a join of two views", `[:find ?x (count ?y) :in $ % :where (reach ?x ?y) (reachb ?y ?x)]`, nil, counts},
			{"a bound invocation", `[:find ?y :in $ % ?x :where (reach ?x ?y)]`, []interface{}{ids[0]}, fromFirst},
			{"a bound invocation of the reversed head", `[:find ?x :in $ % ?y :where (reachb ?y ?x)]`, []interface{}{ids[3]}, toFourth},
			{"a join into an empty view", `[:find ?x ?y :in $ % :where (reach ?x ?y) (none ?x ?y)]`, nil, [][]interface{}{}},
			{"a pattern after a view", `[:find ?n :in $ % :where (mutual ?x ?y) [?y :g/name ?n]]`, nil, mutualNames},
			{"a negation of a view after a view", `[:find ?x ?y :in $ % :where (reach ?x ?y) (not (mutual ?x ?y))]`, nil, notMutual},
		}
		for i := range cases {
			if cases[i].want == nil {
				cases[i].want = [][]interface{}{}
			}
		}

		for _, mode := range legsFor(backendModes) {
			t.Run(g.name+"/"+mode.name, func(t *testing.T) {
				declared := schema.NewBuilder().
					Attribute(":g/name").Type(schema.TypeString).One().Add().
					Attribute(":g/to").Type(schema.TypeRef).Many().Add().
					MustBuild()
				var kept, drained atomic.Int64
				handler := func(ev annotations.Event) {
					if ev.Name != annotations.JoinBuild {
						return
					}
					if b, _ := ev.Data["build_kept"].(bool); b {
						kept.Add(1)
					} else {
						drained.Add(1)
					}
				}
				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: declared, AnnotationHandler: handler})
				t.Cleanup(func() { t.Logf("join builds: kept %d, drained %d", kept.Load(), drained.Load()) })
				tx := db.NewTransaction()
				for i, id := range ids {
					require.NoError(t, tx.Add(id, nameAttr, g.nodes[i]))
				}
				for _, e := range g.edges {
					require.NoError(t, tx.Add(ids[e[0]], to, ids[e[1]]))
				}
				_, err := tx.Commit()
				require.NoError(t, err)
				rules := mustRuleSet(t, joinsIntoViewsRules)
				for _, c := range cases {
					t.Run(c.name, func(t *testing.T) {
						got, err := executor.CollectTuples(db.Query(c.query, append([]interface{}{rules}, c.inputs...)...))
						require.NoError(t, err)
						render := func(tuple []interface{}) string {
							out := ""
							for _, v := range tuple {
								if id, ok := v.(datalog.Identity); ok {
									v = byID[id]
								}
								out += fmt.Sprintf("%v ", v)
							}
							return out
						}
						tuples := make([]string, 0, len(got))
						for _, tuple := range got {
							tuples = append(tuples, render(tuple))
						}
						want := make([]string, 0, len(c.want))
						for _, tuple := range c.want {
							want = append(want, render(tuple))
						}
						require.ElementsMatch(t, want, tuples)
					})
				}
			})
		}
	}
}
