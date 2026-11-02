package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A query whose clauses join answers the join of sets on every route a join
// input reaches it by: a pattern after a join, an aggregate over a join, a
// history scan carrying re-assertions, a cardinality-many attribute, a named
// in-memory source, a negation and a union over a join, a collection and a
// relation input carrying a repeated value, two relation inputs, and a join
// against an empty side (docs/proposals/JOIN_OUTPUT_IS_A_SET_PROOF.md, T1;
// docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, T2 and T5).
func TestJoinRoutesAnswerTheJoinOfSets(t *testing.T) {
	name := datalog.NewKeyword(":person/name")
	dept := datalog.NewKeyword(":person/dept")
	tag := datalog.NewKeyword(":person/tag")
	deptName := datalog.NewKeyword(":dept/name")
	city := datalog.NewKeyword(":dept/city")
	cacheUser := datalog.NewKeyword(":cache/user")
	cacheScore := datalog.NewKeyword(":cache/score")

	alice := datalog.NewIdentity("join-routes:alice")
	bob := datalog.NewIdentity("join-routes:bob")
	carol := datalog.NewIdentity("join-routes:carol")
	eng := datalog.NewIdentity("join-routes:eng")
	ops := datalog.NewIdentity("join-routes:ops")
	cacheOne := datalog.NewIdentity("join-routes:cache-one")
	cacheTwo := datalog.NewIdentity("join-routes:cache-two")

	type fact struct {
		e datalog.Identity
		a datalog.Keyword
		v interface{}
	}
	// Two transactions: the second re-asserts alice's department, so the
	// history holds two operation records of one (E, A, V).
	transactions := [][]fact{
		{
			{alice, name, "Alice"}, {bob, name, "Bob"}, {carol, name, "Carol"},
			{eng, deptName, "Eng"}, {ops, deptName, "Ops"},
			{eng, city, "Paris"}, {ops, city, "Oslo"},
			{alice, dept, eng}, {bob, dept, eng}, {carol, dept, ops},
			{alice, tag, "a"}, {alice, tag, "b"}, {bob, tag, "a"},
		},
		{
			{alice, dept, eng},
		},
	}
	cache := executor.NewMemoryPatternMatcher([]datalog.Datom{
		{E: cacheOne, A: cacheUser, V: "Alice"},
		{E: cacheOne, A: cacheScore, V: int64(95)},
		{E: cacheTwo, A: cacheUser, V: "Bob"},
		{E: cacheTwo, A: cacheScore, V: int64(82)},
	})

	cases := []struct {
		name    string
		history bool
		query   string
		inputs  []interface{}
		want    [][]interface{}
	}{
		{name: "a pattern after a join",
			query: `[:find ?p ?city :where [?p :person/name _] [?p :person/dept ?d] [?d :dept/city ?city]]`,
			want:  [][]interface{}{{alice, "Paris"}, {bob, "Paris"}, {carol, "Oslo"}}},
		{name: "a count over a join",
			query: `[:find ?city (count ?p) :where [?p :person/dept ?d] [?d :dept/city ?city]]`,
			want:  [][]interface{}{{"Paris", int64(2)}, {"Oslo", int64(1)}}},
		{name: "a history join over a re-asserted fact, counted", history: true,
			query: `[:find ?d (count ?p) :where [?p :person/dept ?d] [?d :dept/name ?dn]]`,
			want:  [][]interface{}{{eng, int64(2)}, {ops, int64(1)}}},
		{name: "a history join over a re-asserted fact, projected whole", history: true,
			query: `[:find ?p ?d ?dn :where [?p :person/dept ?d] [?d :dept/name ?dn]]`,
			want:  [][]interface{}{{alice, eng, "Eng"}, {bob, eng, "Eng"}, {carol, ops, "Ops"}}},
		{name: "a join over a cardinality-many attribute, counted",
			query: `[:find ?d (count ?t) :where [?p :person/dept ?d] [?p :person/tag ?t]]`,
			want:  [][]interface{}{{eng, int64(3)}}},
		{name: "a named in-memory source joined with the database",
			query:  `[:find ?p ?score :in $ $m :where [?p :person/name ?n] [$m ?c :cache/user ?n] [$m ?c :cache/score ?score]]`,
			inputs: []interface{}{WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$m"): cache})},
			want:   [][]interface{}{{alice, int64(95)}, {bob, int64(82)}}},
		{name: "a negation over a join",
			query: `[:find ?p :where [?p :person/dept ?d] [?d :dept/city ?city] (not [?p :person/tag "b"])]`,
			want:  [][]interface{}{{bob}, {carol}}},
		{name: "a union over a join",
			query: `[:find ?p :where [?p :person/dept ?d] [?d :dept/name ?dn] (or-join [[?p ?d]] [?d :dept/city "Oslo"] [?p :person/tag "b"])]`,
			want:  [][]interface{}{{alice}, {carol}}},
		{name: "a collection input holding a value twice",
			query:  `[:find ?p ?d :in $ [?n ...] :where [?p :person/name ?n] [?p :person/dept ?d]]`,
			inputs: []interface{}{[]interface{}{"Alice", "Alice", "Bob"}},
			want:   [][]interface{}{{alice, eng}, {bob, eng}}},
		{name: "a collection input holding a value twice, counted",
			query:  `[:find (count ?p) :in $ [?n ...] :where [?p :person/name ?n] [?p :person/dept ?d]]`,
			inputs: []interface{}{[]interface{}{"Alice", "Alice", "Bob"}},
			want:   [][]interface{}{{int64(2)}}},
		{name: "a relation input holding a tuple twice",
			query:  `[:find ?p ?w :in $ [[?n ?w]] :where [?p :person/name ?n] [?p :person/dept ?d] [?d :dept/city "Paris"]]`,
			inputs: []interface{}{[]interface{}{[]interface{}{"Alice", int64(1)}, []interface{}{"Bob", int64(2)}, []interface{}{"Alice", int64(1)}}},
			want:   [][]interface{}{{alice, int64(1)}, {bob, int64(2)}}},
		{name: "two relation inputs sharing a symbol",
			query: `[:find ?p ?w ?z :in $ [[?n ?w]] [[?n ?z]] :where [?p :person/name ?n]]`,
			inputs: []interface{}{
				[]interface{}{[]interface{}{"Alice", int64(1)}, []interface{}{"Bob", int64(2)}},
				[]interface{}{[]interface{}{"Alice", int64(10)}},
			},
			want: [][]interface{}{{alice, int64(1), int64(10)}}},
		{name: "a join against an empty side",
			query: `[:find ?p :where [?p :person/name ?n] [?p :person/dept ?d] [?d :dept/missing ?x]]`,
			want:  [][]interface{}{}},
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})
			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{Ident: tag, ValueType: schema.TypeString, Cardinality: schema.CardinalityMany})
			require.NoError(t, db.SetSchema(s))
			for _, facts := range transactions {
				tx := db.NewTransaction()
				for _, f := range facts {
					require.NoError(t, tx.Add(f.e, f.a, f.v))
				}
				_, err := tx.Commit()
				require.NoError(t, err)
			}
			names := map[datalog.Identity]string{}
			for _, facts := range transactions {
				for _, f := range facts {
					if f.a == name || f.a == deptName {
						names[f.e] = f.v.(string)
					}
				}
			}
			render := func(tuples [][]interface{}) []string {
				out := make([]string, 0, len(tuples))
				for _, tuple := range tuples {
					s := ""
					for _, v := range tuple {
						if id, ok := v.(datalog.Identity); ok {
							v = names[id]
						}
						s += fmt.Sprintf("%v ", v)
					}
					out = append(out, s)
				}
				return out
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					handle := db
					if c.history {
						handle = db.History()
					}
					got, err := executor.CollectTuples(handle.Query(c.query, c.inputs...))
					require.NoError(t, err)
					require.ElementsMatch(t, c.want, got, "want %v, got %v", render(c.want), render(got))
				})
			}
		})
	}
}

// A query whose join has an empty side answers the empty relation, though its
// other side fails to evaluate: no assignment satisfies the empty side's
// clause, and a join over an empty side does not drive the other side
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, T5). The same failing side
// with no empty side answers its failure. With carol carrying :item/missing
// and no name, the queries joining on ?p still meet no assignment and answer
// empty, and the queries joining on ?m answer the failure: the failing side's
// ?m is undefined, so carol's datom holds beside each of its tuples.
func TestAJoinWithAnEmptySideAnswersEmptyThoughTheOtherSideFails(t *testing.T) {
	name := datalog.NewKeyword(":item/name")
	missing := datalog.NewKeyword(":item/missing")
	alice := datalog.NewIdentity("join-fail:alice")
	bob := datalog.NewIdentity("join-fail:bob")
	carol := datalog.NewIdentity("join-fail:carol")
	queries := []string{
		`[:find ?p ?m :where [?p :item/name ?n] [(+ ?n 1) ?m] [?p :item/missing ?x]]`,
		`[:find ?p ?m :where [?p :item/missing ?x] [?p :item/name ?n] [(+ ?n 1) ?m]]`,
		`[:find ?p ?m :where [?p :item/name ?n] [(+ ?n 1) ?m] [?q :item/missing ?m]]`,
		`[:find ?p ?m :where [?q :item/missing ?m] [?p :item/name ?n] [(+ ?n 1) ?m]]`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(alice, name, "Alice"))
			require.NoError(t, tx.Add(bob, name, "Bob"))
			_, err := tx.Commit()
			require.NoError(t, err)
			for i, q := range queries {
				t.Run(fmt.Sprint(i), func(t *testing.T) {
					got, err := executor.CollectTuples(db.Query(q))
					require.NoError(t, err, "query %d: a join with an empty side answers the empty relation", i)
					assert.Empty(t, got, "query %d answers no tuples", i)
				})
			}
			t.Run("reachable", func(t *testing.T) {
				_, err := executor.CollectTuples(db.Query(`[:find ?p ?m :where [?p :item/name ?n] [(+ ?n 1) ?m]]`))
				require.ErrorContains(t, err, "is not a number", "the failing side with no empty side answers its failure")
			})
			t.Run("an other side holding a fact", func(t *testing.T) {
				withMissing := createOptimizerModeDB(t, mode, DatabaseOptions{})
				tx := withMissing.NewTransaction()
				require.NoError(t, tx.Add(alice, name, "Alice"))
				require.NoError(t, tx.Add(bob, name, "Bob"))
				require.NoError(t, tx.Add(carol, missing, int64(5)))
				_, err := tx.Commit()
				require.NoError(t, err)
				for i, q := range queries[:2] {
					got, err := executor.CollectTuples(withMissing.Query(q))
					require.NoError(t, err, "query %d: carol has no name, so no assignment meets the join on ?p", i)
					assert.Empty(t, got, "query %d answers no tuples", i)
				}
				for i, q := range queries[2:] {
					_, err := executor.CollectTuples(withMissing.Query(q))
					require.ErrorContains(t, err, "is not a number", "query %d: carol's datom holds beside each failing tuple", i+2)
				}
			})
		})
	}
}
