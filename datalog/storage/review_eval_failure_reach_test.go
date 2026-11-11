package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A query whose expression fails at an assignment where every other clause
// holds answers the failure, on every route that reads the expression's
// relation: the failure contract
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md §1) states that a query
// failing under one optimizer path fails under the other.
func TestReviewEval_AFailureWhereEveryOtherClauseHoldsFailsTheQuery(t *testing.T) {
	name := datalog.NewKeyword(":item/name")
	v := datalog.NewKeyword(":item/v")
	type fact struct {
		e datalog.Identity
		a datalog.Keyword
		v interface{}
	}
	alice := datalog.NewIdentity("review-eval:alice")
	// Many numeric values beside one string, so whichever tuple a reader
	// pulls first, the failing one is among those left.
	var facts []fact
	facts = append(facts, fact{alice, name, "Alice"})
	for i := 0; i < 16; i++ {
		e := datalog.NewIdentity("review-eval:n" + string(rune('a'+i)))
		facts = append(facts, fact{e, v, int64(i)})
	}
	stringy := datalog.NewIdentity("review-eval:stringy")
	facts = append(facts, fact{stringy, v, "x"})

	cases := []struct {
		name  string
		query string
	}{
		{"the expression's group read only for its existence",
			`[:find ?p :where [?p :item/name _] [?q :item/v ?a] [(+ ?a 1) ?b]]`},
		{"the expression's group read only for its existence, the expression first",
			`[:find ?p :where [?q :item/v ?a] [(+ ?a 1) ?b] [?p :item/name _]]`},
		{"a limit over the expression's relation",
			`[:find ?q ?b :where [?q :item/v ?a] [(+ ?a 1) ?b] :limit 1]`},
		{"a pull over the find's group beside the expression's group",
			`[:find (pull ?p [:item/name]) :where [?p :item/name _] [?q :item/v ?a] [(+ ?a 1) ?b]]`},
		{"a count over the find's group beside the expression's group",
			`[:find (count ?p) :where [?p :item/name _] [?q :item/v ?a] [(+ ?a 1) ?b]]`},
		{"an order-by over the find's group beside the expression's group",
			`[:find ?p :where [?p :item/name _] [?q :item/v ?a] [(+ ?a 1) ?b] :order-by [?p]]`},
		{"a correlated subquery whose body holds the expression's group",
			`[:find ?p ?x :where [?p :item/name _] [(q [:find ?x :in $ ?p :where [?p :item/name ?x] [?q :item/v ?a] [(+ ?a 1) ?b]] $ ?p) [[?x]]]]`},
		{"an or-join branch holding the expression's group",
			`[:find ?p :where [?p :item/name _] (or-join [?p] (and [?p :item/name _] [?q :item/v ?a] [(+ ?a 1) ?b]))]`},
		{"a not-join body holding the expression's group",
			`[:find ?p :where [?p :item/name _] (not-join [?p] [?p :item/name "Alice"] [?q :item/v ?a] [(+ ?a 1) ?b])]`},
		{"an or-default-join branch holding the expression's group",
			`[:find ?p ?k :where [?p :item/name _] (or-default-join [[?p] ?k] (and [?p :item/name ?k] [?q :item/v ?a] [(+ ?a 1) ?b]) [(ground "none") ?k])]`},
		{"the expression's whole relation",
			`[:find ?q ?b :where [?q :item/v ?a] [(+ ?a 1) ?b]]`},
	}
	for _, mode := range legsFor(optimizerModes, algebraOption, sharingOption, fusionOption, aggregationOption) {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := db.NewTransaction()
			for _, f := range facts {
				require.NoError(t, tx.Add(f.e, f.a, f.v))
			}
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					got, err := executor.CollectTuples(db.Query(c.query))
					require.ErrorContains(t, err, "is not a number", "the query answered %d tuples with no error", len(got))
				})
			}
		})
	}
}

// A query with a clause that holds at no assignment answers the empty
// relation with no error, whatever clause fails to evaluate beside it: the
// failure contract (commit a08c3dc; docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md
// §1, T10) states that an evaluation failure fails a query only at an
// assignment where every other clause holds.
func TestReviewEval_AFailureBesideAnEmptyClauseFailsNoQuery(t *testing.T) {
	name := datalog.NewKeyword(":item/name")
	age := datalog.NewKeyword(":item/age")
	alice := datalog.NewIdentity("review-eval-empty:alice")
	bob := datalog.NewIdentity("review-eval-empty:bob")
	cases := []struct {
		name   string
		query  string
		inputs []interface{}
	}{
		{"an arithmetic expression before the empty pattern",
			`[:find ?p :where [?p :item/name ?n] [(+ ?n 1) ?m] [?p :item/missing ?x]]`, nil},
		{"a missing? predicate before the empty pattern",
			`[:find ?p :where [?p :item/age ?a] [(missing? $ ?a :item/other)] [?p :item/missing ?x]]`, nil},
		{"a missing? predicate after the empty pattern",
			`[:find ?p :where [?p :item/missing ?x] [?p :item/age ?a] [(missing? $ ?a :item/other)]]`, nil},
		{"a get-else expression before the empty pattern",
			`[:find ?p :where [?p :item/age ?a] [(get-else $ ?a :item/other "d") ?g] [?p :item/missing ?x]]`, nil},
		{"a get-some expression before the empty pattern",
			`[:find ?p :where [?p :item/age ?a] [(get-some $ ?a :item/other) ?g] [?p :item/missing ?x]]`, nil},
		{"an arithmetic expression before an empty correlated subquery",
			`[:find ?p :where [?p :item/name ?n] [(+ ?n 1) ?m] [(q [:find ?z :in $ ?n :where [?z :item/missing ?n]] $ ?n) [[?z]]]]`, nil},
		{"a missing? predicate before an empty correlated subquery",
			`[:find ?p :where [?p :item/age ?a] [(missing? $ ?a :item/other)] [(q [:find ?z :in $ ?a :where [?z :item/missing ?a]] $ ?a) [[?z]]]]`, nil},
		{"an arithmetic expression joined on its output with an empty pattern",
			`[:find ?p :where [?p :item/name ?n] [(+ ?n 1) ?m] [?q :item/missing ?m]]`, nil},
		{"an expression over an input alone beside an empty pattern",
			`[:find ?p :in $ ?k :where [?p :item/missing ?x] [(+ ?k 1) ?m]]`, []interface{}{"k"}},
		{"a ground expression beside an empty pattern",
			`[:find ?p :where [?p :item/missing ?x] [(+ "k" 1) ?m]]`, nil},
		{"a missing? predicate on the input of an empty pattern",
			`[:find ?p :where [?p :item/age ?a] [(missing? $ ?a :item/other)] [?q :item/missing ?a]]`, nil},
	}
	for _, mode := range legsFor(optimizerModes, algebraOption, fusionOption) {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(alice, name, "Alice"))
			require.NoError(t, tx.Add(bob, name, "Bob"))
			require.NoError(t, tx.Add(alice, age, int64(30)))
			require.NoError(t, tx.Add(bob, age, int64(40)))
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					got, err := executor.CollectTuples(db.Query(c.query, c.inputs...))
					require.NoError(t, err)
					require.Empty(t, got)
				})
			}
			// Each failing clause, with no empty clause beside it, answers
			// its failure.
			reachable := []string{
				`[:find ?p :where [?p :item/name ?n] [(+ ?n 1) ?m]]`,
				`[:find ?p :where [?p :item/age ?a] [(missing? $ ?a :item/other)]]`,
				`[:find ?p :where [?p :item/age ?a] [(get-else $ ?a :item/other "d") ?g]]`,
				`[:find ?p :where [?p :item/age ?a] [(get-some $ ?a :item/other) ?g]]`,
			}
			for _, q := range reachable {
				t.Run("reachable "+q, func(t *testing.T) {
					got, err := executor.CollectTuples(db.Query(q))
					require.Error(t, err, "the failing clause alone answered %d tuples", len(got))
				})
			}
		})
	}
}

// Two databases holding the same values under different entities answer one
// query alike: the failure contract reads a query as the conjunction of its
// clauses, so every plan, and every order a scan yields its tuples in,
// answers alike (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md §1). Each
// database holds alice's name, one numeric :item/v and one string :item/v,
// the entities' seeds varying the order the scan yields them in.
func TestReviewEval_AFailureDoesNotDependOnTheOrderAScanYields(t *testing.T) {
	name := datalog.NewKeyword(":item/name")
	v := datalog.NewKeyword(":item/v")
	const text = `[:find ?p :where [?p :item/name _] [?q :item/v ?a] [(+ ?a 1) ?b]]`
	for _, mode := range legsFor(optimizerModes) {
		t.Run(mode.name, func(t *testing.T) {
			for seed := 0; seed < 8; seed++ {
				t.Run(string(rune('a'+seed)), func(t *testing.T) {
					alice := datalog.NewIdentity("review-eval-order:alice")
					numeric := datalog.NewIdentity("review-eval-order:numeric-" + string(rune('a'+seed)))
					stringy := datalog.NewIdentity("review-eval-order:stringy-" + string(rune('a'+seed)))
					db := createOptimizerModeDB(t, mode, DatabaseOptions{})
					tx := db.NewTransaction()
					require.NoError(t, tx.Add(alice, name, "Alice"))
					require.NoError(t, tx.Add(numeric, v, int64(1)))
					require.NoError(t, tx.Add(stringy, v, "x"))
					_, err := tx.Commit()
					require.NoError(t, err)
					got, err := executor.CollectTuples(db.Query(text))
					require.ErrorContains(t, err, "is not a number", "the query answered %d tuples with no error", len(got))
				})
			}
		})
	}
}
