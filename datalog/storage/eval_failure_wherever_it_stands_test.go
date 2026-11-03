package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A failure at an assignment where every other clause holds fails the query
// wherever the failing clause stands and whichever reader reads it: in one of
// two groups the find reads only for their existence, under an :order-by with
// a :limit, under a :limit over a find spanning the failing group and
// another, and in a correlated subquery's body, an or-join branch or a
// not-join body run over the group in windows
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md §1, the failure contract).
func TestAFailureWhereEveryOtherClauseHoldsFailsTheQueryWhereverItStands(t *testing.T) {
	name := datalog.NewKeyword(":item/name")
	v := datalog.NewKeyword(":item/v")
	w := datalog.NewKeyword(":item/w")
	k := datalog.NewKeyword(":item/k")
	cases := []struct {
		name  string
		query string
	}{
		{"two groups the find does not read, the failing group second",
			`[:find ?p :where [?p :item/name _] [?r :item/w ?c] [(+ ?c 1) ?d] [?q :item/v ?a] [(+ ?a 1) ?b]]`},
		{"two groups the find does not read, the failing group first",
			`[:find ?p :where [?p :item/name _] [?q :item/v ?a] [(+ ?a 1) ?b] [?r :item/w ?c] [(+ ?c 1) ?d]]`},
		{"an order-by with a limit",
			`[:find ?n ?b :where [?q :item/k ?n] [?q :item/v ?a] [(+ ?a 1) ?b] :order-by [?n] :limit 1]`},
		{"a limit over a find spanning the failing group and another",
			`[:find ?p ?b :where [?p :item/name _] [?q :item/v ?a] [(+ ?a 1) ?b] :limit 1]`},
		{"a correlated subquery's body run per window",
			`[:find ?p :where [?p :item/name _] [?q :item/v ?a] [(q [:find ?z :in $ ?a :where [(+ ?a 1) ?z]] $ ?a) [[?z]]]]`},
		{"an or-join branch run over the group",
			`[:find ?p :where [?p :item/name _] [?q :item/v ?a] (or-join [[?a]] (and [(+ ?a 1) ?b] [(> ?b -1)]))]`},
		{"a not-join body run over the group",
			`[:find ?p :where [?p :item/name _] [?q :item/v ?a] (not-join [?a] [(+ ?a 1) ?b] [(< ?b -1)])]`},
		{"an or-join with no required run, a group the find does not read",
			`[:find ?p :where [?p :item/name _] (or-join [?q] (and [?q :item/v ?a] [(+ ?a 1) ?b]))]`},
		{"an or-default-join with no required run, a group the find does not read",
			`[:find ?p :where [?p :item/name _] (or-default-join [?q] (and [?q :item/v ?a] [(+ ?a 1) ?b]) [(ground 0) ?q])]`},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("wherever:alice"), name, "Alice"))
			// Sixteen numeric values beside one string, each with an ordinal,
			// so whichever tuple a reader takes first, the failing one is
			// among those left.
			for i := 0; i < 16; i++ {
				e := datalog.NewIdentity(fmt.Sprintf("wherever:n%d", i))
				require.NoError(t, tx.Add(e, v, int64(i)))
				require.NoError(t, tx.Add(e, k, int64(i)))
			}
			stringy := datalog.NewIdentity("wherever:stringy")
			require.NoError(t, tx.Add(stringy, v, "x"))
			require.NoError(t, tx.Add(stringy, k, int64(16)))
			for i := 0; i < 4; i++ {
				require.NoError(t, tx.Add(datalog.NewIdentity(fmt.Sprintf("wherever:w%d", i)), w, int64(i)))
			}
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					got, err := executor.CollectTuples(d.Query(c.query))
					require.ErrorContains(t, err, "is not a number", "the query answered %d tuples with no error", len(got))
				})
			}
		})
	}
}

// A missing? over a value that names no entity fails the query at an
// assignment where every other clause holds, when the missing? stands in a
// not-join body, an or-join branch or a correlated subquery's body run in
// windows over a group the find reads only for its existence
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md §1, the failure contract).
func TestAMissingFailureWhereEveryOtherClauseHoldsFailsTheQueryFromAWindowedBody(t *testing.T) {
	name := datalog.NewKeyword(":item/name")
	ref := datalog.NewKeyword(":item/ref")
	cases := []struct {
		name  string
		query string
	}{
		{"a not-join body",
			`[:find ?p :where [?p :item/name _] [?q :item/ref ?a] (not-join [?a] [(missing? $ ?a :item/name)])]`},
		{"an or-join branch",
			`[:find ?p :where [?p :item/name _] [?q :item/ref ?a] (or-join [[?a]] [(missing? $ ?a :item/absent)])]`},
		{"a correlated subquery's body",
			`[:find ?p :where [?p :item/name _] [?q :item/ref ?a] [(q [:find ?z :in $ ?a :where [(missing? $ ?a :item/absent)] [(ground 1) ?z]] $ ?a) [[?z]]]]`},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			alice := datalog.NewIdentity("missing-window:alice")
			require.NoError(t, tx.Add(alice, name, "Alice"))
			// Sixteen references to alice and one integer, which names no
			// entity, so the failing value is one tuple of seventeen.
			for i := 0; i < 16; i++ {
				require.NoError(t, tx.Add(datalog.NewIdentity(fmt.Sprintf("missing-window:r%d", i)), ref, alice))
			}
			require.NoError(t, tx.Add(datalog.NewIdentity("missing-window:number"), ref, int64(7)))
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					got, err := executor.CollectTuples(d.Query(c.query))
					require.ErrorContains(t, err, "expected Identity", "the query answered %d tuples with no error", len(got))
				})
			}
		})
	}
}

// A failure beside a clause that holds at no assignment fails no query, and
// the same failing clause beside clauses that each hold somewhere fails it.
// The failing clause extends the find's group beside an empty group, or
// stands in a group the find reads only for its existence beside the find's
// empty group, failing at the group's first tuple or past it. A find spanning
// a failing group and an empty one and an aggregating find over the two read
// them together, and a predicate, an expression and a join on the failing
// expression's output each join them. A comparison reads the failing group's
// output, and a missing? reads an input alone or a constant. A correlated
// subquery over the failing group answers the group's other tuples where its
// body holds at no key of the failing tuple, and the failure where its body
// holds at that key (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md §1, the
// failure contract).
func TestAFailureBesideAClauseHoldingNowhereFailsNoQuery(t *testing.T) {
	name := datalog.NewKeyword(":item/name")
	age := datalog.NewKeyword(":item/age")
	v := datalog.NewKeyword(":item/v")
	k := datalog.NewKeyword(":item/k")
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			alice := datalog.NewIdentity("nowhere:alice")
			require.NoError(t, tx.Add(alice, name, "Alice"))
			require.NoError(t, tx.Add(alice, age, int64(30)))
			// The values of :item/v and :item/k and their entities are the
			// fixture's above, built so a scan yields a numeric value before the
			// string.
			for i := 0; i < 16; i++ {
				e := datalog.NewIdentity(fmt.Sprintf("wherever:n%d", i))
				require.NoError(t, tx.Add(e, v, int64(i)))
				require.NoError(t, tx.Add(e, k, int64(i)))
			}
			stringy := datalog.NewIdentity("wherever:stringy")
			require.NoError(t, tx.Add(stringy, v, "x"))
			require.NoError(t, tx.Add(stringy, k, int64(16)))
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, c := range []struct {
				name   string
				query  string
				inputs []interface{}
			}{
				{"a failing ground expression extending the find's group beside an empty group",
					`[:find ?q :where [?q :item/name _] [(+ "k" 1) ?m] [?p :item/missing ?x]]`, nil},
				{"a failing missing? predicate's group beside the find's empty group",
					`[:find ?p :where [?q :item/age ?a] [(missing? $ ?a :item/other)] [?p :item/missing ?x]]`, nil},
				{"a group failing at its first tuple beside the find's empty group",
					`[:find ?p :where [?q :item/name ?n] [(+ ?n 1) ?m] [?p :item/missing ?x]]`, nil},
				{"a group failing past its first tuple beside the find's empty group",
					`[:find ?p :where [?q :item/v ?a] [(+ ?a 1) ?b] [?p :item/missing ?x]]`, nil},
				{"a comparison over a failing group beside the find's empty group",
					`[:find ?p :where [?q :item/v ?a] [(+ ?a 1) ?b] [(< ?b 100)] [?p :item/missing ?x]]`, nil},
				{"a missing? over an input alone beside an empty pattern",
					`[:find ?p :in $ ?k :where [?p :item/missing ?x] [(missing? $ ?k :item/other)]]`, []interface{}{int64(7)}},
				{"a missing? over a constant beside an empty pattern",
					`[:find ?p :where [?p :item/missing ?x] [(missing? $ 7 :item/other)]]`, nil},
				{"a find spanning a failing group and an empty group",
					`[:find ?q ?p :where [?q :item/v ?a] [(+ ?a 1) ?b] [?p :item/missing ?x]]`, nil},
				{"an aggregating find over a failing group and an empty group",
					`[:find ?p (count ?q) :where [?q :item/v ?a] [(+ ?a 1) ?b] [?p :item/missing ?x]]`, nil},
				{"a predicate over a failing group and an empty group",
					`[:find ?q :where [?q :item/v ?a] [(+ ?a 1) ?b] [?p :item/missing ?x] [(< ?b ?x)]]`, nil},
				{"an expression over a failing group and an empty group",
					`[:find ?q :where [?q :item/v ?a] [(+ ?a 1) ?b] [?p :item/missing ?x] [(str ?b ?x) ?s]]`, nil},
				{"a failing group joined with an empty group on the failing expression's output",
					`[:find ?q :where [?q :item/v ?a] [(+ ?a 1) ?b] [?p :item/missing ?x] [(identity ?x) ?b]]`, nil},
				{"the empty group joined with a failing group on the failing expression's output",
					`[:find ?q :where [?p :item/missing ?x] [(identity ?x) ?b] [?q :item/v ?a] [(+ ?a 1) ?b]]`, nil},
				{"a find spanning a group failing at its first tuple and an empty group",
					`[:find ?q ?p :where [?q :item/name ?n] [(+ ?n 1) ?m] [?p :item/missing ?x]]`, nil},
				{"a find spanning an empty group and a group failing at its first tuple",
					`[:find ?q ?p :where [?p :item/missing ?x] [?q :item/name ?n] [(+ ?n 1) ?m]]`, nil},
				{"an aggregating find over a group failing at its first tuple and an empty group",
					`[:find ?p (count ?q) :where [?q :item/name ?n] [(+ ?n 1) ?m] [?p :item/missing ?x]]`, nil},
				{"an aggregating find over an empty group and a group failing at its first tuple",
					`[:find ?p (count ?q) :where [?p :item/missing ?x] [?q :item/name ?n] [(+ ?n 1) ?m]]`, nil},
				{"a predicate over a failing group and a group with a tuple before an empty pattern",
					`[:find ?q :where [?q :item/v ?a] [(+ ?a 1) ?b] [?r :item/age ?c] [(< ?b ?c)] [?p :item/missing ?x]]`, nil},
				{"an expression over a failing group and a group with a tuple before an empty pattern",
					`[:find ?q :where [?q :item/v ?a] [(+ ?a 1) ?b] [?r :item/age ?c] [(+ ?b ?c) ?s] [?p :item/missing ?x]]`, nil},
			} {
				t.Run(c.name, func(t *testing.T) {
					got, err := executor.CollectTuples(d.Query(c.query, c.inputs...))
					require.NoError(t, err, "no assignment holds the empty clause, so no failure is raised")
					require.Empty(t, got)
				})
			}
			for _, c := range []struct {
				query  string
				inputs []interface{}
			}{
				{`[:find ?q :where [?q :item/name _] [(+ "k" 1) ?m]]`, nil},
				{`[:find ?q :where [?q :item/age ?a] [(missing? $ ?a :item/other)]]`, nil},
				{`[:find ?q :where [?q :item/name ?n] [(+ ?n 1) ?m]]`, nil},
				{`[:find ?q ?b :where [?q :item/v ?a] [(+ ?a 1) ?b]]`, nil},
				{`[:find ?q ?b :where [?q :item/v ?a] [(+ ?a 1) ?b] [(< ?b 100)]]`, nil},
				{`[:find ?q :in $ ?k :where [?q :item/name _] [(missing? $ ?k :item/other)]]`, []interface{}{int64(7)}},
				{`[:find ?q :where [?q :item/name _] [(missing? $ 7 :item/other)]]`, nil},
			} {
				t.Run("reachable "+c.query, func(t *testing.T) {
					got, err := executor.CollectTuples(d.Query(c.query, c.inputs...))
					require.Error(t, err, "the failing clause alone answered %d tuples", len(got))
				})
			}
			t.Run("a correlated subquery holding at no key of the failing tuple", func(t *testing.T) {
				got, err := executor.CollectTuples(d.Query(`[:find ?q ?z :where [?q :item/v ?a] [(+ ?a 1) ?b] [(q [:find ?z :in $ ?a :where [?z :item/k ?a]] $ ?a) [[?z]]]]`))
				require.NoError(t, err, "the body holds at no key of the failing tuple, so its failure is not raised")
				require.Len(t, got, 16, "each numeric key's tuple is answered")
			})
			t.Run("a correlated subquery holding at the failing tuple's key", func(t *testing.T) {
				got, err := executor.CollectTuples(d.Query(`[:find ?q ?z :where [?q :item/v ?a] [(+ ?a 1) ?b] [(q [:find ?z :in $ ?a :where [?z :item/v ?a]] $ ?a) [[?z]]]]`))
				require.ErrorContains(t, err, "is not a number", "the query answered %d tuples with no error", len(got))
			})
			t.Run("a not-join whose body holds at the failing tuple's key", func(t *testing.T) {
				got, err := executor.CollectTuples(d.Query(`[:find ?q :where [?q :item/v ?a] [(+ ?a 1) ?b] (not-join [?a] [(= ?a "x")])]`))
				require.NoError(t, err, "the not-join holds nowhere at the failing tuple's key, so its failure is not raised")
				require.Len(t, got, 16, "each numeric key's tuple is answered")
			})
		})
	}
}

// A destructuring binding reads a database function's value whatever the
// function's entity, and fails on a value of another shape. Such a failure is
// the query's error where every other clause holds, through a limit and
// through the existence verdict. It fails no query beside a clause holding
// nowhere, a comparison over the binding's output standing between them
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md §1, the failure contract).
func TestADestructuredLookupFailsTheQueryOnlyWhereEveryOtherClauseHolds(t *testing.T) {
	name := datalog.NewKeyword(":item/name")
	n := datalog.NewKeyword(":t/n")
	s := datalog.NewKeyword(":t/s")
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("destructured:alice"), name, "Alice"))
			for i := 0; i < 16; i++ {
				require.NoError(t, tx.Add(datalog.NewIdentity(fmt.Sprintf("destructured:n%d", i)), n, int64(i)))
			}
			stringy := datalog.NewIdentity("destructured:stringy")
			require.NoError(t, tx.Add(stringy, n, int64(16)))
			require.NoError(t, tx.Add(stringy, s, "x"))
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, c := range []struct{ name, query string }{
				{"a limit over a get-else's tuple form",
					`[:find ?e ?a :where [?e :t/n _] [(get-else $ ?e :t/s [0 0]) [?a ?b]] :limit 1]`},
				{"a limit over a get-else's collection form",
					`[:find ?e ?a :where [?e :t/n _] [(get-else $ ?e :t/s [0]) [?a ...]] :limit 1]`},
				{"the existence verdict over a get-else's tuple form",
					`[:find ?p :where [?p :item/name _] [?e :t/n _] [(get-else $ ?e :t/s [0 0]) [?a ?b]]]`},
			} {
				t.Run(c.name, func(t *testing.T) {
					got, err := executor.CollectTuples(d.Query(c.query))
					require.ErrorContains(t, err, "requires", "the query answered %d tuples with no error", len(got))
				})
			}
			for _, c := range []struct{ name, query string }{
				{"a comparison over a get-else's tuple form before an empty pattern",
					`[:find ?e :where [?e :t/n _] [(get-else $ ?e :t/s [0 0]) [?a ?b]] [(< ?a 100)] [?p :item/missing ?x]]`},
				{"a comparison over a get-some's relation form before an empty pattern",
					`[:find ?e :where [?e :t/n _] [(get-some $ ?e :t/s) [[?a ?b] ...]] [(< ?a 100)] [?p :item/missing ?x]]`},
			} {
				t.Run(c.name, func(t *testing.T) {
					got, err := executor.CollectTuples(d.Query(c.query))
					require.NoError(t, err, "no assignment holds the empty pattern, so no failure is raised")
					require.Empty(t, got)
				})
			}
		})
	}
}
