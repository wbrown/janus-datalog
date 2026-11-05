package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// The CLI's single query ends as a query that fails to execute when an
// expression fails at an assignment where every other clause holds (commit
// a08c3dc: the CLI reads a result to its end before rendering it, so a
// failure reading it is the query's error).
func TestReviewEval_CLIAFailureWhereEveryOtherClauseHoldsIsTheQuerysError(t *testing.T) {
	name := datalog.NewKeyword(":item/name")
	v := datalog.NewKeyword(":item/v")
	alice := datalog.NewIdentity("review-eval-cli:alice")
	type fact struct {
		e datalog.Identity
		a datalog.Keyword
		v interface{}
	}
	facts := []fact{{alice, name, "Alice"}}
	for i := 0; i < 16; i++ {
		numeric := datalog.NewIdentity("review-eval-cli:n" + string(rune('a'+i)))
		facts = append(facts, fact{numeric, v, int64(i)})
	}
	stringy := datalog.NewIdentity("review-eval-cli:stringy")
	facts = append(facts, fact{stringy, v, "x"})

	db, err := storage.NewDatabase(filepath.Join(t.TempDir(), "review.db"))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	tx := db.NewTransaction()
	for _, f := range facts {
		require.NoError(t, tx.Add(f.e, f.a, f.v))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	cases := []struct {
		name  string
		query string
	}{
		{"the expression's group read only for its existence",
			`[:find ?p :where [?p :item/name _] [?q :item/v ?a] [(+ ?a 1) ?b]]`},
		{"the expression's whole relation",
			`[:find ?q ?b :where [?q :item/v ?a] [(+ ?a 1) ?b]]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.ErrorContains(t, runSingleQuery(db, c.query, false, nil), "is not a number")
		})
	}
}
