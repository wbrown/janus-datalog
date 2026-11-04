package main

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// The CLI's single query answers a :find writing an aggregate before a group
// variable, and the executor it runs the query on answers the :find's
// elements in written order.
func TestTheCLIAnswersAnAggregateFirstFindInWrittenOrder(t *testing.T) {
	db, err := storage.NewDatabase(filepath.Join(t.TempDir(), "written-order.db"))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	tx := db.NewTransaction()
	for i, k := range []int64{1, 1, 2} {
		entity := datalog.NewIdentity(fmt.Sprintf("cli-written-order:e%d", i))
		require.NoError(t, tx.Add(entity, datalog.NewKeyword(":o/k"), k))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	text := `[:find (count ?e) ?k :where [?e :o/k ?k]]`
	require.NoError(t, runSingleQuery(db, text, false, nil), "the CLI answers the query")

	q, err := parser.ParseQuery(text)
	require.NoError(t, err)
	result, err := db.NewExecutor().Execute(q)
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{datalog.NewSymbol("(count ?e)"), datalog.NewSymbol("?k")}, result.Symbols(),
		"the result carries the find's elements in written order")
}
