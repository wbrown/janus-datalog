package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestAlgebraMatrix_ComparisonBindingOr runs the comparison binding + or
// subquery query with and without the algebra optimizer.
func TestAlgebraMatrix_ComparisonBindingOr(t *testing.T) {
	eachMode(t, testAlgebraMatrixComparisonBindingOr, algebraOption, sharingOption, fusionOption, aggregationOption)
}

func testAlgebraMatrixComparisonBindingOr(t *testing.T, db *storage.Database) {
	tx := db.NewTransaction()
	s1 := datalog.NewIdentity("scenario:1")
	s2 := datalog.NewIdentity("scenario:2")
	s3 := datalog.NewIdentity("scenario:3")
	t1 := datalog.NewIdentity("task:1")
	t2 := datalog.NewIdentity("task:2")
	t3 := datalog.NewIdentity("task:3")

	nameAttr := datalog.NewKeyword(":scenario/name")
	taskAttr := datalog.NewKeyword(":scenario/task")

	require.NoError(t, tx.Add(s1, nameAttr, "Scenario One"))
	require.NoError(t, tx.Add(s1, taskAttr, t1))
	require.NoError(t, tx.Add(s1, taskAttr, t2))
	require.NoError(t, tx.Add(s2, nameAttr, "Scenario Two"))
	require.NoError(t, tx.Add(s2, taskAttr, t3))
	require.NoError(t, tx.Add(s3, nameAttr, "Scenario Three"))
	_, err := tx.Commit()
	require.NoError(t, err)

	queryStr := `[:find ?scenario ?name ?taskCount ?complete
	              :where [?scenario :scenario/name ?name]
	                     (or [(q [:find (count ?t)
	                              :in $ ?scenario
	                              :where [?scenario :scenario/task ?t]]
	                            $ ?scenario) [[?taskCount]]]
	                         [(ground 0) ?taskCount])
	                     [(> ?taskCount 0) ?complete]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatal(err)
	}

	// The executor carries the database's planner options, which eachBackendAndMode
	// set from the mode.
	tuples, err := executor.CollectTuples(db.NewExecutor().Execute(q))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// or is the union of its branches: every scenario takes the ground 0, and
	// a scenario with tasks also takes its count. A scenario with none counts
	// nothing. :scenario/task is undeclared, so it resolves as cardinality one
	// and s1's later task supersedes its earlier one.
	assert.ElementsMatch(t, [][]interface{}{
		{s1, "Scenario One", int64(1), true},
		{s1, "Scenario One", int64(0), false},
		{s2, "Scenario Two", int64(1), true},
		{s2, "Scenario Two", int64(0), false},
		{s3, "Scenario Three", int64(0), false},
	}, tuples)
}
