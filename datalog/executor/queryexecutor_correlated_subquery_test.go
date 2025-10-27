package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestQueryExecutorCorrelatedSubquery tests that correlated subqueries work correctly
func TestQueryExecutorCorrelatedSubquery(t *testing.T) {
	// Create test data
	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	charlie := datalog.NewIdentity("charlie")

	nameKw := datalog.NewKeyword(":person/name")
	ageKw := datalog.NewKeyword(":person/age")
	groupKw := datalog.NewKeyword(":person/group")

	datoms := []datalog.Datom{
		{E: alice, A: nameKw, V: "Alice", Tx: 1},
		{E: alice, A: ageKw, V: int64(30), Tx: 1},
		{E: alice, A: groupKw, V: "A", Tx: 1},
		{E: bob, A: nameKw, V: "Bob", Tx: 1},
		{E: bob, A: ageKw, V: int64(25), Tx: 1},
		{E: bob, A: groupKw, V: "A", Tx: 1},
		{E: charlie, A: nameKw, V: "Charlie", Tx: 1},
		{E: charlie, A: ageKw, V: int64(35), Tx: 1},
		{E: charlie, A: groupKw, V: "B", Tx: 1},
	}

	// Query with correlated subquery:
	// For each group, find the max age in that group
	queryStr := `[:find ?group ?max-age
	              :where [?e :person/group ?group]
	                     [(q [:find (max ?a)
	                          :in $ ?g
	                          :where [?p :person/group ?g]
	                                 [?p :person/age ?a]]
	                         $ ?group) [[?max-age]]]]`

	q, err := parser.ParseQuery(queryStr)
	assert.NoError(t, err)

	// Test with QueryExecutor (Stage B)
	t.Run("QueryExecutor", func(t *testing.T) {
		matcher := NewIndexedMemoryMatcher(datoms)
		opts := planner.PlannerOptions{
			UseQueryExecutor: true,
		}
		exec := NewExecutorWithOptions(matcher, opts)
		result, err := exec.Execute(q)

		if err != nil {
			t.Logf("Error: %v", err)
			t.Logf("Result columns (if any): %v", result.Columns())
		}

		assert.NoError(t, err, "QueryExecutor should handle correlated subqueries")

		// Collect results
		it := result.Iterator()
		defer it.Close()

		results := make(map[string]int64)
		for it.Next() {
			tuple := it.Tuple()
			assert.Len(t, tuple, 2)
			group := tuple[0].(string)
			maxAge := tuple[1].(int64)
			results[group] = maxAge
		}

		// Group A has Alice (30) and Bob (25) -> max = 30
		// Group B has Charlie (35) -> max = 35
		assert.Equal(t, int64(30), results["A"], "Group A max age should be 30")
		assert.Equal(t, int64(35), results["B"], "Group B max age should be 35")
	})

	// Compare with legacy executor
	t.Run("LegacyExecutor", func(t *testing.T) {
		matcher := NewIndexedMemoryMatcher(datoms)
		opts := planner.PlannerOptions{
			UseQueryExecutor: false,
		}
		exec := NewExecutorWithOptions(matcher, opts)
		result, err := exec.Execute(q)

		assert.NoError(t, err, "LegacyExecutor should handle correlated subqueries")

		// Collect results
		it := result.Iterator()
		defer it.Close()

		results := make(map[string]int64)
		for it.Next() {
			tuple := it.Tuple()
			assert.Len(t, tuple, 2)
			group := tuple[0].(string)
			maxAge := tuple[1].(int64)
			results[group] = maxAge
		}

		assert.Equal(t, int64(30), results["A"], "Group A max age should be 30")
		assert.Equal(t, int64(35), results["B"], "Group B max age should be 35")
	})
}
