package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestHashJoinColumnIndexBug tests the bug where HashJoinScan confused
// datom position with column index in the binding relation.
//
// Bug scenario:
// - Pattern: [?e :attr ?ref] where ?ref comes from a binding relation
// - ?ref is at datom position 2 (V position)
// - But ?ref is at column index 0 in the binding relation (first/only column)
// - buildHashSet was using position=2 instead of columnIndex=0
// - Tried to access tuple[2] when tuple only had length 1
// - Result: Empty hash set → no matches
//
// This bug was hidden because:
// - With threshold ≤2, IndexNestedLoop was used for small binding sets
// - Only appeared when we changed threshold to 0, making HashJoinScan the default
// - Most benchmarks had patterns where datom position == column index
func TestHashJoinColumnIndexBug(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	assert.NoError(t, err)
	defer db.Close()

	// Create schema-like data: symbol entity referenced by price entities
	symbolKw := datalog.NewKeyword(":symbol/ticker")
	priceSymbol := datalog.NewKeyword(":price/symbol")
	priceValue := datalog.NewKeyword(":price/value")

	tx := db.NewTransaction()

	// Create symbol entity
	symbolEntity := datalog.NewIdentity("AAPL")
	err = tx.Add(symbolEntity, symbolKw, "AAPL")
	assert.NoError(t, err)

	// Create price entities that reference the symbol
	for i := 0; i < 5; i++ {
		priceEntity := datalog.NewIdentity("price-" + string(rune('A'+i)))
		err = tx.Add(priceEntity, priceSymbol, symbolEntity)
		assert.NoError(t, err)
		err = tx.Add(priceEntity, priceValue, float64(100+i))
		assert.NoError(t, err)
	}

	_, err = tx.Commit()
	assert.NoError(t, err)

	// Query that triggers the bug:
	// 1. [?s :symbol/ticker "AAPL"] returns binding with columns=[?s]
	// 2. [?e :price/symbol ?s] joins on ?s
	//    - ?s is at datom position 2 (V position in the pattern)
	//    - But ?s is at column index 0 in the binding relation
	//    - Bug: used position=2 to access tuple[2], but tuple only has length 1
	queryStr := `[:find ?e ?value
	              :where [?s :symbol/ticker "AAPL"]
	                     [?e :price/symbol ?s]
	                     [?e :price/value ?value]]`

	q, err := parser.ParseQuery(queryStr)
	assert.NoError(t, err)

	// Force HashJoinScan by setting threshold to 0 (default behavior after fix)
	matcher := NewBadgerMatcherWithOptions(db.Store(), executor.ExecutorOptions{
		IndexNestedLoopThreshold: 0, // Always use HashJoinScan
	})
	exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{})

	result, err := exec.Execute(q)
	assert.NoError(t, err)
	assert.False(t, result.IsEmpty(), "Should have results when HashJoinScan correctly uses column index")

	// Should find all 5 price entities
	assert.Equal(t, 5, result.Size(), "Should find 5 price entities")

	// Verify we actually got the right entities
	it := result.Iterator()
	defer it.Close()
	count := 0
	for it.Next() {
		tuple := it.Tuple()
		assert.Len(t, tuple, 2, "Should have 2 columns: ?e and ?value")
		// Verify entity is an Identity or pointer to Identity
		switch v := tuple[0].(type) {
		case datalog.Identity, *datalog.Identity:
			// Valid Identity type
		default:
			t.Errorf("First column should be Identity or *Identity, got %T: %v", v, v)
		}
		// Verify value is a float64
		value, ok := tuple[1].(float64)
		assert.True(t, ok, "Second column should be float64, got %T", tuple[1])
		assert.GreaterOrEqual(t, value, 100.0)
		assert.LessOrEqual(t, value, 104.0)
		count++
	}
	assert.Equal(t, 5, count, "Iterator should return 5 tuples")
}

// TestHashJoinColumnIndexMultiColumn tests the fix works with multiple columns
// where the join variable is not the first column.
func TestHashJoinColumnIndexMultiColumn(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	assert.NoError(t, err)
	defer db.Close()

	// Create data
	attr1 := datalog.NewKeyword(":attr1")
	attr2 := datalog.NewKeyword(":attr2")
	attr3 := datalog.NewKeyword(":attr3")

	tx := db.NewTransaction()

	// Entity A with attr1=X and attr2=Y
	entityA := datalog.NewIdentity("A")
	tx.Add(entityA, attr1, "X")
	tx.Add(entityA, attr2, "Y")

	// Entity B references Y via attr3
	entityB := datalog.NewIdentity("B")
	tx.Add(entityB, attr3, "Y")

	_, err = tx.Commit()
	assert.NoError(t, err)

	// Query where join variable is the second column:
	// 1. [?e1 :attr1 ?x] [?e1 :attr2 ?y] returns columns=[?e1, ?x, ?y]
	//    (assuming phases separate these, second might be columns=[?y])
	// 2. [?e2 :attr3 ?y] joins on ?y
	//    - ?y is at datom position 2 (V)
	//    - ?y's column index depends on result of first patterns
	queryStr := `[:find ?e1 ?e2
	              :where [?e1 :attr1 "X"]
	                     [?e1 :attr2 ?y]
	                     [?e2 :attr3 ?y]]`

	q, err := parser.ParseQuery(queryStr)
	assert.NoError(t, err)

	matcher := NewBadgerMatcherWithOptions(db.Store(), executor.ExecutorOptions{
		IndexNestedLoopThreshold: 0,
	})
	exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{})

	result, err := exec.Execute(q)
	assert.NoError(t, err)
	assert.False(t, result.IsEmpty(), "Should find the joined entities")
	assert.Equal(t, 1, result.Size(), "Should find one pair")

	it := result.Iterator()
	defer it.Close()
	assert.True(t, it.Next())
	tuple := it.Tuple()

	// Verify we got Identity types (the actual values are hashes, not the original strings)
	switch v := tuple[0].(type) {
	case datalog.Identity, *datalog.Identity:
		// Valid Identity type
	default:
		t.Fatalf("Expected Identity for first column, got %T", v)
	}
	switch v := tuple[1].(type) {
	case datalog.Identity, *datalog.Identity:
		// Valid Identity type
	default:
		t.Fatalf("Expected Identity for second column, got %T", v)
	}

	// Both entities should be present and different
	// (we can't easily compare against "A" and "B" since Identity uses hashes)
}
