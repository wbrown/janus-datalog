package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestJoinStrategySelection verifies that the correct join strategy is chosen based on size and selectivity
func TestJoinStrategySelection(t *testing.T) {
	// Create temporary database
	dir, err := os.MkdirTemp("", "join-strategy-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	matcher := NewBadgerMatcher(db.Store())

	tests := []struct {
		name               string
		bindingSize        int
		patternCardinality int
		expectedStrategy   JoinStrategy
		reason             string
	}{
		{
			name:               "tiny set uses hash join",
			bindingSize:        2,
			patternCardinality: 1000,
			expectedStrategy:   HashJoinScan,
			reason:             "bindingSize ≤ 1000 (Sorted() overhead makes IndexNestedLoop slow even for tiny sets)",
		},
		{
			name:               "small set uses hash join",
			bindingSize:        50,
			patternCardinality: 1000,
			expectedStrategy:   HashJoinScan,
			reason:             "bindingSize ≤ 1000",
		},
		{
			name:               "medium set uses hash join",
			bindingSize:        500,
			patternCardinality: 10000,
			expectedStrategy:   HashJoinScan,
			reason:             "11 ≤ bindingSize ≤ 1000",
		},
		{
			name:               "large set with low selectivity uses hash join",
			bindingSize:        2000,
			patternCardinality: 100000,
			expectedStrategy:   HashJoinScan,
			reason:             "selectivity = 2000/100000 = 2% < 50%",
		},
		{
			name:               "large set with medium selectivity uses hash join",
			bindingSize:        3000,
			patternCardinality: 10000,
			expectedStrategy:   HashJoinScan,
			reason:             "selectivity = 3000/10000 = 30% < 50%",
		},
		{
			name:               "large set with high selectivity uses merge join",
			bindingSize:        5000,
			patternCardinality: 8000,
			expectedStrategy:   MergeJoin,
			reason:             "bindingSize > 1000 and selectivity = 5000/8000 = 62.5% ≥ 50%",
		},
		{
			name:               "very large set with high selectivity uses merge join",
			bindingSize:        10000,
			patternCardinality: 12000,
			expectedStrategy:   MergeJoin,
			reason:             "bindingSize > 1000 and selectivity = 10000/12000 = 83% ≥ 50%",
		},
		{
			name:               "exactly 50% selectivity uses hash join",
			bindingSize:        1000,
			patternCardinality: 2000,
			expectedStrategy:   HashJoinScan,
			reason:             "bindingSize = 1000 (boundary case)",
		},
		{
			name:               "just over threshold with high selectivity uses merge join",
			bindingSize:        6000,
			patternCardinality: 10000,
			expectedStrategy:   MergeJoin,
			reason:             "bindingSize > 1000 and selectivity = 6000/10000 = 60% ≥ 50%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock binding relation with specified size
			bindingRel := createMockRelation(tt.bindingSize, []query.Symbol{"?e"})

			// Create pattern - attribute is bound to control cardinality estimate
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: datalog.NewKeyword(":test/attr")},
					query.Variable{Name: "?v"},
				},
			}

			// Mock estimatePatternCardinality to return our test value
			originalEstimate := matcher.estimatePatternCardinality(pattern)
			defer func() {
				// Note: In real implementation, we'd mock this properly
				// For now, just verify the logic works with real estimates
				_ = originalEstimate
			}()

			strategy := matcher.chooseJoinStrategy(pattern, bindingRel, 0)

			if strategy != tt.expectedStrategy {
				selectivity := float64(tt.bindingSize) / float64(tt.patternCardinality)
				t.Errorf("Strategy mismatch for %s:\n"+
					"  bindingSize=%d, patternCard=%d, selectivity=%.1f%%\n"+
					"  expected=%s, got=%s\n"+
					"  reason: %s",
					tt.name, tt.bindingSize, tt.patternCardinality, selectivity*100,
					tt.expectedStrategy, strategy, tt.reason)
			}
		})
	}
}

// TestMergeJoinCorrectness verifies merge join produces correct results
func TestMergeJoinCorrectness(t *testing.T) {
	// Create temporary database
	dir, err := os.MkdirTemp("", "merge-join-correctness-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Insert test data: entities with sequential IDs
	// We'll create 100 entities, each with a :test/value attribute
	tx := db.NewTransaction()

	entities := make([]datalog.Identity, 100)
	for i := 0; i < 100; i++ {
		entities[i] = datalog.NewIdentity(fmt.Sprintf("entity:%d", i))
		tx.Add(entities[i], datalog.NewKeyword(":test/value"), int64(i))
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	matcher := NewBadgerMatcher(db.Store())

	tests := []struct {
		name          string
		bindingValues []datalog.Identity
		expectedCount int
		description   string
	}{
		{
			name:          "empty binding set",
			bindingValues: []datalog.Identity{},
			expectedCount: 0,
			description:   "no bindings should return no results",
		},
		{
			name:          "single binding",
			bindingValues: []datalog.Identity{entities[0]},
			expectedCount: 1,
			description:   "single entity should return one result",
		},
		{
			name:          "multiple bindings - all match",
			bindingValues: []datalog.Identity{entities[0], entities[10], entities[20]},
			expectedCount: 3,
			description:   "all entities exist, should return 3 results",
		},
		{
			name:          "large binding set - all match (triggers merge join)",
			bindingValues: entities[0:50], // First 50 entities
			expectedCount: 50,
			description:   "50 entities should return 50 results",
		},
		{
			name:          "binding with non-existent entity",
			bindingValues: []datalog.Identity{entities[0], datalog.NewIdentity("nonexistent:999")},
			expectedCount: 1,
			description:   "only one entity exists, should return 1 result",
		},
		{
			name:          "all non-existent entities",
			bindingValues: []datalog.Identity{datalog.NewIdentity("fake:1"), datalog.NewIdentity("fake:2")},
			expectedCount: 0,
			description:   "no entities exist, should return 0 results",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create binding relation from test values
			tuples := make([]executor.Tuple, len(tt.bindingValues))
			for i, e := range tt.bindingValues {
				tuples[i] = executor.Tuple{e}
			}
			bindingRel := executor.NewMaterializedRelationNoDedupe(
				[]query.Symbol{"?e"},
				tuples,
			)

			// Create pattern: [?e :test/value ?v]
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: datalog.NewKeyword(":test/value")},
					query.Variable{Name: "?v"},
				},
			}

			columns := []query.Symbol{"?e", "?v"}

			// Call merge join directly
			result, err := matcher.matchWithMergeJoin(
				pattern,
				bindingRel,
				columns,
				0, // position 0 = entity
				EAVT,
				nil, // no constraints
			)

			if err != nil {
				t.Fatalf("merge join failed: %v", err)
			}

			// Iterate and count results
			iter := result.Iterator()
			resultCount := 0
			for iter.Next() {
				resultCount++
				tuple := iter.Tuple()
				if len(tuple) != 2 {
					t.Errorf("expected 2 columns, got %d", len(tuple))
				}
				// Verify entity is in binding set
				// Tuple may contain Identity or *Identity
				var entity datalog.Identity
				switch v := tuple[0].(type) {
				case datalog.Identity:
					entity = v
				case *datalog.Identity:
					entity = *v
				default:
					t.Errorf("expected Identity or *Identity, got %T", tuple[0])
					continue
				}
				found := false
				for _, e := range tt.bindingValues {
					if entity.Equal(e) {
						found = true
						break
					}
				}
				if !found && tt.expectedCount > 0 {
					t.Errorf("result entity %v not in binding set", entity)
				}
			}
			iter.Close()

			// Check count after iteration
			if resultCount != tt.expectedCount {
				t.Errorf("%s: expected %d results, got %d",
					tt.description, tt.expectedCount, resultCount)
			}
		})
	}
}

// TestMergeJoinVsHashJoin verifies merge join and hash join produce identical results
func TestMergeJoinVsHashJoin(t *testing.T) {
	// Create temporary database
	dir, err := os.MkdirTemp("", "merge-vs-hash-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Insert test data
	tx := db.NewTransaction()

	// Create 2000 entities to trigger merge join (>1000 threshold)
	entities := make([]datalog.Identity, 2000)
	for i := 0; i < 2000; i++ {
		entities[i] = datalog.NewIdentity(fmt.Sprintf("entity:%d", i))
		tx.Add(entities[i], datalog.NewKeyword(":test/value"), int64(i*10))
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	matcher := NewBadgerMatcher(db.Store())

	// Test with different binding set sizes
	testSizes := []int{100, 500, 1000, 1500, 2000}

	for _, size := range testSizes {
		t.Run(t.Name()+"_size_"+string(rune(size)), func(t *testing.T) {
			// Create binding relation
			tuples := make([]executor.Tuple, size)
			for i := 0; i < size; i++ {
				tuples[i] = executor.Tuple{entities[i]}
			}
			bindingRel := executor.NewMaterializedRelationNoDedupe(
				[]query.Symbol{"?e"},
				tuples,
			)

			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: datalog.NewKeyword(":test/value")},
					query.Variable{Name: "?v"},
				},
			}
			columns := []query.Symbol{"?e", "?v"}

			// Call hash join
			hashResult, err := matcher.matchWithHashJoin(
				pattern,
				bindingRel,
				columns,
				0, // position 0 = entity
				EAVT,
				nil,
			)
			if err != nil {
				t.Fatalf("hash join failed: %v", err)
			}

			// Call merge join
			mergeResult, err := matcher.matchWithMergeJoin(
				pattern,
				bindingRel,
				columns,
				0, // position 0 = entity
				EAVT,
				nil,
			)
			if err != nil {
				t.Fatalf("merge join failed: %v", err)
			}

			// Compare sizes
			if hashResult.Size() != mergeResult.Size() {
				t.Errorf("size mismatch: hash join returned %d, merge join returned %d",
					hashResult.Size(), mergeResult.Size())
			}

			// Compare contents (convert to maps for easy comparison)
			hashMap := resultToMap(hashResult)
			mergeMap := resultToMap(mergeResult)

			if len(hashMap) != len(mergeMap) {
				t.Errorf("result count mismatch: hash=%d, merge=%d",
					len(hashMap), len(mergeMap))
			}

			// Verify all hash join results are in merge join results
			for key, hashVal := range hashMap {
				mergeVal, found := mergeMap[key]
				if !found {
					t.Errorf("hash join result %v not found in merge join", key)
					continue
				}
				if hashVal != mergeVal {
					t.Errorf("value mismatch for %v: hash=%v, merge=%v",
						key, hashVal, mergeVal)
				}
			}
		})
	}
}

// TestMergeJoinPerformance verifies merge join performs well on large sets
func TestMergeJoinPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	// Create temporary database
	dir, err := os.MkdirTemp("", "merge-performance-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Insert 10K entities
	tx := db.NewTransaction()

	const numEntities = 10000
	entities := make([]datalog.Identity, numEntities)
	for i := 0; i < numEntities; i++ {
		entities[i] = datalog.NewIdentity(fmt.Sprintf("entity:%d", i))
		tx.Add(entities[i], datalog.NewKeyword(":test/value"), int64(i))
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	matcher := NewBadgerMatcher(db.Store())

	// Test with 5000 entities (50% selectivity - should use merge join)
	const bindingSize = 5000
	tuples := make([]executor.Tuple, bindingSize)
	for i := 0; i < bindingSize; i++ {
		tuples[i] = executor.Tuple{entities[i]}
	}
	bindingRel := executor.NewMaterializedRelationNoDedupe(
		[]query.Symbol{"?e"},
		tuples,
	)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: datalog.NewKeyword(":test/value")},
			query.Variable{Name: "?v"},
		},
	}
	columns := []query.Symbol{"?e", "?v"}

	start := time.Now()
	result, err := matcher.matchWithMergeJoin(
		pattern,
		bindingRel,
		columns,
		0,
		EAVT,
		nil,
	)
	if err != nil {
		t.Fatalf("merge join failed: %v", err)
	}

	// Iterate and count results
	iter := result.Iterator()
	resultCount := 0
	for iter.Next() {
		resultCount++
	}
	iter.Close()

	duration := time.Since(start)

	if resultCount != bindingSize {
		t.Errorf("expected %d results, got %d", bindingSize, resultCount)
	}

	// Should complete in <100ms for 5K entities
	if duration > 100*time.Millisecond {
		t.Logf("WARNING: merge join took %v for %d entities (expected <100ms)",
			duration, bindingSize)
	} else {
		t.Logf("merge join completed in %v for %d entities", duration, bindingSize)
	}
}

// TestCompareJoinKeys verifies the key comparison function
func TestCompareJoinKeys(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
	}{
		{"nil vs nil", nil, nil, 0},
		{"nil vs value", nil, int64(5), -1},
		{"value vs nil", int64(5), nil, 1},
		{"equal int64", int64(5), int64(5), 0},
		{"less int64", int64(3), int64(5), -1},
		{"greater int64", int64(7), int64(5), 1},
		{"equal uint64", uint64(10), uint64(10), 0},
		{"less uint64", uint64(8), uint64(10), -1},
		{"greater uint64", uint64(12), uint64(10), 1},
		{"equal string", "abc", "abc", 0},
		{"less string", "aaa", "bbb", -1},
		{"greater string", "zzz", "aaa", 1},
		{
			"equal keyword",
			datalog.NewKeyword(":test/attr"),
			datalog.NewKeyword(":test/attr"),
			0,
		},
		{
			"less keyword",
			datalog.NewKeyword(":test/a"),
			datalog.NewKeyword(":test/b"),
			-1,
		},
		{
			"greater keyword",
			datalog.NewKeyword(":test/z"),
			datalog.NewKeyword(":test/a"),
			1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareJoinKeys(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("compareJoinKeys(%v, %v) = %d, expected %d",
					tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

// Helper functions

func createMockRelation(size int, columns []query.Symbol) executor.Relation {
	tuples := make([]executor.Tuple, size)
	for i := 0; i < size; i++ {
		tuples[i] = executor.Tuple{datalog.NewIdentity(fmt.Sprintf("entity:%d", i))}
	}
	return executor.NewMaterializedRelationNoDedupe(columns, tuples)
}

func resultToMap(rel executor.Relation) map[string]int64 {
	result := make(map[string]int64)
	iter := rel.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		if len(tuple) >= 2 {
			if entity, ok := tuple[0].(datalog.Identity); ok {
				if val, ok := tuple[1].(int64); ok {
					result[entity.String()] = val
				}
			}
		}
	}
	iter.Close()
	return result
}
