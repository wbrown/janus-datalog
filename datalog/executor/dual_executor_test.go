package executor

import (
	"fmt"
	"sort"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// ExecutorVariant defines an executor configuration variant for testing
type ExecutorVariant struct {
	Name string
	Opts planner.PlannerOptions
}

// DualExecutorVariants returns both executor variants for parity testing
func DualExecutorVariants() []ExecutorVariant {
	return []ExecutorVariant{
		{
			Name: "legacy",
			Opts: planner.PlannerOptions{UseLegacyExecutor: true},
		},
		{
			Name: "query_executor",
			Opts: planner.PlannerOptions{UseLegacyExecutor: false},
		},
	}
}

// DualExecutorVariantsWithBase returns variants with additional base options applied
func DualExecutorVariantsWithBase(base planner.PlannerOptions) []ExecutorVariant {
	legacyOpts := base
	legacyOpts.UseLegacyExecutor = true

	newOpts := base
	newOpts.UseLegacyExecutor = false

	return []ExecutorVariant{
		{Name: "legacy", Opts: legacyOpts},
		{Name: "query_executor", Opts: newOpts},
	}
}

// RunDualExecutorTest runs a test function against both executors
func RunDualExecutorTest(t *testing.T, matcher PatternMatcher, q *query.Query,
	validate func(t *testing.T, result Relation, variant string)) {
	t.Helper()

	for _, variant := range DualExecutorVariants() {
		t.Run(variant.Name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, variant.Opts)
			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("Execution failed: %v", err)
			}
			validate(t, result, variant.Name)
		})
	}
}

// RunDualExecutorTestWithOpts runs a test function against both executors with base options
func RunDualExecutorTestWithOpts(t *testing.T, matcher PatternMatcher, q *query.Query,
	baseOpts planner.PlannerOptions, validate func(t *testing.T, result Relation, variant string)) {
	t.Helper()

	for _, variant := range DualExecutorVariantsWithBase(baseOpts) {
		t.Run(variant.Name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, variant.Opts)
			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("Execution failed: %v", err)
			}
			validate(t, result, variant.Name)
		})
	}
}

// RunDualExecutorComparisonTest runs and compares results between executors
func RunDualExecutorComparisonTest(t *testing.T, matcher PatternMatcher, q *query.Query) {
	t.Helper()

	var results []Relation
	var names []string

	for _, variant := range DualExecutorVariants() {
		exec := NewExecutorWithOptions(matcher, variant.Opts)
		result, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("%s execution failed: %v", variant.Name, err)
		}
		results = append(results, result)
		names = append(names, variant.Name)
	}

	// Compare results
	if !RelationsEqual(results[0], results[1]) {
		t.Errorf("Results differ between %s and %s", names[0], names[1])
		t.Errorf("  %s columns: %v, size: %d", names[0], results[0].Columns(), results[0].Size())
		t.Errorf("  %s columns: %v, size: %d", names[1], results[1].Columns(), results[1].Size())
		t.Logf("%s tuples:", names[0])
		dumpRelationWithLimit(t, results[0], 10)
		t.Logf("%s tuples:", names[1])
		dumpRelationWithLimit(t, results[1], 10)
	}
}

// RunDualExecutorComparisonTestWithOpts runs and compares results with base options
func RunDualExecutorComparisonTestWithOpts(t *testing.T, matcher PatternMatcher, q *query.Query,
	baseOpts planner.PlannerOptions) {
	t.Helper()

	var results []Relation
	var names []string

	for _, variant := range DualExecutorVariantsWithBase(baseOpts) {
		exec := NewExecutorWithOptions(matcher, variant.Opts)
		result, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("%s execution failed: %v", variant.Name, err)
		}
		results = append(results, result)
		names = append(names, variant.Name)
	}

	if !RelationsEqual(results[0], results[1]) {
		t.Errorf("Results differ between %s and %s", names[0], names[1])
		t.Errorf("  %s columns: %v, size: %d", names[0], results[0].Columns(), results[0].Size())
		t.Errorf("  %s columns: %v, size: %d", names[1], results[1].Columns(), results[1].Size())
		t.Logf("%s tuples:", names[0])
		dumpRelationWithLimit(t, results[0], 10)
		t.Logf("%s tuples:", names[1])
		dumpRelationWithLimit(t, results[1], 10)
	}
}

// RelationsEqual compares two relations for equality (column names and sorted tuples)
func RelationsEqual(a, b Relation) bool {
	// Compare columns
	if !columnsEqual(a.Columns(), b.Columns()) {
		return false
	}

	// Collect and sort tuples for deterministic comparison
	aTuples := collectAndSortTuples(a)
	bTuples := collectAndSortTuples(b)

	if len(aTuples) != len(bTuples) {
		return false
	}

	for i := range aTuples {
		if !tuplesEqualComparison(aTuples[i], bTuples[i]) {
			return false
		}
	}

	return true
}

// RelationsEqualIgnoreColumnOrder compares relations allowing different column orders
func RelationsEqualIgnoreColumnOrder(a, b Relation) bool {
	aCols := a.Columns()
	bCols := b.Columns()

	if len(aCols) != len(bCols) {
		return false
	}

	// Build column index mapping from a to b
	colMap := make(map[int]int) // a index -> b index
	for i, aCol := range aCols {
		found := false
		for j, bCol := range bCols {
			if aCol == bCol {
				colMap[i] = j
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Collect tuples
	aTuples := collectAndSortTuplesReordered(a, nil)        // natural order
	bTuples := collectAndSortTuplesReordered(b, colMap) // reorder b to match a

	if len(aTuples) != len(bTuples) {
		return false
	}

	for i := range aTuples {
		if !tuplesEqualComparison(aTuples[i], bTuples[i]) {
			return false
		}
	}

	return true
}

// collectAndSortTuplesReordered collects tuples and optionally reorders columns
func collectAndSortTuplesReordered(rel Relation, colMap map[int]int) []Tuple {
	var tuples []Tuple
	it := rel.Iterator()
	for it.Next() {
		origTuple := it.Tuple()
		if colMap != nil {
			// Reorder tuple according to column mapping
			newTuple := make(Tuple, len(origTuple))
			for origIdx, newIdx := range colMap {
				newTuple[newIdx] = origTuple[origIdx]
			}
			tuples = append(tuples, newTuple)
		} else {
			tuples = append(tuples, origTuple)
		}
	}

	sort.Slice(tuples, func(i, j int) bool {
		return compareTuples(tuples[i], tuples[j]) < 0
	})

	return tuples
}

// FormatRelation formats a relation for display
func FormatRelation(rel Relation) string {
	var tuples []string
	it := rel.Iterator()
	count := 0
	for it.Next() && count < 20 {
		tuples = append(tuples, fmt.Sprintf("%v", it.Tuple()))
		count++
	}
	if rel.Size() > 20 {
		tuples = append(tuples, fmt.Sprintf("... and %d more", rel.Size()-20))
	}
	return fmt.Sprintf("columns=%v, tuples=%v", rel.Columns(), tuples)
}

func dumpRelationWithLimit(t *testing.T, rel Relation, limit int) {
	t.Helper()
	it := rel.Iterator()
	count := 0
	for it.Next() && count < limit {
		t.Logf("  [%d] %v", count, it.Tuple())
		count++
	}
	if rel.Size() > limit {
		t.Logf("  ... and %d more rows", rel.Size()-limit)
	}
}

// TestDualExecutorBasicParity validates the fixture works with simple queries
func TestDualExecutorBasicParity(t *testing.T) {
	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")

	datoms := []datalog.Datom{
		{E: alice, A: datalog.NewKeyword(":person/name"), V: "Alice", Tx: 1},
		{E: alice, A: datalog.NewKeyword(":person/age"), V: int64(30), Tx: 1},
		{E: bob, A: datalog.NewKeyword(":person/name"), V: "Bob", Tx: 1},
		{E: bob, A: datalog.NewKeyword(":person/age"), V: int64(25), Tx: 1},
	}

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "simple pattern",
			query: `[:find ?name :where [?e :person/name ?name]]`,
		},
		{
			name: "join on entity",
			query: `[:find ?name ?age
			         :where
			         [?e :person/name ?name]
			         [?e :person/age ?age]]`,
		},
		{
			name: "predicate filter",
			query: `[:find ?name
			         :where
			         [?e :person/name ?name]
			         [?e :person/age ?age]
			         [(> ?age 26)]]`,
		},
		{
			name: "expression",
			query: `[:find ?name ?doubled
			         :where
			         [?e :person/name ?name]
			         [?e :person/age ?age]
			         [(* ?age 2) ?doubled]]`,
		},
		{
			name:  "aggregation count",
			query: `[:find (count ?e) :where [?e :person/name ?name]]`,
		},
		{
			name:  "aggregation max",
			query: `[:find (max ?age) :where [?e :person/age ?age]]`,
		},
		{
			name: "grouped aggregation",
			query: `[:find ?name (count ?e)
			         :where [?e :person/name ?name]]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("failed to parse query: %v", err)
			}
			matcher := NewMemoryPatternMatcher(datoms)
			RunDualExecutorComparisonTest(t, matcher, q)
		})
	}
}

// TestRelationInputDualExecutorParity tests that RelationInput iteration works
// identically in both the legacy executor and the new QueryExecutor.
// This is a critical parity test since RelationInput is a special execution path.
func TestRelationInputDualExecutorParity(t *testing.T) {
	// Create test data
	nameAttr := datalog.NewKeyword(":name")
	ageAttr := datalog.NewKeyword(":age")
	yearAttr := datalog.NewKeyword(":year")

	datoms := []datalog.Datom{
		// Ages in different years
		{E: datalog.NewIdentity("a1"), A: nameAttr, V: "Alice", Tx: 1},
		{E: datalog.NewIdentity("a1"), A: yearAttr, V: int64(2020), Tx: 1},
		{E: datalog.NewIdentity("a1"), A: ageAttr, V: int64(25), Tx: 1},

		{E: datalog.NewIdentity("a2"), A: nameAttr, V: "Alice", Tx: 2},
		{E: datalog.NewIdentity("a2"), A: yearAttr, V: int64(2021), Tx: 2},
		{E: datalog.NewIdentity("a2"), A: ageAttr, V: int64(26), Tx: 2},

		{E: datalog.NewIdentity("a3"), A: nameAttr, V: "Bob", Tx: 3},
		{E: datalog.NewIdentity("a3"), A: yearAttr, V: int64(2020), Tx: 3},
		{E: datalog.NewIdentity("a3"), A: ageAttr, V: int64(30), Tx: 3},

		{E: datalog.NewIdentity("a4"), A: nameAttr, V: "Bob", Tx: 4},
		{E: datalog.NewIdentity("a4"), A: yearAttr, V: int64(2021), Tx: 4},
		{E: datalog.NewIdentity("a4"), A: ageAttr, V: int64(31), Tx: 4},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	ctx := NewContext(nil)

	tests := []struct {
		name       string
		queryStr   string
		inputCols  []query.Symbol
		inputTuples []Tuple
	}{
		{
			name: "simple RelationInput with two columns",
			queryStr: `[:find ?n ?y (max ?age)
			            :in $ [[?n ?y] ...]
			            :where [?e :name ?n]
			                   [?e :year ?y]
			                   [?e :age ?age]]`,
			inputCols: []query.Symbol{"?n", "?y"},
			inputTuples: []Tuple{
				{"Alice", int64(2020)},
				{"Alice", int64(2021)},
				{"Bob", int64(2020)},
				{"Bob", int64(2021)},
			},
		},
		{
			name: "single column RelationInput",
			queryStr: `[:find ?n (max ?age)
			            :in $ [[?n] ...]
			            :where [?e :name ?n]
			                   [?e :age ?age]]`,
			inputCols: []query.Symbol{"?n"},
			inputTuples: []Tuple{
				{"Alice"},
				{"Bob"},
			},
		},
		{
			name: "RelationInput with no aggregation",
			queryStr: `[:find ?n ?y ?age
			            :in $ [[?n ?y] ...]
			            :where [?e :name ?n]
			                   [?e :year ?y]
			                   [?e :age ?age]]`,
			inputCols: []query.Symbol{"?n", "?y"},
			inputTuples: []Tuple{
				{"Alice", int64(2020)},
				{"Bob", int64(2021)},
			},
		},
		{
			name: "RelationInput with predicate filter",
			queryStr: `[:find ?n ?y ?age
			            :in $ [[?n ?y] ...]
			            :where [?e :name ?n]
			                   [?e :year ?y]
			                   [?e :age ?age]
			                   [(> ?age 26)]]`,
			inputCols: []query.Symbol{"?n", "?y"},
			inputTuples: []Tuple{
				{"Alice", int64(2020)},
				{"Alice", int64(2021)},
				{"Bob", int64(2020)},
				{"Bob", int64(2021)},
			},
		},
		{
			name: "empty RelationInput",
			queryStr: `[:find ?n ?y (max ?age)
			            :in $ [[?n ?y] ...]
			            :where [?e :name ?n]
			                   [?e :year ?y]
			                   [?e :age ?age]]`,
			inputCols: []query.Symbol{"?n", "?y"},
			inputTuples: []Tuple{},
		},
		{
			name: "RelationInput with non-matching tuples",
			queryStr: `[:find ?n ?y (max ?age)
			            :in $ [[?n ?y] ...]
			            :where [?e :name ?n]
			                   [?e :year ?y]
			                   [?e :age ?age]]`,
			inputCols: []query.Symbol{"?n", "?y"},
			inputTuples: []Tuple{
				{"Charlie", int64(2020)},  // No Charlie in data
				{"Dave", int64(2021)},     // No Dave in data
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := parser.ParseQuery(tt.queryStr)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			// Create input relation
			inputRel := NewMaterializedRelation(tt.inputCols, tt.inputTuples)

			// Execute with both executors and compare
			var results []Relation
			var names []string

			for _, variant := range DualExecutorVariants() {
				exec := NewExecutorWithOptions(matcher, variant.Opts)
				result, err := exec.ExecuteWithRelations(ctx, parsed, []Relation{inputRel})
				if err != nil {
					t.Fatalf("%s execution failed: %v", variant.Name, err)
				}
				results = append(results, result)
				names = append(names, variant.Name)
			}

			// Compare results
			if !RelationsEqual(results[0], results[1]) {
				t.Errorf("Results differ between %s and %s", names[0], names[1])
				t.Errorf("  %s columns: %v, size: %d", names[0], results[0].Columns(), results[0].Size())
				t.Errorf("  %s columns: %v, size: %d", names[1], results[1].Columns(), results[1].Size())
				t.Logf("%s tuples:", names[0])
				dumpRelationWithLimit(t, results[0], 10)
				t.Logf("%s tuples:", names[1])
				dumpRelationWithLimit(t, results[1], 10)
			}
		})
	}
}
