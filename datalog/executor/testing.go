package executor

import (
	"fmt"
	"sort"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestExecutorVariant defines an executor configuration variant for testing.
// Exported for use by external test packages.
type TestExecutorVariant struct {
	Name string
	Opts planner.PlannerOptions
}

// DualTestExecutorVariants returns executor variants for testing.
// Now returns only the QueryExecutor variant since the legacy path was removed.
// Exported for use by external test packages.
func DualTestExecutorVariants() []TestExecutorVariant {
	return []TestExecutorVariant{
		{
			Name: "query_executor",
			Opts: planner.PlannerOptions{},
		},
	}
}

// DualTestExecutorVariantsWithBase returns variants with additional base options applied.
// Now returns only the QueryExecutor variant since the legacy path was removed.
// Exported for use by external test packages.
func DualTestExecutorVariantsWithBase(base planner.PlannerOptions) []TestExecutorVariant {
	return []TestExecutorVariant{
		{Name: "query_executor", Opts: base},
	}
}

// CompareRelations compares two relations for equality (column names and sorted tuples).
// Returns true if they are equal.
func CompareRelations(a, b Relation) bool {
	// Compare columns
	if !compareColumns(a.Columns(), b.Columns()) {
		return false
	}

	// Collect and sort tuples for deterministic comparison
	aTuples := collectSortedTuples(a)
	bTuples := collectSortedTuples(b)

	if len(aTuples) != len(bTuples) {
		return false
	}

	for i := range aTuples {
		if !compareTuplesEqual(aTuples[i], bTuples[i]) {
			return false
		}
	}

	return true
}

// CompareRelationsIgnoreColumnOrder compares relations allowing different column orders.
func CompareRelationsIgnoreColumnOrder(a, b Relation) bool {
	aCols := a.Columns()
	bCols := b.Columns()

	if len(aCols) != len(bCols) {
		return false
	}

	// Build column index mapping from b to a's order
	colMap := make(map[int]int) // b index -> a index
	for i, aCol := range aCols {
		found := false
		for j, bCol := range bCols {
			if aCol == bCol {
				colMap[j] = i
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Collect tuples
	aTuples := collectSortedTuples(a)
	bTuples := collectSortedTuplesReordered(b, colMap)

	if len(aTuples) != len(bTuples) {
		return false
	}

	for i := range aTuples {
		if !compareTuplesEqual(aTuples[i], bTuples[i]) {
			return false
		}
	}

	return true
}

// RelationDiff returns a human-readable diff between two relations.
func RelationDiff(a, b Relation) string {
	result := ""

	// Compare columns
	if !compareColumns(a.Columns(), b.Columns()) {
		result += fmt.Sprintf("Column mismatch:\n  a: %v\n  b: %v\n", a.Columns(), b.Columns())
	}

	// Compare sizes
	if a.Size() != b.Size() {
		result += fmt.Sprintf("Size mismatch: a=%d, b=%d\n", a.Size(), b.Size())
	}

	// Collect tuples
	aTuples := collectSortedTuples(a)
	bTuples := collectSortedTuples(b)

	// Find differences
	maxLen := len(aTuples)
	if len(bTuples) > maxLen {
		maxLen = len(bTuples)
	}

	diffCount := 0
	for i := 0; i < maxLen && diffCount < 10; i++ {
		var aTuple, bTuple Tuple
		if i < len(aTuples) {
			aTuple = aTuples[i]
		}
		if i < len(bTuples) {
			bTuple = bTuples[i]
		}

		if !compareTuplesEqual(aTuple, bTuple) {
			result += fmt.Sprintf("Row %d differs:\n  a: %v\n  b: %v\n", i, aTuple, bTuple)
			diffCount++
		}
	}

	if diffCount >= 10 {
		result += "... more differences not shown\n"
	}

	if result == "" {
		result = "Relations are equal"
	}

	return result
}

// FormatRelationSummary formats a relation for display (first 20 rows).
func FormatRelationSummary(rel Relation) string {
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

// Helper functions (unexported)

func compareColumns(a, b []query.Symbol) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func collectSortedTuples(rel Relation) []Tuple {
	var tuples []Tuple
	it := rel.Iterator()
	for it.Next() {
		tuples = append(tuples, it.Tuple())
	}

	sort.Slice(tuples, func(i, j int) bool {
		return compareTuplesOrder(tuples[i], tuples[j]) < 0
	})

	return tuples
}

func collectSortedTuplesReordered(rel Relation, colMap map[int]int) []Tuple {
	var tuples []Tuple
	it := rel.Iterator()
	for it.Next() {
		origTuple := it.Tuple()
		newTuple := make(Tuple, len(origTuple))
		for origIdx, newIdx := range colMap {
			newTuple[newIdx] = origTuple[origIdx]
		}
		tuples = append(tuples, newTuple)
	}

	sort.Slice(tuples, func(i, j int) bool {
		return compareTuplesOrder(tuples[i], tuples[j]) < 0
	})

	return tuples
}

func compareTuplesOrder(a, b Tuple) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		cmp := compareValuesOrder(a[i], b[i])
		if cmp != 0 {
			return cmp
		}
	}

	return len(a) - len(b)
}

func compareValuesOrder(a, b interface{}) int {
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)

	if aStr < bStr {
		return -1
	}
	if aStr > bStr {
		return 1
	}
	return 0
}

func compareTuplesEqual(a, b Tuple) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !compareValuesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func compareValuesEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Handle Identity
	if aId, ok := a.(datalog.Identity); ok {
		if bId, ok := b.(datalog.Identity); ok {
			return aId.L85() == bId.L85()
		}
		return false
	}

	// Simple comparison
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}
