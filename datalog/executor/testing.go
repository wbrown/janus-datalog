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

// CompareRelations compares two relations for equality (symbol names and sorted tuples).
// Returns true if they are equal.
func CompareRelations(a, b Relation) bool {
	// Compare symbols
	if !compareSymbols(a.Symbols(), b.Symbols()) {
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

// CompareRelationsIgnoreColumnOrder compares relations allowing different symbol orders.
func CompareRelationsIgnoreColumnOrder(a, b Relation) bool {
	aSyms := a.Symbols()
	bSyms := b.Symbols()

	if len(aSyms) != len(bSyms) {
		return false
	}

	// Build symbol index mapping from b to a's order
	symMap := make(map[int]int) // b index -> a index
	for i, aSym := range aSyms {
		found := false
		for j, bSym := range bSyms {
			if aSym == bSym {
				symMap[j] = i
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
	bTuples := collectSortedTuplesReordered(b, symMap)

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

	// Compare symbols
	if !compareSymbols(a.Symbols(), b.Symbols()) {
		result += fmt.Sprintf("Symbol mismatch:\n  a: %v\n  b: %v\n", a.Symbols(), b.Symbols())
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
			result += fmt.Sprintf("Tuple %d differs:\n  a: %v\n  b: %v\n", i, aTuple, bTuple)
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

// FormatRelationSummary formats a relation for display (first 20 tuples).
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
	return fmt.Sprintf("symbols=%v, tuples=%v", rel.Symbols(), tuples)
}

// Helper functions (unexported)

func compareSymbols(a, b []query.Symbol) bool {
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

func collectSortedTuplesReordered(rel Relation, symMap map[int]int) []Tuple {
	var tuples []Tuple
	it := rel.Iterator()
	for it.Next() {
		origTuple := it.Tuple()
		newTuple := make(Tuple, len(origTuple))
		for origIdx, newIdx := range symMap {
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
		if !datalog.ValuesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}
