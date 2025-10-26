package executor

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// JoinCondition represents a condition for joining two relations
type JoinCondition struct {
	LeftSymbol  query.Symbol
	RightSymbol query.Symbol
	Operator    query.CompareOp // Comparison operator (typically OpEQ for joins)
}

// JoinWithConditions performs a join with additional equality conditions
func JoinWithConditions(left, right Relation, conditions []JoinCondition) Relation {
	if len(conditions) == 0 {
		// No conditions, use regular join
		return left.Join(right)
	}

	// For now, we only support equality conditions
	for _, cond := range conditions {
		if cond.Operator != query.OpEQ {
			// Fallback to regular join + filtering
			return left.Join(right)
		}
	}

	// Extract the join columns from conditions
	leftJoinCols := make([]query.Symbol, len(conditions))
	rightJoinCols := make([]query.Symbol, len(conditions))

	for i, cond := range conditions {
		leftJoinCols[i] = cond.LeftSymbol
		rightJoinCols[i] = cond.RightSymbol
	}

	// Also include any natural join columns (columns with same name)
	commonCols := CommonColumns(left, right)

	// Build a map to avoid duplicates
	leftColMap := make(map[query.Symbol]bool)
	rightColMap := make(map[query.Symbol]bool)

	// Add condition columns
	for i := range conditions {
		leftColMap[leftJoinCols[i]] = true
		rightColMap[rightJoinCols[i]] = true
	}

	// Add common columns
	for _, col := range commonCols {
		leftColMap[col] = true
		rightColMap[col] = true
	}

	// Convert back to slices
	allLeftCols := make([]query.Symbol, 0, len(leftColMap))
	allRightCols := make([]query.Symbol, 0, len(rightColMap))

	// First add the condition columns in order
	for i := range conditions {
		allLeftCols = append(allLeftCols, leftJoinCols[i])
		allRightCols = append(allRightCols, rightJoinCols[i])
	}

	// Then add common columns that aren't already included
	for _, col := range commonCols {
		found := false
		for i := range conditions {
			if leftJoinCols[i] == col || rightJoinCols[i] == col {
				found = true
				break
			}
		}
		if !found {
			allLeftCols = append(allLeftCols, col)
			allRightCols = append(allRightCols, col)
		}
	}

	// Perform the multi-column hash join
	return MultiColumnHashJoin(left, right, allLeftCols, allRightCols, conditions)
}

// MultiColumnHashJoin performs a hash join on multiple columns with aliasing support
func MultiColumnHashJoin(left, right Relation, leftCols, rightCols []query.Symbol, conditions []JoinCondition) Relation {
	// Build column indices
	leftIndices := make([]int, len(leftCols))
	rightIndices := make([]int, len(rightCols))

	for i := range leftCols {
		leftIndices[i] = ColumnIndex(left, leftCols[i])
		rightIndices[i] = ColumnIndex(right, rightCols[i])
		if leftIndices[i] < 0 || rightIndices[i] < 0 {
			// Column not found, return empty
			return NewMaterializedRelation(nil, nil)
		}
	}

	// Determine output columns
	outputCols := append([]query.Symbol{}, left.Columns()...)

	// Track which right columns are part of join conditions or already exist
	skipRightCols := make(map[query.Symbol]bool)

	// For equi-join conditions, skip the right column if it has the same name as left
	// but keep it if names are different (aliasing case)
	for _, cond := range conditions {
		if cond.LeftSymbol == cond.RightSymbol {
			// Same name on both sides, skip right column
			skipRightCols[cond.RightSymbol] = true
		}
		// If names differ, we'll keep both columns
	}

	// Also skip columns that naturally exist in both relations
	for _, leftCol := range left.Columns() {
		for _, rightCol := range right.Columns() {
			if leftCol == rightCol {
				skipRightCols[rightCol] = true
			}
		}
	}

	// Add right columns that we're not skipping
	for _, col := range right.Columns() {
		if !skipRightCols[col] {
			outputCols = append(outputCols, col)
		}
	}

	// Build hash table from smaller relation
	var buildRel, probeRel Relation
	var buildIndices, probeIndices []int
	var buildOnLeft bool

	if left.Size() <= right.Size() {
		buildRel, probeRel = left, right
		buildIndices, probeIndices = leftIndices, rightIndices
		buildOnLeft = true
	} else {
		buildRel, probeRel = right, left
		buildIndices, probeIndices = rightIndices, leftIndices
		buildOnLeft = false
	}

	// Build phase: create hash table
	hashTable := make(map[string][]Tuple)
	buildIt := buildRel.Iterator()
	for buildIt.Next() {
		tuple := buildIt.Tuple()
		// Build composite key from all join columns
		key := buildCompositeKey(tuple, buildIndices)
		hashTable[key] = append(hashTable[key], tuple)
	}
	buildIt.Close()

	// Probe phase: find matches
	var result []Tuple
	probeIt := probeRel.Iterator()
	for probeIt.Next() {
		probeTuple := probeIt.Tuple()
		key := buildCompositeKey(probeTuple, probeIndices)

		if buildTuples, found := hashTable[key]; found {
			for _, buildTuple := range buildTuples {
				// Create output tuple
				var leftTuple, rightTuple Tuple
				if buildOnLeft {
					leftTuple, rightTuple = buildTuple, probeTuple
				} else {
					leftTuple, rightTuple = probeTuple, buildTuple
				}

				// Build output tuple
				outputTuple := make(Tuple, 0, len(outputCols))

				// Add all left columns
				outputTuple = append(outputTuple, leftTuple...)

				// Add right columns that aren't being skipped
				rightIdx := 0
				for _, col := range right.Columns() {
					if !skipRightCols[col] && rightIdx < len(rightTuple) {
						outputTuple = append(outputTuple, rightTuple[rightIdx])
					}
					rightIdx++
				}

				result = append(result, outputTuple)
			}
		}
	}
	probeIt.Close()

	return NewMaterializedRelation(outputCols, result)
}

// buildCompositeKey builds a string key from multiple column values
func buildCompositeKey(tuple Tuple, indices []int) string {
	parts := make([]string, len(indices))
	for i, idx := range indices {
		if idx < len(tuple) {
			parts[i] = valueToString(tuple[idx])
		} else {
			parts[i] = ""
		}
	}
	// Use a delimiter that won't appear in values
	return strings.Join(parts, "\x00")
}

// valueToString converts a value to a string for use in hash keys
func valueToString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case int:
		return strconv.Itoa(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'g', -1, 64)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case datalog.Identity:
		return val.L85()
	case datalog.Keyword:
		return val.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}
