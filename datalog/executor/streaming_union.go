package executor

import "github.com/wbrown/janus-datalog/datalog/query"

// StreamingUnionBuilder combines multiple relations efficiently
// Uses streaming (UnionRelation) or materialization based on configuration
type StreamingUnionBuilder struct {
	opts ExecutorOptions
}

// NewStreamingUnionBuilder creates a new union builder
func NewStreamingUnionBuilder(opts ExecutorOptions) *StreamingUnionBuilder {
	return &StreamingUnionBuilder{opts: opts}
}

// Union combines multiple relations into one
// Uses streaming if enabled, otherwise materializes all results
//
// Parameters:
// - relations: Relations to combine (must have same schema)
//
// Returns: Combined relation (streaming or materialized)
func (s *StreamingUnionBuilder) Union(relations []Relation) Relation {
	if len(relations) == 0 {
		return nil
	}
	if len(relations) == 1 {
		return relations[0]
	}

	// Check if streaming is enabled
	if s.opts.UseStreamingSubqueryUnion {
		return s.unionStreaming(relations)
	}
	return s.unionMaterialized(relations)
}

// unionStreaming creates a streaming union via channel
// Results are consumed lazily as they're iterated
func (s *StreamingUnionBuilder) unionStreaming(relations []Relation) Relation {
	columns := relations[0].Columns()

	// Create channel for streaming
	unionChan := make(chan relationItem, 1)

	go func() {
		defer close(unionChan)
		for _, rel := range relations {
			unionChan <- relationItem{relation: rel}
		}
	}()

	return NewUnionRelation(unionChan, columns, s.opts)
}

// unionMaterialized combines all relations by materializing
// All results are collected before returning
func (s *StreamingUnionBuilder) unionMaterialized(relations []Relation) Relation {
	columns := relations[0].Columns()
	var allTuples []Tuple

	for _, rel := range relations {
		it := rel.Iterator()
		defer it.Close()
		for it.Next() {
			allTuples = append(allTuples, it.Tuple())
		}
	}

	return NewMaterializedRelation(columns, allTuples)
}

// UnionWithColumns combines relations and ensures specific column schema
// Useful when relations might have different column orders
//
// Parameters:
// - relations: Relations to combine
// - columns: Desired column schema for result
//
// Returns: Combined relation with specified column schema
func (s *StreamingUnionBuilder) UnionWithColumns(relations []Relation, columns []query.Symbol) (Relation, error) {
	if len(relations) == 0 {
		return NewMaterializedRelation(columns, []Tuple{}), nil
	}
	if len(relations) == 1 {
		// Project to ensure correct column order
		rel := relations[0]
		if !symbolsEqual(rel.Columns(), columns) {
			projected, err := rel.Project(columns)
			if err != nil {
				return nil, err
			}
			return projected, nil
		}
		return rel, nil
	}

	// Check if all relations have same column schema
	allMatch := true
	for _, rel := range relations {
		if !symbolsEqual(rel.Columns(), columns) {
			allMatch = false
			break
		}
	}

	if allMatch {
		// Fast path: all relations have matching schema
		return s.Union(relations), nil
	}

	// Slow path: need to project each relation first
	projectedRelations := make([]Relation, len(relations))
	for i, rel := range relations {
		projected, err := rel.Project(columns)
		if err != nil {
			return nil, err
		}
		projectedRelations[i] = projected
	}

	return s.Union(projectedRelations), nil
}

// symbolsEqual checks if two symbol lists are equal
func symbolsEqual(a, b []query.Symbol) bool {
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
