package executor

import (
	"fmt"
	"reflect"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// AttributeSchema maps keywords to accessor functions for SliceSource.
// Each entry defines how to extract a value from a slice item for a given attribute.
type AttributeSchema[T any] map[datalog.Keyword]func(T) any

// SliceSource wraps a Go slice, making it queryable as a PatternMatcher via a schema.
// Each slice item becomes an entity with attributes defined by the schema.
// Multi-valued attributes (slices/arrays) are expanded into multiple datoms.
//
// Example:
//
//	type Rule struct {
//	    Key       string
//	    DependsOn []string
//	}
//
//	source := NewSliceSource(rules, AttributeSchema[Rule]{
//	    kw(":rule/key"):        func(r Rule) any { return r.Key },
//	    kw(":rule/depends-on"): func(r Rule) any { return r.DependsOn },
//	})
type SliceSource[T any] struct {
	matcher PatternMatcher
}

// NewSliceSource creates a PatternMatcher from a Go slice and an attribute schema.
// Each item at index i gets entity ID "slice:i". Multi-valued attributes (slices/arrays
// returned by accessor functions) are expanded into one datom per element.
func NewSliceSource[T any](items []T, schema AttributeSchema[T]) *SliceSource[T] {
	var datoms []datalog.Datom
	for i, item := range items {
		entity := datalog.NewIdentity(fmt.Sprintf("slice:%d", i))
		for attr, accessor := range schema {
			value := accessor(item)
			// Handle multi-valued attributes (slices/arrays)
			rv := reflect.ValueOf(value)
			if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
				for j := 0; j < rv.Len(); j++ {
					datoms = append(datoms, datalog.Datom{
						E: entity,
						A: attr,
						V: rv.Index(j).Interface(),
					})
				}
			} else {
				datoms = append(datoms, datalog.Datom{
					E: entity,
					A: attr,
					V: value,
				})
			}
		}
	}
	return &SliceSource[T]{
		matcher: NewMemoryPatternMatcher(datoms),
	}
}

// Match implements PatternMatcher.
func (s *SliceSource[T]) Match(q *query.Query, bindings Relations) (Relation, error) {
	return s.matcher.Match(q, bindings)
}

// Compile-time verification that SliceSource implements PatternMatcher
var _ PatternMatcher = (*SliceSource[any])(nil)
