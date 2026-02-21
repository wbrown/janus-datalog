package db

import (
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// PatternMatcher is the interface all data sources implement.
// Database, memory sources, and slice sources all satisfy this.
type PatternMatcher = executor.PatternMatcher

// QueryOption configures query execution (e.g., adding sources).
type QueryOption = storage.QueryOption

// AttributeSchema maps keywords to accessor functions for SliceSource.
type AttributeSchema[T any] = executor.AttributeSchema[T]

// SliceSource wraps a Go slice as a queryable PatternMatcher.
type SliceSource[T any] = executor.SliceSource[T]

// WithSources adds named data sources for multi-source queries.
// The default source ($) is always the database itself.
//
// Example:
//
//	cache := db.NewMemorySource(datoms)
//	results, _ := d.Query(
//	    `[:find ?name ?score
//	      :in $ $cache
//	      :where [?e :user/name ?name]
//	             [$cache ?c :cache/score ?score]]`,
//	    db.WithSources(map[datalog.Symbol]db.PatternMatcher{
//	        datalog.NewSymbol("$cache"): cache,
//	    }),
//	)
var WithSources = storage.WithSources

// NewSliceSource creates a PatternMatcher from a Go slice and attribute schema.
// Each item at index i gets entity ID "slice:i". Multi-valued attributes
// (slices/arrays returned by accessor functions) are expanded into one datom
// per element.
//
// Example:
//
//	type Rule struct {
//	    Key       string
//	    DependsOn []string
//	}
//
//	source := db.NewSliceSource(rules, db.AttributeSchema[Rule]{
//	    datalog.NewKeyword(":rule/key"):        func(r Rule) any { return r.Key },
//	    datalog.NewKeyword(":rule/depends-on"): func(r Rule) any { return r.DependsOn },
//	})
func NewSliceSource[T any](items []T, schema AttributeSchema[T]) *SliceSource[T] {
	return executor.NewSliceSource(items, schema)
}

// NewMemorySource creates a PatternMatcher from a slice of datoms.
// Use this for ad-hoc in-memory data sources.
//
// Example:
//
//	facts := []datalog.Datom{
//	    {E: datalog.NewIdentity("e1"), A: datalog.NewKeyword(":item/name"), V: "Sword"},
//	}
//	source := db.NewMemorySource(facts)
var NewMemorySource = executor.NewMemoryPatternMatcher
