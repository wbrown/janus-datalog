package db

import (
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// PatternMatcher is the interface all data sources implement. A database
// satisfies it, the one a slice's facts open into included.
type PatternMatcher = executor.PatternMatcher

// QueryOption configures query execution (e.g., adding sources).
type QueryOption = storage.QueryOption

// AttributeSchema maps keywords to accessor functions for NewSliceSource.
type AttributeSchema[T any] = storage.AttributeSchema[T]

// WithSources adds named data sources for multi-source queries.
// The default source ($) is always the database itself. A database is a
// source, so facts held in memory are a database opened with OpenMemory and
// written through a transaction.
//
// Example:
//
//	cache, err := db.OpenMemory()
//	if err != nil {
//	    return err
//	}
//	defer cache.Close()
//	tx := cache.NewTransaction()
//	if err := tx.Add(datalog.NewIdentity("cache:1"), datalog.NewKeyword(":cache/score"), int64(95)); err != nil {
//	    return err
//	}
//	if _, err := tx.Commit(); err != nil {
//	    return err
//	}
//	results, err := d.Query(
//	    `[:find ?name ?score
//	      :in $ $cache
//	      :where [?e :user/name ?name]
//	             [$cache ?c :cache/score ?score]]`,
//	    db.WithSources(map[datalog.Symbol]db.PatternMatcher{
//	        datalog.NewSymbol("$cache"): cache,
//	    }),
//	)
//	if err != nil {
//	    return err
//	}
var WithSources = storage.WithSources

// WithContext cancels the query with ctx. A query whose ctx is done is refused
// at entry, before any work, and a rule evaluation whose ctx is done stops at
// its next iteration. ctx's error is the error Query, QueryInto, QueryOneInto
// and Analyze return. Explain and ExplainAlgebra accept the option and plan
// without executing.
//
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//	defer cancel()
//	err := d.QueryInto(&results, q, db.WithContext(ctx))
var WithContext = storage.WithContext

// NewSliceSource opens a memory database holding a Go slice's facts under an
// attribute schema. The item at index i is the entity "slice:i", holding at
// each attribute the value its accessor answers, or every element of the
// slice or array it answers. The caller closes the database.
//
// Example:
//
//	type Rule struct {
//	    Key       string
//	    DependsOn []string
//	}
//
//	source, err := db.NewSliceSource(rules, db.AttributeSchema[Rule]{
//	    datalog.NewKeyword(":rule/key"):        func(r Rule) any { return r.Key },
//	    datalog.NewKeyword(":rule/depends-on"): func(r Rule) any { return r.DependsOn },
//	})
//	if err != nil {
//	    return err
//	}
//	defer source.Close()
func NewSliceSource[T any](items []T, schema AttributeSchema[T]) (*DB, error) {
	return storage.NewSliceSource(items, schema)
}
