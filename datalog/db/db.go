// Package db provides the public API for janus-datalog.
//
// This is the primary entry point for consumers. It wraps the internal
// storage, executor, and parser packages into a clean interface:
//
//	d, err := db.Open("/path/to/data")
//	defer d.Close()
//
//	tx := d.NewTransaction()
//	tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice")
//	txID, _ := tx.Commit()
//
//	name, found, _ := d.GetString(alice, datalog.NewKeyword(":person/name"))
//
//	asOf := d.AsOf(txID)
//	hist := d.History()
package db

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// DB is the main database handle. It is a type alias for storage.Database;
// all methods on *storage.Database (Query, GetString, AsOfDatabase, etc.)
// are available directly.
type DB = storage.Database

// Transaction provides write access to the database. It is a type alias
// for storage.Transaction; all methods (Add, Commit, Rollback, etc.)
// are available directly.
type Transaction = storage.Transaction

// Open creates or opens a database at the given path.
func Open(path string, opts ...Option) (*DB, error) {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}
	d, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
		Path:                 path,
		Store:                cfg.store,
		Schema:               cfg.schema,
		ReplicaID:            cfg.replicaID,
		AnnotationHandler:    cfg.annotationHandler,
		DisableCache:         cfg.disableCache,
		PlannerOptions:       cfg.plannerOptions,
		CompressionThreshold: cfg.compressionThreshold,
	})
	if err != nil {
		return nil, fmt.Errorf("db.Open: %w", err)
	}
	return d, nil
}

// OpenMemory creates a database backed by the pure-Go ordered memory store.
func OpenMemory(opts ...Option) (*DB, error) {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}
	store := cfg.store
	if store == nil {
		store = storage.NewMemoryStore(nil)
	}
	d, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
		Store:                store,
		Schema:               cfg.schema,
		ReplicaID:            cfg.replicaID,
		AnnotationHandler:    cfg.annotationHandler,
		DisableCache:         cfg.disableCache,
		PlannerOptions:       cfg.plannerOptions,
		CompressionThreshold: cfg.compressionThreshold,
	})
	if err != nil {
		return nil, fmt.Errorf("db.OpenMemory: %w", err)
	}
	return d, nil
}

// MustParseQuery parses a Datalog query string, panicking on error.
// Use this for package-level query constants known at compile time:
//
//	var myQuery = db.MustParseQuery(`[:find ?e :where [?e :person/name "Alice"]]`)
func MustParseQuery(input string) *query.Query {
	q, err := parser.ParseQuery(input)
	if err != nil {
		panic(fmt.Sprintf("db.MustParseQuery: %v\n%s", err, input))
	}
	return q
}
