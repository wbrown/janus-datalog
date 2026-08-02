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
	"bytes"
	"fmt"
	"io"
	"os"

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

// Open creates or opens a database from source, which is one of:
//
//   - string — a filesystem path. A directory goes to the backend, the build's
//     default unless WithBackend names one: Badger natively, the memory store
//     under js/wasm, where there is no filesystem to hold it. A path ending in
//     .edn or .jdzl is an exported dump, opened and loaded.
//   - io.ReadSeeker — an open dump. *os.File, the fs.File an embed.FS hands
//     out, and bytes.Reader all qualify, which is what lets a database ship
//     inside a binary. JDZL needs the seek to read its trailer index.
//   - []byte — a dump already in memory, the usual go:embed spelling.
//   - storage.Store — a store the caller opened and continues to own. Open's
//     error paths do not Close it; on success Database.Close does.
//
// A dump loads into the backend WithBackend names, defaulting to the memory
// store. A persistent backend has nowhere of its own to put one, so Open makes
// a temporary directory that the database removes when it closes.
func Open(source interface{}, opts ...Option) (*DB, error) {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}
	dbOpts := storage.DatabaseOptions{
		BackendName:          cfg.backendName,
		Schema:               cfg.schema,
		ReplicaID:            cfg.replicaID,
		AnnotationHandler:    cfg.annotationHandler,
		DisableCache:         cfg.disableCache,
		PlannerOptions:       cfg.plannerOptions,
		CompressionThreshold: cfg.compressionThreshold,
	}

	var dump io.ReadSeeker
	switch src := source.(type) {
	case string:
		if !storage.IsDumpPath(src) {
			dbOpts.Path = src
			break
		}
		file, err := os.Open(src)
		if err != nil {
			return nil, fmt.Errorf("db.Open: %w", err)
		}
		defer file.Close()
		dump = file
	case []byte:
		dump = bytes.NewReader(src)
	case io.ReadSeeker:
		dump = src
	case storage.Store:
		dbOpts.Store = src
	default:
		return nil, fmt.Errorf("db.Open: cannot open %T: want a path, an io.ReadSeeker or []byte holding a dump, or a storage.Store", source)
	}

	if dump != nil {
		if err := setDumpDestination(&dbOpts); err != nil {
			return nil, fmt.Errorf("db.Open: %w", err)
		}
	}
	d, err := storage.NewDatabaseWithOptions(dbOpts)
	if err != nil {
		return nil, fmt.Errorf("db.Open: %w", err)
	}
	if dump != nil {
		if err := d.ImportDump(dump); err != nil {
			_ = d.Close()
			return nil, fmt.Errorf("db.Open: %w", err)
		}
	}
	return d, nil
}

// setDumpDestination fills in where a dump lands. A dump carries the data but
// no location, so an in-process backend takes it as it is, and a persistent one
// gets scratch space the database owns and removes when it closes.
func setDumpDestination(opts *storage.DatabaseOptions) error {
	name := opts.BackendName
	if name == "" {
		name = "memory-trees"
	}
	backend, err := storage.BackendNamed(name)
	if err != nil {
		return err
	}
	opts.BackendName = backend.Name
	if !backend.Persistent {
		return nil
	}
	dir, err := os.MkdirTemp("", "janus-dump-*")
	if err != nil {
		return fmt.Errorf("scratch directory for the %s backend: %w", backend.Name, err)
	}
	opts.Path = dir
	opts.RemovePathOnClose = true
	return nil
}

// OpenMemory creates a database backed by the in-process tree store, which is
// also what a wasm build's Open reaches with no backend named. It is Open with
// that backend and no path, so every option behaves as it does there —
// including a later WithBackend, which names a different one.
func OpenMemory(opts ...Option) (*DB, error) {
	return Open("", append([]Option{WithBackend("memory-trees")}, opts...)...)
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
