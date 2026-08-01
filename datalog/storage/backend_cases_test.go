package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type storeContractCase struct {
	name string
	open func(testing.TB, *BinaryKeyEncoder) Store
}

// storeContractCases is every backend this build has. Deriving it from
// AvailableBackends is what keeps a new backend from reaching production
// without reaching the contract — the state the typed store was in when three
// separate lists each had to be edited by hand.
func storeContractCases() []storeContractCase {
	backends := AvailableBackends()
	cases := make([]storeContractCase, 0, len(backends))
	for _, backend := range backends {
		cases = append(cases, contractCaseFor(backend))
	}
	return cases
}

// inProcessStores are the backends that need no disk: the candidates for the
// wasm default, and what a backend-vs-backend measurement compares without
// paying for a filesystem.
func inProcessStores() []storeContractCase {
	var cases []storeContractCase
	for _, backend := range AvailableBackends() {
		if backend.Persistent {
			continue
		}
		cases = append(cases, contractCaseFor(backend))
	}
	return cases
}

func contractCaseFor(backend Backend) storeContractCase {
	return storeContractCase{
		name: backend.Name,
		open: func(tb testing.TB, encoder *BinaryKeyEncoder) Store {
			store, err := backend.Open(tb.TempDir(), encoder)
			require.NoError(tb, err)
			return store
		},
	}
}

// reopenBackendCase is a backend that can end a database session and start a
// new one over the same stored state. For path-backed stores the state is the
// durable directory; for the memory backend the state is the live store
// instance — the Database wrapper is the session, so a reopen is a fresh
// Database over the same store.
type reopenBackendCase struct {
	name string
	// open returns a database session over the case's stored state. Each
	// call after the first is a reopen: the case ends the prior session per
	// its backend's semantics (Badger closes the prior database to release
	// the directory lock; memory drops it — Database.Close would close the
	// shared store). Options carry schema/replica/compression settings; the
	// case supplies the store or path.
	open func(t testing.TB, opts DatabaseOptions) *Database
}

func reopenBackendCases() []reopenBackendCase {
	cases := make([]reopenBackendCase, 0, len(inProcessStores()))
	for _, store := range inProcessStores() {
		cases = append(cases, inProcessReopenCase(store))
	}
	return appendNativeReopenCases(cases)
}

// inProcessReopenCase builds a reopen case whose stored state is a live store
// rather than a directory. The store is constructed once and every session
// reopens over it.
func inProcessReopenCase(backend storeContractCase) reopenBackendCase {
	var backing Store
	return reopenBackendCase{
		name: backend.name,
		open: func(t testing.TB, opts DatabaseOptions) *Database {
			t.Helper()
			if backing == nil {
				encoder := &BinaryKeyEncoder{}
				if opts.CompressionThreshold != 0 {
					encoder.CompressionThreshold = opts.CompressionThreshold
				}
				backing = backend.open(t, encoder)
				t.Cleanup(func() { _ = backing.Close() })
			}
			opts.Store = backing
			db, err := NewDatabaseWithOptions(opts)
			require.NoError(t, err)
			return db
		},
	}
}

func openContractDatabase(t testing.TB, testCase storeContractCase, opts DatabaseOptions) *Database {
	t.Helper()
	if opts.Store == nil {
		encoder := &BinaryKeyEncoder{}
		if opts.CompressionThreshold != 0 {
			encoder.CompressionThreshold = opts.CompressionThreshold
		}
		opts.Store = testCase.open(t, encoder)
	}
	database, err := NewDatabaseWithOptions(opts)
	if err != nil {
		t.Fatalf("open %s database: %v", testCase.name, err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}
