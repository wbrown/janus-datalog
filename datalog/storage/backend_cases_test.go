package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type storeContractCase struct {
	name string
	open func(testing.TB, *BinaryKeyEncoder) Store
}

func storeContractCases() []storeContractCase {
	cases := []storeContractCase{
		{
			name: "memory",
			open: func(_ testing.TB, encoder *BinaryKeyEncoder) Store {
				return NewMemoryStore(encoder)
			},
		},
	}
	return appendNativeBackendCases(cases)
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
	memory := reopenBackendCase{name: "memory"}
	var memStore *MemoryStore
	memory.open = func(t testing.TB, opts DatabaseOptions) *Database {
		t.Helper()
		if memStore == nil {
			encoder := &BinaryKeyEncoder{}
			if opts.CompressionThreshold != 0 {
				encoder.CompressionThreshold = opts.CompressionThreshold
			}
			memStore = NewMemoryStore(encoder)
			t.Cleanup(func() { _ = memStore.Close() })
		}
		opts.Store = memStore
		db, err := NewDatabaseWithOptions(opts)
		require.NoError(t, err)
		return db
	}
	return appendNativeReopenCases([]reopenBackendCase{memory})
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
