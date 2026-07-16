package storage

import (
	"testing"
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
