package storage_test

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

var (
	_ func(string, *storage.BinaryKeyEncoder) (*storage.BadgerStore, error)                         = storage.NewBadgerStore
	_ func(storage.IndexType, []byte, *storage.BinaryKeyEncoder, *badger.DB) (datalog.Datom, error) = storage.DatomFromKey

	_ executor.PatternMatcher        = (*storage.Database)(nil)
	_ executor.PatternMatcher        = (*storage.BadgerMatcher)(nil)
	_ executor.PredicateAwareMatcher = (*storage.BadgerMatcher)(nil)
	_ executor.EntityLookupMatcher   = (*storage.BadgerMatcher)(nil)
)
