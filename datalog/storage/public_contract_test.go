package storage_test

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

var (
	_ func(storage.IndexType, []byte, *storage.BinaryKeyEncoder, storage.BlobReader) (datalog.Datom, error) = storage.DatomFromKey

	_ executor.PatternMatcher        = (*storage.Database)(nil)
	_ executor.PatternMatcher        = (*storage.PatternMatcher)(nil)
	_ executor.PredicateAwareMatcher = (*storage.PatternMatcher)(nil)
	_ executor.EntityLookupMatcher   = (*storage.PatternMatcher)(nil)
	_ storage.Store                  = (*storage.MemoryStore)(nil)
)
