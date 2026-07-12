package executor_test

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Compile-time conformance guards for the supported external matcher surface.
// These live outside package executor so unexported implementation details
// cannot accidentally satisfy the contract.
var (
	_ executor.PatternMatcher      = (*executor.IndexedMemoryMatcher)(nil)
	_ executor.PatternMatcher      = (*executor.SourceRouter)(nil)
	_ executor.EntityLookupMatcher = (*executor.SourceRouter)(nil)
	_ executor.Relation            = (*executor.MaterializedRelation)(nil)
	_ executor.Relation            = (*executor.StreamingRelation)(nil)

	_ func(*executor.SourceRouter, *query.Query, executor.Relations) (executor.Relation, error)  = (*executor.SourceRouter).Match
	_ func(*executor.SourceRouter, datalog.Identity, datalog.Keyword) (interface{}, bool, error) = (*executor.SourceRouter).LookupAttribute
	_ func(*executor.ScanRegistry, string, *executor.SharedScan)                                 = (*executor.ScanRegistry).Put
)
