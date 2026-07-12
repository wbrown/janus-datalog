package executor_test

import "github.com/wbrown/janus-datalog/datalog/executor"

// Compile-time conformance guards for the supported external matcher surface.
// These live outside package executor so unexported implementation details
// cannot accidentally satisfy the contract.
var (
	_ executor.PatternMatcher      = (*executor.IndexedMemoryMatcher)(nil)
	_ executor.PatternMatcher      = (*executor.SourceRouter)(nil)
	_ executor.EntityLookupMatcher = (*executor.SourceRouter)(nil)
)
