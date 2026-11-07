package errjoin

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestJoinAnswersALoneErrorAsItself pins how a function's own error and a
// Close or Rollback error combine: a lone error is answered as itself, and
// two are joined. A lone error wrapped in a join of one hides a join the
// caller enumerates behind a join that holds it alone.
func TestJoinAnswersALoneErrorAsItself(t *testing.T) {
	first := errors.New("first")
	second := errors.New("second")
	enumerated := errors.Join(first, second)

	require.NoError(t, Join(nil, nil), "no error combines to none")
	require.Same(t, first, Join(first, nil), "a lone first error is answered as itself")
	require.Same(t, second, Join(nil, second), "a lone second error is answered as itself")
	require.Same(t, enumerated, Join(enumerated, nil),
		"a lone join is answered as itself, so its caller enumerates its members")

	joined, ok := Join(first, second).(interface{ Unwrap() []error })
	require.True(t, ok, "two errors are joined")
	require.Equal(t, []error{first, second}, joined.Unwrap(), "both errors reach the caller, in order")
}
