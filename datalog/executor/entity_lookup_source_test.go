package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// LookupAttribute is a data-answer method: a wrapper over a matcher that
// cannot look up an entity's attribute answers an error, never (nil, false,
// nil), which reads as the attribute being absent.
func TestScanSharingMatcher_LookupOverAMatcherThatCannotLookUpIsAnError(t *testing.T) {
	rows := NewSliceSource([]string{"subject"}, AttributeSchema[string]{
		datalog.NewKeyword(":t/name"): func(name string) any { return name },
	})
	m := NewScanSharingMatcher(rows, NewScanRegistry(), nil)
	_, found, err := m.LookupAttribute(datalog.NewIdentity("lookup:subject"), datalog.NewKeyword(":t/a"))
	require.Error(t, err)
	require.False(t, found)
}
