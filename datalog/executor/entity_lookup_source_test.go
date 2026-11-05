package executor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// A scan-sharing matcher's lookup is the lookup of the matcher it wraps: over
// a slice source, the slice's fact.
func TestScanSharingMatcher_LookupIsTheLookupOfTheMatcherItWraps(t *testing.T) {
	slice := sliceDatabase(t, []string{"subject"}, storage.AttributeSchema[string]{
		datalog.NewKeyword(":t/name"): func(name string) any { return name },
	})
	m := NewScanSharingMatcher(slice, NewScanRegistry(), nil)
	value, found, err := m.LookupAttribute(datalog.NewIdentity("slice:0"), datalog.NewKeyword(":t/name"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "subject", value)
}
