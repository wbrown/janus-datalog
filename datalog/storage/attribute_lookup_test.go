//go:build !(js && wasm)

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

func requireAttributeLookup(
	tb testing.TB,
	matcher *BadgerMatcher,
	entity datalog.Identity,
	attribute datalog.Keyword,
) (interface{}, bool) {
	tb.Helper()
	value, found, err := matcher.LookupAttribute(entity, attribute)
	require.NoError(tb, err)
	return value, found
}
