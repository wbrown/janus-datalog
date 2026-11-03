package executor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// A router's lookup for a source is the lookup of the source the name stands
// for: a database's, a slice source's among them. A name it does not hold is
// an error naming it, never a lookup read from another source.
func TestSourceRouter_LookupSourceIsTheLookupOfTheSourceNamed(t *testing.T) {
	src, rows, nope := datalog.NewSymbol("$src"), datalog.NewSymbol("$rows"), datalog.NewSymbol("$nope")
	router := executor.NewSourceRouter(map[query.Symbol]executor.PatternMatcher{
		datalog.SymDollar: valueDatabase(t, "from-default"),
		src:               valueDatabase(t, "from-src"),
		rows: sliceDatabase(t, []string{"one"}, storage.AttributeSchema[string]{
			datalog.NewKeyword(":t/name"): func(name string) any { return name },
		}),
	})
	selector, ok := executor.PatternMatcher(router).(executor.EntityLookupMatcher)
	require.True(t, ok, "a router is an entity lookup matcher")

	lookup, err := selector.LookupSource(src)
	require.NoError(t, err)
	value, found, err := lookup.LookupAttribute(datalog.NewIdentity("lookup:one"), datalog.NewKeyword(":t/a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "from-src", value, "the lookup for $src reads $src")

	_, err = selector.LookupSource(nope)
	require.ErrorContains(t, err, "$nope", "a source the router does not hold is an error naming it")
	sliceLookup, err := selector.LookupSource(rows)
	require.NoError(t, err)
	value, found, err = sliceLookup.LookupAttribute(datalog.NewIdentity("slice:0"), datalog.NewKeyword(":t/name"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "one", value, "the lookup for $rows reads the slice's fact")
}
