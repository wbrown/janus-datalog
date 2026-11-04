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
	src, slice, nope := datalog.NewSymbol("$src"), datalog.NewSymbol("$slice"), datalog.NewSymbol("$nope")
	router := executor.NewSourceRouter(map[query.Symbol]executor.PatternMatcher{
		datalog.SymDollar: valueDatabase(t, "from-default"),
		src:               valueDatabase(t, "from-src"),
		slice: sliceDatabase(t, []string{"one"}, storage.AttributeSchema[string]{
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
	sliceLookup, err := selector.LookupSource(slice)
	require.NoError(t, err)
	value, found, err = sliceLookup.LookupAttribute(datalog.NewIdentity("slice:0"), datalog.NewKeyword(":t/name"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "one", value, "the lookup for $slice reads the slice's fact")
}

// A router's lookup for a source that cannot look an attribute up is an error
// naming the source, and so is the router's own lookup where its $ is such a
// source. Each source's database holds :t/a for lookup:one, so an answer of
// absence would differ from the error.
func TestSourceRouter_LookupSourceOfASourceThatCannotLookUpIsAnError(t *testing.T) {
	custom := datalog.NewSymbol("$custom")
	router := executor.NewSourceRouter(map[query.Symbol]executor.PatternMatcher{
		datalog.SymDollar: customSource{valueDatabase(t, "from-default")},
		custom:            customSource{valueDatabase(t, "from-custom")},
	})
	selector, ok := executor.PatternMatcher(router).(executor.EntityLookupMatcher)
	require.True(t, ok, "a router is an entity lookup matcher")

	_, err := selector.LookupSource(custom)
	require.ErrorContains(t, err, "$custom", "a source that cannot look up is an error naming it")

	_, found, err := selector.LookupAttribute(datalog.NewIdentity("lookup:one"), datalog.NewKeyword(":t/a"))
	require.Error(t, err, "the router's lookup over a $ that cannot look up is an error, never an absence")
	require.False(t, found)
}
