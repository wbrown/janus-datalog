package executor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// skillsDatabase opens a database whose schema declares :t/skills a vector
// of strings, holding "stealth" for one entity, and returns the database,
// the entity and the attribute.
func skillsDatabase(t *testing.T) (*storage.Database, datalog.Identity, datalog.Keyword) {
	t.Helper()
	skills := datalog.NewKeyword(":t/skills")
	s, err := schema.NewBuilder().
		Attribute(":t/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	d, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{Path: t.TempDir(), Schema: s})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	one := datalog.NewIdentity("wrapped-lookup:one")
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(one, skills, "stealth"))
	_, err = tx.Commit()
	require.NoError(t, err)
	return d, one, skills
}

// requireAnswersLookupsAsTheMatcherItWraps asserts that wrapped, a wrapper
// over a skillsDatabase's matcher, answers an entity lookup as the matcher it
// wraps does: the value the entity holds, and, through the lookup of its
// source, the value again and a get-else default in the type the wrapped
// matcher's schema gives the attribute.
func requireAnswersLookupsAsTheMatcherItWraps(t *testing.T, wrapped executor.PatternMatcher, one datalog.Identity, skills datalog.Keyword) {
	t.Helper()
	lookup, ok := wrapped.(executor.EntityLookupMatcher)
	require.True(t, ok, "%T over a matcher that looks up is an entity lookup matcher", wrapped)

	value, found, err := lookup.LookupAttribute(one, skills)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"stealth"}, value)

	source, err := lookup.LookupSource(datalog.SymDollar)
	require.NoError(t, err)
	value, found, err = source.LookupAttribute(one, skills)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"stealth"}, value, "the lookup of $ reads the wrapped matcher")
	typed, ok := source.(query.TypedDefaulter)
	require.True(t, ok, "the lookup of $ types defaults by the wrapped matcher's schema")
	require.Equal(t, []string{}, typed.TypeDefault(skills, []interface{}{}),
		"the default takes the type the wrapped matcher's schema gives :t/skills")
}

func TestWrapMatcher_LooksUpAndTypesDefaultsAsTheMatcherItWraps(t *testing.T) {
	d, one, skills := skillsDatabase(t)
	requireAnswersLookupsAsTheMatcherItWraps(t, executor.WrapMatcher(d.Matcher(), func(annotations.Event) {}), one, skills)
}

// requireRefusesLookupsOverAMatcherThatCannotLookUp asserts that wrapped, a
// wrapper over a custom source whose database holds :t/a for lookup:one,
// answers an entity lookup with an error and finds nothing, and answers the
// lookup of its source $ with an error.
func requireRefusesLookupsOverAMatcherThatCannotLookUp(t *testing.T, wrapped executor.PatternMatcher) {
	t.Helper()
	lookup, ok := wrapped.(executor.EntityLookupMatcher)
	require.True(t, ok, "%T is an entity lookup matcher whatever it wraps", wrapped)

	_, found, err := lookup.LookupAttribute(datalog.NewIdentity("lookup:one"), datalog.NewKeyword(":t/a"))
	require.Error(t, err, "a lookup over a matcher that cannot look up is an error, never an absence")
	require.False(t, found)

	_, err = lookup.LookupSource(datalog.SymDollar)
	require.Error(t, err, "the lookup of $ over a matcher that cannot look up is an error")
}

// An annotation wrapper over a matcher that cannot look an attribute up
// answers each lookup with an error, never (nil, false, nil), which reads as
// the attribute being absent.
func TestWrapMatcher_LookupOverAMatcherThatCannotLookUpIsAnError(t *testing.T) {
	requireRefusesLookupsOverAMatcherThatCannotLookUp(t,
		executor.WrapMatcher(customSource{valueDatabase(t, "held")}, func(annotations.Event) {}))
}
