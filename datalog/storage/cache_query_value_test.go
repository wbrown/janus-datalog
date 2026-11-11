package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestAQueryAnswersTheValueItsSessionOpenedOn pins that a query answers the
// database value its read session opened on, whatever a later reader cached
// before the query's result was read. A transaction draws a datom before the
// query opens and commits it after. The query answers without that datom, on
// the latest handle and on an as-of handle whose target lies above the datom,
// for each cardinality and each route a pattern takes, with the cache on as
// with it off.
func TestAQueryAnswersTheValueItsSessionOpenedOn(t *testing.T) {
	name := datalog.NewKeyword(":value/name")
	one := datalog.NewKeyword(":value/one")
	tags := datalog.NewKeyword(":value/tags")
	cells := datalog.NewKeyword(":value/cells")
	handle := datalog.NewKeyword(":value/handle")
	s, err := schema.NewBuilder().
		Attribute(name.String()).Type(schema.TypeString).Add().
		Attribute(one.String()).Type(schema.TypeString).Add().
		Attribute(tags.String()).Type(schema.TypeString).Many().Add().
		Attribute(cells.String()).Type(schema.TypeString).Vector().Add().
		Attribute(handle.String()).Type(schema.TypeString).Unique(schema.UniqueIdentity).Add().
		Build()
	require.NoError(t, err)
	e := datalog.NewIdentity("query-value:e")

	kinds := []struct {
		name string
		attr datalog.Keyword
		// committed is the value committed before the query opens; late is the
		// one a transaction draws before the query opens and commits after.
		committed string
		late      string
		// literal is the committed value written in a pattern's value position.
		literal string
		// held is the attribute's value in the query's session, rendered.
		held string
	}{
		{"cardinality one", one, "s1", "s2", `"s1"`, "[s1]"},
		{"a set", tags, "a", "b", `"a"`, "[a]"},
		{"a vector", cells, "a", "b", `["a"]`, "[[a]]"},
		{"a unique attribute", handle, "k1", "k2", `"k1"`, "[k1]"},
	}
	routes := []struct {
		name string
		text func(attr datalog.Keyword, literal string) string
		// want is the query's answer from the value its session opened on.
		want func(held string) string
	}{
		{
			"an unbound-entity scan",
			func(attr datalog.Keyword, _ string) string {
				return fmt.Sprintf(`[:find ?v :where [?e %s ?v]]`, attr)
			},
			func(held string) string { return held },
		},
		{
			"an entity bound by a join",
			func(attr datalog.Keyword, _ string) string {
				return fmt.Sprintf(`[:find ?v :where [?e %s "e"] [?e %s ?v]]`, name, attr)
			},
			func(held string) string { return held },
		},
		{
			"a match bound to the committed value",
			func(attr datalog.Keyword, literal string) string {
				return fmt.Sprintf(`[:find ?e :where [?e %s %s]]`, attr, literal)
			},
			func(string) string { return fmt.Sprint([]string{fmt.Sprint(e)}) },
		},
	}

	answer := func(t *testing.T, rel executor.Relation, err error) string {
		t.Helper()
		tuples, err := executor.CollectTuples(rel, err)
		require.NoError(t, err)
		values := make([]string, 0, len(tuples))
		for _, tuple := range tuples {
			values = append(values, fmt.Sprint(tuple[len(tuple)-1]))
		}
		sort.Strings(values)
		return fmt.Sprint(values)
	}

	run := func(t *testing.T, mode optimizerMode, attr datalog.Keyword, committed, late, text string, asOf, disableCache bool) string {
		d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: disableCache})
		tx := d.NewTransaction()
		require.NoError(t, tx.Add(e, name, "e"))
		require.NoError(t, tx.Add(e, attr, committed))
		_, err := tx.Commit()
		require.NoError(t, err)

		open := d.NewTransaction()
		require.NoError(t, open.Add(e, attr, late))

		view := d
		if asOf {
			unrelated := d.NewTransaction()
			require.NoError(t, unrelated.Add(datalog.NewIdentity("query-value:other"), name, "other"))
			mark, err := unrelated.Commit()
			require.NoError(t, err)
			view = d.AsOf(mark)
		}
		rel, queryErr := view.Query(text)
		_, err = open.Commit()
		require.NoError(t, err)

		later, laterErr := view.Query(fmt.Sprintf(`[:find ?v :in $ ?e :where [?e %s ?v]]`, attr), e)
		_, err = executor.CollectTuples(later, laterErr)
		require.NoError(t, err)
		return answer(t, rel, queryErr)
	}

	for _, mode := range legsFor(backendModes) {
		for _, kind := range kinds {
			for _, route := range routes {
				for _, asOf := range []bool{false, true} {
					handleName := "latest"
					if asOf {
						handleName = "as-of"
					}
					t.Run(mode.name+"/"+kind.name+"/"+route.name+"/"+handleName, func(t *testing.T) {
						text := route.text(kind.attr, kind.literal)
						uncached := run(t, mode, kind.attr, kind.committed, kind.late, text, asOf, true)
						cached := run(t, mode, kind.attr, kind.committed, kind.late, text, asOf, false)
						require.Equal(t, route.want(kind.held), uncached, "the uncached query answers the value its session opened on")
						require.Equal(t, uncached, cached, "the cached query answers as the uncached one")
					})
				}
			}
		}
	}
}
