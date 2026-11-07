package db_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestAQueryOpenedBeforeACommitAnswersItsSnapshotOnEveryRoute pins that a
// query opened through db.OpenMemory before a transaction that was already
// open commits answers its read session's state on each pattern route, after
// a later reader has read the key, as the same query does on a database
// opened WithoutCache. The open transaction's writes are the key's highest.
func TestAQueryOpenedBeforeACommitAnswersItsSnapshotOnEveryRoute(t *testing.T) {
	name := datalog.NewKeyword(":r/name")
	tags := datalog.NewKeyword(":r/tags")
	st := datalog.NewKeyword(":r/st")
	routes := []struct {
		name string
		text string
	}{
		{"unbound-entity set scan", `[:find ?v :where [?e :r/tags ?v]]`},
		{"entity bound by a join, set", `[:find ?v :where [?e :r/name "n"] [?e :r/tags ?v]]`},
		{"entity bound by a join, one", `[:find ?v :where [?e :r/name "n"] [?e :r/st ?v]]`},
		{"value-bound match", `[:find ?e :where [?e :r/st "s1"]]`},
		{"get-else", `[:find ?e ?v :where [?e :r/name "n"] [(get-else $ ?e :r/st "none") ?v]]`},
		{"pull", `[:find (pull ?e [:r/tags :r/st]) :where [?e :r/name "n"]]`},
	}
	run := func(t *testing.T, text string, opts ...db.Option) string {
		s, err := schema.NewBuilder().
			Attribute(":r/name").Type(schema.TypeString).Add().
			Attribute(":r/tags").Type(schema.TypeString).Many().Add().
			Attribute(":r/st").Type(schema.TypeString).Add().
			Build()
		require.NoError(t, err)
		d, err := db.OpenMemory(append([]db.Option{db.WithSchema(s)}, opts...)...)
		require.NoError(t, err)
		defer func() { require.NoError(t, d.Close()) }()
		e := datalog.NewIdentity("r:e")

		tx := d.NewTransaction()
		require.NoError(t, tx.Add(e, name, "n"))
		require.NoError(t, tx.Add(e, tags, "a"))
		require.NoError(t, tx.Add(e, st, "s1"))
		_, err = tx.Commit()
		require.NoError(t, err)

		open := d.NewTransaction()
		require.NoError(t, open.Add(e, tags, "b"))
		require.NoError(t, open.Add(e, st, "s2"))

		rel, queryErr := d.Query(text)
		_, err = open.Commit()
		require.NoError(t, err)

		for _, later := range []string{
			`[:find ?v :in $ ?e :where [?e :r/tags ?v]]`,
			`[:find ?v :in $ ?e :where [?e :r/st ?v]]`,
		} {
			laterRel, laterErr := d.Query(later, e)
			_, err := executor.CollectTuples(laterRel, laterErr)
			require.NoError(t, err)
		}

		tuples, err := executor.CollectTuples(rel, queryErr)
		require.NoError(t, err)
		out := make([]string, 0, len(tuples))
		for _, tuple := range tuples {
			out = append(out, fmt.Sprint(tuple[len(tuple)-1]))
		}
		sort.Strings(out)
		return fmt.Sprint(out)
	}
	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			uncached := run(t, route.text, db.WithoutCache())
			cached := run(t, route.text)
			require.Equal(t, uncached, cached, "the cached query answers as the uncached one")
		})
	}
}
