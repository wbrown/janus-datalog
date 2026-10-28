package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/qb"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A pull's (limit ...) of a negative count is refused by the parser, so it is
// refused at every boundary: Build refuses the built pull holding one, at the
// pattern's top level and under a ref spec, as ParseQuery refuses its
// rendering, and Query never runs it.
func TestAPullLimitOfANegativeCountIsRefusedAtEveryBoundary(t *testing.T) {
	alice := datalog.NewIdentity("pull-limit-alice")
	bob := datalog.NewIdentity("pull-limit-bob")
	s := schema.NewBuilder().
		Attribute(":t/name").Type(schema.TypeString).Add().
		Attribute(":t/tags").Type(schema.TypeString).Many().Add().
		Attribute(":t/friend").Type(schema.TypeRef).Add().
		MustBuild()
	d, err := db.OpenMemory(db.WithSchema(s))
	require.NoError(t, err)
	defer d.Close()
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, datalog.NewKeyword(":t/name"), "alice"))
	require.NoError(t, tx.Add(alice, datalog.NewKeyword(":t/tags"), "a"))
	require.NoError(t, tx.Add(alice, datalog.NewKeyword(":t/tags"), "b"))
	require.NoError(t, tx.Add(bob, datalog.NewKeyword(":t/name"), "bob"))
	require.NoError(t, tx.Add(bob, datalog.NewKeyword(":t/friend"), alice))
	_, err = tx.Commit()
	require.NoError(t, err)

	e := qb.NewVar("e")
	cases := []struct {
		name string
		text string
		pull qb.PullExpr
	}{
		{"at the pattern's top level",
			`[:find (pull ?e [(limit :t/tags -1)]) :where [?e :t/name _]]`,
			qb.Pull(e, qb.Limit(qb.Kw(":t/tags"), -1))},
		{"under a ref spec",
			`[:find (pull ?e [{:t/friend [(limit :t/tags -1)]}]) :where [?e :t/name _]]`,
			qb.Pull(e, qb.Ref(qb.Kw(":t/friend"), qb.Limit(qb.Kw(":t/tags"), -1)))},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, parseErr := parser.ParseQuery(c.text)
			require.Error(t, parseErr, "the parser refuses the text")

			built, buildErr := qb.Query().
				Find(c.pull).
				Where(qb.Pat(e, qb.Kw(":t/name"), qb.Blank())).
				Build()
			var answered [][]interface{}
			var queryErr error
			require.NotPanics(t, func() {
				if buildErr == nil {
					answered, queryErr = executor.CollectTuples(d.Query(built))
				}
			})
			require.Error(t, buildErr, "Build refuses the built pull as the parser refuses its rendering; Query answered %v, error %v", answered, queryErr)
		})
	}
}
