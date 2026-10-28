package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A direct pull is refused as a find pull of the same pattern is: a limit
// below zero or a default outside the value domain, at the pattern's top
// level or under a ref spec, is refused by Pull, PullMany and an AsOf
// handle's Pull with the refusal Query gives the find pull, and never
// answers.
func TestADirectPullIsRefusedAsAFindPullIs(t *testing.T) {
	alice := datalog.NewIdentity("direct-shape-alice")
	bob := datalog.NewIdentity("direct-shape-bob")
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
	require.NoError(t, tx.Add(bob, datalog.NewKeyword(":t/name"), "bob"))
	require.NoError(t, tx.Add(bob, datalog.NewKeyword(":t/friend"), alice))
	committed, err := tx.Commit()
	require.NoError(t, err)

	patterns := []struct {
		name  string
		text  string
		names string
	}{
		{"a limit of -1", `[:t/name (limit :t/tags -1)]`, "must be non-negative, got -1"},
		{"a limit of -1 under a ref spec", `[:t/name {:t/friend [(limit :t/tags -1)]}]`, "must be non-negative, got -1"},
		{"a NaN default", `[:t/name (default :t/age ##NaN)]`, "NaN"},
		{"a NaN default under a ref spec", `[:t/name {:t/friend [(default :t/age ##NaN)]}]`, "NaN"},
	}
	entries := []struct {
		name string
		pull func(pattern string) (interface{}, error)
	}{
		{"Pull", func(pattern string) (interface{}, error) { return d.Pull(bob, pattern) }},
		{"PullMany", func(pattern string) (interface{}, error) { return d.PullMany([]datalog.Identity{bob}, pattern) }},
		{"an AsOf handle's Pull", func(pattern string) (interface{}, error) { return d.AsOf(committed).Pull(bob, pattern) }},
	}
	for _, p := range patterns {
		t.Run(p.name, func(t *testing.T) {
			_, findErr := d.Query(`[:find (pull ?e ` + p.text + `) :where [?e :t/name _]]`)
			require.ErrorContains(t, findErr, p.names, "the find pull is refused")
			for _, entry := range entries {
				t.Run(entry.name, func(t *testing.T) {
					answered, err := entry.pull(p.text)
					require.Error(t, err, "the direct pull is refused; answered %v", answered)
					require.ErrorContains(t, err, p.names)
				})
			}
		})
	}
}
