package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
)

// A pull default is a constant of the pattern: one outside the value domain
// is refused by a direct pull as a find pull refuses it, and never answers.
func TestADirectPullRefusesADefaultOutsideTheDomain(t *testing.T) {
	alice := datalog.NewIdentity("direct-pull-alice")
	d, err := db.OpenMemory()
	require.NoError(t, err)
	defer d.Close()
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, datalog.NewKeyword(":t/name"), "alice"))
	_, err = tx.Commit()
	require.NoError(t, err)

	pattern := `[:t/name (default :t/age ##NaN)]`
	_, findErr := d.Query(`[:find (pull ?e ` + pattern + `) :where [?e :t/name _]]`)
	require.Error(t, findErr, "a find pull refuses the default")

	cases := []struct {
		name string
		pull func() (interface{}, error)
	}{
		{"Pull", func() (interface{}, error) { return d.Pull(alice, pattern) }},
		{"PullMany", func() (interface{}, error) { return d.PullMany([]datalog.Identity{alice}, pattern) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			answered, err := c.pull()
			require.Error(t, err, "the default is refused; answered %v", answered)
		})
	}
}
