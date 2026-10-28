package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A keyword wider than storage holds names no stored attribute, so a pattern
// whose attribute position it binds reads nothing under it: the 32-byte
// attribute sharing its first 32 bytes is never answered in its place,
// whatever binds the position.
func TestAWideKeywordBoundAtTheAttributePositionNeverReadsTheAttributeItTruncatesTo(t *testing.T) {
	short := ":t/aaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	wide := short + "z"
	require.Len(t, short, 32)

	alice := datalog.NewIdentity("wide-binding-alice")
	pointer := datalog.NewIdentity("wide-binding-pointer")
	d, err := db.OpenMemory()
	require.NoError(t, err)
	defer d.Close()
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, datalog.NewKeyword(":t/name"), "alice"))
	require.NoError(t, tx.Add(alice, datalog.NewKeyword(short), "short"))
	require.NoError(t, tx.Add(pointer, datalog.NewKeyword(":t/name"), "pointer"))
	require.NoError(t, tx.Add(pointer, datalog.NewKeyword(":t/names"), datalog.NewKeyword(wide)))
	require.NoError(t, tx.Add(pointer, datalog.NewKeyword(":t/target"), alice))
	_, err = tx.Commit()
	require.NoError(t, err)

	cases := []struct {
		name string
		text string
	}{
		{"a ground, the entity bound", `[:find ?e ?v :where [?e :t/name "alice"] [(ground ` + wide + `) ?a] [?e ?a ?v]]`},
		{"a ground, the entity unbound", `[:find ?e ?v :where [(ground ` + wide + `) ?a] [?e ?a ?v]]`},
		{"a stored keyword value, the entity bound", `[:find ?e ?v :where [?p :t/names ?a] [?p :t/target ?e] [?e ?a ?v]]`},
		{"a stored keyword value, the entity unbound", `[:find ?e ?v :where [?p :t/names ?a] [?e ?a ?v]]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			answered, err := executor.CollectTuples(d.Query(c.text))
			if err != nil {
				require.Contains(t, err.Error(), "exceeds the 32-byte storage limit", "a refusal is the width rule's one message: %s", c.text)
				return
			}
			require.Empty(t, answered, "no attribute named %s is stored", wide)
		})
	}
}
