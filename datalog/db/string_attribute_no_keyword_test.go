package db_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
)

// A database function's attribute given as a string is read as a keyword, and
// a string that is no keyword, one with no name after the colon or one holding
// a space, is refused by the query's text boundary with an error, never by a
// panic out of Query.
func TestAStringAttributeThatIsNoKeywordIsRefusedByQueryWithAnError(t *testing.T) {
	alice := datalog.NewIdentity("string-attribute-alice")
	d, err := db.OpenMemory()
	require.NoError(t, err)
	defer d.Close()
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, datalog.NewKeyword(":t/name"), "alice"))
	_, err = tx.Commit()
	require.NoError(t, err)

	cases := []struct {
		name string
		text string
	}{
		{"get-else, a space", `[:find ?v :where [?e :t/name _] [(get-else $ ?e ":t/first name" "none") ?v]]`},
		{"get-else, no name", `[:find ?v :where [?e :t/name _] [(get-else $ ?e ":" "none") ?v]]`},
		{"get-some, a space", `[:find ?a ?v :where [?e :t/name _] [(get-some $ ?e ":t/first name") [?a ?v]]]`},
		{"missing?, a space", `[:find ?e :where [?e :t/name _] [(missing? $ ?e ":t/first name")]]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var queryErr error
			require.NotPanics(t, func() {
				_, queryErr = d.Query(c.text)
			}, c.text)
			require.Error(t, queryErr, fmt.Sprintf("%s is refused", c.text))
		})
	}
}
