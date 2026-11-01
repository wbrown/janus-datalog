package db_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// db.Open with its default options compresses a string past 512 bytes; the
// value is one value under ValuesEqual, so binding it with its entity joins
// the cardinality-many datom holding it.
func TestDefaultOpenJoinsABoundLongMember(t *testing.T) {
	declared := schema.NewBuilder().
		Attribute(":folio/note").Type(schema.TypeString).Many().Add().
		MustBuild()
	note := datalog.NewKeyword(":folio/note")
	long := strings.Repeat("the quire is sewn and folded. ", 40)
	quire := datalog.NewIdentity("folio:quire")
	sheet := datalog.NewIdentity("folio:sheet")

	d, err := db.Open(t.TempDir(), db.WithSchema(declared))
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(quire, note, long))
	require.NoError(t, tx.Add(sheet, note, "short"))
	_, err = tx.Commit()
	require.NoError(t, err)

	tuples, err := executor.CollectTuples(d.Query(`[:find ?e :in $ [[?e ?v]] :where [?e :folio/note ?v]]`,
		[][]interface{}{{quire, long}, {sheet, long}}))
	require.NoError(t, err)
	var got []datalog.Identity
	for _, tuple := range tuples {
		got = append(got, tuple[0].(datalog.Identity))
	}
	require.ElementsMatch(t, []datalog.Identity{quire}, got)
}
