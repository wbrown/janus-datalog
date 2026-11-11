package db_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A traced query's match line names a result's tuple count when the result
// is sized, and names no count when it is a stream that declines to size
// itself (-1): no line of the trace reports "-1 Tuples", a count no relation
// holds, and the match line of the streamed pattern reads Relation([?e ?n]).
func TestReproAnUnsizedMatchLineNamesNoCount(t *testing.T) {
	name := datalog.NewKeyword(":person/name")
	age := datalog.NewKeyword(":person/age")
	ada := datalog.NewIdentity("person:ada")
	bob := datalog.NewIdentity("person:bob")
	facts := []struct {
		e datalog.Identity
		a datalog.Keyword
		v interface{}
	}{
		{ada, name, "Ada"}, {ada, age, int64(36)},
		{bob, name, "Bob"}, {bob, age, int64(41)},
	}
	var lines []string
	d, err := db.OpenMemory(db.WithVerboseCallback(func(line string) { lines = append(lines, line) }))
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()
	tx := d.NewTransaction()
	for _, f := range facts {
		require.NoError(t, tx.Add(f.e, f.a, f.v))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	lines = nil
	tuples, err := executor.CollectTuples(d.Query(`[:find ?n ?a :where [?e :person/name ?n] [?e :person/age ?a]]`))
	require.NoError(t, err)
	require.Len(t, tuples, 2)

	var matchLines []string
	for _, line := range lines {
		require.NotContains(t, line, "-1 Tuples", "a trace line reports a count no relation holds")
		if strings.Contains(line, "Pattern([?e :person/name ?n])") {
			matchLines = append(matchLines, line)
		}
	}
	require.NotEmpty(t, matchLines, "the trace holds the match line of [?e :person/name ?n]")
	t.Logf("match lines: %q", matchLines)
}
