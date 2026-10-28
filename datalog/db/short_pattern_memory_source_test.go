package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A data pattern of fewer than three elements leaves the positions its text
// omits unbound, at every source: on a memory source named in :in, [$m ?e :t/n]
// answers what [$m ?e :t/n _] answers, and [$m ?e] what [$m ?e _ _] answers.
func TestAShortPatternOnAMemorySourceAnswersWhatItsPaddedPatternAnswers(t *testing.T) {
	n1 := datalog.NewIdentity("short-memory-n1")
	n2 := datalog.NewIdentity("short-memory-n2")
	m1 := datalog.NewIdentity("short-memory-m1")
	name := datalog.NewKeyword(":t/name")
	tn := datalog.NewKeyword(":t/n")
	tm := datalog.NewKeyword(":t/m")
	facts := []datalog.Datom{
		{E: n1, A: name, V: "n1"},
		{E: n1, A: tn, V: int64(1)},
		{E: n2, A: name, V: "n2"},
		{E: n2, A: tn, V: int64(3)},
		{E: m1, A: name, V: "m1"},
		{E: m1, A: tm, V: int64(4)},
	}

	d, err := db.OpenMemory()
	require.NoError(t, err)
	defer d.Close()
	source := db.WithSources(map[datalog.Symbol]db.PatternMatcher{
		datalog.NewSymbol("$m"): db.NewMemorySource(facts),
	})

	names := map[datalog.Identity]string{}
	nameTuples, err := executor.CollectTuples(d.Query(`[:find ?e ?n :in $ $m :where [$m ?e :t/name ?n]]`, source))
	require.NoError(t, err)
	for _, tuple := range nameTuples {
		names[tuple[0].(datalog.Identity)] = tuple[1].(string)
	}

	cases := []struct {
		name   string
		short  string
		padded string
	}{
		{"the value omitted", `[:find ?e :in $ $m :where [$m ?e :t/n]]`, `[:find ?e :in $ $m :where [$m ?e :t/n _]]`},
		{"the attribute and the value omitted", `[:find ?e :in $ $m :where [$m ?e]]`, `[:find ?e :in $ $m :where [$m ?e _ _]]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			want, err := executor.CollectTuples(d.Query(c.padded, source))
			require.NoError(t, err, c.padded)
			require.NotEmpty(t, want, "the padded pattern answers entities, so the comparison compares something")
			got, err := executor.CollectTuples(d.Query(c.short, source))
			require.NoError(t, err, c.short)
			require.ElementsMatch(t, want, got, "the short pattern answers what the padded pattern answers: want %v, got %v",
				renderIdentities(want, names), renderIdentities(got, names))
		})
	}
}

func renderIdentities(tuples [][]interface{}, names map[datalog.Identity]string) []string {
	rendered := make([]string, 0, len(tuples))
	for _, tuple := range tuples {
		rendered = append(rendered, names[tuple[0].(datalog.Identity)])
	}
	return rendered
}
