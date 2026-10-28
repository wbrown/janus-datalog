package db_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A data pattern of fewer than three elements leaves the positions its text
// omits unbound, at every source and on every route into the source: with the
// entity written as a constant, with the entity bound from the default source,
// and on a slice source, the short pattern answers what its blank-padded
// pattern answers.
func TestAShortPatternAnswersWhatItsPaddedPatternAnswersAtEverySource(t *testing.T) {
	n1 := datalog.NewIdentity("every-source-n1")
	n2 := datalog.NewIdentity("every-source-n2")
	m1 := datalog.NewIdentity("every-source-m1")
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
	type row struct {
		Name string
		N    int64
	}
	rows := []row{{"r1", 1}, {"r2", 3}}

	d, err := db.OpenMemory()
	require.NoError(t, err)
	defer d.Close()
	tx := d.NewTransaction()
	for _, f := range facts {
		require.NoError(t, tx.Add(f.E, name, f.V))
	}
	_, err = tx.Commit()
	require.NoError(t, err)
	sources := db.WithSources(map[datalog.Symbol]db.PatternMatcher{
		datalog.NewSymbol("$m"): db.NewMemorySource(facts),
		datalog.NewSymbol("$s"): executor.NewSliceSource(rows, executor.AttributeSchema[row]{
			name: func(r row) any { return r.Name },
			tn:   func(r row) any { return r.N },
		}),
	})
	entity := fmt.Sprintf("#identity %q", n1.L85())

	cases := []struct {
		name   string
		short  string
		padded string
		inputs []interface{}
	}{
		{"the entity written, the value omitted",
			fmt.Sprintf(`[:find ?a :in $ $m :where [$m %s ?a]]`, entity),
			fmt.Sprintf(`[:find ?a :in $ $m :where [$m %s ?a _]]`, entity), nil},
		{"the entity given as an input, the attribute and the value omitted",
			`[:find ?e :in $ $m ?e :where [$m ?e]]`,
			`[:find ?e :in $ $m ?e :where [$m ?e _ _]]`, []interface{}{n1}},
		{"the entity bound from the default source, the value omitted",
			`[:find ?e :in $ $m :where [?e :t/name _] [$m ?e :t/n]]`,
			`[:find ?e :in $ $m :where [?e :t/name _] [$m ?e :t/n _]]`, nil},
		{"the entity bound from the default source, the attribute and the value omitted",
			`[:find ?e :in $ $m :where [?e :t/name _] [$m ?e]]`,
			`[:find ?e :in $ $m :where [?e :t/name _] [$m ?e _ _]]`, nil},
		{"a slice source, the value omitted",
			`[:find ?e :in $ $s :where [$s ?e :t/n]]`,
			`[:find ?e :in $ $s :where [$s ?e :t/n _]]`, nil},
		{"a slice source, the attribute and the value omitted",
			`[:find ?e :in $ $s :where [$s ?e]]`,
			`[:find ?e :in $ $s :where [$s ?e _ _]]`, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			arguments := append(append([]interface{}{}, c.inputs...), sources)
			want, err := executor.CollectTuples(d.Query(c.padded, arguments...))
			require.NoError(t, err, c.padded)
			require.NotEmpty(t, want, "the padded pattern answers tuples, so the comparison compares something")
			got, err := executor.CollectTuples(d.Query(c.short, arguments...))
			require.NoError(t, err, c.short)
			require.ElementsMatch(t, want, got, "the short pattern answers what the padded pattern answers")
		})
	}
}
