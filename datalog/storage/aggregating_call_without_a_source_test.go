package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// An aggregating subquery whose inner :in declares a binding entry and no
// source reads the database its enclosing scope reads, and answers the same
// under both optimizer paths, through Query, QueryInto and Explain: each
// entity is answered the sum of :p/w over the entities sharing its :p/n.
func TestAnAggregatingCallWhoseInnerDeclaresNoSourceAnswers(t *testing.T) {
	e1, e2, e3 := named("e1"), named("e2"), named("e3")
	facts := []fact{
		{e1, ":p/n", int64(1)}, {e1, ":p/w", int64(100)},
		{e2, ":p/n", int64(1)}, {e2, ":p/w", int64(10)},
		{e3, ":p/n", int64(2)}, {e3, ":p/w", int64(1)},
	}
	byN := [][]interface{}{{e1.id, int64(110)}, {e2.id, int64(110)}, {e3.id, int64(1)}}
	texts := []struct{ name, text string }{
		{"an inner :in naming a scalar alone",
			`[:find ?e ?s :where [?e :p/n ?a] [(q [:find (sum ?v) :in ?n :where [?x :p/n ?n] [?x :p/w ?v]] ?a) [[?s]]]]`},
		{"an inner :in naming a source and a scalar",
			`[:find ?e ?s :where [?e :p/n ?a] [(q [:find (sum ?v) :in $ ?n :where [?x :p/n ?n] [?x :p/w ?v]] $ ?a) [[?s]]]]`},
	}
	type sumOf struct {
		E datalog.Identity `datalog:"?e"`
		S int64            `datalog:"?s"`
	}
	entries := []struct {
		name   string
		answer func(d *Database, text string) ([][]interface{}, error)
	}{
		{"Query", func(d *Database, text string) ([][]interface{}, error) {
			return executor.CollectTuples(d.Query(text))
		}},
		{"QueryInto", func(d *Database, text string) ([][]interface{}, error) {
			var sums []sumOf
			if err := d.QueryInto(&sums, text); err != nil {
				return nil, err
			}
			tuples := make([][]interface{}, len(sums))
			for i, s := range sums {
				tuples[i] = []interface{}{s.E, s.S}
			}
			return tuples, nil
		}},
		{"Explain", func(d *Database, text string) ([][]interface{}, error) {
			if _, err := d.Explain(text); err != nil {
				return nil, err
			}
			return executor.CollectTuples(d.Query(text))
		}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, facts)
			for _, entry := range entries {
				for _, c := range texts {
					t.Run(entry.name+"/"+c.name, func(t *testing.T) {
						var tuples [][]interface{}
						var err error
						func() {
							defer func() {
								if r := recover(); r != nil {
									err = fmt.Errorf("panic: %v", r)
								}
							}()
							tuples, err = entry.answer(d, c.text)
						}()
						require.NoError(t, err)
						require.ElementsMatch(t, byN, tuples, "names %v", names)
					})
				}
			}
		})
	}
}
