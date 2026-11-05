package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A get-else answers the same with the algebra optimizer on and off
// (optimizer_modes_test.go) on a slice source and on a memory source, each a
// database: the slice's facts, and the log's resolved value, the datom with
// the latest transaction.
func TestReviewAlgebra_GetElseOnASourceAnswersAlikeUnderBothOptimizerPaths(t *testing.T) {
	type item struct{ Name, A string }
	sliceSource := func() executor.PatternMatcher {
		return sliceDatabase(t, []item{{"one", "a-one"}, {"two", "a-two"}}, AttributeSchema[item]{
			kw(":t/name"): func(i item) any { return i.Name },
			kw(":t/a"):    func(i item) any { return i.A },
		})
	}
	sliceFacts := [][]interface{}{{datalog.NewIdentity("slice:0"), "a-one"}, {datalog.NewIdentity("slice:1"), "a-two"}}
	one := datalog.NewIdentity("review-algebra:one")
	memorySource := func() executor.PatternMatcher {
		first := datalog.ElementID{Lamport: 1, ReplicaID: 1}
		second := datalog.ElementID{Lamport: 2, ReplicaID: 1}
		return logDatabase(t, []datalog.Datom{
			{E: one, A: kw(":t/name"), V: "one", Tx: first},
			{E: one, A: kw(":t/a"), V: "old", Tx: first},
			{E: one, A: kw(":t/a"), V: "new", Tx: second},
		})
	}
	cases := []struct {
		name    string
		source  string
		matcher func() executor.PatternMatcher
		text    string
		want    [][]interface{}
		wantErr string
	}{
		{
			name:    "slice source named $cache, every entity carries the attribute",
			source:  "$cache",
			matcher: sliceSource,
			text:    `[:find ?e ?v :in $ $cache :where [$cache ?e :t/name _] [(get-else $cache ?e :t/a "none") ?v]]`,
			want:    sliceFacts,
		},
		{
			name:    "slice source passed as $, every entity carries the attribute",
			source:  "$",
			matcher: sliceSource,
			text:    `[:find ?e ?v :where [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]`,
			want:    sliceFacts,
		},
		{
			name:    "memory source named $mem, an entity holding two datoms at the attribute",
			source:  "$mem",
			matcher: memorySource,
			text:    `[:find ?e ?v :in $ $mem :where [$mem ?e :t/name _] [(get-else $mem ?e :t/a "none") ?v]]`,
			want:    [][]interface{}{{one, "new"}},
		},
		{
			name:    "memory source passed as $, an entity holding two datoms at the attribute",
			source:  "$",
			matcher: memorySource,
			text:    `[:find ?e ?v :where [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]`,
			want:    [][]interface{}{{one, "new"}},
		},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				got, err := executor.CollectTuples(d.Query(c.text,
					WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol(c.source): c.matcher()})))
				if c.wantErr != "" {
					require.Error(t, err, "answered %v", got)
					require.Contains(t, err.Error(), c.wantErr)
					return
				}
				require.NoError(t, err)
				require.ElementsMatch(t, c.want, got)
			})
		}
	}
}
