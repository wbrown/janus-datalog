package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

type reviewMemoryRule struct {
	Key       string
	DependsOn []string
}

// A slice source answers the slice's facts and nothing else: a pattern whose
// attribute is a variable, outside the transaction entities, answers each of
// the slice's facts, whether or not some item holds two elements at an
// attribute.
func TestReviewMemory_ASliceSourceAnswersOnlyTheSlicesFacts(t *testing.T) {
	key, dependsOn := datalog.NewKeyword(":rule/key"), datalog.NewKeyword(":rule/depends-on")
	slice0, slice1 := datalog.NewIdentity("slice:0"), datalog.NewIdentity("slice:1")
	cases := []struct {
		name  string
		rules []reviewMemoryRule
		want  [][]interface{}
	}{
		{"every item holds one element",
			[]reviewMemoryRule{{Key: "a", DependsOn: []string{"x"}}, {Key: "b", DependsOn: []string{"y"}}},
			[][]interface{}{{slice0, key, "a"}, {slice0, dependsOn, "x"}, {slice1, key, "b"}, {slice1, dependsOn, "y"}}},
		{"one item holds two elements",
			[]reviewMemoryRule{{Key: "a", DependsOn: []string{"x", "z"}}, {Key: "b", DependsOn: []string{"y"}}},
			[][]interface{}{{slice0, key, "a"}, {slice0, dependsOn, "x"}, {slice0, dependsOn, "z"}, {slice1, key, "b"}, {slice1, dependsOn, "y"}}},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				source, err := db.NewSliceSource(c.rules, db.AttributeSchema[reviewMemoryRule]{
					key:       func(r reviewMemoryRule) any { return r.Key },
					dependsOn: func(r reviewMemoryRule) any { return r.DependsOn },
				})
				require.NoError(t, err)
				defer func() { require.NoError(t, source.Close()) }()
				host, err := db.OpenMemory(db.WithPlannerOptions(mode.plannerOptions()))
				require.NoError(t, err)
				defer func() { require.NoError(t, host.Close()) }()
				got, err := executor.CollectTuples(host.Query(`[:find ?e ?a ?v :in $ $s :where [$s ?e ?a ?v] (not [$s ?e :db/txInstant _])]`,
					db.WithSources(map[datalog.Symbol]db.PatternMatcher{datalog.NewSymbol("$s"): source})))
				require.NoError(t, err)
				require.ElementsMatch(t, c.want, got)
			})
		}
	}
}

// A wildcard pull answers every attribute an entity carries. On a slice
// source's database, the item slice:1 answers its key and its element whether
// or not another item holds two elements.
func TestReviewMemory_AWildcardPullOnASliceSourceAnswersEveryAttribute(t *testing.T) {
	cases := []struct {
		name  string
		rules []reviewMemoryRule
	}{
		{"every item holds one element",
			[]reviewMemoryRule{{Key: "a", DependsOn: []string{"x"}}, {Key: "b", DependsOn: []string{"y"}}}},
		{"another item holds two elements",
			[]reviewMemoryRule{{Key: "a", DependsOn: []string{"x", "z"}}, {Key: "b", DependsOn: []string{"y"}}}},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				source, err := db.NewSliceSource(c.rules, db.AttributeSchema[reviewMemoryRule]{
					datalog.NewKeyword(":rule/key"):        func(r reviewMemoryRule) any { return r.Key },
					datalog.NewKeyword(":rule/depends-on"): func(r reviewMemoryRule) any { return r.DependsOn },
				})
				require.NoError(t, err)
				defer func() { require.NoError(t, source.Close()) }()
				got, err := executor.CollectTuples(source.Query(`[:find ?k (pull ?e [*]) :where [?e :rule/key ?k] [(= ?k "b")]]`))
				require.NoError(t, err)
				require.Len(t, got, 1)
				pulled, ok := got[0][1].(map[string]interface{})
				require.True(t, ok, "the pull answers a map, got %T", got[0][1])
				require.Equal(t, "b", pulled["rule/key"], "slice:1 answers its key")
				require.Contains(t, pulled, "rule/depends-on", "slice:1 answers its element")
			})
		}
	}
}
