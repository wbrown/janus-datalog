package executor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestPullManyDoesNotBatchMixedWildcardPattern(t *testing.T) {
	name := datalog.NewKeyword(":entity/name")
	first := datalog.NewIdentity("mixed-pull-first")
	second := datalog.NewIdentity("mixed-pull-second")
	resolver := NewRecordingBatchEntityResolver(map[[20]byte]map[datalog.Keyword]interface{}{
		first.Hash():  {name: "First"},
		second.Hash(): {name: "Second"},
	})
	matcher := factsDatabase(t, []datalog.Datom{
		{E: first, A: name, V: "First"},
		{E: second, A: name, V: "Second"},
	})
	puller := NewPullExecutor(matcher, resolver)
	pattern := &query.PullPattern{Specs: []query.PullAttrSpec{
		&query.PullWildcard{},
		&query.PullAttribute{Attr: name},
	}}

	results, err := puller.PullMany([]datalog.Identity{first, second}, pattern)
	require.NoError(t, err)
	require.Zero(t, resolver.BatchCalls())
	require.Equal(t, 2, resolver.SingleCalls())
	require.Equal(t, "First", results[0]["entity/name"])
	require.Equal(t, "Second", results[1]["entity/name"])
}
