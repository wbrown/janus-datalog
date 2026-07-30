package executor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

type recordingBatchEntityResolver struct {
	values      map[[20]byte]map[datalog.Keyword]interface{}
	singleCalls int
	batchCalls  int
	batchErr    error
}

func (r *recordingBatchEntityResolver) ResolveAllAttributes(
	entity datalog.Identity,
) (map[datalog.Keyword]interface{}, error) {
	r.singleCalls++
	return r.values[entity.Hash()], nil
}

func (r *recordingBatchEntityResolver) ResolveAllAttributesMany(
	entities []datalog.Identity,
) ([]map[datalog.Keyword]interface{}, error) {
	r.batchCalls++
	if r.batchErr != nil {
		return nil, r.batchErr
	}
	results := make([]map[datalog.Keyword]interface{}, len(entities))
	for i, entity := range entities {
		results[i] = r.values[entity.Hash()]
	}
	return results, nil
}

func TestApplyFindPullsBatchesWildcardEntities(t *testing.T) {
	entitySymbol := datalog.NewSymbol("?entity")
	indexSymbol := datalog.NewSymbol("?index")
	name := datalog.NewKeyword(":entity/name")
	first := datalog.NewIdentity("batch-pull-first")
	second := datalog.NewIdentity("batch-pull-second")
	var events []annotations.Event
	input := NewMaterializedRelationWithOptions(
		[]query.Symbol{entitySymbol, indexSymbol},
		[]Tuple{{first, int64(0)}, {second, int64(1)}, {first, int64(2)}},
		ExecutorOptions{Handler: func(event annotations.Event) {
			events = append(events, event)
		}},
	)
	resolver := &recordingBatchEntityResolver{
		values: map[[20]byte]map[datalog.Keyword]interface{}{
			first.Hash():  {name: "First"},
			second.Hash(): {name: "Second"},
		},
	}
	find := []query.FindElement{query.FindPull{
		Variable: entitySymbol,
		Pattern: &query.PullPattern{
			Specs: []query.PullAttrSpec{&query.PullWildcard{}},
		},
	}}

	result, err := applyFindPulls(nil, resolver, input, find)
	require.NoError(t, err)
	require.Equal(t, 1, resolver.batchCalls)
	require.Zero(t, resolver.singleCalls)

	tuples, err := CollectTuples(result, nil)
	require.NoError(t, err)
	require.Len(t, tuples, 3)
	require.Equal(t, "First", tuples[0][0].(map[string]interface{})["entity/name"])
	require.Equal(t, first, tuples[0][0].(map[string]interface{})[query.DBIDKey])
	require.Equal(t, "Second", tuples[1][0].(map[string]interface{})["entity/name"])
	require.Equal(t, second, tuples[1][0].(map[string]interface{})[query.DBIDKey])
	require.Equal(t, "First", tuples[2][0].(map[string]interface{})["entity/name"])
	require.Equal(t, first, tuples[2][0].(map[string]interface{})[query.DBIDKey])
	require.Len(t, events, 2)
	require.Equal(t, annotations.PullBatchBegin, events[0].Name)
	require.Equal(t, 3, events[0].Data["entity_count"])
	require.Equal(t, annotations.PullBatchComplete, events[1].Name)
	require.Equal(t, true, events[1].Data["success"])
}

func TestApplyFindPullsPropagatesBatchWildcardError(t *testing.T) {
	entitySymbol := datalog.NewSymbol("?entity")
	entity := datalog.NewIdentity("batch-pull-error")
	input := NewMaterializedRelation(
		[]query.Symbol{entitySymbol},
		[]Tuple{{entity}},
	)
	batchErr := errors.New("batch wildcard failure")
	resolver := &recordingBatchEntityResolver{batchErr: batchErr}
	find := []query.FindElement{query.FindPull{
		Variable: entitySymbol,
		Pattern: &query.PullPattern{
			Specs: []query.PullAttrSpec{&query.PullWildcard{}},
		},
	}}

	_, err := applyFindPulls(nil, resolver, input, find)
	require.ErrorIs(t, err, batchErr)
}

func TestPullManyDoesNotBatchMixedWildcardPattern(t *testing.T) {
	name := datalog.NewKeyword(":entity/name")
	first := datalog.NewIdentity("mixed-pull-first")
	second := datalog.NewIdentity("mixed-pull-second")
	resolver := &recordingBatchEntityResolver{
		values: map[[20]byte]map[datalog.Keyword]interface{}{
			first.Hash():  {name: "First"},
			second.Hash(): {name: "Second"},
		},
	}
	matcher := NewIndexedMemoryMatcher([]datalog.Datom{
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
	require.Zero(t, resolver.batchCalls)
	require.Equal(t, 2, resolver.singleCalls)
	require.Equal(t, "First", results[0]["entity/name"])
	require.Equal(t, "Second", results[1]["entity/name"])
}

func TestApplyFindPullsPropagatesInputIteratorAndCloseErrors(t *testing.T) {
	entitySymbol := datalog.NewSymbol("?entity")
	entity := datalog.NewIdentity("batch-pull-input-error")
	base := NewMaterializedRelation(
		[]query.Symbol{entitySymbol},
		[]Tuple{{entity}},
	)
	resolver := &recordingBatchEntityResolver{}
	find := []query.FindElement{query.FindPull{
		Variable: entitySymbol,
		Pattern: &query.PullPattern{
			Specs: []query.PullAttrSpec{&query.PullWildcard{}},
		},
	}}

	_, err := applyFindPulls(
		nil,
		resolver,
		failingRelation{Relation: base, failAfter: 0},
		find,
	)
	require.ErrorIs(t, err, errInjectedIterator)
	require.Zero(t, resolver.batchCalls)

	closeErr := errors.New("pull input close failure")
	_, err = applyFindPulls(
		nil,
		resolver,
		failingRelation{Relation: base, failAfter: 100, closeErr: closeErr},
		find,
	)
	require.ErrorIs(t, err, closeErr)
	require.Zero(t, resolver.batchCalls)
}
