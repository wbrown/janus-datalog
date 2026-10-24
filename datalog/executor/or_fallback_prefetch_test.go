package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// prefetchingMatcher is a memory matcher that also serves an attribute by
// lookup and prefetches: it records the entities of each PrefetchEntities
// call and counts the lookups the EA arm makes.
type prefetchingMatcher struct {
	PatternMatcher
	values     map[bundleLookupKey]interface{}
	lookups    int
	prefetched [][]datalog.Identity
}

func (m *prefetchingMatcher) LookupAttribute(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool, error) {
	m.lookups++
	value, ok := m.values[bundleLookupKey{entity: entity, attr: attr}]
	return value, ok, nil
}

func (m *prefetchingMatcher) CanFuseAttributeFetch(query.Symbol, datalog.Keyword) bool {
	return true
}

func (m *prefetchingMatcher) PrefetchEntities(entities []datalog.Identity) error {
	m.prefetched = append(m.prefetched, append([]datalog.Identity(nil), entities...))
	return nil
}

// Under EnableEntityPrefetch a fallback prefetches a window's distinct
// entities before the window's branches are built when the window holds more
// entities than the threshold and a branch of one data pattern reads the
// subject's entity symbol, reports the trigger under the form, and serves
// that branch from the EA cache; a window at or below the threshold
// prefetches nothing, and with the option off nothing is prefetched and the
// branch is built from its plan
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.10).
func TestFallbackPrefetchesAWindowsEntitiesAboveTheThreshold(t *testing.T) {
	// Under a cap of 128 the subject is read in windows of 1, 2, 4, 8, 16, 32
	// and 57 tuples, the last the one window above the threshold.
	const subjectSize = 120
	const lastWindow = 57
	symE, symV := datalog.NewSymbol("?e"), datalog.NewSymbol("?v")
	attr := datalog.NewKeyword(":sheet/folio")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	values := map[bundleLookupKey]interface{}{}
	var datoms []datalog.Datom
	subject := make([]Tuple, 0, subjectSize)
	want := make([][]interface{}, 0, subjectSize)
	for i := 0; i < subjectSize; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("prefetch:sheet%d", i))
		values[bundleLookupKey{entity: e, attr: attr}] = int64(i)
		datoms = append(datoms, datalog.Datom{E: e, A: attr, V: int64(i), Tx: tx})
		subject = append(subject, Tuple{e})
		want = append(want, []interface{}{e, int64(i)})
	}
	branch := []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: symE}, query.Constant{Value: attr}, query.Variable{Name: symV},
	}}}
	fallback := []query.Clause{&query.Expression{Function: &query.GroundFunction{Value: int64(-1)}, Binding: symV}}

	run := func(t *testing.T, prefetch bool) (*prefetchingMatcher, []annotations.Event, [][]interface{}) {
		matcher := &prefetchingMatcher{PatternMatcher: NewMemoryPatternMatcher(datoms), values: values}
		var triggers []annotations.Event
		options := ExecutorOptions{EnableEntityPrefetch: prefetch, ScopeWindowCap: 128, Handler: func(e annotations.Event) {
			if e.Name == annotations.PrefetchTrigger {
				triggers = append(triggers, e)
			}
		}}
		relation, err := NewOrFallbackRelation(
			newQueryExecutor(matcher, nil, options),
			NewContext(),
			[]query.Symbol{symE},
			[]query.Symbol{symV},
			[][]query.Clause{branch, fallback},
			NewMaterializedRelation([]query.Symbol{symE}, subject),
			options,
			true,
		)
		require.NoError(t, err)
		result, err := CollectTuples(relation, nil)
		require.NoError(t, err)
		return matcher, triggers, result
	}

	t.Run("the option on", func(t *testing.T) {
		matcher, triggers, result := run(t, true)
		require.ElementsMatch(t, want, result, "every subject tuple takes its folio, from the plan before the trigger and from the cache after it")
		require.Len(t, matcher.prefetched, 1, "the one window above the threshold is prefetched")
		require.Len(t, matcher.prefetched[0], lastWindow, "the window's distinct entities are prefetched")
		require.Len(t, triggers, 1, "the trigger is reported once")
		require.Equal(t, lastWindow, triggers[0].Data["entity_count"], "the trigger reports the window's entity count")
		require.Equal(t, "or-default-join", triggers[0].Data[annotations.KeyForm], "the trigger reports the form")
		require.Equal(t, lastWindow, matcher.lookups, "the branch is served from the cache for the prefetched window alone")
	})

	t.Run("the option off", func(t *testing.T) {
		matcher, triggers, result := run(t, false)
		require.ElementsMatch(t, want, result)
		require.Empty(t, matcher.prefetched, "nothing is prefetched with the option off")
		require.Empty(t, triggers, "no trigger is reported with the option off")
		require.Zero(t, matcher.lookups, "the branch is built from its plan with the option off")
	})
}
