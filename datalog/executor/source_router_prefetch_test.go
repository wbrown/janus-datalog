package executor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// matchesNothing is a source with no capability beyond the match, which
// answers the empty relation.
type matchesNothing struct{}

func (matchesNothing) Match(*query.Query, Relations) (Relation, error) {
	return NewMaterializedRelation(nil, nil), nil
}

// recordingPrefetcher is a source that records each set of entities it is
// asked to prefetch.
type recordingPrefetcher struct {
	PatternMatcher
	prefetched [][]datalog.Identity
}

func (p *recordingPrefetcher) PrefetchEntities(entities []datalog.Identity) error {
	p.prefetched = append(p.prefetched, entities)
	return nil
}

// The router forwards a prefetch to the default source, the source a
// fallback's prefetched branch reads, and to no named source; a default
// source that is no prefetcher prefetches nothing and fails nothing, as the
// matcher wrappers answer over an inner matcher without the capability.
func TestTheRouterForwardsPrefetchToTheDefaultSource(t *testing.T) {
	entities := []datalog.Identity{datalog.NewIdentity("router:one"), datalog.NewIdentity("router:two")}
	t.Run("the default source prefetches", func(t *testing.T) {
		defaultSource := &recordingPrefetcher{PatternMatcher: matchesNothing{}}
		namedSource := &recordingPrefetcher{PatternMatcher: matchesNothing{}}
		var router EntityPrefetcher = NewSourceRouter(map[query.Symbol]PatternMatcher{
			datalog.SymDollar:           defaultSource,
			datalog.NewSymbol("$other"): namedSource,
		})
		require.NoError(t, router.PrefetchEntities(entities))
		require.Equal(t, [][]datalog.Identity{entities}, defaultSource.prefetched, "the default source is asked once, with the entities")
		require.Empty(t, namedSource.prefetched, "a named source is not asked")
	})
	t.Run("a default source that is no prefetcher", func(t *testing.T) {
		var router EntityPrefetcher = NewSourceRouter(map[query.Symbol]PatternMatcher{datalog.SymDollar: matchesNothing{}})
		require.NoError(t, router.PrefetchEntities(entities), "nothing is prefetched and nothing fails")
	})
}
