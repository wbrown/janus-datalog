package executor

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// countingMatcher wraps a PatternMatcher and counts Match() calls.
type countingMatcher struct {
	inner     PatternMatcher
	callCount int32
}

func (m *countingMatcher) Match(q *query.Query, bindings Relations) (Relation, error) {
	atomic.AddInt32(&m.callCount, 1)
	return m.inner.Match(q, bindings)
}

// fixedMatcher returns a fixed relation for any Match() call.
type fixedMatcher struct {
	tuples     []Tuple
	symbols    []query.Symbol
	options    ExecutorOptions
	properties RelationProperties
}

func (m *fixedMatcher) Match(q *query.Query, bindings Relations) (Relation, error) {
	return NewMaterializedRelationWithProperties(
		m.symbols,
		m.tuples,
		m.options,
		m.properties,
	), nil
}

// registryListing is a phase reading each of sites unbound, and a registry
// listing the sites its plan reads each shared scan at.
func registryListing(sites ...*query.DataPattern) (*query.Query, *ScanRegistry) {
	phase := &query.Query{}
	plan := &planner.RealizedPlan{Phases: []planner.RealizedPhase{{Query: phase}}}
	for _, site := range sites {
		phase.Where = append(phase.Where, site)
		plan.Phases[0].Patterns = append(plan.Phases[0].Patterns, planner.PatternPlan{Pattern: site})
	}
	registry := NewScanRegistry()
	registry.expect(sharedScanSites(plan))
	return phase, registry
}

// The plan reads one status pattern unbound at two sites, so the registry
// serves the two sites one scan and holds its head until both have read it.
// A match after that, at either site, is a new scan. A site whose entity a
// group binds is no site of the shared scan, and its match goes to the inner
// matcher with its bindings.
func TestTheRegistryReleasesAScansHeadWhenItsLastSiteReads(t *testing.T) {
	status := func(entity string) *query.DataPattern {
		return &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol(entity)},
			query.Constant{Value: datalog.NewKeyword(":task/status")},
			query.Constant{Value: datalog.NewKeyword(":status/complete")},
		}}
	}
	first, second, bound := status("?t"), status("?u"), status("?w")
	phase := &query.Query{
		Find:  []query.FindElement{query.FindVariable{Symbol: datalog.NewSymbol("?t")}},
		Where: []query.Clause{first, second, bound},
	}
	plan := &planner.RealizedPlan{Phases: []planner.RealizedPhase{{
		Query: phase,
		Patterns: []planner.PatternPlan{
			{Pattern: first},
			{Pattern: second},
			{Pattern: bound, BoundMask: planner.BoundMask{E: true}},
		},
	}}}
	inner := &countingMatcher{inner: &fixedMatcher{tuples: []Tuple{{"t1"}, {"t2"}}, symbols: first.Symbols()}}
	registry := NewScanRegistry()
	registry.expect(sharedScanSites(plan))
	matcher := NewScanSharingMatcher(inner, registry, nil)
	read := func(site *query.DataPattern, bindings Relations) [][]interface{} {
		rel, err := matcher.Match(patternMatchQuery(phase, site), bindings)
		require.NoError(t, err)
		tuples, err := CollectTuples(rel, nil)
		require.NoError(t, err)
		return tuples
	}
	want := [][]interface{}{{"t1"}, {"t2"}}

	require.Equal(t, want, read(first, nil), "the first site reads the scan")
	require.Equal(t, want, read(second, nil), "the second site reads the scan")
	require.Equal(t, int32(1), atomic.LoadInt32(&inner.callCount), "the two sites read one scan")
	shared := registry.lookup(FingerprintScan(patternMatchQuery(phase, first), first))
	require.NotNil(t, shared, "the registry holds the shared scan")
	require.Nil(t, shared.Seq, "the registry holds no head once every site has read")

	require.Equal(t, want, read(first, nil), "a site reading again reads the whole scan")
	require.Equal(t, int32(2), atomic.LoadInt32(&inner.callCount), "a match after the release is a new scan")

	group := Relations{NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?w")}, []Tuple{{"t1"}})}
	read(bound, group)
	require.Equal(t, int32(3), atomic.LoadInt32(&inner.callCount), "a site a group binds goes to the inner matcher")
}

// TestScanSharingMatcher_UnboundScanShared verifies that when the plan reads
// one unbound pattern at two sites, the inner matcher is called once.
func TestScanSharingMatcher_UnboundScanShared(t *testing.T) {
	inner := &countingMatcher{inner: &fixedMatcher{
		tuples:  []Tuple{{"e1", "s1"}, {"e2", "s2"}, {"e3", "s3"}},
		symbols: []query.Symbol{datalog.NewSymbol("?t"), datalog.NewSymbol("?s")},
	}}
	root := func() *query.DataPattern {
		return &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?t")},
				query.Constant{Value: datalog.NewKeyword(":task/root")},
				query.Variable{Name: datalog.NewSymbol("?s")},
			},
		}
	}
	first, second := root(), root()
	phase, reg := registryListing(first, second)
	matcher := NewScanSharingMatcher(inner, reg, nil)

	// First site — should hit inner matcher
	rel1, err := matcher.Match(patternMatchQuery(phase, first), nil)
	require.NoError(t, err)
	tuples1, err := CollectTuples(rel1, nil)
	require.NoError(t, err)
	assert.Len(t, tuples1, 3)

	// Second site — should hit registry, not inner matcher
	rel2, err := matcher.Match(patternMatchQuery(phase, second), nil)
	require.NoError(t, err)
	tuples2, err := CollectTuples(rel2, nil)
	require.NoError(t, err)
	assert.Len(t, tuples2, 3)

	assert.Equal(t, int32(1), atomic.LoadInt32(&inner.callCount),
		"inner matcher should be called exactly once")
}

// TestScanSharingMatcher_BoundScanNotShared verifies that sites whose entity
// the plan binds are not shared: each match goes to the inner matcher with
// its bindings.
func TestScanSharingMatcher_BoundScanNotShared(t *testing.T) {
	inner := &countingMatcher{inner: &fixedMatcher{
		tuples:  []Tuple{{"e1"}},
		symbols: []query.Symbol{datalog.NewSymbol("?t")},
	}}
	root := func() *query.DataPattern {
		return &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?t")},
				query.Constant{Value: datalog.NewKeyword(":task/root")},
				query.Variable{Name: datalog.NewSymbol("?s")},
			},
		}
	}
	first, second := root(), root()
	phase := &query.Query{Where: []query.Clause{first, second}}
	plan := &planner.RealizedPlan{Phases: []planner.RealizedPhase{{
		Query: phase,
		Patterns: []planner.PatternPlan{
			{Pattern: first, BoundMask: planner.BoundMask{E: true}},
			{Pattern: second, BoundMask: planner.BoundMask{E: true}},
		},
	}}}
	reg := NewScanRegistry()
	reg.expect(sharedScanSites(plan))
	matcher := NewScanSharingMatcher(inner, reg, nil)

	bindings := Relations{NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?t")},
		[]Tuple{{"e1"}},
	)}

	for _, site := range []*query.DataPattern{first, second} {
		rel, err := matcher.Match(patternMatchQuery(phase, site), bindings)
		require.NoError(t, err)
		tuples, err := CollectTuples(rel, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"e1"}}, tuples)
	}

	assert.Equal(t, int32(2), atomic.LoadInt32(&inner.callCount),
		"bound scans should not be shared — inner called every time")
}

// bindingsRecordingMatcher records the bindings each Match call passes it.
type bindingsRecordingMatcher struct {
	inner PatternMatcher
	seen  []Relations
}

func (m *bindingsRecordingMatcher) Match(q *query.Query, bindings Relations) (Relation, error) {
	m.seen = append(m.seen, bindings)
	return m.inner.Match(q, bindings)
}

// A site the plan reads unbound is shared though its match carries a group of
// another scope's variable, which binds none of the pattern's variables. The
// inner matcher is asked for the shared scan with no bindings. A subquery body
// run over a window of another scope's variable matches its leading pattern
// so.
func TestAScanWhoseBindingsBindNoneOfItsVariablesIsShared(t *testing.T) {
	status := func() *query.DataPattern {
		return &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?t")},
			query.Constant{Value: datalog.NewKeyword(":task/status")},
			query.Constant{Value: datalog.NewKeyword(":status/complete")},
		}}
	}
	sites := []*query.DataPattern{status(), status(), status()}
	phase, registry := registryListing(sites...)
	inner := &bindingsRecordingMatcher{inner: &fixedMatcher{
		tuples:  []Tuple{{"t1"}, {"t2"}},
		symbols: sites[0].Symbols(),
	}}
	matcher := NewScanSharingMatcher(inner, registry, nil)
	scenarios := Relations{NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?s")},
		[]Tuple{{"s1"}, {"s2"}},
	)}

	for consumer, site := range sites {
		rel, err := matcher.Match(patternMatchQuery(phase, site), scenarios)
		require.NoError(t, err)
		tuples, err := CollectTuples(rel, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"t1"}, {"t2"}}, tuples, "consumer %d answers the scan's tuples", consumer)
	}
	require.Len(t, inner.seen, 1, "the three consumers share one scan")
	require.Empty(t, inner.seen[0], "the shared scan is asked for with no bindings")
}

// TestScanSharingMatcher_DifferentPatternsIndependent verifies that
// different patterns are not shared.
func TestScanSharingMatcher_DifferentPatternsIndependent(t *testing.T) {
	inner := &countingMatcher{inner: &fixedMatcher{
		tuples:  []Tuple{{"e1"}},
		symbols: []query.Symbol{datalog.NewSymbol("?t")},
	}}
	pattern := func(attr string) *query.DataPattern {
		return &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?t")},
				query.Constant{Value: datalog.NewKeyword(attr)},
				query.Variable{Name: datalog.NewSymbol("?s")},
			},
		}
	}
	root, status := pattern(":task/root"), pattern(":task/status")
	phase, reg := registryListing(root, pattern(":task/root"), status, pattern(":task/status"))
	matcher := NewScanSharingMatcher(inner, reg, nil)

	for _, site := range []*query.DataPattern{root, status} {
		rel, err := matcher.Match(patternMatchQuery(phase, site), nil)
		require.NoError(t, err)
		tuples, err := CollectTuples(rel, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"e1"}}, tuples)
	}

	assert.Equal(t, int32(2), atomic.LoadInt32(&inner.callCount),
		"different patterns should each call inner matcher")
}

// TestScanSharingMatcher_SymbolRemapping verifies that when the second
// call uses different variable names, the returned relation has the
// correct symbols but identical tuple data.
func TestScanSharingMatcher_SymbolRemapping(t *testing.T) {
	inner := &countingMatcher{inner: &fixedMatcher{
		tuples:  []Tuple{{"e1", "v1"}, {"e2", "v2"}},
		symbols: []query.Symbol{datalog.NewSymbol("?t"), datalog.NewSymbol("?s")},
	}}
	p1 := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?t")},
			query.Constant{Value: datalog.NewKeyword(":task/root")},
			query.Variable{Name: datalog.NewSymbol("?s")},
		},
	}
	p2 := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?x")},
			query.Constant{Value: datalog.NewKeyword(":task/root")},
			query.Variable{Name: datalog.NewSymbol("?y")},
		},
	}
	phase, reg := registryListing(p1, p2)
	matcher := NewScanSharingMatcher(inner, reg, nil)

	rel1, err := matcher.Match(patternMatchQuery(phase, p1), nil)
	require.NoError(t, err)
	rel2, err := matcher.Match(patternMatchQuery(phase, p2), nil)
	require.NoError(t, err)

	assert.Equal(t, []query.Symbol{datalog.NewSymbol("?t"), datalog.NewSymbol("?s")}, rel1.Symbols())
	assert.Equal(t, []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}, rel2.Symbols())

	// Same tuple data
	tuples1, err := CollectTuples(rel1, nil)
	require.NoError(t, err)
	tuples2, err := CollectTuples(rel2, nil)
	require.NoError(t, err)
	require.Len(t, tuples1, 2)
	require.Len(t, tuples2, 2)
	assert.Equal(t, tuples1[0][0], tuples2[0][0])
	assert.Equal(t, tuples1[0][1], tuples2[0][1])

	assert.Equal(t, int32(1), atomic.LoadInt32(&inner.callCount))
}

// TestScanSharingMatcher_ThreeConsumers verifies that three sites of one
// pattern all return correct data from a single inner call.
func TestScanSharingMatcher_ThreeConsumers(t *testing.T) {
	tuples := make([]Tuple, 100)
	for i := range tuples {
		tuples[i] = Tuple{i}
	}
	inner := &countingMatcher{inner: &fixedMatcher{
		tuples:  tuples,
		symbols: []query.Symbol{datalog.NewSymbol("?i")},
	}}
	pattern := func() *query.DataPattern {
		return &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?i")},
				query.Constant{Value: datalog.NewKeyword(":attr")},
			},
		}
	}
	sites := []*query.DataPattern{pattern(), pattern(), pattern()}
	phase, reg := registryListing(sites...)
	matcher := NewScanSharingMatcher(inner, reg, nil)

	for consumer, site := range sites {
		rel, err := matcher.Match(patternMatchQuery(phase, site), nil)
		require.NoError(t, err)
		read, err := CollectTuples(rel, nil)
		require.NoError(t, err)
		assert.Len(t, read, 100, "site %d reads every tuple", consumer)
	}

	assert.Equal(t, int32(1), atomic.LoadInt32(&inner.callCount),
		"three consumers should share a single inner Match() call")
}

// TestScanSharingMatcher_AnnotationEvents verifies that cache-miss is
// emitted on first call and cache-hit on second.
func TestScanSharingMatcher_AnnotationEvents(t *testing.T) {
	inner := &fixedMatcher{
		tuples:  []Tuple{{"e1"}},
		symbols: []query.Symbol{datalog.NewSymbol("?t")},
	}
	pattern := func() *query.DataPattern {
		return &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?t")},
				query.Constant{Value: datalog.NewKeyword(":attr")},
			},
		}
	}
	sites := []*query.DataPattern{pattern(), pattern()}
	phase, reg := registryListing(sites...)

	var events []string
	handler := annotations.Handler(func(e annotations.Event) {
		events = append(events, e.Name)
	})
	matcher := NewScanSharingMatcher(inner, reg, handler)

	for _, site := range sites {
		_, err := matcher.Match(patternMatchQuery(phase, site), nil)
		require.NoError(t, err)
	}

	assert.Contains(t, events, annotations.ScanSharingCacheMiss)
	assert.Contains(t, events, annotations.ScanSharingCacheHit)
}

// Sites whose queries ask one pattern for a different order or limit read
// separate scans, each shared by the two sites that ask for it.
func TestScanSharingMatcherSeparatesPhysicalRequirements(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	pattern := func() *query.DataPattern {
		return &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity},
			query.Constant{Value: datalog.NewKeyword(":item/value")},
			query.Variable{Name: datalog.NewSymbol("?value")},
		}}
	}
	one, two := 1, 2
	shapes := []struct {
		orderBy []query.OrderByClause
		limit   *int
	}{
		{limit: &one},
		{limit: &two},
		{orderBy: []query.OrderByClause{{Variable: entity, Descending: false}}, limit: &one},
	}
	plan := &planner.RealizedPlan{}
	var phases []*query.Query
	var sites []*query.DataPattern
	for _, shape := range shapes {
		for reader := 0; reader < 2; reader++ {
			site := pattern()
			phase := &query.Query{Where: []query.Clause{site}, OrderBy: shape.orderBy, Limit: shape.limit}
			plan.Phases = append(plan.Phases, planner.RealizedPhase{
				Query:    phase,
				Patterns: []planner.PatternPlan{{Pattern: site}},
			})
			phases = append(phases, phase)
			sites = append(sites, site)
		}
	}
	registry := NewScanRegistry()
	registry.expect(sharedScanSites(plan))
	inner := &countingMatcher{inner: &fixedMatcher{
		tuples:  []Tuple{{"e1", int64(1)}, {"e2", int64(2)}},
		symbols: sites[0].Symbols(),
	}}
	matcher := NewScanSharingMatcher(inner, registry, nil)

	for i, site := range sites {
		_, err := matcher.Match(patternMatchQuery(phases[i], site), nil)
		require.NoError(t, err)
	}

	require.Equal(t, int32(3), atomic.LoadInt32(&inner.callCount))
}

func TestScanSharingMatcherPreservesAndRemapsProperties(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	renamedEntity := datalog.NewSymbol("?renamed-entity")
	renamedValue := datalog.NewSymbol("?renamed-value")
	first := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity},
		query.Constant{Value: datalog.NewKeyword(":item/value")},
		query.Variable{Name: value},
	}}
	second := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: renamedEntity},
		query.Constant{Value: datalog.NewKeyword(":item/value")},
		query.Variable{Name: renamedValue},
	}}
	options := ExecutorOptions{EnableTrueStreaming: true}
	inner := &fixedMatcher{
		tuples:  []Tuple{{"e1", int64(1)}},
		symbols: first.Symbols(),
		options: options,
		properties: RelationProperties{
			Ordering: []query.OrderByClause{{Variable: entity, Descending: false}},
			Keys:     [][]query.Symbol{{entity}},
		},
	}
	phase, registry := registryListing(first, second)
	matcher := NewScanSharingMatcher(inner, registry, nil)

	miss, err := matcher.Match(patternMatchQuery(phase, first), nil)
	require.NoError(t, err)
	hit, err := matcher.Match(patternMatchQuery(phase, second), nil)
	require.NoError(t, err)

	require.Equal(t, options, miss.Options())
	require.Equal(t, options, hit.Options())
	require.Equal(t, inner.properties, miss.Properties())
	require.Equal(t, RelationProperties{
		Ordering: []query.OrderByClause{{Variable: renamedEntity, Descending: false}},
		Keys:     [][]query.Symbol{{renamedEntity}},
	}, hit.Properties())
}

func TestScanSharingMatcherConcurrentSameFingerprintUsesOneScan(t *testing.T) {
	inner := &countingMatcher{inner: &fixedMatcher{
		tuples:  []Tuple{{"e1", int64(1)}, {"e2", int64(2)}, {"e3", int64(3)}},
		symbols: []query.Symbol{datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")},
	}}
	const consumers = 20
	sites := make([]*query.DataPattern, consumers)
	for consumer := range sites {
		sites[consumer] = &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?entity")},
			query.Constant{Value: datalog.NewKeyword(":item/value")},
			query.Variable{Name: datalog.NewSymbol("?value")},
		}}
	}
	phase, registry := registryListing(sites...)
	matcher := NewScanSharingMatcher(inner, registry, nil)

	start := make(chan struct{})
	results := make([][][]interface{}, consumers)
	errors := make([]error, consumers)
	var wait sync.WaitGroup
	for consumer := 0; consumer < consumers; consumer++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			relation, err := matcher.Match(patternMatchQuery(phase, sites[index]), nil)
			if err != nil {
				errors[index] = err
				return
			}
			results[index], errors[index] = CollectTuples(relation, nil)
		}(consumer)
	}
	close(start)
	wait.Wait()

	for consumer := 0; consumer < consumers; consumer++ {
		require.NoError(t, errors[consumer])
		require.Equal(t, results[0], results[consumer])
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&inner.callCount))
}
