package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// seekExtent is how many entities carry each attribute of the seek fixture:
// past twice the boundary, so a binding above the boundary leaves most of an
// attribute unbound, and a scan of the attribute is told from the seeks of a
// binding's keys by its intake.
const seekExtent = 2*executor.SmallBindingBoundary + 7

// seekTags are the values of the fixture's many-valued attribute, shared
// among the entities in turn.
var seekTags = [...]string{"folio", "octavo", "quarto"}

// seekFixture is the data the per-key seek tests bind against: seekExtent
// entities, each carrying :seek/n with a value of its own, :seek/tag with one
// of seekTags, and an attribute of its own, :seek/a<i>, with a value of its
// own.
type seekFixture struct {
	db       *Database
	entities []datalog.Identity
	ownAttrs []datalog.Keyword
	n        map[datalog.Identity]string
	tag      map[datalog.Identity]string
	ownValue map[datalog.Keyword]string
}

// newSeekFixture writes the fixture on the mode's backend under opts, with
// the handler registered at open and the EA cache off, so a binding-driven
// pattern reaches the matcher's scan paths rather than the cache.
func newSeekFixture(t testing.TB, mode optimizerMode, opts planner.PlannerOptions, handler annotations.Handler) *seekFixture {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(":seek/n").Type(schema.TypeString).One().Add().
		Attribute(":seek/tag").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	db := openBackendDB(t, mode.backend, DatabaseOptions{
		Schema:            s,
		DisableCache:      true,
		PlannerOptions:    &opts,
		AnnotationHandler: handler,
	})
	f := &seekFixture{
		db:       db,
		n:        make(map[datalog.Identity]string, seekExtent),
		tag:      make(map[datalog.Identity]string, seekExtent),
		ownValue: make(map[datalog.Keyword]string, seekExtent),
	}
	nAttr := datalog.NewKeyword(":seek/n")
	tagAttr := datalog.NewKeyword(":seek/tag")
	tx := db.NewTransaction()
	for i := 0; i < seekExtent; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("seek-%d", i))
		own := datalog.NewKeyword(fmt.Sprintf(":seek/a%d", i))
		n := fmt.Sprintf("n-%d", i)
		tag := seekTags[i%len(seekTags)]
		ownValue := fmt.Sprintf("a-%d", i)
		require.NoError(t, tx.Add(e, nAttr, n))
		require.NoError(t, tx.Add(e, tagAttr, tag))
		require.NoError(t, tx.Add(e, own, ownValue))
		f.entities = append(f.entities, e)
		f.ownAttrs = append(f.ownAttrs, own)
		f.n[e] = n
		f.tag[e] = tag
		f.ownValue[own] = ownValue
	}
	_, err = tx.Commit()
	require.NoError(t, err)
	return f
}

// attributeBinding binds ?a to the first keys entities' own attributes.
func (f *seekFixture) attributeBinding(keys int) executor.Relation {
	tuples := make([]executor.Tuple, keys)
	for i, a := range f.ownAttrs[:keys] {
		tuples[i] = executor.Tuple{a}
	}
	return executor.NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?a")}, tuples)
}

// tagBinding binds ?v to the first keys tags.
func (f *seekFixture) tagBinding(keys int) executor.Relation {
	tuples := make([]executor.Tuple, keys)
	for i, tag := range seekTags[:keys] {
		tuples[i] = executor.Tuple{tag}
	}
	return executor.NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?v")}, tuples)
}

// nTuples is what [?e :seek/n ?v] answers for the first keys entities.
func (f *seekFixture) nTuples(keys int) [][]interface{} {
	want := make([][]interface{}, keys)
	for i, e := range f.entities[:keys] {
		want[i] = []interface{}{e, f.n[e]}
	}
	return want
}

// ownTuples is what [?e ?a ?v] answers for the first keys entities' own
// attributes.
func (f *seekFixture) ownTuples(keys int) [][]interface{} {
	want := make([][]interface{}, keys)
	for i, a := range f.ownAttrs[:keys] {
		want[i] = []interface{}{f.entities[i], a, f.ownValue[a]}
	}
	return want
}

// tagTuples is what [?e :seek/tag ?v] answers for the first keys tags: every
// entity carrying one of them.
func (f *seekFixture) tagTuples(keys int) [][]interface{} {
	bound := make(map[string]bool, keys)
	for _, tag := range seekTags[:keys] {
		bound[tag] = true
	}
	var want [][]interface{}
	for _, e := range f.entities {
		if bound[f.tag[e]] {
			want = append(want, []interface{}{e, f.tag[e]})
		}
	}
	return want
}

// TestPerKeySeekAtOrBelowTheBoundary pins the scope design's §4.6
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md): a binding relation of
// several keys at or below executor.SmallBindingBoundary is served by one
// narrowed scan per key, read through the per-key completion event's scan
// count and its intake, which is the keys' datoms and nothing of the rest of
// the attribute; one above the boundary by the hash-join scan, whose intake is
// the attribute's extent; a binding at the attribute position per key at any
// count; a binding whose keys the index cannot bind, strings at the value
// position, by the hash-join scan at any count; and each path answers the
// reference at every key count.
//
// Each path is driven directly, as the funnel pins in
// scan_bound_reporting_test.go drive theirs, with the index the dispatch picks
// for the position (analyzeReuseStrategy). The algebra axis is pinned off: the
// planner is not on this path, and the backend is what each store implements
// differently.
func TestPerKeySeekAtOrBelowTheBoundary(t *testing.T) {
	for _, mode := range pinnedOptimizerModes(false) {
		t.Run(mode.name, func(t *testing.T) {
			testPerKeySeekAtOrBelowTheBoundary(t, mode)
		})
	}
}

func testPerKeySeekAtOrBelowTheBoundary(t *testing.T, mode optimizerMode) {
	var events []annotations.Event
	f := newSeekFixture(t, mode, mode.plannerOptions(),
		func(e annotations.Event) { events = append(events, e) })
	m := f.db.Matcher().(*PatternMatcher)

	e := datalog.NewSymbol("?e")
	a := datalog.NewSymbol("?a")
	v := datalog.NewSymbol("?v")
	nPattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: e},
		query.Constant{Value: datalog.NewKeyword(":seek/n")},
		query.Variable{Name: v},
	}}
	ownPattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: e},
		query.Variable{Name: a},
		query.Variable{Name: v},
	}}
	tagPattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: e},
		query.Constant{Value: datalog.NewKeyword(":seek/tag")},
		query.Variable{Name: v},
	}}
	const boundary = executor.SmallBindingBoundary

	for _, tc := range []struct {
		name     string
		pattern  *query.DataPattern
		binding  executor.Relation
		position int
		index    IndexType
		symbols  []query.Symbol
		strategy annotations.ScanStrategy
		keys     int // the distinct keys the binding carries
		scanned  int // the intake the strategy pays: the keys' datoms, or the attribute's extent
		want     [][]interface{}
	}{
		{name: "two entities", pattern: nPattern, binding: entityBindings(f.entities[:2]),
			position: 0, index: AETV, symbols: []query.Symbol{e, v},
			strategy: annotations.ScanPerKeySeek, keys: 2, scanned: 2, want: f.nTuples(2)},
		{name: "entities at the boundary", pattern: nPattern, binding: entityBindings(f.entities[:boundary]),
			position: 0, index: AETV, symbols: []query.Symbol{e, v},
			strategy: annotations.ScanPerKeySeek, keys: boundary, scanned: boundary, want: f.nTuples(boundary)},
		{name: "entities above the boundary", pattern: nPattern, binding: entityBindings(f.entities[:boundary+1]),
			position: 0, index: AETV, symbols: []query.Symbol{e, v},
			strategy: annotations.ScanHashJoin, keys: boundary + 1, scanned: seekExtent, want: f.nTuples(boundary + 1)},
		{name: "attributes above the boundary", pattern: ownPattern, binding: f.attributeBinding(boundary + 1),
			position: 1, index: AETV, symbols: []query.Symbol{e, a, v},
			strategy: annotations.ScanPerKeySeek, keys: boundary + 1, scanned: boundary + 1, want: f.ownTuples(boundary + 1)},
		{name: "values the index cannot bind", pattern: tagPattern, binding: f.tagBinding(2),
			position: 2, index: AVET, symbols: []query.Symbol{e, v},
			strategy: annotations.ScanHashJoin, keys: 2, scanned: seekExtent, want: f.tagTuples(2)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events = nil
			rel, err := m.matchWithHashJoin(tc.pattern, tc.binding, tc.symbols, tc.position, tc.index, nil)
			require.NoError(t, err)
			got, err := executor.CollectTuples(rel, nil)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.want, got,
				"%d keys at position %d answer the reference whatever path serves them", tc.keys, tc.position)

			complete := lastScanComplete(events, tc.strategy)
			require.NotNil(t, complete,
				"%d keys at position %d are served by the %s strategy, and no completion among %d events reports it",
				tc.keys, tc.position, tc.strategy, len(events))
			require.Equal(t, tc.scanned, complete.Data[annotations.KeyDatomsScanned],
				"the %s strategy's intake for %d keys at position %d", tc.strategy, tc.keys, tc.position)
			require.Equal(t, tc.binding.Size(), complete.Data[annotations.KeyBindingSize])
			switch tc.strategy {
			case annotations.ScanPerKeySeek:
				require.Equal(t, tc.keys, complete.Data[annotations.KeyScansOpened],
					"one narrowed scan per distinct key")
				require.NotContains(t, complete.Data, annotations.KeyIndex,
					"a path of one run per key names no single run")
				require.Nil(t, lastScanComplete(events, annotations.ScanHashJoin),
					"a binding sought per key opens no scan of the attribute's extent")
			case annotations.ScanHashJoin:
				require.Equal(t, tc.index, complete.Data[annotations.KeyIndex])
				require.Nil(t, lastScanComplete(events, annotations.ScanPerKeySeek),
					"a binding served by one scan of the extent seeks no key")
			}
		})
	}
}

// TestQueryBindingIsSoughtPerKeyAtOrBelowTheBoundary pins that the per-key
// seek is reached from a query on both planner paths: a collection input of
// entities at or below the boundary drives [?e :seek/n ?v] through one
// narrowed scan per key, and one above the boundary through the hash-join
// scan, each answering the reference. Attribute-fetch fusion is off, since a
// fused fetch reads the EA cache per tuple and never reaches the matcher's
// binding-driven paths.
func TestQueryBindingIsSoughtPerKeyAtOrBelowTheBoundary(t *testing.T) {
	const boundary = executor.SmallBindingBoundary
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var events []annotations.Event
			opts := mode.plannerOptions()
			opts.EnableAttributeFetchFusion = false
			f := newSeekFixture(t, mode, opts,
				func(e annotations.Event) { events = append(events, e) })

			for _, tc := range []struct {
				name     string
				keys     int
				strategy annotations.ScanStrategy
			}{
				{name: "three entities", keys: 3, strategy: annotations.ScanPerKeySeek},
				{name: "entities above the boundary", keys: boundary + 1, strategy: annotations.ScanHashJoin},
			} {
				t.Run(tc.name, func(t *testing.T) {
					events = nil
					members := make([]interface{}, tc.keys)
					for i, e := range f.entities[:tc.keys] {
						members[i] = e
					}
					got, err := executor.CollectTuples(f.db.Query(
						`[:find ?e ?v :in $ [?e ...] :where [?e :seek/n ?v]]`, members))
					require.NoError(t, err)
					require.ElementsMatch(t, f.nTuples(tc.keys), got)

					complete := lastScanComplete(events, tc.strategy)
					require.NotNil(t, complete,
						"a collection input of %d entities is served by the %s strategy, and no completion among %d events reports it",
						tc.keys, tc.strategy, len(events))
					if tc.strategy == annotations.ScanPerKeySeek {
						require.Equal(t, tc.keys, complete.Data[annotations.KeyScansOpened],
							"one narrowed scan per distinct key")
					}
				})
			}
		})
	}
}
