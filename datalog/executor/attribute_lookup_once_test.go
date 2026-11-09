package executor_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// A reader of one attribute across many entities reads it through one lookup
// of the attribute, made where the reading clause runs, and never asks the
// source for an entity's attribute one entity at a time: a database function
// once per clause, a fused fetch once per fetched attribute, and an
// or-default branch built from the EA cache once per build.

// lookupCountingMatcher counts, per attribute, the lookups a query asks of a
// database: the entity lookups LookupAttribute answers, and the lookups of
// one attribute AttributeLookup makes. It is the lookup of every source, so a
// database function's lookups reach it. It declines attribute-fetch fusion,
// and with it the get-else scan rewrite, unless fuses is set.
type lookupCountingMatcher struct {
	*storage.Database
	fuses            bool
	entityLookups    map[datalog.Keyword]int
	attributeLookups map[datalog.Keyword]int
}

func newLookupCountingMatcher(d *storage.Database, fuses bool) *lookupCountingMatcher {
	return &lookupCountingMatcher{
		Database:         d,
		fuses:            fuses,
		entityLookups:    make(map[datalog.Keyword]int),
		attributeLookups: make(map[datalog.Keyword]int),
	}
}

func (m *lookupCountingMatcher) LookupAttribute(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool, error) {
	m.entityLookups[attr]++
	return m.Database.LookupAttribute(entity, attr)
}

func (m *lookupCountingMatcher) AttributeLookup(attr datalog.Keyword) (query.AttributeLookup, error) {
	m.attributeLookups[attr]++
	return m.Database.AttributeLookup(attr)
}

func (m *lookupCountingMatcher) LookupSource(query.Symbol) (EntityLookupMatcher, error) {
	return m, nil
}

func (m *lookupCountingMatcher) CanFuseAttributeFetch(source query.Symbol, attr datalog.Keyword) bool {
	return m.fuses && m.Database.CanFuseAttributeFetch(source, attr)
}

// lookupItems is the item fixture: every item has an id and a label, every
// second item a kind, every third a nick, and every fifth is retired.
type lookupItems struct {
	count                        int
	id, kind, nick, label, retired datalog.Keyword
}

func newLookupItems() lookupItems {
	return lookupItems{
		count:   20,
		id:      datalog.NewKeyword(":item/id"),
		kind:    datalog.NewKeyword(":item/kind"),
		nick:    datalog.NewKeyword(":item/nick"),
		label:   datalog.NewKeyword(":item/label"),
		retired: datalog.NewKeyword(":item/retired"),
	}
}

func (f lookupItems) facts() []datalog.Datom {
	var facts []datalog.Datom
	var lamport uint64
	add := func(e datalog.Identity, a datalog.Keyword, v interface{}) {
		lamport++
		facts = append(facts, datalog.Datom{E: e, A: a, V: v, Tx: datalog.ElementID{Lamport: lamport, ReplicaID: 1}})
	}
	for i := 0; i < f.count; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("lookup-item/%d", i))
		add(e, f.id, int64(i))
		add(e, f.label, fmt.Sprintf("label-%d", i))
		if i%2 == 0 {
			add(e, f.kind, "widget")
		}
		if i%3 == 0 {
			add(e, f.nick, fmt.Sprintf("nick-%d", i))
		}
		if i%5 == 0 {
			add(e, f.retired, true)
		}
	}
	return facts
}

// runCountingLookups runs text over the item fixture under every optimizer
// mode, with the EA cache on and off, through a lookupCountingMatcher, and
// hands check the matcher, the answer's tuples and the attributes the fused
// fetch read.
func runCountingLookups(t *testing.T, f lookupItems, fuses bool, text string, check func(t *testing.T, m *lookupCountingMatcher, tuples [][]interface{}, fused map[datalog.Keyword]bool)) {
	for _, mode := range OptimizerModes {
		for _, disableCache := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cache-off=%v", mode.Name(), disableCache), func(t *testing.T) {
				d, err := storage.OpenFacts(f.facts(), storage.DatabaseOptions{DisableCache: disableCache})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, d.Close()) })
				m := newLookupCountingMatcher(d, fuses)
				fused := make(map[datalog.Keyword]bool)
				opts := mode.PlannerOptions()
				opts.EnableAttributeFetchFusion = fuses
				opts.Handler = func(e annotations.Event) {
					if e.Name == annotations.PatternFusedFetch || e.Name == annotations.PatternFusedConstraint {
						fused[e.Data["attr"].(datalog.Keyword)] = true
					}
				}
				rel, err := NewExecutorWithOptions(m, nil, opts).Execute(MustParse(t, text))
				require.NoError(t, err)
				tuples, err := CollectTuples(rel, nil)
				require.NoError(t, err)
				check(t, m, tuples, fused)
			})
		}
	}
}

func TestADatabaseFunctionReadsEachAttributeThroughOneLookupPerClause(t *testing.T) {
	f := newLookupItems()
	text := `[:find ?id ?kind ?named ?name
	          :where [?e :item/id ?id]
	                 [(get-else $ ?e :item/kind "none") ?kind]
	                 [(get-some $ ?e :item/nick :item/label) [?named ?name]]
	                 [(missing? $ ?e :item/retired)]]`
	var want [][]interface{}
	for i := 0; i < f.count; i++ {
		if i%5 == 0 {
			continue
		}
		kind := "none"
		if i%2 == 0 {
			kind = "widget"
		}
		named, name := f.label, fmt.Sprintf("label-%d", i)
		if i%3 == 0 {
			named, name = f.nick, fmt.Sprintf("nick-%d", i)
		}
		want = append(want, []interface{}{int64(i), kind, named, name})
	}
	runCountingLookups(t, f, false, text, func(t *testing.T, m *lookupCountingMatcher, tuples [][]interface{}, _ map[datalog.Keyword]bool) {
		require.ElementsMatch(t, want, tuples, "the database functions answer each item's kind, its nick or its label, and every item not retired")
		for _, attr := range []datalog.Keyword{f.kind, f.nick, f.label, f.retired} {
			require.Equal(t, 1, m.attributeLookups[attr], "the clause reading %s makes one lookup of it", attr)
			require.Zero(t, m.entityLookups[attr], "no entity's %s is looked up one entity at a time", attr)
		}
	})
}

func TestAFusedFetchReadsEachAttributeThroughOneLookup(t *testing.T) {
	f := newLookupItems()
	text := `[:find ?id ?kind ?label
	          :where [?e :item/id ?id]
	                 [?e :item/kind ?kind]
	                 [?e :item/label ?label]]`
	var want [][]interface{}
	for i := 0; i < f.count; i += 2 {
		want = append(want, []interface{}{int64(i), "widget", fmt.Sprintf("label-%d", i)})
	}
	runCountingLookups(t, f, true, text, func(t *testing.T, m *lookupCountingMatcher, tuples [][]interface{}, fused map[datalog.Keyword]bool) {
		require.ElementsMatch(t, want, tuples, "every item with a kind answers its id, kind and label")
		require.NotEmpty(t, fused, "the query's same-entity patterns are fetched by the fused fetch")
		for attr := range fused {
			require.Equal(t, 1, m.attributeLookups[attr], "the fused fetch of %s makes one lookup of it", attr)
			require.Zero(t, m.entityLookups[attr], "no entity's %s is looked up one entity at a time", attr)
		}
	})
}

func TestAnOrDefaultBranchBuiltFromTheCacheReadsItsAttributeThroughALookup(t *testing.T) {
	f := newLookupItems()
	text := `[:find ?id ?kind
	          :where [?e :item/id ?id]
	                 (or-default [?e :item/kind ?kind]
	                             [(ground "none") ?kind])]`
	var want [][]interface{}
	for i := 0; i < f.count; i++ {
		kind := "none"
		if i%2 == 0 {
			kind = "widget"
		}
		want = append(want, []interface{}{int64(i), kind})
	}
	runCountingLookups(t, f, true, text, func(t *testing.T, m *lookupCountingMatcher, tuples [][]interface{}, _ map[datalog.Keyword]bool) {
		require.ElementsMatch(t, want, tuples, "every item answers its kind, or none")
		require.Positive(t, m.attributeLookups[f.kind], "the branch over %s is built through a lookup of it", f.kind)
		require.Zero(t, m.entityLookups[f.kind], "no entity's %s is looked up one entity at a time", f.kind)
	})
}
