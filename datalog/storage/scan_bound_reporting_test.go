package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// bindingDrivenPeople is the fixture the binding-driven pins share: three
// entities under one cardinality-one attribute, written bindingDrivenWrites
// times each.
//
// The depth is the point. With one write per entity every term of the funnel is
// three, so a counter wired to the wrong term — or to nothing — still passes,
// and a fixture like that cannot tell the three counters apart. Three writes
// and last-write-wins make intake and resolution nine and three.
// An array rather than a slice so len() is a constant expression and the
// counts derived from it below can be constants too.
var bindingDrivenPeople = [3]string{"person:alice", "person:bob", "person:carol"}

const bindingDrivenWrites = 3

// bindingDrivenFixture opens a database whose schema declares the attribute
// cardinality-one — so resolution is unambiguously LWW rather than inferred —
// writes the fixture, and returns the pattern, the binding relation over E and
// the output symbols the three strategies share.
//
// The annotation handler is installed by the caller after the writes, so the
// transactions' own events stay out of the assertion.
func bindingDrivenFixture(t *testing.T) (*Database, *query.DataPattern, executor.Relation, []query.Symbol) {
	t.Helper()

	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)

	// One mode pinned explicitly rather than the package's optimizer-mode loop:
	// these tests drive the matcher directly, so the planner — and therefore the
	// algebra optimizer — is not on their path.
	opts := optimizerMode{name: "algebra_off", algebra: false}.plannerOptions()
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:           t.TempDir(),
		Schema:         s,
		PlannerOptions: &opts,
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	name := datalog.NewKeyword(":person/name")
	bindings := make([]executor.Tuple, len(bindingDrivenPeople))
	for i, seed := range bindingDrivenPeople {
		who := datalog.NewIdentity(seed)
		bindings[i] = executor.Tuple{who}
		// Separate transactions: each Set must land on its own Tx for the
		// earlier ones to be history rather than overwritten in the buffer.
		for w := 0; w < bindingDrivenWrites; w++ {
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(who, name, fmt.Sprintf("%s-%d", seed, w)))
			_, err = tx.Commit()
			require.NoError(t, err)
		}
	}

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: name},
			query.Variable{Name: datalog.NewSymbol("?n")},
		},
	}
	bindingRel := executor.NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?e")}, bindings)
	symbols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?n")}

	return db, pattern, bindingRel, symbols
}

// drainRelation iterates a relation to exhaustion and returns the tuple count.
// The completion events these tests read are emitted on Close, so a strategy
// left un-drained reports nothing at all.
func drainRelation(t *testing.T, rel executor.Relation) int {
	t.Helper()
	iter := rel.Iterator()
	tuples := 0
	for iter.Next() {
		tuples++
	}
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())
	return tuples
}

// TestBindingDrivenStrategiesReportTheirFunnel pins all three intake counters
// the binding-driven paths maintain — one per strategy, each a separate
// implementation.
//
// Nothing else holds them. Hardcode hash_join_matcher.go's datoms.scanned to
// zero, or replace nonReusingIterator's accumulate-before-discard with a
// discard, and the rest of the package stays green. The counter's contract is
// that a key the membership rule rejected is still counted, which is only
// checkable against a fixture where intake and resolution differ — hence
// bindingDrivenFixture's history depth.
//
// Each strategy is driven directly rather than reached through a query. Which
// one chooseJoinStrategy picks depends on binding-set size and its own
// thresholds, so a query-level test would pin whichever it picks today and
// silently stop covering the other two.
func TestBindingDrivenStrategiesReportTheirFunnel(t *testing.T) {
	const (
		entities = len(bindingDrivenPeople)
		// Every write is intake: the scan reads the whole (A, E) group and CRDT
		// resolution emits the highest-Tx entry, so intake exceeds output by the
		// history depth. That gap is what the funnel exists to show.
		wantScanned = entities * bindingDrivenWrites
	)

	for _, tc := range []struct {
		strategy string
		event    string
		// namesRun distinguishes the two strategies that open one scan from the
		// one that opens a scan per binding. Hash join and merge join each build
		// a single ScanBound from the pattern's constant A, so each names AETV
		// bound at A; the per-binding path runs chooseIndex again for every
		// binding tuple, so naming one run would mean naming whichever came
		// last.
		namesRun bool
		match    func(m *PatternMatcher, pattern *query.DataPattern,
			bindingRel executor.Relation, symbols []query.Symbol) (executor.Relation, error)
	}{
		{strategy: "HashJoinScan", event: annotations.PatternHashJoinComplete, namesRun: true,
			match: func(m *PatternMatcher, pattern *query.DataPattern,
				bindingRel executor.Relation, symbols []query.Symbol) (executor.Relation, error) {
				return m.matchWithHashJoin(pattern, bindingRel, symbols, 0, AETV, nil)
			}},
		{strategy: "MergeJoin", event: annotations.PatternMergeJoinComplete, namesRun: true,
			match: func(m *PatternMatcher, pattern *query.DataPattern,
				bindingRel executor.Relation, symbols []query.Symbol) (executor.Relation, error) {
				return m.matchWithMergeJoin(pattern, bindingRel, symbols, 0, AETV, nil)
			}},
		{strategy: "NoReuse", event: annotations.PatternPerBindingScanComplete,
			match: func(m *PatternMatcher, pattern *query.DataPattern,
				bindingRel executor.Relation, symbols []query.Symbol) (executor.Relation, error) {
				return m.matchWithoutIteratorReuse(pattern, bindingRel, symbols, nil)
			}},
	} {
		t.Run(tc.strategy, func(t *testing.T) {
			db, pattern, bindingRel, symbols := bindingDrivenFixture(t)

			var events []annotations.Event
			db.AnnotationHandler = func(e annotations.Event) { events = append(events, e) }

			rel, err := tc.match(db.Matcher().(*PatternMatcher), pattern, bindingRel, symbols)
			require.NoError(t, err)
			require.Equal(t, entities, drainRelation(t, rel),
				"last write wins, so each entity contributes one tuple")

			complete := lastEventNamed(events, tc.event)
			require.NotNil(t, complete, "%s must report what its scan cost", tc.strategy)

			require.Equal(t, wantScanned, complete.Data[annotations.KeyDatomsScanned],
				"%s read every write in each group; reporting %d would be reporting resolution's output",
				tc.strategy, entities)
			require.Equal(t, entities, complete.Data[annotations.KeyDatomsResolved])
			require.Equal(t, entities, complete.Data[annotations.KeyDatomsMatched])
			require.Equal(t, entities, complete.Data[annotations.KeyBindingSize])
			require.Positive(t, complete.Latency,
				"a completion event without a duration reports as 0 ms in Analyze")

			// The run, on the event that prices it. These paths emit no
			// pattern/index-selection — they are reached through
			// analyzeReuseStrategy rather than the announcing dispatch — so the
			// completion is the only event that can name what was walked, and an
			// index without its bound leaves that unanswerable.
			if !tc.namesRun {
				require.NotContains(t, complete.Data, annotations.KeyIndex,
					"%s runs chooseIndex per binding; one index here would name whichever was last",
					tc.strategy)
				require.NotContains(t, complete.Data, annotations.KeyBound)
				return
			}
			require.Equal(t, AETV, complete.Data[annotations.KeyIndex])
			require.Equal(t, []string{"A"}, complete.Data[annotations.KeyBound],
				"%s scanned under the pattern's constant A", tc.strategy)
			require.Equal(t, []string{":person/name"}, complete.Data["bound.values"])
		})
	}
}

// TestBindingSizeCountsTuplesNotDistinctKeys pins the unit of binding.size
// across the three strategies that report it.
//
// One key, one unit. Hash join tracked distinct join keys because it needs that
// count for its own reason — exactly one key lets a bound value narrow the scan
// — and reported the same number as its binding size, where merge join and the
// per-binding path report tuples. The unit matters to a reader comparing two
// traces and to the number chooseJoinStrategy actually selects on, which is
// bindingRel.Size(): tuples.
//
// TestBindingDrivenStrategiesReportTheirFunnel cannot see this. Its bindings
// carry one symbol, and a Relation is a set, so tuples and distinct keys are
// necessarily the same number there — the assertion passes against either unit.
// Separating them takes a second symbol, which is what this fixture adds: the
// same three entities, each appearing under two slots.
//
// Tuple counts are deliberately not asserted. The strategies legitimately differ
// on repeated keys — hash join emits once per datom, merge join once per
// (datom, binding tuple) pair, both correct because the output tuple is built
// from the datom alone and the matcher's single exit restores set semantics.
// Pinning tuple counts here would pin that difference rather than the unit.
func TestBindingSizeCountsTuplesNotDistinctKeys(t *testing.T) {
	const slotsPerEntity = 2
	const wantTuples = len(bindingDrivenPeople) * slotsPerEntity

	for _, tc := range []struct {
		strategy string
		event    string
		match    func(m *PatternMatcher, pattern *query.DataPattern,
			bindingRel executor.Relation, symbols []query.Symbol) (executor.Relation, error)
	}{
		{strategy: "HashJoinScan", event: annotations.PatternHashJoinComplete,
			match: func(m *PatternMatcher, pattern *query.DataPattern,
				bindingRel executor.Relation, symbols []query.Symbol) (executor.Relation, error) {
				return m.matchWithHashJoin(pattern, bindingRel, symbols, 0, AETV, nil)
			}},
		{strategy: "MergeJoin", event: annotations.PatternMergeJoinComplete,
			match: func(m *PatternMatcher, pattern *query.DataPattern,
				bindingRel executor.Relation, symbols []query.Symbol) (executor.Relation, error) {
				return m.matchWithMergeJoin(pattern, bindingRel, symbols, 0, AETV, nil)
			}},
		{strategy: "NoReuse", event: annotations.PatternPerBindingScanComplete,
			match: func(m *PatternMatcher, pattern *query.DataPattern,
				bindingRel executor.Relation, symbols []query.Symbol) (executor.Relation, error) {
				return m.matchWithoutIteratorReuse(pattern, bindingRel, symbols, nil)
			}},
	} {
		t.Run(tc.strategy, func(t *testing.T) {
			db, pattern, _, symbols := bindingDrivenFixture(t)

			// The entity stays at tuple position 0, which is where all three
			// strategies read the join key from; the second symbol only
			// multiplies the tuples behind each key.
			entity := datalog.NewSymbol("?e")
			slot := datalog.NewSymbol("?slot")
			tuples := make([]executor.Tuple, 0, wantTuples)
			for _, seed := range bindingDrivenPeople {
				who := datalog.NewIdentity(seed)
				for s := 0; s < slotsPerEntity; s++ {
					tuples = append(tuples, executor.Tuple{who, int64(s)})
				}
			}
			bindingRel := executor.NewMaterializedRelation(
				[]query.Symbol{entity, slot}, tuples)

			var events []annotations.Event
			db.AnnotationHandler = func(e annotations.Event) { events = append(events, e) }

			rel, err := tc.match(db.Matcher().(*PatternMatcher), pattern, bindingRel, symbols)
			require.NoError(t, err)
			drainRelation(t, rel)

			complete := lastEventNamed(events, tc.event)
			require.NotNil(t, complete, "%s must report what its scan cost", tc.strategy)
			require.Equal(t, wantTuples, complete.Data[annotations.KeyBindingSize],
				"%s reports binding.size in distinct join keys (%d) rather than binding "+
					"tuples (%d); two strategies reporting different units under one key "+
					"cannot be compared", tc.strategy, len(bindingDrivenPeople), wantTuples)
		})
	}
}

// TestPerBindingScanReportsOneCountedEvent pins the reporting shape for the
// scan-per-binding path: one completion event carrying the scan count, never
// one event per binding.
//
// This path opens a scan per binding tuple, so it is the one place where making
// the annotations complete could have meant flooding them. A reader tracing a
// query needs to know that N scans happened, not to read N events; the count is
// the datum, and the enumeration would put the instrumentation in front of the
// query it exists to describe.
//
// Its funnel is asserted by TestBindingDrivenStrategiesReportTheirFunnel above,
// which shares the fixture; what is left here is the count of events and the
// scan count only this strategy carries.
func TestPerBindingScanReportsOneCountedEvent(t *testing.T) {
	db, pattern, bindingRel, symbols := bindingDrivenFixture(t)

	var events []annotations.Event
	db.AnnotationHandler = func(e annotations.Event) { events = append(events, e) }

	m := db.Matcher().(*PatternMatcher)
	rel, err := m.matchWithoutIteratorReuse(pattern, bindingRel, symbols, nil)
	require.NoError(t, err)
	require.Equal(t, len(bindingDrivenPeople), drainRelation(t, rel))

	var complete []annotations.Event
	for _, e := range events {
		if e.Name == annotations.PatternPerBindingScanComplete {
			complete = append(complete, e)
		}
	}
	require.Len(t, complete, 1,
		"the path reports once for the whole run; %d events means it reports per binding", len(complete))

	require.Equal(t, len(bindingDrivenPeople), complete[0].Data[annotations.KeyScansOpened],
		"the count is the datum a per-binding path owes its reader")
}

// TestScanBoundErrorNamesItsIndex pins that the loud failure the typed bound
// added is readable: the operator is told which index the bound was against by
// the name the rest of the engine uses, not by the prefix byte.
//
// What it pins is that the error routes through IndexType.String(). The
// assertion compares that method against itself, so it reds when String() is
// deleted and never when an arm returns a wrong name — the mapping itself is
// TestIndexTypeNamesEveryConstant's job.
func TestScanBoundErrorNamesItsIndex(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	_, err := encoder.EncodeScanBound(ScanBound{
		Index:  AVET,
		Prefix: []datalog.Value{"not-a-keyword"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), AVET.String(),
		"the error must name the index the way the rest of the engine does; got %q", err.Error())
}

// TestIndexTypeNamesEveryConstant pins the name each IndexType renders to, and
// that the set of named constants is the set of indices. Swapping two arms of
// String() would leave every error message and every annotation index entry on
// those two indices lying, with the rest of the suite green.
func TestIndexTypeNamesEveryConstant(t *testing.T) {
	named := []struct {
		index IndexType
		name  string
	}{
		{EAVT, "EAVT"},
		{EATV, "EATV"},
		{AEVT, "AEVT"},
		{AETV, "AETV"},
		{ATEV, "ATEV"},
		{AVET, "AVET"},
		{VAET, "VAET"},
		{TAEV, "TAEV"},
	}
	require.Len(t, named, len(Indices),
		"every index must have a pinned name; Indices grew without this table")
	for _, tc := range named {
		require.Equal(t, tc.name, tc.index.String())
	}
	require.Equal(t, "IndexType(99)", IndexType(99).String(),
		"an index outside the eight must render loudly, never as one of them")
}

// TestBoundAnnotationKeyHasOneType pins that one key in one event family
// carries one payload type. addBoundFields writes "bound" as the run's bound
// positions ([]string); the rendered bound value is a different datum and
// travels under "bound_v" across the whole v-validation family. If a sibling
// event wrote the rendered value under "bound" as well, a handler filtering
// v-validation/* would have to type-switch to learn which of the two it had
// been handed.
func TestBoundAnnotationKeyHasOneType(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var events []annotations.Event
			opts := mode.plannerOptions()
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:              t.TempDir(),
				PlannerOptions:    &opts,
				AnnotationHandler: func(e annotations.Event) { events = append(events, e) },
			})
			require.NoError(t, err)
			defer db.Close()

			name := datalog.NewKeyword(":person/name")
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("person:alice"), name, "Alice"))
			_, err = tx.Commit()
			require.NoError(t, err)

			result, err := db.Query(`[:find ?e :where [?e :person/name "Alice"]]`)
			require.NoError(t, err)
			_, err = executor.CollectTuples(result, nil)
			require.NoError(t, err)

			seen := map[string]bool{}
			for _, e := range events {
				raw, ok := e.Data[annotations.KeyBound]
				if !ok {
					continue
				}
				switch raw.(type) {
				case []string:
					seen["positions"] = true
				case string:
					seen["value"] = true
				default:
					t.Fatalf("event %s carries an unexpected type for \"bound\": %T", e.Name, raw)
				}
			}

			require.NotEmpty(t, seen, "the V-validation path must report its bound")
			require.Len(t, seen, 1,
				`one key, one type: the "bound" key carries both bound positions and a rendered `+
					`bound value across the v-validation family; saw %v`, seen)
		})
	}
}

// TestIndexAnnotationKeyCarriesOnlyAnIndexType is the sibling of the bound pin
// above, and it closes a collision that was live: the subquery input-relation
// event wrote its relation's ordinal under "index", the same key the scan events
// use for the physical ordering they walked. Database.Analyze prints
// Data[KeyIndex] for every event it traces, so a subquery rendered "(index=0)"
// as though it named a run.
//
// The query is deliberately wide — a scan under a constant E and a subquery that
// scans again per value — because a key only collides where two event families
// meet in one trace. Both are required to appear, so this cannot pass by
// exercising neither.
//
// One optimizer mode rather than the package's loop: with the algebra optimizer
// on, decorrelation rewrites this correlated subquery into a join and the
// subquery family never reaches the trace. Where the producer does not run there
// is no collision to pin, and the scan events' own typing is covered in both
// modes by the tests above.
func TestIndexAnnotationKeyCarriesOnlyAnIndexType(t *testing.T) {
	var events []annotations.Event
	opts := optimizerMode{name: "algebra_off", algebra: false}.plannerOptions()
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:           t.TempDir(),
		Schema:         funnelSchema(t),
		PlannerOptions: &opts,
	})
	require.NoError(t, err)
	defer db.Close()

	funnelFixture(t, db)
	db.AnnotationHandler = func(e annotations.Event) { events = append(events, e) }

	result, err := db.Query(`[:find ?v ?c
	     :where [#id "funnel:alice" :person/tag ?v]
	            [(q [:find (count ?e) :in $ ?t
	                 :where [?e :person/tag ?t]] $ ?v) [[?c]]]]`)
	require.NoError(t, err)
	_, err = executor.CollectTuples(result, nil)
	require.NoError(t, err)

	var sawIndex, sawSubqueryInput bool
	for _, e := range events {
		if e.Name == annotations.SubqueryInputRelation {
			sawSubqueryInput = true
		}
		raw, ok := e.Data[annotations.KeyIndex]
		if !ok {
			continue
		}
		sawIndex = true
		require.IsType(t, IndexType(0), raw,
			"event %s carries %T under the index key; that key names one of the "+
				"eight physical orderings and nothing else", e.Name, raw)
	}
	require.True(t, sawIndex, "the trace must contain an event naming the run it walked")
	require.True(t, sawSubqueryInput,
		"the subquery family must be in the trace, or this pins nothing")
}
