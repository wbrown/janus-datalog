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
// three, and a counter wired to the wrong term — or to nothing — still passes.
// That is how three separate implementations of the intake counter shipped with
// no assertion between them: the fixtures made intake and resolution the same
// number. Three writes and last-write-wins make them nine and three.
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

// drainRelation iterates a relation to exhaustion and returns the row count.
// The completion events these tests read are emitted on Close, so a strategy
// left un-drained reports nothing at all.
func drainRelation(t *testing.T, rel executor.Relation) int {
	t.Helper()
	iter := rel.Iterator()
	rows := 0
	for iter.Next() {
		rows++
	}
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())
	return rows
}

// TestBindingDrivenStrategiesReportTheirFunnel pins all three intake counters
// the binding-driven paths maintain — one per strategy, each a separate
// implementation, and until this test each unasserted.
//
// Hardcoding hash_join_matcher.go's datoms.scanned to zero, or replacing
// nonReusingIterator's accumulate-before-discard with a discard, left the whole
// package green. The counter's contract is that a key the membership rule
// rejected is still counted, which is only checkable against a fixture where
// intake and resolution differ — hence bindingDrivenFixture's history depth.
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
			db, pattern, bindingRel, symbols := bindingDrivenFixture(t)

			var events []annotations.Event
			db.AnnotationHandler = func(e annotations.Event) { events = append(events, e) }

			rel, err := tc.match(db.Matcher().(*PatternMatcher), pattern, bindingRel, symbols)
			require.NoError(t, err)
			require.Equal(t, entities, drainRelation(t, rel),
				"last write wins, so each entity contributes one row")

			complete := lastEventNamed(events, tc.event)
			require.NotNil(t, complete, "%s must report what its scan cost", tc.strategy)

			require.Equal(t, wantScanned, complete.Data["datoms.scanned"],
				"%s read every write in each group; reporting %d would be reporting resolution's output",
				tc.strategy, entities)
			require.Equal(t, entities, complete.Data["datoms.resolved"])
			require.Equal(t, entities, complete.Data["datoms.matched"])
			require.Equal(t, entities, complete.Data["binding.size"])
			require.Positive(t, complete.Latency,
				"a completion event without a duration reports as 0 ms in Analyze")
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

	require.Equal(t, len(bindingDrivenPeople), complete[0].Data["scans.opened"],
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
// travels under "bound_v" across the whole v-validation family. Before this
// was pinned, two sibling events wrote the rendered value under "bound" too,
// so a handler filtering v-validation/* had to type-switch to learn which of
// the two it had been given.
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
				raw, ok := e.Data["bound"]
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
