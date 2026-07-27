package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

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
// It drives matchWithoutIteratorReuse directly rather than fishing for a query
// shape that routes to NoReuse, so the pin holds whatever the strategy chooser
// does later. That also takes the planner — and therefore the algebra optimizer
// — off its path, so it pins one mode explicitly instead of looping the axis.
func TestPerBindingScanReportsOneCountedEvent(t *testing.T) {
	var events []annotations.Event
	opts := optimizerMode{name: "algebra_off", algebra: false}.plannerOptions()
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:              t.TempDir(),
		PlannerOptions:    &opts,
		AnnotationHandler: func(e annotations.Event) { events = append(events, e) },
	})
	require.NoError(t, err)
	defer db.Close()

	name := datalog.NewKeyword(":person/name")
	people := []datalog.Identity{
		datalog.NewIdentity("person:alice"),
		datalog.NewIdentity("person:bob"),
		datalog.NewIdentity("person:carol"),
	}
	tx := db.NewTransaction()
	for i, who := range people {
		require.NoError(t, tx.Add(who, name, []string{"Alice", "Bob", "Carol"}[i]))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: name},
			query.Variable{Name: datalog.NewSymbol("?n")},
		},
	}
	bindings := make([]executor.Tuple, len(people))
	for i, who := range people {
		bindings[i] = executor.Tuple{who}
	}
	bindingRel := executor.NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?e")}, bindings)

	m := db.Matcher().(*PatternMatcher)
	symbols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?n")}

	events = nil
	rel, err := m.matchWithoutIteratorReuse(pattern, bindingRel, symbols, nil)
	require.NoError(t, err)

	iter := rel.Iterator()
	rows := 0
	for iter.Next() {
		rows++
	}
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())
	require.Equal(t, len(people), rows)

	var complete []annotations.Event
	for _, e := range events {
		if e.Name == "pattern/per-binding-scan-complete" {
			complete = append(complete, e)
		}
	}
	require.Len(t, complete, 1,
		"the path reports once for the whole run; %d events means it reports per binding", len(complete))

	e := complete[0]
	require.Equal(t, len(people), e.Data["scans.opened"],
		"the count is the datum a per-binding path owes its reader")
	require.Equal(t, len(people), e.Data["binding.size"])
	require.Equal(t, len(people), e.Data["datoms.matched"])
	require.Positive(t, e.Latency,
		"a completion event without a duration reports as 0 ms in Analyze")
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
