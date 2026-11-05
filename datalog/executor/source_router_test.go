package executor_test

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The router sends each pattern to the source its symbol names, sends a
// pattern naming no source to the default source, and refuses a symbol it
// does not hold. Each source is a memory database holding one datom of its
// own, so the value a pattern returns says which source served it.

// sourceOf opens a memory database holding one datom, entity carrying value
// under attr, and closes it at the test's end.
func sourceOf(t testing.TB, entity, attr, value string) PatternMatcher {
	return factsDatabase(t, []datalog.Datom{{
		E: datalog.NewIdentity(entity), A: datalog.NewKeyword(attr), V: value,
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
	}})
}

// valuesOf matches pattern through pm and returns the values its tuples
// bind to ?v.
func valuesOf(t *testing.T, pm PatternMatcher, pattern *query.DataPattern) []string {
	t.Helper()
	rel, err := pm.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	var tuples []Tuple
	if err := CollectTuplesInto(&tuples, rel); err != nil {
		t.Fatalf("collect: %v", err)
	}
	var values []string
	for _, tuple := range tuples {
		values = append(values, tuple[len(tuple)-1].(string))
	}
	return values
}

func TestSourceRouterRoutes(t *testing.T) {
	users, perms := datalog.NewSymbol("$users"), datalog.NewSymbol("$perms")
	router := NewSourceRouter(map[query.Symbol]PatternMatcher{
		users: sourceOf(t, "u1", ":user/name", "alice"),
		perms: sourceOf(t, "p1", ":perm/name", "read"),
	})
	if got := valuesOf(t, router, PatternOn(users, ":user/name")); len(got) != 1 || got[0] != "alice" {
		t.Errorf("a pattern on $users reads the datom $users holds, got %v", got)
	}
	if got := valuesOf(t, router, PatternOn(perms, ":perm/name")); len(got) != 1 || got[0] != "read" {
		t.Errorf("a pattern on $perms reads the datom $perms holds, got %v", got)
	}
	if got := valuesOf(t, router, PatternOn(users, ":perm/name")); len(got) != 0 {
		t.Errorf("a pattern on $users reads nothing $perms holds, got %v", got)
	}
}

func TestSourceRouterDefaultSource(t *testing.T) {
	router := NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.SymDollar: sourceOf(t, "d1", ":doc/title", "default"),
	})
	if got := valuesOf(t, router, PatternOn(nil, ":doc/title")); len(got) != 1 || got[0] != "default" {
		t.Errorf("a pattern naming no source reads the datom the default source holds, got %v", got)
	}
}

func TestSourceRouterImplementsPatternMatcher(t *testing.T) {
	var pm PatternMatcher = NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.SymDollar: sourceOf(t, "d1", ":doc/title", "default"),
	})
	if got := valuesOf(t, pm, PatternOn(nil, ":doc/title")); len(got) != 1 || got[0] != "default" {
		t.Errorf("the router serves a pattern through the PatternMatcher interface, got %v", got)
	}
}
