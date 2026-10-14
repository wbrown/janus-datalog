package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The router sends each pattern to the source its symbol names, sends a
// pattern naming no source to the default source, and refuses a symbol it
// does not hold. Each source is a memory matcher holding one datom of its
// own, so the value a pattern returns says which source served it.

// sourceOf builds a memory matcher holding one datom: entity carrying value
// under attr.
func sourceOf(entity, attr, value string) PatternMatcher {
	return NewMemoryPatternMatcher([]datalog.Datom{{
		E: datalog.NewIdentity(entity), A: datalog.NewKeyword(attr), V: value,
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
	}})
}

// patternOn builds [source ?e attr ?v]; a nil source names the default.
func patternOn(source query.Symbol, attr string) *query.DataPattern {
	return &query.DataPattern{Source: source, Elements: []query.PatternElement{
		query.Variable{Name: datalog.NewSymbol("?e")},
		query.Constant{Value: datalog.NewKeyword(attr)},
		query.Variable{Name: datalog.NewSymbol("?v")},
	}}
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
	if err := collectTuplesInto(&tuples, rel); err != nil {
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
		users: sourceOf("u1", ":user/name", "alice"),
		perms: sourceOf("p1", ":perm/name", "read"),
	})
	if got := valuesOf(t, router, patternOn(users, ":user/name")); len(got) != 1 || got[0] != "alice" {
		t.Errorf("a pattern on $users reads the datom $users holds, got %v", got)
	}
	if got := valuesOf(t, router, patternOn(perms, ":perm/name")); len(got) != 1 || got[0] != "read" {
		t.Errorf("a pattern on $perms reads the datom $perms holds, got %v", got)
	}
	if got := valuesOf(t, router, patternOn(users, ":perm/name")); len(got) != 0 {
		t.Errorf("a pattern on $users reads nothing $perms holds, got %v", got)
	}
}

func TestSourceRouterDefaultSource(t *testing.T) {
	router := NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.SymDollar: sourceOf("d1", ":doc/title", "default"),
	})
	if got := valuesOf(t, router, patternOn(nil, ":doc/title")); len(got) != 1 || got[0] != "default" {
		t.Errorf("a pattern naming no source reads the datom the default source holds, got %v", got)
	}
}

func TestSourceRouterUnknownSource(t *testing.T) {
	router := NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.SymDollar: sourceOf("d1", ":doc/title", "default"),
	})
	_, err := router.Match(query.PatternQuery(patternOn(datalog.NewSymbol("$nonexistent"), ":doc/title")), nil)
	if err == nil {
		t.Fatal("a pattern on a source the router does not hold is refused, got nil")
	}
}

func TestSourceRouterImplementsPatternMatcher(t *testing.T) {
	var pm PatternMatcher = NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.SymDollar: sourceOf("d1", ":doc/title", "default"),
	})
	if got := valuesOf(t, pm, patternOn(nil, ":doc/title")); len(got) != 1 || got[0] != "default" {
		t.Errorf("the router serves a pattern through the PatternMatcher interface, got %v", got)
	}
}
