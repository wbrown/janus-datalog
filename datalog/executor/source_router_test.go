package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"

	"github.com/wbrown/janus-datalog/datalog"
)

// mockPatternMatcher is a test double for PatternMatcher
type mockPatternMatcher struct {
	matchFunc func(pattern *query.DataPattern, bindings Relations) (Relation, error)
	wasCalled bool
}

func (m *mockPatternMatcher) Match(pattern *query.DataPattern, bindings Relations) (Relation, error) {
	m.wasCalled = true
	if m.matchFunc != nil {
		return m.matchFunc(pattern, bindings)
	}
	return NewMaterializedRelation(pattern.Symbols(), nil), nil
}

var _ PatternMatcher = (*mockPatternMatcher)(nil)

func TestSourceRouterRoutes(t *testing.T) {
	mockUsers := &mockPatternMatcher{}
	mockPerms := &mockPatternMatcher{}

	router := NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.NewSymbol("$users"): mockUsers,
		datalog.NewSymbol("$perms"): mockPerms,
	})

	pattern := &query.DataPattern{
		Source: datalog.NewSymbol("$users"),
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: ":attr"},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	_, err := router.Match(pattern, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mockUsers.wasCalled {
		t.Error("expected $users source to be called")
	}
	if mockPerms.wasCalled {
		t.Error("expected $perms source NOT to be called")
	}

	// Reset and route to $perms
	mockUsers.wasCalled = false
	pattern = &query.DataPattern{
		Source: datalog.NewSymbol("$perms"),
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: ":attr"},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	_, err = router.Match(pattern, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mockUsers.wasCalled {
		t.Error("expected $users source NOT to be called")
	}
	if !mockPerms.wasCalled {
		t.Error("expected $perms source to be called")
	}
}

func TestSourceRouterDefaultSource(t *testing.T) {
	mockDefault := &mockPatternMatcher{}

	router := NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.NewSymbol("$"): mockDefault,
	})

	// Empty source should route to "$"
	pattern := &query.DataPattern{
		Source: nil,
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: ":attr"},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	_, err := router.Match(pattern, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mockDefault.wasCalled {
		t.Error("expected default source to be called for empty source")
	}
}

func TestSourceRouterUnknownSource(t *testing.T) {
	router := NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.NewSymbol("$"): &mockPatternMatcher{},
	})

	pattern := &query.DataPattern{
		Source: datalog.NewSymbol("$nonexistent"),
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: ":attr"},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	_, err := router.Match(pattern, nil)
	if err == nil {
		t.Fatal("expected error for unknown source, got nil")
	}
}

func TestSourceRouterImplementsPatternMatcher(t *testing.T) {
	// Verify SourceRouter can be used as a PatternMatcher
	mockDefault := &mockPatternMatcher{}
	router := NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.NewSymbol("$"): mockDefault,
	})

	var pm PatternMatcher = router
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: ":attr"},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	_, err := pm.Match(pattern, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mockDefault.wasCalled {
		t.Error("expected default source to be called via PatternMatcher interface")
	}
}
