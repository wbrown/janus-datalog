package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// mockPatternMatcher is a test helper for PatternMatcher
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
		query.Symbol("$users"): mockUsers,
		query.Symbol("$perms"): mockPerms,
	})

	pattern := &query.DataPattern{
		Source: query.Symbol("$users"),
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: ":attr"},
			query.Variable{Name: "?v"},
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
		Source: query.Symbol("$perms"),
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: ":attr"},
			query.Variable{Name: "?v"},
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
		query.Symbol("$"): mockDefault,
	})

	// Empty source should route to "$"
	pattern := &query.DataPattern{
		Source: "",
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: ":attr"},
			query.Variable{Name: "?v"},
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
		query.Symbol("$"): &mockPatternMatcher{},
	})

	pattern := &query.DataPattern{
		Source: query.Symbol("$nonexistent"),
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: ":attr"},
			query.Variable{Name: "?v"},
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
		query.Symbol("$"): mockDefault,
	})

	var pm PatternMatcher = router
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: ":attr"},
			query.Variable{Name: "?v"},
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
