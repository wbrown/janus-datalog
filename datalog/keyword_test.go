package datalog

import "testing"

func TestKeyword_NamespaceAndName(t *testing.T) {
	tests := []struct {
		input string
		ns    string
		name  string
	}{
		// Basic cases
		{":foo", "", "foo"},
		{":foo/bar", "foo", "bar"},

		// Multiple slashes - only first splits
		{":foo/bar/baz", "foo", "bar/baz"},

		// Edge cases
		{":/", "", "/"},
		{":foo/", "foo", ""},

		// Colons in name (downstream use case)
		{":scenario/characterEval:Alice", "scenario", "characterEval:Alice"},

		// No leading colon (after NewKeyword normalization)
		{"foo/bar", "foo", "bar"},
		{"foo", "", "foo"},
	}

	for _, tt := range tests {
		k := NewKeyword(tt.input)
		if got := k.Namespace(); got != tt.ns {
			t.Errorf("NewKeyword(%q).Namespace() = %q, want %q", tt.input, got, tt.ns)
		}
		if got := k.Name(); got != tt.name {
			t.Errorf("NewKeyword(%q).Name() = %q, want %q", tt.input, got, tt.name)
		}
	}
}

func TestKeyword_IsQualified(t *testing.T) {
	tests := []struct {
		input     string
		qualified bool
	}{
		{":foo", false},
		{":foo/bar", true},
		{":foo/bar/baz", true},
		{":/", false}, // "/" is the name, not a separator - unqualified
		{":foo/", true},
	}

	for _, tt := range tests {
		k := NewKeyword(tt.input)
		if got := k.IsQualified(); got != tt.qualified {
			t.Errorf("NewKeyword(%q).IsQualified() = %v, want %v", tt.input, got, tt.qualified)
		}
	}
}

func TestKeyword_InNamespace(t *testing.T) {
	k := NewKeyword(":scenario/premise")

	if !k.InNamespace("scenario") {
		t.Error("Expected :scenario/premise to be in namespace 'scenario'")
	}
	if k.InNamespace("other") {
		t.Error("Expected :scenario/premise NOT to be in namespace 'other'")
	}

	// Unqualified keywords have empty namespace
	k2 := NewKeyword(":foo")
	if !k2.InNamespace("") {
		t.Error("Expected :foo to be in empty namespace")
	}
	if k2.InNamespace("foo") {
		t.Error("Expected :foo NOT to be in namespace 'foo'")
	}
}

func TestKeyword_Matches(t *testing.T) {
	k := Kw("scenario", "premise")

	tests := []struct {
		pattern Keyword
		matches bool
		desc    string
	}{
		// Exact match
		{Kw("scenario", "premise"), true, "exact match"},

		// Wildcard namespace
		{Kw("*", "premise"), true, "wildcard namespace"},

		// Wildcard name
		{Kw("scenario", "*"), true, "wildcard name"},

		// Both wildcards
		{Kw("*", "*"), true, "both wildcards"},

		// No match - wrong namespace
		{Kw("other", "premise"), false, "wrong namespace"},

		// No match - wrong name
		{Kw("scenario", "other"), false, "wrong name"},

		// No match - both wrong
		{Kw("other", "other"), false, "both wrong"},
	}

	for _, tt := range tests {
		if got := k.Matches(tt.pattern); got != tt.matches {
			t.Errorf("%s: %v.Matches(%v) = %v, want %v",
				tt.desc, k.String(), tt.pattern.String(), got, tt.matches)
		}
	}
}

func TestKeyword_Matches_Unqualified(t *testing.T) {
	k := NewKeyword(":foo")

	// Unqualified keyword matches empty namespace
	if !k.Matches(Kw("", "foo")) {
		t.Error("Expected :foo to match Kw(\"\", \"foo\")")
	}

	// Wildcard namespace matches unqualified
	if !k.Matches(Kw("*", "foo")) {
		t.Error("Expected :foo to match Kw(\"*\", \"foo\")")
	}

	// Doesn't match if namespace required
	if k.Matches(Kw("bar", "foo")) {
		t.Error("Expected :foo NOT to match Kw(\"bar\", \"foo\")")
	}
}

func TestKw_Constructor(t *testing.T) {
	tests := []struct {
		ns     string
		name   string
		expect string
	}{
		{"scenario", "premise", ":scenario/premise"},
		{"", "foo", ":foo"},
		{"db", "id", ":db/id"},
		{"user", "name", ":user/name"},
	}

	for _, tt := range tests {
		k := Kw(tt.ns, tt.name)
		if got := k.String(); got != tt.expect {
			t.Errorf("Kw(%q, %q).String() = %q, want %q", tt.ns, tt.name, got, tt.expect)
		}
	}
}

func TestNewKeyword_AutoPrefix(t *testing.T) {
	tests := []struct {
		input  string
		expect string
	}{
		// Already has colon
		{":foo/bar", ":foo/bar"},
		{":foo", ":foo"},

		// Missing colon - auto-prefix
		{"foo/bar", ":foo/bar"},
		{"foo", ":foo"},

		// Empty string - still gets colon prefix
		{"", ":"},
	}

	for _, tt := range tests {
		k := NewKeyword(tt.input)
		if got := k.String(); got != tt.expect {
			t.Errorf("NewKeyword(%q).String() = %q, want %q", tt.input, got, tt.expect)
		}
	}
}

func TestKw_RoundTrip(t *testing.T) {
	// Verify that Kw(ns, name) produces keyword where Namespace()/Name() return ns/name
	tests := []struct {
		ns   string
		name string
	}{
		{"scenario", "premise"},
		{"", "foo"},
		{"db", "id"},
		{"foo", "bar/baz"}, // Name can contain slashes
	}

	for _, tt := range tests {
		k := Kw(tt.ns, tt.name)
		if got := k.Namespace(); got != tt.ns {
			t.Errorf("Kw(%q, %q).Namespace() = %q, want %q", tt.ns, tt.name, got, tt.ns)
		}
		if got := k.Name(); got != tt.name {
			t.Errorf("Kw(%q, %q).Name() = %q, want %q", tt.ns, tt.name, got, tt.name)
		}
	}
}
