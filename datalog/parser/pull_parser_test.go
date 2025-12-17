package parser

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestParsePullPattern_SimpleAttributes(t *testing.T) {
	input := `[:entity/code :entity/name :entity/score]`

	pattern, err := ParsePullPattern(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pattern.Specs) != 3 {
		t.Fatalf("expected 3 specs, got %d", len(pattern.Specs))
	}

	// Check each spec
	expectedAttrs := []string{":entity/code", ":entity/name", ":entity/score"}
	for i, spec := range pattern.Specs {
		attr, ok := spec.(*query.PullAttribute)
		if !ok {
			t.Errorf("spec %d: expected PullAttribute, got %T", i, spec)
			continue
		}
		if attr.Attr.String() != expectedAttrs[i] {
			t.Errorf("spec %d: expected %s, got %s", i, expectedAttrs[i], attr.Attr.String())
		}
	}
}

func TestParsePullPattern_Wildcard(t *testing.T) {
	input := `[*]`

	pattern, err := ParsePullPattern(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pattern.Specs) != 1 {
		t.Fatalf("expected 1 spec, got %d", len(pattern.Specs))
	}

	_, ok := pattern.Specs[0].(*query.PullWildcard)
	if !ok {
		t.Errorf("expected PullWildcard, got %T", pattern.Specs[0])
	}
}

func TestParsePullPattern_MapSpec(t *testing.T) {
	input := `[:entity/code {:entity/region [:region/code :region/name]}]`

	pattern, err := ParsePullPattern(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pattern.Specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(pattern.Specs))
	}

	// First spec is simple attribute
	attr, ok := pattern.Specs[0].(*query.PullAttribute)
	if !ok {
		t.Errorf("spec 0: expected PullAttribute, got %T", pattern.Specs[0])
	} else if attr.Attr.String() != ":entity/code" {
		t.Errorf("spec 0: expected :entity/code, got %s", attr.Attr.String())
	}

	// Second spec is map spec
	mapSpec, ok := pattern.Specs[1].(*query.PullMapSpec)
	if !ok {
		t.Fatalf("spec 1: expected PullMapSpec, got %T", pattern.Specs[1])
	}

	if mapSpec.Attr.String() != ":entity/region" {
		t.Errorf("map spec attr: expected :entity/region, got %s", mapSpec.Attr.String())
	}

	if len(mapSpec.Pattern.Specs) != 2 {
		t.Fatalf("nested pattern: expected 2 specs, got %d", len(mapSpec.Pattern.Specs))
	}

	// Check nested pattern attributes
	nestedExpected := []string{":region/code", ":region/name"}
	for i, spec := range mapSpec.Pattern.Specs {
		nestedAttr, ok := spec.(*query.PullAttribute)
		if !ok {
			t.Errorf("nested spec %d: expected PullAttribute, got %T", i, spec)
			continue
		}
		if nestedAttr.Attr.String() != nestedExpected[i] {
			t.Errorf("nested spec %d: expected %s, got %s", i, nestedExpected[i], nestedAttr.Attr.String())
		}
	}
}

func TestParsePullPattern_NestedMapSpecs(t *testing.T) {
	input := `[:entity/name {:entity/region [:region/code {:region/nation [:nation/name]}]}]`

	pattern, err := ParsePullPattern(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pattern.Specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(pattern.Specs))
	}

	// Navigate to deeply nested map spec
	mapSpec, ok := pattern.Specs[1].(*query.PullMapSpec)
	if !ok {
		t.Fatalf("expected PullMapSpec, got %T", pattern.Specs[1])
	}

	if len(mapSpec.Pattern.Specs) != 2 {
		t.Fatalf("nested pattern: expected 2 specs, got %d", len(mapSpec.Pattern.Specs))
	}

	nestedMap, ok := mapSpec.Pattern.Specs[1].(*query.PullMapSpec)
	if !ok {
		t.Fatalf("deeply nested: expected PullMapSpec, got %T", mapSpec.Pattern.Specs[1])
	}

	if nestedMap.Attr.String() != ":region/nation" {
		t.Errorf("deeply nested attr: expected :region/nation, got %s", nestedMap.Attr.String())
	}
}

func TestParsePullPattern_LimitExpr(t *testing.T) {
	input := `[:entity/name (limit :entity/tags 10)]`

	pattern, err := ParsePullPattern(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pattern.Specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(pattern.Specs))
	}

	limitExpr, ok := pattern.Specs[1].(*query.PullLimitExpr)
	if !ok {
		t.Fatalf("expected PullLimitExpr, got %T", pattern.Specs[1])
	}

	if limitExpr.Attr.String() != ":entity/tags" {
		t.Errorf("limit attr: expected :entity/tags, got %s", limitExpr.Attr.String())
	}

	if limitExpr.Limit != 10 {
		t.Errorf("limit value: expected 10, got %d", limitExpr.Limit)
	}
}

func TestParsePullPattern_DefaultExpr(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		attr     string
		expected interface{}
	}{
		{
			name:     "string default",
			input:    `[(default :entity/status "unknown")]`,
			attr:     ":entity/status",
			expected: "unknown",
		},
		{
			name:     "int default",
			input:    `[(default :entity/count 0)]`,
			attr:     ":entity/count",
			expected: int64(0),
		},
		{
			name:     "bool default",
			input:    `[(default :entity/active true)]`,
			attr:     ":entity/active",
			expected: true,
		},
		{
			name:     "keyword default",
			input:    `[(default :entity/type :type/unknown)]`,
			attr:     ":entity/type",
			expected: datalog.NewKeyword(":type/unknown"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pattern, err := ParsePullPattern(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(pattern.Specs) != 1 {
				t.Fatalf("expected 1 spec, got %d", len(pattern.Specs))
			}

			defaultExpr, ok := pattern.Specs[0].(*query.PullDefaultExpr)
			if !ok {
				t.Fatalf("expected PullDefaultExpr, got %T", pattern.Specs[0])
			}

			if defaultExpr.Attr.String() != tt.attr {
				t.Errorf("attr: expected %s, got %s", tt.attr, defaultExpr.Attr.String())
			}

			// For keywords, compare string representation
			if kw, ok := tt.expected.(datalog.Keyword); ok {
				actualKw, ok := defaultExpr.Default.(datalog.Keyword)
				if !ok {
					t.Errorf("expected Keyword default, got %T", defaultExpr.Default)
				} else if actualKw.String() != kw.String() {
					t.Errorf("default value: expected %v, got %v", kw.String(), actualKw.String())
				}
			} else if defaultExpr.Default != tt.expected {
				t.Errorf("default value: expected %v (%T), got %v (%T)", tt.expected, tt.expected, defaultExpr.Default, defaultExpr.Default)
			}
		})
	}
}

func TestParseFindPull_InQuery(t *testing.T) {
	input := `[:find (pull ?e [:entity/code :entity/name])
              :where [?e :entity/type :type/stock]]`

	q, err := ParseQuery(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(q.Find) != 1 {
		t.Fatalf("expected 1 find element, got %d", len(q.Find))
	}

	pull, ok := q.Find[0].(query.FindPull)
	if !ok {
		t.Fatalf("expected FindPull, got %T", q.Find[0])
	}

	if pull.Variable != "?e" {
		t.Errorf("expected variable ?e, got %s", pull.Variable)
	}

	if len(pull.Pattern.Specs) != 2 {
		t.Errorf("expected 2 pull specs, got %d", len(pull.Pattern.Specs))
	}
}

func TestParseFindPull_MixedWithVariables(t *testing.T) {
	input := `[:find ?type (pull ?e [:entity/code :entity/name {:entity/region [:region/name]}])
              :where [?e :entity/type ?type]]`

	q, err := ParseQuery(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(q.Find) != 2 {
		t.Fatalf("expected 2 find elements, got %d", len(q.Find))
	}

	// First element is a variable
	_, ok := q.Find[0].(query.FindVariable)
	if !ok {
		t.Errorf("find[0]: expected FindVariable, got %T", q.Find[0])
	}

	// Second element is a pull
	pull, ok := q.Find[1].(query.FindPull)
	if !ok {
		t.Fatalf("find[1]: expected FindPull, got %T", q.Find[1])
	}

	if pull.Variable != "?e" {
		t.Errorf("expected variable ?e, got %s", pull.Variable)
	}

	// Check nested map spec
	if len(pull.Pattern.Specs) != 3 {
		t.Fatalf("expected 3 pull specs, got %d", len(pull.Pattern.Specs))
	}

	mapSpec, ok := pull.Pattern.Specs[2].(*query.PullMapSpec)
	if !ok {
		t.Fatalf("spec[2]: expected PullMapSpec, got %T", pull.Pattern.Specs[2])
	}

	if mapSpec.Attr.String() != ":entity/region" {
		t.Errorf("map spec attr: expected :entity/region, got %s", mapSpec.Attr.String())
	}
}

func TestParsePullPattern_Errors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "not a vector",
			input: `(:entity/code)`,
		},
		{
			name:  "invalid symbol",
			input: `[foo]`,
		},
		{
			name:  "empty map spec",
			input: `[{}]`,
		},
		{
			name:  "map with non-keyword key",
			input: `[{foo [:bar]}]`,
		},
		{
			name:  "map with non-vector value",
			input: `[{:foo :bar}]`,
		},
		{
			name:  "limit missing args",
			input: `[(limit :foo)]`,
		},
		{
			name:  "limit non-integer",
			input: `[(limit :foo "ten")]`,
		},
		{
			name:  "default missing args",
			input: `[(default :foo)]`,
		},
		{
			name:  "unknown function",
			input: `[(unknown :foo 1)]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParsePullPattern(tt.input)
			if err == nil {
				t.Errorf("expected error for input: %s", tt.input)
			}
		})
	}
}

func TestParseFindPull_Errors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "pull missing pattern",
			input: `[:find (pull ?e) :where [?e :foo :bar]]`,
		},
		{
			name:  "pull non-variable",
			input: `[:find (pull :entity [:foo]) :where [?e :foo :bar]]`,
		},
		{
			name:  "pull non-vector pattern",
			input: `[:find (pull ?e (:foo :bar)) :where [?e :foo :bar]]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseQuery(tt.input)
			if err == nil {
				t.Errorf("expected error for input: %s", tt.input)
			}
		})
	}
}

func TestParsePullPattern_String(t *testing.T) {
	input := `[:entity/code * {:entity/region [:region/name]} (limit :tags 5) (default :status "unknown")]`

	pattern, err := ParsePullPattern(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	str := pattern.String()

	// Verify string contains expected parts
	if str == "" {
		t.Error("String() returned empty string")
	}

	// The string should round-trip correctly through parsing
	// (though order might differ for some implementations)
	t.Logf("Pattern string: %s", str)
}
