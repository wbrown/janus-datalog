package qb

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestPull_Wildcard(t *testing.T) {
	e := NewVar("e")
	pull := Pull(e)

	elem := pull.toFindElement()
	findPull, ok := elem.(query.FindPull)
	if !ok {
		t.Fatalf("expected FindPull, got %T", elem)
	}

	if findPull.Variable.String() != "?e" {
		t.Errorf("expected variable ?e, got %s", findPull.Variable.String())
	}

	if len(findPull.Pattern.Specs) != 1 {
		t.Fatalf("expected 1 spec, got %d", len(findPull.Pattern.Specs))
	}

	if _, ok := findPull.Pattern.Specs[0].(*query.PullWildcard); !ok {
		t.Errorf("expected PullWildcard, got %T", findPull.Pattern.Specs[0])
	}
}

func TestPull_SpecificAttributes(t *testing.T) {
	e := NewVar("e")
	PersonName := Kw(":person/name")
	PersonAge := Kw(":person/age")

	pull := Pull(e, PersonName, PersonAge)

	elem := pull.toFindElement()
	findPull, ok := elem.(query.FindPull)
	if !ok {
		t.Fatalf("expected FindPull, got %T", elem)
	}

	if len(findPull.Pattern.Specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(findPull.Pattern.Specs))
	}

	attr1, ok := findPull.Pattern.Specs[0].(*query.PullAttribute)
	if !ok {
		t.Fatalf("expected PullAttribute, got %T", findPull.Pattern.Specs[0])
	}
	if attr1.Attr.String() != ":person/name" {
		t.Errorf("expected :person/name, got %s", attr1.Attr.String())
	}

	attr2, ok := findPull.Pattern.Specs[1].(*query.PullAttribute)
	if !ok {
		t.Fatalf("expected PullAttribute, got %T", findPull.Pattern.Specs[1])
	}
	if attr2.Attr.String() != ":person/age" {
		t.Errorf("expected :person/age, got %s", attr2.Attr.String())
	}
}

func TestPull_Limit(t *testing.T) {
	e := NewVar("e")
	PersonName := Kw(":person/name")
	PersonTags := Kw(":person/tags")

	pull := Pull(e, PersonName, Limit(PersonTags, 5))

	elem := pull.toFindElement()
	findPull := elem.(query.FindPull)

	if len(findPull.Pattern.Specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(findPull.Pattern.Specs))
	}

	limitExpr, ok := findPull.Pattern.Specs[1].(*query.PullLimitExpr)
	if !ok {
		t.Fatalf("expected PullLimitExpr, got %T", findPull.Pattern.Specs[1])
	}
	if limitExpr.Attr.String() != ":person/tags" {
		t.Errorf("expected :person/tags, got %s", limitExpr.Attr.String())
	}
	if limitExpr.Limit != 5 {
		t.Errorf("expected limit=5, got %d", limitExpr.Limit)
	}
}

func TestPull_LimitWithString(t *testing.T) {
	e := NewVar("e")
	// Test that strings also work
	pull := Pull(e, Limit(":person/tags", 3))

	elem := pull.toFindElement()
	findPull := elem.(query.FindPull)

	limitExpr := findPull.Pattern.Specs[0].(*query.PullLimitExpr)
	if limitExpr.Attr.String() != ":person/tags" {
		t.Errorf("expected :person/tags, got %s", limitExpr.Attr.String())
	}
	if limitExpr.Limit != 3 {
		t.Errorf("expected limit=3, got %d", limitExpr.Limit)
	}
}

func TestPull_Default(t *testing.T) {
	e := NewVar("e")
	PersonName := Kw(":person/name")
	PersonStatus := Kw(":person/status")

	pull := Pull(e, PersonName, Default(PersonStatus, "active"))

	elem := pull.toFindElement()
	findPull := elem.(query.FindPull)

	if len(findPull.Pattern.Specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(findPull.Pattern.Specs))
	}

	defaultExpr, ok := findPull.Pattern.Specs[1].(*query.PullDefaultExpr)
	if !ok {
		t.Fatalf("expected PullDefaultExpr, got %T", findPull.Pattern.Specs[1])
	}
	if defaultExpr.Attr.String() != ":person/status" {
		t.Errorf("expected :person/status, got %s", defaultExpr.Attr.String())
	}
	if defaultExpr.Default != "active" {
		t.Errorf("expected default='active', got %v", defaultExpr.Default)
	}
}

func TestPull_NestedRef(t *testing.T) {
	e := NewVar("e")
	PersonName := Kw(":person/name")
	PersonDept := Kw(":person/dept")
	DeptName := Kw(":dept/name")
	DeptCode := Kw(":dept/code")

	pull := Pull(e,
		PersonName,
		Ref(PersonDept, DeptName, DeptCode),
	)

	elem := pull.toFindElement()
	findPull := elem.(query.FindPull)

	if len(findPull.Pattern.Specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(findPull.Pattern.Specs))
	}

	mapSpec, ok := findPull.Pattern.Specs[1].(*query.PullMapSpec)
	if !ok {
		t.Fatalf("expected PullMapSpec, got %T", findPull.Pattern.Specs[1])
	}
	if mapSpec.Attr.String() != ":person/dept" {
		t.Errorf("expected :person/dept, got %s", mapSpec.Attr.String())
	}

	// Check nested pattern
	if len(mapSpec.Pattern.Specs) != 2 {
		t.Fatalf("expected 2 nested specs, got %d", len(mapSpec.Pattern.Specs))
	}

	nestedAttr1, ok := mapSpec.Pattern.Specs[0].(*query.PullAttribute)
	if !ok {
		t.Fatalf("expected PullAttribute, got %T", mapSpec.Pattern.Specs[0])
	}
	if nestedAttr1.Attr.String() != ":dept/name" {
		t.Errorf("expected :dept/name, got %s", nestedAttr1.Attr.String())
	}
}

func TestPull_DeepNesting(t *testing.T) {
	// Test deeply nested refs
	e := NewVar("e")
	PersonCompany := Kw(":person/company")
	CompanyName := Kw(":company/name")
	CompanyCEO := Kw(":company/ceo")
	PersonName := Kw(":person/name")

	pull := Pull(e,
		Ref(PersonCompany,
			CompanyName,
			Ref(CompanyCEO,
				PersonName,
			),
		),
	)

	elem := pull.toFindElement()
	findPull := elem.(query.FindPull)

	mapSpec := findPull.Pattern.Specs[0].(*query.PullMapSpec)
	if mapSpec.Attr.String() != ":person/company" {
		t.Errorf("expected :person/company, got %s", mapSpec.Attr.String())
	}

	// Check second level nesting
	nestedMapSpec, ok := mapSpec.Pattern.Specs[1].(*query.PullMapSpec)
	if !ok {
		t.Fatalf("expected nested PullMapSpec, got %T", mapSpec.Pattern.Specs[1])
	}
	if nestedMapSpec.Attr.String() != ":company/ceo" {
		t.Errorf("expected :company/ceo, got %s", nestedMapSpec.Attr.String())
	}

	// Check deepest level
	deepAttr := nestedMapSpec.Pattern.Specs[0].(*query.PullAttribute)
	if deepAttr.Attr.String() != ":person/name" {
		t.Errorf("expected :person/name, got %s", deepAttr.Attr.String())
	}
}

func TestPull_InQuery(t *testing.T) {
	e := NewVar("e")
	name := NewVar("name")
	PersonName := Kw(":person/name")
	PersonAge := Kw(":person/age")

	// Build a query with pull in Find
	q := Query().
		Find(Pull(e, PersonName, PersonAge)).
		Where(
			Pat(e, PersonName, name),
		).
		MustBuild()

	if len(q.Find) != 1 {
		t.Fatalf("expected 1 find element, got %d", len(q.Find))
	}

	findPull, ok := q.Find[0].(query.FindPull)
	if !ok {
		t.Fatalf("expected FindPull, got %T", q.Find[0])
	}

	if len(findPull.Pattern.Specs) != 2 {
		t.Errorf("expected 2 pull specs, got %d", len(findPull.Pattern.Specs))
	}
}

func TestPull_MixedFind(t *testing.T) {
	e := NewVar("e")
	name := NewVar("name")
	PersonName := Kw(":person/name")
	PersonAge := Kw(":person/age")

	// Mix regular variable and pull in Find
	q := Query().
		Find(name, Pull(e, PersonAge)).
		Where(
			Pat(e, PersonName, name),
		).
		MustBuild()

	if len(q.Find) != 2 {
		t.Fatalf("expected 2 find elements, got %d", len(q.Find))
	}

	// First should be variable
	if _, ok := q.Find[0].(query.FindVariable); !ok {
		t.Errorf("expected FindVariable, got %T", q.Find[0])
	}

	// Second should be pull
	if _, ok := q.Find[1].(query.FindPull); !ok {
		t.Errorf("expected FindPull, got %T", q.Find[1])
	}
}

func TestPull_PatternString(t *testing.T) {
	e := NewVar("e")
	PersonName := Kw(":person/name")
	PersonAge := Kw(":person/age")
	PersonTags := Kw(":person/tags")
	PersonStatus := Kw(":person/status")
	PersonDept := Kw(":person/dept")
	DeptName := Kw(":dept/name")

	tests := []struct {
		name     string
		pull     PullExpr
		expected string
	}{
		{
			name:     "wildcard",
			pull:     Pull(e),
			expected: "(pull ?e [*])",
		},
		{
			name:     "single attr",
			pull:     Pull(e, PersonName),
			expected: "(pull ?e [:person/name])",
		},
		{
			name:     "multiple attrs",
			pull:     Pull(e, PersonName, PersonAge),
			expected: "(pull ?e [:person/name :person/age])",
		},
		{
			name:     "with limit",
			pull:     Pull(e, Limit(PersonTags, 5)),
			expected: "(pull ?e [(limit :person/tags 5)])",
		},
		{
			name:     "with default",
			pull:     Pull(e, Default(PersonStatus, "active")),
			expected: "(pull ?e [(default :person/status active)])",
		},
		{
			name:     "nested ref",
			pull:     Pull(e, Ref(PersonDept, DeptName)),
			expected: "(pull ?e [{:person/dept [:dept/name]}])",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			elem := tt.pull.toFindElement()
			findPull := elem.(query.FindPull)
			got := findPull.String()
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}
