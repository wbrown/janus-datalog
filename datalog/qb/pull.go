package qb

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// PullExpr represents a pull expression for use in Find().
// Pull expressions retrieve attributes of an entity.
type PullExpr struct {
	variable *Var
	pattern  *query.PullPattern
}

// PullSpec is the interface for pull pattern specifications.
// Implementations: Attr (simple attribute), PullLimit, PullDefault, PullRef
type PullSpec interface {
	toPullAttrSpec() query.PullAttrSpec
}

// Make Attr implement PullSpec so it can be used directly in Pull()
func (a Attr) toPullAttrSpec() query.PullAttrSpec {
	return &query.PullAttribute{Attr: a.kw}
}

// PullLimit represents a limit expression for cardinality-many attributes.
type PullLimit struct {
	keyword datalog.Keyword
	limit   int
}

func (p PullLimit) toPullAttrSpec() query.PullAttrSpec {
	return &query.PullLimitExpr{Attr: p.keyword, Limit: p.limit}
}

// PullDefault represents a default value for missing attributes.
type PullDefault struct {
	keyword      datalog.Keyword
	defaultValue interface{}
}

func (p PullDefault) toPullAttrSpec() query.PullAttrSpec {
	return &query.PullDefaultExpr{Attr: p.keyword, Default: p.defaultValue}
}

// PullRef represents a nested reference pull (follows ref and pulls nested attrs).
type PullRef struct {
	keyword datalog.Keyword
	specs   []PullSpec
}

func (p PullRef) toPullAttrSpec() query.PullAttrSpec {
	nestedSpecs := make([]query.PullAttrSpec, len(p.specs))
	for i, spec := range p.specs {
		nestedSpecs[i] = spec.toPullAttrSpec()
	}
	return &query.PullMapSpec{
		Attr:    p.keyword,
		Pattern: &query.PullPattern{Specs: nestedSpecs},
	}
}

// Limit creates a limit expression for cardinality-many attributes.
//
// Example:
//
//	PersonTags := qb.Kw(":person/tags")
//	Pull(e, PersonName, Limit(PersonTags, 5))
//	// → (pull ?e [:person/name (limit :person/tags 5)])
func Limit(attr interface{}, limit int) PullLimit {
	return PullLimit{keyword: attrToKeyword(attr), limit: limit}
}

// Default creates a default value spec for missing attributes.
//
// Example:
//
//	PersonStatus := qb.Kw(":person/status")
//	Pull(e, PersonName, Default(PersonStatus, "active"))
//	// → (pull ?e [:person/name (default :person/status "active")])
func Default(attr interface{}, defaultValue interface{}) PullDefault {
	return PullDefault{keyword: attrToKeyword(attr), defaultValue: defaultValue}
}

// Ref creates a nested reference pull that follows a ref attribute
// and pulls nested attributes from the referenced entity.
//
// Example:
//
//	PersonDept := qb.Kw(":person/dept")
//	DeptName := qb.Kw(":dept/name")
//	DeptCode := qb.Kw(":dept/code")
//	Pull(e, PersonName, Ref(PersonDept, DeptName, DeptCode))
//	// → (pull ?e [:person/name {:person/dept [:dept/name :dept/code]}])
func Ref(attr interface{}, specs ...PullSpec) PullRef {
	return PullRef{keyword: attrToKeyword(attr), specs: specs}
}

// Pull creates a pull expression for use in Find().
//
// With no arguments, pulls all attributes (wildcard):
//
//	Pull(e)  // → (pull ?e [*])
//
// With specific attributes (use Kw to define attributes):
//
//	PersonName := qb.Kw(":person/name")
//	PersonAge := qb.Kw(":person/age")
//	Pull(e, PersonName, PersonAge)
//	// → (pull ?e [:person/name :person/age])
//
// With limit, default, and nested refs:
//
//	Pull(e,
//	    PersonName,
//	    Limit(PersonTags, 5),
//	    Default(PersonStatus, "active"),
//	    Ref(PersonDept, DeptName),
//	)
func Pull(v *Var, specs ...PullSpec) PullExpr {
	var pattern *query.PullPattern

	if len(specs) == 0 {
		// No specs = wildcard [*]
		pattern = &query.PullPattern{
			Specs: []query.PullAttrSpec{&query.PullWildcard{}},
		}
	} else {
		// Convert specs to query.PullAttrSpec
		attrSpecs := make([]query.PullAttrSpec, len(specs))
		for i, spec := range specs {
			attrSpecs[i] = spec.toPullAttrSpec()
		}
		pattern = &query.PullPattern{Specs: attrSpecs}
	}

	return PullExpr{
		variable: v,
		pattern:  pattern,
	}
}

// toFindElement converts PullExpr to a query.FindElement
func (p PullExpr) toFindElement() query.FindElement {
	return query.FindPull{
		Variable: p.variable.Symbol(),
		Pattern:  p.pattern,
	}
}

// attrToKeyword converts Attr or string to a datalog.Keyword
func attrToKeyword(v interface{}) datalog.Keyword {
	switch k := v.(type) {
	case Attr:
		return k.kw
	case datalog.Keyword:
		return k
	case string:
		return datalog.NewKeyword(k)
	default:
		panic("expected Attr, Keyword, or string for pull attribute")
	}
}
