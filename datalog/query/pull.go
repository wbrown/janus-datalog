package query

import (
	"fmt"
	"strings"

	"github.com/wbrown/janus-datalog/datalog"
)

// PullPattern represents a pull specification: [:attr1 :attr2 {:ref [...]}]
type PullPattern struct {
	Specs []PullAttrSpec
}

// String returns the string representation of the pull pattern
func (p *PullPattern) String() string {
	if p == nil {
		return "[]"
	}
	var parts []string
	for _, spec := range p.Specs {
		parts = append(parts, spec.String())
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// PullAttrSpec is the interface for all pull attribute specification types
type PullAttrSpec interface {
	isPullAttrSpec()
	String() string
}

// PullAttribute represents a simple attribute specification: :entity/name
type PullAttribute struct {
	Attr datalog.Keyword
}

func (p *PullAttribute) isPullAttrSpec() {}

func (p *PullAttribute) String() string {
	return p.Attr.String()
}

// PullWildcard represents a wildcard that pulls all attributes: *
type PullWildcard struct{}

func (p *PullWildcard) isPullAttrSpec() {}

func (p *PullWildcard) String() string {
	return "*"
}

// PullMapSpec represents a reference follow specification: {:entity/region [:region/code]}
// This follows a reference attribute and pulls nested attributes from the referenced entity
type PullMapSpec struct {
	Attr    datalog.Keyword
	Pattern *PullPattern
}

func (p *PullMapSpec) isPullAttrSpec() {}

func (p *PullMapSpec) String() string {
	return fmt.Sprintf("{%s %s}", p.Attr.String(), p.Pattern.String())
}

// PullLimitExpr represents a cardinality limit: (limit :entity/tags 10)
// This limits the number of values returned for cardinality-many attributes
type PullLimitExpr struct {
	Attr  datalog.Keyword
	Limit int
}

func (p *PullLimitExpr) isPullAttrSpec() {}

func (p *PullLimitExpr) String() string {
	return fmt.Sprintf("(limit %s %d)", p.Attr.String(), p.Limit)
}

// PullDefaultExpr represents a default value specification: (default :entity/status "unknown")
// This provides a default value when an attribute is missing
type PullDefaultExpr struct {
	Attr    datalog.Keyword
	Default interface{}
}

func (p *PullDefaultExpr) isPullAttrSpec() {}

func (p *PullDefaultExpr) String() string {
	return fmt.Sprintf("(default %s %v)", p.Attr.String(), p.Default)
}

// FindPull represents a pull expression in the find clause
// Example: (pull ?e [:entity/code :entity/name {:entity/region [:region/code]}])
type FindPull struct {
	Variable Symbol       // The entity variable to pull (e.g., ?e)
	Pattern  *PullPattern // The pull specification
}

// String returns the string representation of the FindPull
func (f FindPull) String() string {
	return fmt.Sprintf("(pull %s %s)", f.Variable, f.Pattern.String())
}

// IsAggregate returns false - pull is not an aggregate
func (f FindPull) IsAggregate() bool {
	return false
}

// KeyName returns the attribute name without the leading colon
// e.g., ":entity/name" -> "entity/name"
func KeyName(k datalog.Keyword) string {
	s := k.String()
	if len(s) > 0 && s[0] == ':' {
		return s[1:]
	}
	return s
}
