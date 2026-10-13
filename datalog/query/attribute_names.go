package query

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// DefaultSourceAttributes returns every attribute the query names on the
// default source, in order of first appearance and each once: the attributes
// of each pull pattern in :find, then, in clause order, the keyword constant
// in the attribute position of a data pattern whose Source is nil or $, and
// the attributes of get-else, get-some and missing? as an expression or as a
// predicate. The walk descends through NOT/NOT-JOIN bodies,
// OR/OR-JOIN/OR-DEFAULT branches and subquery bodies. A pattern on a named
// source reads that source's attributes, not the database's, and a variable
// or blank in the attribute position names none.
//
// The roster is collected on the first call and returned on every later one.
// It is the query's own; a caller reads it and does not change it.
func (q *Query) DefaultSourceAttributes() []datalog.Keyword {
	q.attributesOnce.Do(func() {
		roster := &attributeRoster{seen: map[datalog.Keyword]bool{}}
		roster.addQuery(q)
		q.attributes = roster.order
	})
	return q.attributes
}

// DefaultSourceAttributeVariables returns every variable in the attribute
// position of a data pattern on the default source, in order of first
// appearance and each once, through NOT/NOT-JOIN bodies and
// OR/OR-JOIN/OR-DEFAULT branches. A pattern on a named source reads that
// source's attributes, a constant or blank in the attribute position is no
// variable, and a subquery body's inputs are the subquery's own to check when
// it runs.
func DefaultSourceAttributeVariables(clauses []Clause) []Symbol {
	seen := make(map[Symbol]bool)
	var variables []Symbol
	WalkClauses(clauses, func(clause Clause) bool {
		pattern, ok := clause.(*DataPattern)
		if !ok || (pattern.Source != nil && pattern.Source != datalog.SymDollar) {
			return true
		}
		if v, ok := pattern.GetA().(Variable); ok && !seen[v.Name] {
			seen[v.Name] = true
			variables = append(variables, v.Name)
		}
		return true
	})
	return variables
}

// SourceAttributes returns every attribute the query names on the named
// source, in order of first appearance and each once: the keyword constant in
// the attribute position of a data pattern whose Source is the symbol, through
// NOT/NOT-JOIN bodies, OR/OR-JOIN/OR-DEFAULT branches and subquery bodies. A
// database function and a pull pattern read the default source, whose roster
// is DefaultSourceAttributes, and name nothing on a named source.
func SourceAttributes(q *Query, source Symbol) []datalog.Keyword {
	roster := &attributeRoster{source: source, seen: map[datalog.Keyword]bool{}}
	roster.addQuery(q)
	return roster.order
}

// PullPatternAttributes returns every attribute a pull pattern names, through
// nested map specs, in order of first appearance and each once. A wildcard
// names none.
func PullPatternAttributes(p *PullPattern) []datalog.Keyword {
	roster := &attributeRoster{seen: map[datalog.Keyword]bool{}}
	roster.addPullPattern(p)
	return roster.order
}

// attributeRoster accumulates the attributes named on one source, the
// default source when source is nil and the named source otherwise, in order
// of first appearance and each once. Keywords are interned, so the set is
// keyed on the keyword itself.
type attributeRoster struct {
	source Symbol
	seen   map[datalog.Keyword]bool
	order  []datalog.Keyword
}

// reads reports whether a pattern whose Source is patternSource reads the
// roster's source: a nil or $ Source reads the default source.
func (r *attributeRoster) reads(patternSource Symbol) bool {
	if r.source == nil {
		return patternSource == nil || patternSource == datalog.SymDollar
	}
	return patternSource == r.source
}

func (r *attributeRoster) add(attr datalog.Keyword) {
	if r.seen[attr] {
		return
	}
	r.seen[attr] = true
	r.order = append(r.order, attr)
}

// addQuery records the attributes a query names on the roster's source. A
// pull pattern in :find reads the default source.
func (r *attributeRoster) addQuery(q *Query) {
	if r.source == nil {
		for _, elem := range q.Find {
			if pull, ok := elem.(FindPull); ok {
				r.addPullPattern(pull.Pattern)
			}
		}
	}
	r.addClauses(q.Where)
}

// addClauses records the attributes clauses name on the roster's source. A
// database function reads the default source. The visitor descends into a
// subquery's body itself, which WalkClauses leaves to its visitors. A query's
// :where and a rule's body are collected alike.
func (r *attributeRoster) addClauses(clauses []Clause) {
	WalkClauses(clauses, func(clause Clause) bool {
		switch c := clause.(type) {
		case *DataPattern:
			if r.reads(c.Source) {
				if constant, ok := c.GetA().(Constant); ok {
					if attr, ok := constant.Value.(datalog.Keyword); ok {
						r.add(attr)
					}
				}
			}
		case *Expression:
			if f, ok := c.Function.(DatabaseFunction); ok && r.source == nil {
				r.addDatabaseFunction(f)
			}
		case *DatabaseFunctionPredicate:
			if r.source == nil {
				r.addDatabaseFunction(c.Function)
			}
		case *SubqueryPattern:
			r.addQuery(c.Query)
		}
		return true
	})
}

// addDatabaseFunction records the attributes a database function reads. The
// implementers are enumerated completely, so a new one panics here instead of
// escaping the roster.
func (r *attributeRoster) addDatabaseFunction(f DatabaseFunction) {
	switch f := f.(type) {
	case *GetElseFunction:
		r.add(f.Attr)
	case *GetSomeFunction:
		for _, attr := range f.Attrs {
			r.add(attr)
		}
	case *MissingFunction:
		r.add(f.Attr)
	default:
		panic(fmt.Sprintf("BUG: unknown database function type %T in attributeRoster", f))
	}
}

// addPullPattern records every attribute a pull pattern names, recursing
// through map specs. The spec taxonomy is closed (unexported marker method)
// and enumerated completely here.
func (r *attributeRoster) addPullPattern(p *PullPattern) {
	for _, spec := range p.Specs {
		switch s := spec.(type) {
		case *PullAttribute:
			r.add(s.Attr)
		case *PullWildcard:
			// A wildcard names no attribute.
		case *PullMapSpec:
			r.add(s.Attr)
			r.addPullPattern(s.Pattern)
		case *PullLimitExpr:
			r.add(s.Attr)
		case *PullDefaultExpr:
			r.add(s.Attr)
		default:
			panic(fmt.Sprintf("BUG: unknown pull spec type %T in attributeRoster", spec))
		}
	}
}
