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
// The rosters are collected in one walk on the first call of this method or
// its siblings and returned on every later one. They are the query's own; a
// caller reads them and does not change them.
func (q *Query) DefaultSourceAttributes() []datalog.Keyword {
	return q.collectRosters().order
}

// DefaultSourceAttributeVariables returns every variable in the attribute
// position of a data pattern on the default source, in order of first
// appearance and each once, through NOT/NOT-JOIN bodies and
// OR/OR-JOIN/OR-DEFAULT branches. A pattern on a named source reads that
// source's attributes, a constant or blank in the attribute position is no
// variable, and a subquery body's inputs are the subquery's own to check when
// it runs, so its variables are the inner query's.
func (q *Query) DefaultSourceAttributeVariables() []Symbol {
	return q.collectRosters().variables
}

// SourceAttributes returns every attribute the query names on the named
// source, in order of first appearance and each once: the keyword constant in
// the attribute position of a data pattern whose Source is the symbol, through
// NOT/NOT-JOIN bodies, OR/OR-JOIN/OR-DEFAULT branches and subquery bodies. A
// database function and a pull pattern read the default source, whose roster
// is DefaultSourceAttributes, and name nothing on a named source. A source no
// pattern names has no roster.
func (q *Query) SourceAttributes(source Symbol) []datalog.Keyword {
	if on := q.collectRosters().sources[source]; on != nil {
		return on.order
	}
	return nil
}

// collectRosters walks the query on the first call and returns what it
// names.
func (q *Query) collectRosters() *attributeRoster {
	q.rostersOnce.Do(func() {
		roster := newAttributeRoster()
		roster.addQuery(q, true)
		q.rosters = roster
	})
	return q.rosters
}

// PullPatternAttributes returns every attribute a pull pattern names, through
// nested map specs, in order of first appearance and each once. A wildcard
// names none.
func PullPatternAttributes(p *PullPattern) []datalog.Keyword {
	roster := newAttributeRoster()
	roster.addPullPattern(p)
	return roster.order
}

// attributeRoster accumulates what a query names, in order of first
// appearance and each once: the attributes on the default source, the
// variables in the attribute position of a default-source pattern, and the
// attributes on each named source. Keywords and symbols are interned, so each
// set is keyed on the value itself. The variable set and the named-source map
// are allocated when the first of their kind is recorded.
type attributeRoster struct {
	seen      map[datalog.Keyword]bool
	order     []datalog.Keyword
	seenVars  map[Symbol]bool
	variables []Symbol
	sources   map[Symbol]*sourceRoster
}

// sourceRoster accumulates the attributes named on one named source.
type sourceRoster struct {
	seen  map[datalog.Keyword]bool
	order []datalog.Keyword
}

func newAttributeRoster() *attributeRoster {
	return &attributeRoster{seen: map[datalog.Keyword]bool{}}
}

// add records an attribute named on the default source.
func (r *attributeRoster) add(attr datalog.Keyword) {
	if r.seen[attr] {
		return
	}
	r.seen[attr] = true
	r.order = append(r.order, attr)
}

// addVariable records a variable in the attribute position of a
// default-source pattern.
func (r *attributeRoster) addVariable(sym Symbol) {
	if r.seenVars[sym] {
		return
	}
	if r.seenVars == nil {
		r.seenVars = map[Symbol]bool{}
	}
	r.seenVars[sym] = true
	r.variables = append(r.variables, sym)
}

// addOn records an attribute named on a named source.
func (r *attributeRoster) addOn(source Symbol, attr datalog.Keyword) {
	on := r.sources[source]
	if on == nil {
		if r.sources == nil {
			r.sources = map[Symbol]*sourceRoster{}
		}
		on = &sourceRoster{seen: map[datalog.Keyword]bool{}}
		r.sources[source] = on
	}
	if on.seen[attr] {
		return
	}
	on.seen[attr] = true
	on.order = append(on.order, attr)
}

// addQuery records what a query names. A pull pattern in :find reads the
// default source. outer holds for the query whose inputs may bind a variable
// in an attribute position; a subquery's inputs are its own to check when it
// runs, so its attribute variables are not recorded.
func (r *attributeRoster) addQuery(q *Query, outer bool) {
	for _, elem := range q.Find {
		if pull, ok := elem.(FindPull); ok {
			r.addPullPattern(pull.Pattern)
		}
	}
	r.addClauses(q.Where, outer)
}

// addClauses records what clauses name: a pattern's attribute on its source,
// a pattern's attribute variable on the default source when outer holds, and
// a database function's attributes on the default source. The visitor
// descends into a subquery's body itself, which WalkClauses leaves to its
// visitors. A query's :where and a rule's body are collected alike.
func (r *attributeRoster) addClauses(clauses []Clause, outer bool) {
	WalkClauses(clauses, func(clause Clause) bool {
		switch c := clause.(type) {
		case *DataPattern:
			if c.Source != nil && c.Source != datalog.SymDollar {
				if constant, ok := c.GetA().(Constant); ok {
					if attr, ok := constant.Value.(datalog.Keyword); ok {
						r.addOn(c.Source, attr)
					}
				}
				return true
			}
			switch a := c.GetA().(type) {
			case Constant:
				if attr, ok := a.Value.(datalog.Keyword); ok {
					r.add(attr)
				}
			case Variable:
				if outer {
					r.addVariable(a.Name)
				}
			}
		case *Expression:
			if f, ok := c.Function.(DatabaseFunction); ok {
				r.addDatabaseFunction(f)
			}
		case *DatabaseFunctionPredicate:
			r.addDatabaseFunction(c.Function)
		case *SubqueryPattern:
			r.addQuery(c.Query, false)
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
