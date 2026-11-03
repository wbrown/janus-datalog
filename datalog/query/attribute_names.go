package query

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// Rosters is what a prepared query names on each source, collected once by
// CollectRosters and held with the query's plan, which the vocabulary check
// reads. Per source, $ among them, it holds the keyword constants written in
// the attribute position of the data patterns on the source and the
// attributes of the get-else, get-some and missing? that read it, at any
// depth of header forms and subquery bodies, an inner query's under the
// sources its call passes, and on $ the attributes of the :find pull
// patterns. It holds the sources in the order the query first names an
// attribute on each. A keyword that reaches an attribute position through a
// variable is a value, whatever binds the variable, and is named on no
// source. Keywords and symbols are interned, so each set is keyed on the
// value itself.
type Rosters struct {
	sources map[Symbol]*sourceRoster
	order   []Symbol
}

// CollectRosters walks a prepared query once and returns what it names. The
// query holds header forms alone, since Prepare writes every header before
// planning; a headerless form reaching the walk is a skipped preparation and
// panics, as ScopeOf panics on it.
func CollectRosters(q *Query) *Rosters {
	r := newRosters()
	r.addQuery(q)
	return r
}

// SourceAttributes returns every attribute the query names on the source,
// in order of first appearance and each once: on $, the attributes of each
// pull pattern in :find first, then, in clause order, the keyword constant in
// the attribute position of each data pattern on the source, a pattern whose
// Source is nil reading $, and the attributes of each get-else, get-some and
// missing? that reads the source, as an expression or as a predicate. A
// pattern or function inside a subquery body is read under the source the
// call passes at its inner source's place. A source the query names no
// attribute on has no roster.
func (r *Rosters) SourceAttributes(source Symbol) []datalog.Keyword {
	if on := r.sources[source]; on != nil {
		return on.order
	}
	return nil
}

// Sources returns every source the query names an attribute on, in order of
// first appearance and each once.
func (r *Rosters) Sources() []Symbol {
	return r.order
}

// PullPatternAttributes returns every attribute a pull pattern names, through
// nested map specs, in order of first appearance and each once. A wildcard
// names none.
func PullPatternAttributes(p *PullPattern) []datalog.Keyword {
	roster := newRosters()
	roster.addPullPattern(p)
	return roster.SourceAttributes(datalog.SymDollar)
}

// sourceRoster accumulates the attributes named on one source.
type sourceRoster struct {
	seen  map[datalog.Keyword]bool
	order []datalog.Keyword
}

func newRosters() *Rosters {
	return &Rosters{sources: map[Symbol]*sourceRoster{}}
}

// add records an attribute named on a source, and the source the first time
// the query names an attribute on it.
func (r *Rosters) add(source Symbol, attr datalog.Keyword) {
	on := r.sources[source]
	if on == nil {
		on = &sourceRoster{seen: map[datalog.Keyword]bool{}}
		r.sources[source] = on
		r.order = append(r.order, source)
	}
	if on.seen[attr] {
		return
	}
	on.seen[attr] = true
	on.order = append(on.order, attr)
}

// addQuery records what a query names. A pull pattern in :find reads the
// default source.
func (r *Rosters) addQuery(q *Query) {
	for _, elem := range q.Find {
		if pull, ok := elem.(FindPull); ok {
			r.addPullPattern(pull.Pattern)
		}
	}
	r.addClauses(q.Where)
}

// addClauses records what a query's :where or a rule's body names: a
// pattern's constant attribute on its source, nil standing for $, and a
// database function's attributes on the source it reads, descending through each header form's
// branches and bodies and into each subquery's body, whose clauses are read
// under the sources the call passes (RenameSources, CallSources), since
// WalkClauses leaves a subquery's body to its visitors. A headerless form is
// a skipped preparation and panics.
func (r *Rosters) addClauses(clauses []Clause) {
	WalkClauses(clauses, func(clause Clause) bool {
		switch c := clause.(type) {
		case *DataPattern:
			source := c.Source
			if source == nil {
				source = datalog.SymDollar
			}
			if constant, ok := c.GetA().(Constant); ok {
				if attr, ok := constant.Value.(datalog.Keyword); ok {
					r.add(source, attr)
				}
			}
		case *Expression:
			if f, ok := c.Function.(DatabaseFunction); ok {
				r.addDatabaseFunction(f)
			}
		case *DatabaseFunctionPredicate:
			r.addDatabaseFunction(c.Function)
		case *SubqueryPattern:
			r.addClauses(RenameSources(c.Query.Where, CallSources(c)))
		case *NotClause, *OrClause, *OrDefaultClause:
			panic(fmt.Sprintf("BUG: headerless form %T reached the roster; Prepare writes every header before a query is planned", clause))
		}
		return true
	})
}

// addDatabaseFunction records the attributes a database function reads, on
// the source it reads. The implementers are enumerated completely, so a new
// one panics here instead of escaping the roster.
func (r *Rosters) addDatabaseFunction(f DatabaseFunction) {
	source := f.ReadsFrom()
	switch f := f.(type) {
	case *GetElseFunction:
		r.add(source, f.Attr)
	case *GetSomeFunction:
		for _, attr := range f.Attrs {
			r.add(source, attr)
		}
	case *MissingFunction:
		r.add(source, f.Attr)
	default:
		panic(fmt.Sprintf("BUG: unknown database function type %T in Rosters", f))
	}
}

// addPullPattern records every attribute a pull pattern names, on the
// default source, recursing through map specs. The spec taxonomy is closed
// (unexported marker method) and enumerated completely here.
func (r *Rosters) addPullPattern(p *PullPattern) {
	for _, spec := range p.Specs {
		switch s := spec.(type) {
		case *PullAttribute:
			r.add(datalog.SymDollar, s.Attr)
		case *PullWildcard:
			// A wildcard names no attribute.
		case *PullMapSpec:
			r.add(datalog.SymDollar, s.Attr)
			r.addPullPattern(s.Pattern)
		case *PullLimitExpr:
			r.add(datalog.SymDollar, s.Attr)
		case *PullDefaultExpr:
			r.add(datalog.SymDollar, s.Attr)
		default:
			panic(fmt.Sprintf("BUG: unknown pull spec type %T in Rosters", spec))
		}
	}
}
