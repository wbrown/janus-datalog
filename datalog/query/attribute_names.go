package query

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// DefaultSourceAttributes returns every attribute the query names on the
// default source, in order of first appearance and each once: the attributes
// of each pull pattern in :find, then, in clause order, the keyword constant
// in the attribute position of a data pattern whose Source is nil or $, the
// attributes of get-else, get-some and missing? as an expression or as a
// predicate, and, at the end of each clause scope, the keyword an expression
// of the scope binds to a variable standing in such an attribute position:
// ground with a keyword, identity of a keyword, or identity of a variable so
// bound. The walk descends through NOT/NOT-JOIN bodies, OR/OR-JOIN/OR-DEFAULT
// branches and subquery bodies, a subquery body being a scope of its own. A
// pattern on a named source reads that source's attributes, not the
// database's, and a blank, or a variable nothing in the scope binds to a
// keyword, names none in the attribute position.
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

// InvocationConstant is a keyword a rule invocation passes at a position: the
// predicate, the position and the keyword. The rule set decides whether the
// position reads the keyword as an attribute (RuleSet.InvocationAttributes).
type InvocationConstant struct {
	Name     Symbol
	Position int
	Attr     datalog.Keyword
}

// InvocationConstants returns every keyword a rule invocation in the query
// passes, at any depth and in subquery bodies, with its predicate and
// position, in order of first appearance and each once: a keyword constant
// argument, or, at the end of the invocation's scope, a variable argument the
// scope binds to a keyword through ground or identity. A constant of another
// type and a variable nothing binds name nothing.
func (q *Query) InvocationConstants() []InvocationConstant {
	return q.collectRosters().invocations
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
// variables in the attribute position of a default-source pattern, the
// attributes on each named source, and the keywords rule invocations pass by
// position. Keywords and symbols are interned, so each set is keyed on the
// value itself. The variable set, the named-source map and the invocation set
// are allocated when the first of their kind is recorded.
type attributeRoster struct {
	seen            map[datalog.Keyword]bool
	order           []datalog.Keyword
	seenVars        map[Symbol]bool
	variables       []Symbol
	sources         map[Symbol]*sourceRoster
	seenInvocations map[InvocationConstant]bool
	invocations     []InvocationConstant
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

// addInvocationConstant records a keyword a rule invocation passes at a
// position.
func (r *attributeRoster) addInvocationConstant(c InvocationConstant) {
	if r.seenInvocations[c] {
		return
	}
	if r.seenInvocations == nil {
		r.seenInvocations = map[InvocationConstant]bool{}
	}
	r.seenInvocations[c] = true
	r.invocations = append(r.invocations, c)
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

// addClauses records what a query's :where or a rule's body names, as a tree
// of scopes: the clause list itself, and under it each branch or body of a
// nested form, which shares with its enclosing scope the variables the form's
// interface names and no other. At the end of the walk every scope resolves
// its variables bound to a keyword: one standing in a default-source
// pattern's attribute position names the keyword, and one passed to an
// invocation is a keyword passed there. A subquery's body is a tree of its
// own.
func (r *attributeRoster) addClauses(clauses []Clause, outer bool) {
	root := &clauseScope{}
	r.walkScope(root, clauses, outer)
	r.resolve(root)
}

// clauseScope is one scope of the walk: a query's :where, a rule's body, or a
// branch or body of a nested form. A nested scope reads its parent's bindings
// for the variables the form's interface shares with the enclosing scope, the
// provides and correlates ScopeOf reports, and for no other.
type clauseScope struct {
	parent             *clauseScope
	shared             map[Symbol]bool
	bound              keywordBindings
	attributeVariables []Symbol
	passed             []passedVariable
	children           []*clauseScope
}

// walkScope records what the clauses of one scope name: a pattern's attribute
// on its source, a pattern's attribute variable on the default source when
// outer holds, a database function's attributes on the default source, the
// keywords a rule invocation passes by position, and what the scope's
// expressions bind. Each nested form's branches are walked as scopes of
// their own, and the visitor descends into a subquery's body itself, which
// WalkClauses leaves to its visitors.
func (r *attributeRoster) walkScope(s *clauseScope, clauses []Clause, outer bool) {
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
				s.attributeVariables = append(s.attributeVariables, a.Name)
			}
		case *Expression:
			if f, ok := c.Function.(DatabaseFunction); ok {
				r.addDatabaseFunction(f)
			}
			s.bound.bind(c)
		case *DatabaseFunctionPredicate:
			r.addDatabaseFunction(c.Function)
		case *RuleInvocation:
			for i, arg := range c.Args {
				switch a := arg.(type) {
				case Constant:
					if attr, ok := a.Value.(datalog.Keyword); ok {
						r.addInvocationConstant(InvocationConstant{Name: c.Name, Position: i, Attr: attr})
					}
				case Variable:
					s.passed = append(s.passed, passedVariable{name: c.Name, position: i, variable: a.Name})
				}
			}
		case *SubqueryPattern:
			r.addQuery(c.Query, false)
		case *NotClause:
			r.walkBranches(s, c, [][]Clause{c.Clauses}, outer)
			return false
		case *NotJoinClause:
			r.walkBranches(s, c, [][]Clause{c.Clauses}, outer)
			return false
		case *OrClause:
			r.walkBranches(s, c, c.Branches, outer)
			return false
		case *OrJoinClause:
			r.walkBranches(s, c, c.Branches, outer)
			return false
		case *OrDefaultClause:
			r.walkBranches(s, c, c.Branches, outer)
			return false
		case *OrDefaultJoinClause:
			r.walkBranches(s, c, c.Branches, outer)
			return false
		}
		return true
	})
}

// walkBranches walks each branch of form as a scope under s, sharing with s
// the variables form's interface names: the provides and correlates ScopeOf
// reports for the form, read with no rule set, under which an invocation
// provides every variable argument.
func (r *attributeRoster) walkBranches(s *clauseScope, form Clause, branches [][]Clause, outer bool) {
	scope := ScopeOf(form, nil)
	shared := make(map[Symbol]bool, len(scope.Provides)+len(scope.Correlates))
	for _, v := range scope.Provides {
		shared[v] = true
	}
	for _, v := range scope.Correlates {
		shared[v] = true
	}
	for _, branch := range branches {
		child := &clauseScope{parent: s, shared: shared}
		s.children = append(s.children, child)
		r.walkScope(child, branch, outer)
	}
}

// resolve records what the variables of s, and then of each scope under it,
// resolve to: a keyword an attribute variable is bound to names an attribute
// on the default source, and a keyword a passed variable is bound to is a
// keyword passed at its position.
func (r *attributeRoster) resolve(s *clauseScope) {
	for _, v := range s.attributeVariables {
		if attr, ok := s.keyword(v); ok {
			r.add(attr)
		}
	}
	for _, p := range s.passed {
		if attr, ok := s.keyword(p.variable); ok {
			r.addInvocationConstant(InvocationConstant{Name: p.name, Position: p.position, Attr: attr})
		}
	}
	for _, child := range s.children {
		r.resolve(child)
	}
}

// keyword returns the keyword sym is bound to in s: by s's own expressions,
// following identity aliases, or, for a variable the form shares with the
// enclosing scope, by the enclosing scope's. A chain is followed at most one
// step per alias the scope records, so a cycle of identities, which the text
// admits, resolves to nothing.
func (s *clauseScope) keyword(sym Symbol) (datalog.Keyword, bool) {
	for steps := 0; steps <= len(s.bound.aliases); steps++ {
		if attr, ok := s.bound.keywords[sym]; ok {
			return attr, true
		}
		if next, ok := s.bound.aliases[sym]; ok {
			sym = next
			continue
		}
		if s.parent != nil && s.shared[sym] {
			return s.parent.keyword(sym)
		}
		return nil, false
	}
	return nil, false
}

// passedVariable is a variable a rule invocation passes at a position.
type passedVariable struct {
	name     Symbol
	position int
	variable Symbol
}

// keywordBindings is what one clause scope binds through its expressions: a
// variable bound to a keyword by ground with a keyword or identity of a
// keyword, and a variable bound to another variable by identity of it. The
// bindings are read at the end of the walk, so the order of the clauses does
// not decide what resolves. The maps are allocated when the first binding is
// recorded.
type keywordBindings struct {
	keywords map[Symbol]datalog.Keyword
	aliases  map[Symbol]Symbol
}

// bind records what e binds its variables to, when e is ground or identity:
// a symbol to the ground's value, or to identity's constant, or as an alias
// of identity's variable; a tuple's variables each to the element of the
// ground's vector at its position. The parser builds both functions as
// pointers and a query literal builds them as values, so both forms are read.
func (b *keywordBindings) bind(e *Expression) {
	var value interface{}
	var identity *IdentityFunction
	switch f := e.Function.(type) {
	case GroundFunction:
		value = f.Value
	case *GroundFunction:
		value = f.Value
	case IdentityFunction:
		identity = &f
	case *IdentityFunction:
		identity = f
	default:
		return
	}
	switch binding := e.Binding.(type) {
	case Symbol:
		if identity != nil {
			switch arg := identity.Arg.(type) {
			case ConstantTerm:
				value = arg.Value
			case VariableTerm:
				if b.aliases == nil {
					b.aliases = map[Symbol]Symbol{}
				}
				b.aliases[binding] = arg.Symbol
				return
			}
		}
		b.bindKeyword(binding, value)
	case TupleBinding:
		values, ok := value.([]interface{})
		if !ok {
			return
		}
		for i, sym := range binding.Variables {
			if i < len(values) {
				b.bindKeyword(sym, values[i])
			}
		}
	}
}

// bindKeyword records sym as bound to value when value is a keyword; a value
// of any other type binds no attribute.
func (b *keywordBindings) bindKeyword(sym Symbol, value interface{}) {
	attr, ok := value.(datalog.Keyword)
	if !ok {
		return
	}
	if b.keywords == nil {
		b.keywords = map[Symbol]datalog.Keyword{}
	}
	b.keywords[sym] = attr
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
