package query

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// DefaultSourceAttributes returns every attribute the query names on the
// default source under the rule set it runs with, in order of first
// appearance and each once: the attributes of each pull pattern in :find,
// then, in clause order, the keyword constant in the attribute position of a
// data pattern whose Source is nil or $, the attributes of get-else, get-some
// and missing? as an expression or as a predicate, and, at the end of each
// clause scope, the keyword a variable standing in such an attribute position
// is bound to: by an expression of the scope, ground with a keyword, ground
// with a vector holding a keyword at the variable's position, identity of a
// keyword, or identity of a variable so bound, or, for a variable the scope
// shares with the enclosing one, by the enclosing scope's. A branch of an or,
// or-default or not shares every variable with the enclosing scope; a branch
// of an or-join, or-default-join or not-join shares the variables its header
// names, and every other variable of the branch is its own. The walk descends
// through NOT/NOT-JOIN bodies, OR/OR-JOIN/OR-DEFAULT branches and subquery
// bodies, a subquery body being a scope of its own. A subquery's call names,
// at the place of an inner :in symbol standing in an attribute position of
// the inner query, a keyword constant argument, and a variable argument the
// scope binds to a keyword as it binds a pattern's attribute variable. A
// pattern on a named source reads that source's attributes, not the
// database's, and a blank, or a variable nothing binds to a keyword, names
// none in the attribute position.
//
// The attributes named under no rule set are collected in one walk on the
// first call of this method or its siblings and returned on every later one;
// they are the query's own, and a caller reads them and does not change them.
// Under a rule set the result carries, after them, the keywords the query's
// invocations pass at the positions the rule set reads as attributes
// (RuleSet.invocationAttributes), in a slice of the call's own: a keyword
// constant argument, or, at the end of the invocation's scope, a variable
// argument bound to a keyword as an attribute variable is.
func (q *Query) DefaultSourceAttributes(rules *RuleSet) []datalog.Keyword {
	rosters := q.collectRosters()
	if rules == nil {
		return rosters.order
	}
	invoked := rules.invocationAttributes(rosters.invocations)
	if len(invoked) == 0 {
		return rosters.order
	}
	order := rosters.order[:len(rosters.order):len(rosters.order)]
	for _, attr := range invoked {
		if !rosters.seen[attr] {
			order = append(order, attr)
		}
	}
	return order
}

// DefaultSourceAttributeVariables returns every variable in the attribute
// position of a data pattern on the default source, in order of first
// appearance and each once, through NOT/NOT-JOIN bodies and
// OR/OR-JOIN/OR-DEFAULT branches, and every variable a subquery's call passes
// at the place of an inner :in symbol that stands in such a position of the
// inner query, at any depth of calls. A pattern on a named source reads that
// source's attributes, a constant or blank in the attribute position is no
// variable, and the inner query's other attribute variables are its own.
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

// invocationConstant is a keyword a rule invocation passes at a position: the
// predicate, the position and the keyword. The rule set decides whether the
// position reads the keyword as an attribute (RuleSet.invocationAttributes).
type invocationConstant struct {
	Name     Symbol
	Position int
	Attr     datalog.Keyword
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
	seenInvocations map[invocationConstant]bool
	invocations     []invocationConstant
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
func (r *attributeRoster) addInvocationConstant(c invocationConstant) {
	if r.seenInvocations[c] {
		return
	}
	if r.seenInvocations == nil {
		r.seenInvocations = map[invocationConstant]bool{}
	}
	r.seenInvocations[c] = true
	r.invocations = append(r.invocations, c)
}

// addQuery records what a query names.
// A pull pattern in :find reads the default source.
// When outer holds, the query's inputs may bind a variable in an attribute
// position, so its attribute variables are recorded; an inner query's are
// not, and the variables its call passes at its attribute-position :in
// symbols are the enclosing scope's, which addCall records.
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
// nested form. At the end of the walk the tree resolves: every scope's
// variables bound to a keyword, one standing in a default-source pattern's
// attribute position naming the keyword and one passed to an invocation being
// a keyword passed there. A subquery's body is a tree of its own.
func (r *attributeRoster) addClauses(clauses []Clause, outer bool) {
	root := &clauseScope{}
	r.walkScope(root, clauses, outer)
	r.resolve(&scopeFrame{scope: root})
}

// clauseScope is one scope of the walk: a query's :where, a rule's body, or a
// branch or body of a nested form. It holds what its own expressions bind,
// the variables its patterns read in the attribute position, the variables
// its invocations pass, and its nested forms, each with its branches as
// scopes. What a branch shares with the enclosing scope is the form's
// (nestedForm), read when the tree resolves.
type clauseScope struct {
	bound              keywordBindings
	attributeVariables []Symbol
	passed             []passedVariable
	forms              []*nestedForm
}

// nestedForm is a nested form of a scope with each branch or body as a scope
// of its own. A branch of an inference form, or, or-default or not, shares
// every variable with the enclosing scope and reads its bindings for all of
// them. A branch of an explicit-header form, or-join, or-default-join or
// not-join, shares the variables its header names, the provides and
// correlates ScopeOf reports, and reads the enclosing scope's bindings for
// those alone. header is the explicit-header form, and nil for an inference
// form.
type nestedForm struct {
	header   Clause
	branches []*clauseScope
}

// walkScope records what the clauses of one scope name: a pattern's attribute
// on its source, a pattern's attribute variable on the default source when
// outer holds, a database function's attributes on the default source, the
// keywords a rule invocation passes by position, and what the scope's
// expressions bind. A variable an invocation passes is the scope's to
// resolve. Each nested form's branches are walked as scopes of their own, and
// the visitor descends into a subquery's body itself, which WalkClauses
// leaves to its visitors.
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
						r.addInvocationConstant(invocationConstant{Name: c.Name, Position: i, Attr: attr})
					}
				case Variable:
					s.passed = append(s.passed, passedVariable{name: c.Name, position: i, variable: a.Name})
				}
			}
		case *SubqueryPattern:
			r.addQuery(c.Query, false)
			r.addCall(s, c, outer)
		case *NotClause:
			r.walkForm(s, nil, [][]Clause{c.Clauses}, outer)
			return false
		case *NotJoinClause:
			r.walkForm(s, c, [][]Clause{c.Clauses}, outer)
			return false
		case *OrClause:
			r.walkForm(s, nil, c.Branches, outer)
			return false
		case *OrJoinClause:
			r.walkForm(s, c, c.Branches, outer)
			return false
		case *OrDefaultClause:
			r.walkForm(s, nil, c.Branches, outer)
			return false
		case *OrDefaultJoinClause:
			r.walkForm(s, c, c.Branches, outer)
			return false
		}
		return true
	})
}

// walkForm walks each branch or body of a nested form as a scope of its own
// under s. header is the explicit-header form whose header the branches
// share, and nil for an inference form.
func (r *attributeRoster) walkForm(s *clauseScope, header Clause, branches [][]Clause, outer bool) {
	form := &nestedForm{header: header}
	s.forms = append(s.forms, form)
	for _, branch := range branches {
		child := &clauseScope{}
		form.branches = append(form.branches, child)
		r.walkScope(child, branch, outer)
	}
}

// addCall records what a subquery's call names through its inner :in. An
// inner :in symbol standing in an attribute position of the inner query, at
// any depth of calls, reads the argument at its place: a keyword constant is
// an attribute the query names, and a variable is an attribute variable of
// the enclosing scope, recorded when outer holds, which the scope resolves at
// its end as it resolves a pattern's.
func (r *attributeRoster) addCall(s *clauseScope, c *SubqueryPattern, outer bool) {
	inner := c.Query.DefaultSourceAttributeVariables()
	if len(inner) == 0 {
		return
	}
	for position, sym := range InputSymbols(c.Query.In) {
		if sym == nil || !ContainsSymbol(inner, sym) {
			continue
		}
		switch a := c.Inputs[position].(type) {
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
	}
}

// scopeFrame is one scope in a resolution: the scope, the frame of the
// enclosing scope, and what the scope shares with it: every variable for a
// branch of an inference form, the header's variables for a branch of an
// explicit-header form. A resolution reads the tree and writes nothing into
// it.
type scopeFrame struct {
	scope     *clauseScope
	parent    *scopeFrame
	sharesAll bool
	shared    map[Symbol]bool
}

// resolve records what the variables of f's scope, and then of each scope
// under it, resolve to: a keyword an attribute variable is bound to names an
// attribute on the default source, and a keyword a passed variable is bound
// to is a keyword passed at its position.
func (r *attributeRoster) resolve(f *scopeFrame) {
	for _, v := range f.scope.attributeVariables {
		for _, attr := range f.keywords(v) {
			r.add(attr)
		}
	}
	for _, p := range f.scope.passed {
		for _, attr := range f.keywords(p.variable) {
			r.addInvocationConstant(invocationConstant{Name: p.name, Position: p.position, Attr: attr})
		}
	}
	for _, form := range f.scope.forms {
		var shared map[Symbol]bool
		if form.header != nil {
			shared = interfaceVariables(form.header)
		}
		for _, branch := range form.branches {
			r.resolve(&scopeFrame{scope: branch, parent: f, sharesAll: form.header == nil, shared: shared})
		}
	}
}

// interfaceVariables returns the variables an explicit-header form's header
// names: the provides and correlates ScopeOf reports, which on a header form
// are the header alone.
func interfaceVariables(form Clause) map[Symbol]bool {
	scope := ScopeOf(form, nil)
	shared := make(map[Symbol]bool, len(scope.Provides)+len(scope.Correlates))
	for _, v := range scope.Provides {
		shared[v] = true
	}
	for _, v := range scope.Correlates {
		shared[v] = true
	}
	return shared
}

// keywords returns the keywords sym is bound to in the frame's scope: by the
// scope's own expressions, following identity aliases, or, for a variable
// the scope shares with the enclosing one, by the enclosing frame's, and
// none for a variable nothing binds to a keyword. A chain is followed at
// most one step per alias the scope records, so a cycle of identities, which
// the text admits, resolves to nothing.
func (f *scopeFrame) keywords(sym Symbol) []datalog.Keyword {
	bound := &f.scope.bound
	for steps := 0; steps <= len(bound.aliases); steps++ {
		if attrs, ok := bound.keywords[sym]; ok {
			return attrs
		}
		if next, ok := bound.aliases[sym]; ok {
			sym = next
			continue
		}
		if f.parent != nil && (f.sharesAll || f.shared[sym]) {
			return f.parent.keywords(sym)
		}
		return nil
	}
	return nil
}

// passedVariable is a variable a rule invocation passes at a position.
type passedVariable struct {
	name     Symbol
	position int
	variable Symbol
}

// keywordBindings is what one clause scope binds through its expressions: a
// variable bound to keywords by ground with a keyword or identity of a
// keyword, one per inner vector of a relation ground, and a variable bound
// to another variable by identity of it. The bindings are read at the end of
// the walk, so the order of the clauses does not decide what resolves. The
// maps are allocated when the first binding is recorded.
type keywordBindings struct {
	keywords map[Symbol][]datalog.Keyword
	aliases  map[Symbol]Symbol
}

// bind records what e binds its variables to, when e is ground or identity:
// a symbol to the ground's value, or to identity's constant, or as an alias
// of identity's variable; a tuple's variables each to the element of the
// ground's vector at its position; a relation's variables each to the
// element at its position of every inner vector. The parser builds both
// functions as pointers and a query literal builds them as values, so both
// forms are read.
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
		values, ok := VectorElements(value)
		if !ok {
			return
		}
		for i, sym := range binding.Variables {
			if i < len(values) {
				b.bindKeyword(sym, values[i])
			}
		}
	case CollectionBinding:
		values, ok := VectorElements(value)
		if !ok {
			return
		}
		for _, element := range values {
			b.bindKeyword(binding.Variable, element)
		}
	case RelationBinding:
		vectors, ok := VectorElements(value)
		if !ok {
			return
		}
		for _, vector := range vectors {
			values, ok := VectorElements(vector)
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
}

// bindKeyword records sym as bound to value when value is a keyword; a value
// of any other type binds no attribute.
func (b *keywordBindings) bindKeyword(sym Symbol, value interface{}) {
	attr, ok := value.(datalog.Keyword)
	if !ok {
		return
	}
	if b.keywords == nil {
		b.keywords = map[Symbol][]datalog.Keyword{}
	}
	b.keywords[sym] = append(b.keywords[sym], attr)
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
