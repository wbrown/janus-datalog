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
// of an or-join, or-default-join or not-join shares the variables the form's
// interface names under the rule set, the provides and correlates ScopeOf
// reports. The walk descends through NOT/NOT-JOIN bodies, OR/OR-JOIN/OR-DEFAULT
// branches and subquery bodies, a subquery body being a scope of its own. A
// pattern on a named source reads that source's attributes, not the
// database's, and a blank, or a variable nothing binds to a keyword, names
// none in the attribute position.
//
// The attributes named under no rule set are collected in one walk on the
// first call of this method or its siblings and returned on every later one;
// they are the query's own, and a caller reads them and does not change them.
// Under a rule set the result carries, after them, what the query names under
// that rule set, in a slice of the call's own. When the rule set requires a
// position at which an invocation inside an explicit-header form's branches
// passes a variable no other clause of the branch provides, the form's
// interface widens to that variable, and the attributes a resolution under
// the rule set so names come first. Then come the keywords the query's
// invocations pass at the positions the rule set reads as attributes
// (RuleSet.invocationAttributes): a keyword constant argument, or, at the end
// of the invocation's scope, a variable argument bound to a keyword as an
// attribute variable is.
func (q *Query) DefaultSourceAttributes(rules *RuleSet) []datalog.Keyword {
	rosters := q.collectRosters()
	if rules == nil {
		return rosters.order
	}
	added := rosters.under(rules)
	constants := rosters.invocations
	if added != nil {
		constants = append(constants[:len(constants):len(constants)], added.invocations...)
	}
	invoked := rules.invocationAttributes(constants)
	if added == nil && len(invoked) == 0 {
		return rosters.order
	}
	order := rosters.order[:len(rosters.order):len(rosters.order)]
	if added != nil {
		order = append(order, added.order...)
	}
	for _, attr := range invoked {
		if rosters.seen[attr] || (added != nil && added.seen[attr]) {
			continue
		}
		order = append(order, attr)
	}
	return order
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

// invocationConstant is a keyword a rule invocation passes at a position: the
// predicate, the position and the keyword. The rule set decides whether the
// position reads the keyword as an attribute (RuleSet.invocationAttributes).
type invocationConstant struct {
	Name     Symbol
	Position int
	Attr     datalog.Keyword
}

// collectRosters walks the query on the first call, under no rule set, and
// returns what it names.
func (q *Query) collectRosters() *attributeRoster {
	q.rostersOnce.Do(func() {
		roster := newAttributeRoster()
		roster.addQuery(q, true, nil)
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
// are allocated when the first of their kind is recorded. The scope tree of
// each clause list walked, the query's and each subquery body's, is kept in
// trees for a resolution under a rule set (under), and the positions
// invocations under an explicit-header form pass a variable at are kept in
// headerPassed to gate that resolution (widenedBy).
type attributeRoster struct {
	seen            map[datalog.Keyword]bool
	order           []datalog.Keyword
	seenVars        map[Symbol]bool
	variables       []Symbol
	sources         map[Symbol]*sourceRoster
	seenInvocations map[invocationConstant]bool
	invocations     []invocationConstant
	trees           []*clauseScope
	headerPassed    []passedVariable
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

// addQuery records what a query names under rules.
// A pull pattern in :find reads the default source.
// When outer holds, the query's inputs may bind a variable in an attribute
// position, so its attribute variables are recorded; a subquery's inputs are
// its own to check when it runs, so its are not.
func (r *attributeRoster) addQuery(q *Query, outer bool, rules *RuleSet) {
	for _, elem := range q.Find {
		if pull, ok := elem.(FindPull); ok {
			r.addPullPattern(pull.Pattern)
		}
	}
	r.addClauses(q.Where, outer, rules)
}

// addClauses records what a query's :where or a rule's body names, as a tree
// of scopes: the clause list itself, and under it each branch or body of a
// nested form. At the end of the walk the tree resolves under rules: every
// scope's variables bound to a keyword, one standing in a default-source
// pattern's attribute position naming the keyword and one passed to an
// invocation being a keyword passed there. The tree is kept for a resolution
// under another rule set (under). A subquery's body is a tree of its own.
func (r *attributeRoster) addClauses(clauses []Clause, outer bool, rules *RuleSet) {
	root := &clauseScope{}
	r.walkScope(root, clauses, outer, false, rules)
	r.resolve(&scopeFrame{scope: root}, rules)
	r.trees = append(r.trees, root)
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
// not-join, shares the variables the form's interface names under the rule
// set the resolution runs with, the provides and correlates ScopeOf reports,
// and reads the enclosing scope's bindings for those alone. header is the
// explicit-header form, and nil for an inference form.
type nestedForm struct {
	header   Clause
	branches []*clauseScope
}

// walkScope records what the clauses of one scope name: a pattern's attribute
// on its source, a pattern's attribute variable on the default source when
// outer holds, a database function's attributes on the default source, the
// keywords a rule invocation passes by position, and what the scope's
// expressions bind. A variable an invocation passes is the scope's to
// resolve, and under an explicit-header form it is recorded for the gate on
// a resolution under a rule set too, since the form's interface widens only
// at a position the rule set requires. Each nested form's branches are walked as
// scopes of their own, and the visitor descends into a subquery's body
// itself, which WalkClauses leaves to its visitors.
func (r *attributeRoster) walkScope(s *clauseScope, clauses []Clause, outer, underHeader bool, rules *RuleSet) {
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
					passed := passedVariable{name: c.Name, position: i, variable: a.Name}
					s.passed = append(s.passed, passed)
					if underHeader {
						r.headerPassed = append(r.headerPassed, passed)
					}
				}
			}
		case *SubqueryPattern:
			r.addQuery(c.Query, false, rules)
		case *NotClause:
			r.walkForm(s, nil, [][]Clause{c.Clauses}, outer, underHeader, rules)
			return false
		case *NotJoinClause:
			r.walkForm(s, c, [][]Clause{c.Clauses}, outer, underHeader, rules)
			return false
		case *OrClause:
			r.walkForm(s, nil, c.Branches, outer, underHeader, rules)
			return false
		case *OrJoinClause:
			r.walkForm(s, c, c.Branches, outer, underHeader, rules)
			return false
		case *OrDefaultClause:
			r.walkForm(s, nil, c.Branches, outer, underHeader, rules)
			return false
		case *OrDefaultJoinClause:
			r.walkForm(s, c, c.Branches, outer, underHeader, rules)
			return false
		}
		return true
	})
}

// walkForm walks each branch or body of a nested form as a scope of its own
// under s. header is the explicit-header form whose interface the branches
// share, and nil for an inference form.
func (r *attributeRoster) walkForm(s *clauseScope, header Clause, branches [][]Clause, outer, underHeader bool, rules *RuleSet) {
	form := &nestedForm{header: header}
	s.forms = append(s.forms, form)
	for _, branch := range branches {
		child := &clauseScope{}
		form.branches = append(form.branches, child)
		r.walkScope(child, branch, outer, underHeader || header != nil, rules)
	}
}

// scopeFrame is one scope in a resolution: the scope, the frame of the
// enclosing scope, and what the scope shares with it under the rule set the
// resolution runs with: every variable for a branch of an inference form,
// the form's interface for a branch of an explicit-header form. A resolution
// reads the tree and writes nothing into it, so resolutions under different
// rule sets run on one tree together.
type scopeFrame struct {
	scope     *clauseScope
	parent    *scopeFrame
	sharesAll bool
	shared    map[Symbol]bool
}

// resolve records what the variables of f's scope, and then of each scope
// under it, resolve to under rules: a keyword an attribute variable is bound
// to names an attribute on the default source, and a keyword a passed
// variable is bound to is a keyword passed at its position.
func (r *attributeRoster) resolve(f *scopeFrame, rules *RuleSet) {
	for _, v := range f.scope.attributeVariables {
		if attr, ok := f.keyword(v); ok {
			r.add(attr)
		}
	}
	for _, p := range f.scope.passed {
		if attr, ok := f.keyword(p.variable); ok {
			r.addInvocationConstant(invocationConstant{Name: p.name, Position: p.position, Attr: attr})
		}
	}
	for _, form := range f.scope.forms {
		var shared map[Symbol]bool
		if form.header != nil {
			shared = interfaceVariables(form.header, rules)
		}
		for _, branch := range form.branches {
			r.resolve(&scopeFrame{scope: branch, parent: f, sharesAll: form.header == nil, shared: shared}, rules)
		}
	}
}

// interfaceVariables returns the variables an explicit-header form's
// interface names under rules: the provides and correlates ScopeOf reports.
func interfaceVariables(form Clause, rules *RuleSet) map[Symbol]bool {
	scope := ScopeOf(form, rules)
	shared := make(map[Symbol]bool, len(scope.Provides)+len(scope.Correlates))
	for _, v := range scope.Provides {
		shared[v] = true
	}
	for _, v := range scope.Correlates {
		shared[v] = true
	}
	return shared
}

// keyword returns the keyword sym is bound to in the frame's scope: by the
// scope's own expressions, following identity aliases, or, for a variable
// the scope shares with the enclosing one, by the enclosing frame's. A chain
// is followed at most one step per alias the scope records, so a cycle of
// identities, which the text admits, resolves to nothing.
func (f *scopeFrame) keyword(sym Symbol) (datalog.Keyword, bool) {
	bound := &f.scope.bound
	for steps := 0; steps <= len(bound.aliases); steps++ {
		if attr, ok := bound.keywords[sym]; ok {
			return attr, true
		}
		if next, ok := bound.aliases[sym]; ok {
			sym = next
			continue
		}
		if f.parent != nil && (f.sharesAll || f.shared[sym]) {
			return f.parent.keyword(sym)
		}
		return nil, false
	}
	return nil, false
}

// under returns what the trees name under rules beyond what they named under
// none: nil when rules is nil or widens no form's interface, otherwise a
// roster of the attributes and invocation constants a resolution under rules
// adds, in the order that resolution meets them.
func (r *attributeRoster) under(rules *RuleSet) *attributeRoster {
	if rules == nil || !r.widenedBy(rules) {
		return nil
	}
	again := newAttributeRoster()
	for _, tree := range r.trees {
		again.resolve(&scopeFrame{scope: tree}, rules)
	}
	added := again.order[:0]
	for _, attr := range again.order {
		if !r.seen[attr] {
			added = append(added, attr)
		}
	}
	again.order = added
	constants := again.invocations[:0]
	for _, c := range again.invocations {
		if !r.seenInvocations[c] {
			constants = append(constants, c)
		}
	}
	again.invocations = constants
	return again
}

// widenedBy reports whether rules requires a position an invocation under an
// explicit-header form passes a variable at. Read with a rule set, ScopeOf
// differs from its reading with none at such a position alone, so no other
// rule set names a form's interface differently.
func (r *attributeRoster) widenedBy(rules *RuleSet) bool {
	for _, p := range r.headerPassed {
		if required := rules.Required(p.name); p.position < len(required) && required[p.position] {
			return true
		}
	}
	return false
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
