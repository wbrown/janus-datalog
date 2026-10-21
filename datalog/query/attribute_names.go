package query

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// Rosters is what a prepared query names, collected once by CollectRosters
// and held with the query's plan, which the vocabulary check reads: the
// attributes named on the default source (DefaultSourceAttributes), the
// variables of the query's own scope that reach an attribute position there
// (AttributeVariables), the attributes named on each named source
// (SourceAttributes), and the keywords and variables the query's invocations
// pass by position, which the rule set the query runs with reads at the
// check. Keywords and symbols are interned, so each set is keyed on the value
// itself. The named-source map and the invocation set are allocated when the
// first of their kind is recorded.
type Rosters struct {
	seen            map[datalog.Keyword]bool
	order           []datalog.Keyword
	variables       []Symbol
	passed          []passedVariable
	sources         map[Symbol]*sourceRoster
	seenInvocations map[invocationConstant]bool
	invocations     []invocationConstant
}

// CollectRosters walks a prepared query once and returns what it names. The
// query holds header forms alone, since Prepare writes every header before
// planning; a headerless form reaching the walk is a skipped preparation and
// panics, as ScopeOf panics on it.
func CollectRosters(q *Query) *Rosters {
	r := newRosters()
	root := r.addQuery(q)
	r.variables, r.passed = root.variables, root.passed
	return r
}

// DefaultSourceAttributes returns every attribute the query names on the
// default source under the rule set it runs with, in order of first
// appearance and each once: the attributes of each pull pattern in :find,
// then, in clause order, the keyword constant in the attribute position of a
// data pattern whose Source is nil or $, the attributes of get-else, get-some
// and missing? as an expression or as a predicate, and, at the end of each
// clause scope, the keyword a variable standing in such an attribute position
// is bound to: by an expression of the scope, ground with a keyword, ground
// with a vector holding a keyword at the variable's position, identity of a
// keyword, or identity of a variable so bound, or, for a variable the form's
// header shares with the enclosing scope, by the enclosing scope's. A branch
// of an or-join, or-default-join or not-join shares the variables its header
// names, and every other variable of the branch is its own. The walk descends
// through the header forms' bodies and branches and through subquery bodies,
// a subquery body being a scope of its own. A subquery's call names, at the
// place of an inner :in symbol standing in an attribute position of the inner
// query, a keyword constant argument, and a variable argument the scope binds
// to a keyword as it binds a pattern's attribute variable. A pattern on a
// named source reads that source's attributes, not the database's, and a
// blank, or a variable nothing binds to a keyword, names none in the
// attribute position.
//
// The attributes named under no rule set are the query's own, and a caller
// reads them and does not change them. Under a rule set the result carries,
// after them, the keywords the query's invocations pass at the positions the
// rule set reads as attributes (RuleSet.invocationAttributes), in a slice of
// the call's own: a keyword constant argument, or, at the end of the
// invocation's scope, a variable argument bound to a keyword as an attribute
// variable is.
func (r *Rosters) DefaultSourceAttributes(rules *RuleSet) []datalog.Keyword {
	if rules == nil {
		return r.order
	}
	invoked := rules.invocationAttributes(r.invocations)
	if len(invoked) == 0 {
		return r.order
	}
	order := r.order[:len(r.order):len(r.order)]
	for _, attr := range invoked {
		if !r.seen[attr] {
			order = append(order, attr)
		}
	}
	return order
}

// AttributeVariables returns every variable of the query's own scope that
// reaches the attribute position of a data pattern on the default source
// under the rule set the query runs with, in order of first appearance and
// each once: a variable standing in that position in the query's scope, or in
// a scope under it that each header on the way shares it with, an identity
// alias of such a variable counting as the variable it aliases; a variable a
// subquery's call passes at the place of an inner :in symbol that so reaches
// a position of the inner query, at any depth of calls; and, under a rule
// set, a variable an invocation passes at a position the rule set reads as
// an attribute (RuleSet.AttributePositions), directly or through a call. The
// variables an execution's inputs bind are among them, so the check reads
// the inputs at these alone. A pattern on a named source reads that source's
// attributes, a constant or blank in the attribute position is no variable,
// a branch variable outside the header is the branch's own, and the inner
// query's other attribute variables are its own.
func (r *Rosters) AttributeVariables(rules *RuleSet) []Symbol {
	if rules == nil || len(r.passed) == 0 {
		return r.variables
	}
	variables := r.variables[:len(r.variables):len(r.variables)]
	for _, p := range r.passed {
		mask := rules.attributePositions[p.name]
		if p.position < len(mask) && mask[p.position] && !ContainsSymbol(variables, p.variable) {
			variables = append(variables, p.variable)
		}
	}
	return variables
}

// SourceAttributes returns every attribute the query names on the named
// source, in order of first appearance and each once: the keyword constant in
// the attribute position of a data pattern whose Source is the symbol, through
// the header forms' bodies and branches and subquery bodies. A database
// function and a pull pattern read the default source, whose roster is
// DefaultSourceAttributes, and name nothing on a named source. A source no
// pattern names has no roster.
func (r *Rosters) SourceAttributes(source Symbol) []datalog.Keyword {
	if on := r.sources[source]; on != nil {
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

// PullPatternAttributes returns every attribute a pull pattern names, through
// nested map specs, in order of first appearance and each once. A wildcard
// names none.
func PullPatternAttributes(p *PullPattern) []datalog.Keyword {
	roster := newRosters()
	roster.addPullPattern(p)
	return roster.order
}

// sourceRoster accumulates the attributes named on one named source.
type sourceRoster struct {
	seen  map[datalog.Keyword]bool
	order []datalog.Keyword
}

func newRosters() *Rosters {
	return &Rosters{seen: map[datalog.Keyword]bool{}}
}

// add records an attribute named on the default source.
func (r *Rosters) add(attr datalog.Keyword) {
	if r.seen[attr] {
		return
	}
	r.seen[attr] = true
	r.order = append(r.order, attr)
}

// addOn records an attribute named on a named source.
func (r *Rosters) addOn(source Symbol, attr datalog.Keyword) {
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
func (r *Rosters) addInvocationConstant(c invocationConstant) {
	if r.seenInvocations[c] {
		return
	}
	if r.seenInvocations == nil {
		r.seenInvocations = map[invocationConstant]bool{}
	}
	r.seenInvocations[c] = true
	r.invocations = append(r.invocations, c)
}

// addQuery records what a query names and returns what its own scope reaches.
// A pull pattern in :find reads the default source.
func (r *Rosters) addQuery(q *Query) reach {
	for _, elem := range q.Find {
		if pull, ok := elem.(FindPull); ok {
			r.addPullPattern(pull.Pattern)
		}
	}
	return r.addClauses(q.Where)
}

// addClauses records what a query's :where or a rule's body names, as a tree
// of scopes: the clause list itself, and under it each branch or body of a
// nested form. At the end of the walk the tree resolves: every scope's
// variables bound to a keyword, one standing in a default-source pattern's
// attribute position naming the keyword and one passed to an invocation being
// a keyword passed there, and every scope's attribute and passed variables
// that the headers share up to the root being the root's. A subquery's body
// is a tree of its own. The root's reach is returned.
func (r *Rosters) addClauses(clauses []Clause) reach {
	root := &clauseScope{}
	r.walkScope(root, clauses)
	var reached reach
	r.resolve(&scopeFrame{scope: root}, &reached)
	return reached
}

// reach is what a scope's root reaches on the default source: the root
// variables standing in a pattern's attribute position, in the scope or in a
// scope under it through the headers and identity aliases between them, and
// the root variables passed to an invocation, each with the predicate and the
// position, directly or through a subquery call. Each is recorded once, in
// order of first appearance.
type reach struct {
	variables []Symbol
	passed    []passedVariable
}

func (c *reach) addVariable(sym Symbol) {
	if !ContainsSymbol(c.variables, sym) {
		c.variables = append(c.variables, sym)
	}
}

func (c *reach) addPassed(p passedVariable) {
	for _, recorded := range c.passed {
		if recorded == p {
			return
		}
	}
	c.passed = append(c.passed, p)
}

// clauseScope is one scope of the walk: a query's :where, a rule's body, or a
// branch or body of a nested form. It holds what its own expressions bind,
// the variables its patterns read in the attribute position, the variables
// its invocations pass, and its nested forms, each with its branches as
// scopes. What a branch shares with the enclosing scope is the form's header
// (nestedForm), read when the tree resolves.
type clauseScope struct {
	bound              keywordBindings
	attributeVariables []Symbol
	passed             []passedVariable
	forms              []*nestedForm
}

// nestedForm is a nested form of a scope with each branch or body as a scope
// of its own. A branch shares the variables the form's header names, the
// provides and correlates ScopeOf reports, and reads the enclosing scope's
// bindings for those alone; every other variable of the branch is its own.
type nestedForm struct {
	header   Clause
	branches []*clauseScope
}

// walkScope records what the clauses of one scope name: a pattern's attribute
// on its source, a pattern's attribute variable on the default source, a
// database function's attributes on the default source, the keywords a rule
// invocation passes by position, and what the scope's expressions bind. A
// variable an invocation passes is the scope's to resolve. Each header form's
// branches are walked as scopes of their own, and the visitor descends into a
// subquery's body itself, which WalkClauses leaves to its visitors. A
// headerless form is a skipped preparation and panics.
func (r *Rosters) walkScope(s *clauseScope, clauses []Clause) {
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
			inner := r.addQuery(c.Query)
			r.addCall(s, c, inner)
		case *NotJoinClause:
			r.walkForm(s, c, [][]Clause{c.Clauses})
			return false
		case *OrJoinClause:
			r.walkForm(s, c, c.Branches)
			return false
		case *OrDefaultJoinClause:
			r.walkForm(s, c, c.Branches)
			return false
		case *NotClause, *OrClause, *OrDefaultClause:
			panic(fmt.Sprintf("BUG: headerless form %T reached the roster; Prepare writes every header before a query is planned", clause))
		}
		return true
	})
}

// walkForm walks each branch or body of a header form as a scope of its own
// under s, sharing the variables the form's header names.
func (r *Rosters) walkForm(s *clauseScope, header Clause, branches [][]Clause) {
	form := &nestedForm{header: header}
	s.forms = append(s.forms, form)
	for _, branch := range branches {
		child := &clauseScope{}
		form.branches = append(form.branches, child)
		r.walkScope(child, branch)
	}
}

// addCall records what a subquery's call names through its inner :in, from
// the inner query's reach. An inner :in symbol the inner query's root reaches
// in an attribute position, at any depth of calls, reads the argument at its
// place: a keyword constant is an attribute the query names, and a variable
// is an attribute variable of the enclosing scope, which the scope resolves
// at its end as it resolves a pattern's. An inner :in symbol the inner query
// passes to an invocation is passed at that predicate and position by the
// argument at its place: a keyword constant is a keyword passed there, and a
// variable is passed there by the enclosing scope.
func (r *Rosters) addCall(s *clauseScope, c *SubqueryPattern, inner reach) {
	for position, sym := range InputSymbols(c.Query.In) {
		if sym == nil {
			continue
		}
		argument := c.Inputs[position]
		if ContainsSymbol(inner.variables, sym) {
			switch a := argument.(type) {
			case Constant:
				if attr, ok := a.Value.(datalog.Keyword); ok {
					r.add(attr)
				}
			case Variable:
				s.attributeVariables = append(s.attributeVariables, a.Name)
			}
		}
		for _, p := range inner.passed {
			if p.variable != sym {
				continue
			}
			switch a := argument.(type) {
			case Constant:
				if attr, ok := a.Value.(datalog.Keyword); ok {
					r.addInvocationConstant(invocationConstant{Name: p.name, Position: p.position, Attr: attr})
				}
			case Variable:
				s.passed = append(s.passed, passedVariable{name: p.name, position: p.position, variable: a.Name})
			}
		}
	}
}

// scopeFrame is one scope in a resolution: the scope, the frame of the
// enclosing scope, and the variables the form's header shares between them,
// nil at the root. A resolution reads the tree and writes nothing into it.
type scopeFrame struct {
	scope  *clauseScope
	parent *scopeFrame
	shared map[Symbol]bool
}

// resolve records what the variables of f's scope, and then of each scope
// under it, resolve to: a keyword an attribute variable is bound to names an
// attribute on the default source, a keyword a passed variable is bound to is
// a keyword passed at its position, and an attribute or passed variable that
// stands for a root variable through the headers and aliases on the way is
// the root's.
func (r *Rosters) resolve(f *scopeFrame, root *reach) {
	for _, v := range f.scope.attributeVariables {
		for _, attr := range f.keywords(v) {
			r.add(attr)
		}
		if sym, ok := f.rootVariable(v); ok {
			root.addVariable(sym)
		}
	}
	for _, p := range f.scope.passed {
		for _, attr := range f.keywords(p.variable) {
			r.addInvocationConstant(invocationConstant{Name: p.name, Position: p.position, Attr: attr})
		}
		if sym, ok := f.rootVariable(p.variable); ok {
			root.addPassed(passedVariable{name: p.name, position: p.position, variable: sym})
		}
	}
	for _, form := range f.scope.forms {
		shared := interfaceVariables(form.header)
		for _, branch := range form.branches {
			r.resolve(&scopeFrame{scope: branch, parent: f, shared: shared}, root)
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
// the header shares with the enclosing scope, by the enclosing frame's, and
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
		if f.parent != nil && f.shared[sym] {
			return f.parent.keywords(sym)
		}
		return nil
	}
	return nil
}

// rootVariable returns the root scope's variable sym stands for: at the root,
// sym itself after the identity aliases the scope records; under a header,
// the enclosing frame's answer for the variable the aliases reach when the
// header shares it; and none for a variable the header does not share or an
// alias cycle, which resolves to nothing as in keywords.
func (f *scopeFrame) rootVariable(sym Symbol) (Symbol, bool) {
	aliases := f.scope.bound.aliases
	for steps := 0; steps <= len(aliases); steps++ {
		if next, ok := aliases[sym]; ok {
			sym = next
			continue
		}
		if f.parent == nil {
			return sym, true
		}
		if f.shared[sym] {
			return f.parent.rootVariable(sym)
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
func (r *Rosters) addDatabaseFunction(f DatabaseFunction) {
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
		panic(fmt.Sprintf("BUG: unknown database function type %T in Rosters", f))
	}
}

// addPullPattern records every attribute a pull pattern names, recursing
// through map specs. The spec taxonomy is closed (unexported marker method)
// and enumerated completely here.
func (r *Rosters) addPullPattern(p *PullPattern) {
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
			panic(fmt.Sprintf("BUG: unknown pull spec type %T in Rosters", spec))
		}
	}
}
