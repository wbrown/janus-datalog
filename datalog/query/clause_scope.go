package query

import "fmt"

// ClauseScope is a clause's interface to its enclosing scope. Provides are
// the variables the clause can bind there; Correlates are its free variables
// that unify with the enclosing scope when bound there. Variables scoped
// inside a clause — explicit-join bodies, subquery inner queries, or/
// or-default branch locals — appear in neither. This is the canonical
// scoping definition; the planner's scheduling, the executor's anti-join
// keys, and the algebra bridge's inference all derive from it.
//
// Mandatory is the subset of Correlates the enclosing scope must bind
// before the clause runs: a predicate's, an expression's or a subquery's
// inputs, an explicit-join header, an invocation's required positions, and
// the such inputs of a plain NOT's body or an or's or an or-default's
// branches that nothing in that body or branch binds. One the query cannot bind is a
// query error the planner must surface. The other correlates tolerate
// absence: a plain NOT's body variables are existential when unbound
// (Datomic's unification rule), and an or-default's fall back to global
// evaluation. Whether one of those actually unifies is a property of the
// enclosing query, and resolving that is the scheduler's job — ScopeOf
// reports the interface only.
type ClauseScope struct {
	Provides   []Symbol
	Correlates []Symbol
	Mandatory  []Symbol
}

// ScopeOf returns the scope interface of a clause under the rule set the
// query runs with. A rule invocation's interface is declared by its
// predicate: the variables at the positions the predicate requires bound are
// mandatory correlates, and the rest are provided. Read with no rule set, an
// invocation provides every variable argument. The clause taxonomy is
// closed; a form without a case here panics rather than silently exposing
// nothing.
func ScopeOf(clause Clause, rules *RuleSet) ClauseScope {
	switch c := clause.(type) {
	case *DataPattern:
		return ClauseScope{Provides: c.Symbols()}

	case *Expression:
		var provides []Symbol
		switch b := c.Binding.(type) {
		case Symbol:
			if b != nil {
				provides = []Symbol{b}
			}
		case TupleBinding:
			provides = b.Variables
		}
		inputs := c.Function.RequiredSymbols()
		return ClauseScope{Provides: provides, Correlates: inputs, Mandatory: inputs}

	case *SubqueryPattern:
		var inputs []Symbol
		for _, input := range c.Inputs {
			if v, ok := input.(Variable); ok && !v.Name.IsSource() {
				inputs = append(inputs, v.Name)
			}
		}
		return ClauseScope{Provides: c.Binding.BoundVariables(), Correlates: inputs, Mandatory: inputs}

	case *RuleInvocation:
		var required []bool
		if rules != nil {
			required = rules.Required(c.Name)
		}
		var provides, correlates []Symbol
		for i, arg := range c.Args {
			v, ok := arg.(Variable)
			if !ok {
				continue
			}
			if i < len(required) && required[i] {
				if !ContainsSymbol(correlates, v.Name) {
					correlates = append(correlates, v.Name)
				}
			} else if !ContainsSymbol(provides, v.Name) {
				provides = append(provides, v.Name)
			}
		}
		return ClauseScope{Provides: provides, Correlates: correlates, Mandatory: correlates}

	case *NotJoinClause:
		// The header is the complete interface by language contract.
		return ClauseScope{Correlates: c.RequiredVars, Mandatory: c.RequiredVars}

	case *OrJoinClause:
		// The header is the complete interface: the required run correlates,
		// mandatory, and the outputs are provided. Every other variable of a
		// branch is the branch's own.
		return ClauseScope{Provides: c.OutputVars, Correlates: c.RequiredVars, Mandatory: c.RequiredVars}

	case *OrDefaultJoinClause:
		// The declared header is the complete interface (Validate enforces
		// it at the boundaries): required vars correlate — mandatory, they
		// quantify the per-group fallback decision — and output vars are
		// bound by every branch. Branch locals never escape.
		return ClauseScope{Provides: c.OutputVars, Correlates: c.RequiredVars, Mandatory: c.RequiredVars}

	case Predicate:
		// Every predicate form: consumes, never binds.
		inputs := c.RequiredSymbols()
		return ClauseScope{Correlates: inputs, Mandatory: inputs}

	default:
		panic(fmt.Sprintf("BUG: unknown clause type %T in ScopeOf", clause))
	}
}

// FreeVariables returns the variables a clause list exposes to its enclosing
// scope — every Provides and Correlates across the clauses, deduplicated in
// first-appearance order. Variables scoped inside the clauses do not appear.
func FreeVariables(clauses []Clause, rules *RuleSet) []Symbol {
	var free []Symbol
	for _, clause := range clauses {
		scope := ScopeOf(clause, rules)
		for _, sym := range scope.Provides {
			if !ContainsSymbol(free, sym) {
				free = append(free, sym)
			}
		}
		for _, sym := range scope.Correlates {
			if !ContainsSymbol(free, sym) {
				free = append(free, sym)
			}
		}
	}
	return free
}

// branchInterface computes one branch's interface: the symbols its clauses
// provide, and the symbols they correlate on split by obligation. Mandatory
// correlates must be supplied by the enclosing scope before the clause can
// run — predicates, expressions, subquery inputs, explicit-join headers,
// and the such inputs of a nested body nothing in that body binds. Optional
// correlates tolerate absence (a plain NOT's body variables are existential
// when unbound; or-default falls back to global evaluation), so they never
// create declaration requirements — flattening them into the mandatory set
// is how legal nested-existential shapes get rejected. Each list
// deduplicates in first-appearance order; the lists may overlap when
// different clauses need the same symbol with different obligations, and
// consumers deciding requirements read the mandatory list.
func branchInterface(branch []Clause, rules *RuleSet) (provided, mandatory, optional []Symbol) {
	for _, clause := range branch {
		scope := ScopeOf(clause, rules)
		for _, sym := range scope.Provides {
			if !ContainsSymbol(provided, sym) {
				provided = append(provided, sym)
			}
		}
		for _, sym := range scope.Correlates {
			if ContainsSymbol(scope.Mandatory, sym) {
				if !ContainsSymbol(mandatory, sym) {
					mandatory = append(mandatory, sym)
				}
			} else if !ContainsSymbol(optional, sym) {
				optional = append(optional, sym)
			}
		}
	}
	return provided, mandatory, optional
}

// Validate enforces the shape of the declared interface: at least one
// variable in the header, at least one branch, and required and output
// sets disjoint. The parser, the qb builder, and the executor call this at
// their boundaries; ScopeOf reads the declaration without re-checking.
// Whether every branch binds every output, and reads nothing outside the
// header, is InferHeaders' check, under the rule set.
func (o *OrJoinClause) Validate() error {
	if len(o.RequiredVars)+len(o.OutputVars) == 0 {
		return fmt.Errorf("or-join must declare at least one variable in its header")
	}
	if len(o.Branches) == 0 {
		return fmt.Errorf("or-join must have at least one branch")
	}
	for _, req := range o.RequiredVars {
		if ContainsSymbol(o.OutputVars, req) {
			return fmt.Errorf("or-join variable %s cannot be both required and output", req)
		}
	}
	return nil
}

// Validate enforces the shape of the declared interface: at least one
// variable in the header, at least two branches, and required and output
// sets disjoint. The parser, the qb builder, and the executor call this at
// their boundaries; ScopeOf reads the declaration without re-checking.
// Whether every branch binds every output, and reads nothing outside the
// header, is InferHeaders' check, under the rule set.
func (o *OrDefaultJoinClause) Validate() error {
	if len(o.RequiredVars)+len(o.OutputVars) == 0 {
		return fmt.Errorf("or-default-join must declare at least one variable in its header")
	}
	if len(o.Branches) < 2 {
		return fmt.Errorf("or-default-join must have at least two branches")
	}
	for _, req := range o.RequiredVars {
		if ContainsSymbol(o.OutputVars, req) {
			return fmt.Errorf("or-default-join variable %s cannot be both required and output", req)
		}
	}
	return nil
}

// Validate enforces the subquery clause's static shape: the binding form's
// arity must match the inner query's :find arity, and the call passes one
// input per position of the inner :in. Each is a property of the clause
// text alone — no data, planning, or mode is needed to check it — so the
// parser, the qb builder, and the executor entry enforce it with one
// message; the execution paths' deeper checks remain as backstops for
// internally constructed plans. Result cardinality (tuple/scalar bindings
// demanding exactly one tuple) is data-dependent and stays at execution.
// Nested subqueries inside the inner query are part of this clause's text
// and validate recursively. See
// docs/bugs/BUG_SUBQUERY_BINDING_ARITY_VALIDATED_AT_DIFFERENT_LAYERS.md.
func (p *SubqueryPattern) Validate() error {
	if p.Query == nil {
		return fmt.Errorf("subquery has no inner query")
	}
	if p.Binding == nil {
		return fmt.Errorf("subquery has no binding form")
	}
	if arity := InputArity(p.Query.In); len(p.Inputs) != arity {
		return fmt.Errorf("subquery passes %d input(s), but its inner :in binds %d value(s): one per source, %%, scalar and collection input, one per symbol of a tuple or relation input, and one for the $ an omitted :in defaults to", len(p.Inputs), arity)
	}
	arity := len(p.Query.Find)
	switch b := p.Binding.(type) {
	case ScalarBinding:
		if arity != 1 {
			return fmt.Errorf("subquery scalar binding expects the inner :find to have exactly 1 element, got %d", arity)
		}
	case CollectionBinding:
		if arity != 1 {
			return fmt.Errorf("subquery collection binding expects the inner :find to have exactly 1 element, got %d", arity)
		}
	case TupleBinding:
		if len(b.Variables) != arity {
			return fmt.Errorf("subquery tuple binding declares %d symbol(s), but the inner :find has %d element(s)", len(b.Variables), arity)
		}
	case RelationBinding:
		if len(b.Variables) != arity {
			return fmt.Errorf("subquery relation binding declares %d symbol(s), but the inner :find has %d element(s)", len(b.Variables), arity)
		}
	default:
		return fmt.Errorf("subquery has unknown binding form %T", p.Binding)
	}
	return ValidateStaticClauseShapes(p.Query.Where)
}

// Validate enforces the shape of the not-join's header: it names at least
// one variable. Enforced at the same boundaries as SubqueryPattern above.
// Whether the body reads a variable it neither binds nor names in the
// header, and whether the header names a variable the body never touches,
// is InferHeaders' check, under the rule set.
func (n *NotJoinClause) Validate() error {
	if len(n.RequiredVars) == 0 {
		// The parser rejects empty headers at parse; this is the backstop
		// for constructed ASTs.
		return fmt.Errorf("not-join header cannot be empty")
	}
	return ValidateStaticClauseShapes(n.Clauses)
}

// ValidateStaticClauseShapes walks clauses and validates every static
// clause-shape rule — the checks that depend on the query text alone. It is
// the one walk the user boundaries (parser, qb builder, executor entry)
// share, so hand-built ASTs get the same rejections, with the same
// messages, as parsed text. Compound clauses descend; subquery inner
// queries are covered by SubqueryPattern.Validate's own recursion.
func ValidateStaticClauseShapes(clauses []Clause) error {
	var validateErr error
	WalkClauses(clauses, func(c Clause) bool {
		if validateErr != nil {
			return false
		}
		switch v := c.(type) {
		case *SubqueryPattern:
			validateErr = v.Validate()
		case *NotJoinClause:
			validateErr = v.Validate()
		case *OrJoinClause:
			validateErr = v.Validate()
		case *OrDefaultJoinClause:
			validateErr = v.Validate()
		case *FunctionPredicate:
			validateErr = v.Validate()
		}
		return validateErr == nil
	})
	return validateErr
}

// InputArity is the number of values a call supplies for an :in clause: one
// per source, rules, scalar and collection input, one per symbol of a tuple
// or relation input, and one for the $ an omitted :in defaults to. A
// subquery's call is checked against it as a static shape, and the executor
// binds a call's values by it.
func InputArity(in []InputSpec) int {
	if len(in) == 0 {
		return 1
	}
	arity := 0
	for _, spec := range in {
		switch spec := spec.(type) {
		case RelationInput:
			arity += len(spec.Symbols)
		case TupleInput:
			arity += len(spec.Symbols)
		default:
			arity++
		}
	}
	return arity
}

// ValidateInputShapes validates every static input-shape rule — the checks
// that depend on the :in clause alone: a query declares % at most once. The
// user boundaries share it beside ValidateStaticClauseShapes, so a
// hand-built query gets the same rejection as parsed text.
func ValidateInputShapes(in []InputSpec) error {
	declared := false
	for _, spec := range in {
		if _, ok := spec.(RulesInput); !ok {
			continue
		}
		if declared {
			return fmt.Errorf("a query declares %% at most once")
		}
		declared = true
	}
	return nil
}
