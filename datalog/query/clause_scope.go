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
// the such inputs of a plain NOT's body or an or-default's branches that
// nothing in that body or branch binds. One the query cannot bind is a
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

	case *NotClause:
		// A plain NOT unifies on every free variable of its body that the
		// enclosing query binds; the rest are existential. An input of the
		// body that nothing in the body binds is the enclosing scope's to
		// bind.
		_, _, mandatory := branchInterfaces([][]Clause{c.Clauses}, intersectSymbolSets, rules)
		return ClauseScope{Correlates: FreeVariables(c.Clauses, rules), Mandatory: mandatory}

	case *NotJoinClause:
		// The header is the complete interface by language contract.
		return ClauseScope{Correlates: c.JoinVars, Mandatory: c.JoinVars}

	case *OrClause:
		// Union semantics: only variables every branch binds are provided.
		provides, externals, _ := branchInterfaces(c.Branches, intersectSymbolSets, rules)
		return ClauseScope{Provides: provides, Correlates: externals, Mandatory: externals}

	case *OrJoinClause:
		return headerScope(c.JoinVars, c.Branches, intersectSymbolSets, rules)

	case *OrDefaultClause:
		// Fallback semantics: exactly one branch's results are used, so
		// only symbols every branch binds are reliably in the output — and
		// the executor's schema is outer ∪ branch intersection
		// (computeOrBranchOutputSymbols); a union-only symbol never
		// appears in the relation. Correlation additionally rides the
		// entity variable of each branch's first pattern (per-tuple
		// evaluation). A branch input nothing in the branch binds is the
		// enclosing scope's to bind.
		provides, externals, mandatory := branchInterfaces(c.Branches, intersectSymbolSets, rules)
		correlates := externals
		for _, branch := range c.Branches {
			for _, clause := range branch {
				if pattern, ok := clause.(*DataPattern); ok {
					if len(pattern.Elements) > 0 {
						if v, ok := pattern.Elements[0].(Variable); ok && !ContainsSymbol(correlates, v.Name) {
							correlates = append(correlates, v.Name)
						}
					}
					break
				}
			}
		}
		return ClauseScope{Provides: provides, Correlates: correlates, Mandatory: mandatory}

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

// branchInterfaces computes a branch set's combined interface: provides
// combined across branches by combine (intersection for union semantics,
// union for fallback semantics); externals — every branch's Correlates not
// self-provided within that branch, with source symbols filtered out; and
// mandatory — the externals some branch must have bound before it can run.
func branchInterfaces(
	branches [][]Clause,
	combine func([][]Symbol) []Symbol,
	rules *RuleSet,
) (provides, externals, mandatory []Symbol) {
	if len(branches) == 0 {
		return nil, nil, nil
	}

	branchProvides := make([][]Symbol, len(branches))
	for i, branch := range branches {
		provided, branchMandatory, branchOptional := branchInterface(branch, rules)
		branchProvides[i] = provided
		for _, sym := range branchMandatory {
			if ContainsSymbol(provided, sym) || sym.IsSource() {
				continue
			}
			if !ContainsSymbol(externals, sym) {
				externals = append(externals, sym)
			}
			if !ContainsSymbol(mandatory, sym) {
				mandatory = append(mandatory, sym)
			}
		}
		for _, sym := range branchOptional {
			if !ContainsSymbol(provided, sym) && !sym.IsSource() && !ContainsSymbol(externals, sym) {
				externals = append(externals, sym)
			}
		}
	}

	return combine(branchProvides), externals, mandatory
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

// Validate enforces the declared interface: at least one output variable, at
// least two branches, required and output sets disjoint, every branch binds
// every output variable, and every variable a branch consumes without
// binding is a declared required variable. The parser, the qb builder, and
// the executor call this at their boundaries; ScopeOf reads the declaration
// without re-checking. The branches are read with no rule set: the check
// runs at the parser's boundary, where no program is in hand.
func (o *OrDefaultJoinClause) Validate() error {
	if len(o.OutputVars) == 0 {
		return fmt.Errorf("or-default-join must declare at least one output variable")
	}
	if len(o.Branches) < 2 {
		return fmt.Errorf("or-default-join must have at least two branches")
	}
	for _, req := range o.RequiredVars {
		if ContainsSymbol(o.OutputVars, req) {
			return fmt.Errorf("or-default-join variable %s cannot be both required and output", req)
		}
	}
	for i, branch := range o.Branches {
		// Only MANDATORY correlates create declaration requirements; a
		// nested plain NOT's existential body variables (optional
		// correlates) unify when bound and are existential otherwise.
		provided, mandatory, _ := branchInterface(branch, nil)
		for _, out := range o.OutputVars {
			if !ContainsSymbol(provided, out) {
				return fmt.Errorf("or-default-join branch %d does not bind output variable %s; every branch must bind every output", i+1, out)
			}
		}
		for _, sym := range mandatory {
			if sym.IsSource() || ContainsSymbol(provided, sym) {
				continue
			}
			if !ContainsSymbol(o.RequiredVars, sym) {
				return fmt.Errorf("or-default-join branch %d consumes %s, which is neither bound in the branch nor a declared required variable", i+1, sym)
			}
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

// Validate enforces the not-join clause's static header-completeness rule:
// the header is the clause's complete declared interface, so every variable
// the body consumes without binding — including predicate-only inputs — must
// be declared in it. Enforced at the same boundaries as SubqueryPattern
// above; the algebra bridge's own analysis remains as a backstop. The body is
// read with no rule set: the check runs at the parser's boundary, where no
// program is in hand. See
// docs/bugs/BUG_NOTJOIN_HEADER_VALIDATION_ONLY_ON_ALGEBRA_PATH.md.
func (n *NotJoinClause) Validate() error {
	if len(n.JoinVars) == 0 {
		// The parser rejects empty headers at parse; this is the backstop
		// for constructed ASTs.
		return fmt.Errorf("not-join header cannot be empty")
	}
	// Only MANDATORY correlates are outer requirements the header must
	// declare; symbols the body itself binds are internal (same subtraction
	// as OrDefaultJoinClause.Validate), and optional correlates — a nested
	// plain NOT's existential body variables — unify when bound and are
	// existential otherwise, never a header demand.
	provided, mandatory, _ := branchInterface(n.Clauses, nil)
	for _, sym := range mandatory {
		if sym.IsSource() || ContainsSymbol(provided, sym) {
			continue
		}
		if !ContainsSymbol(n.JoinVars, sym) {
			return fmt.Errorf("not-join header %v does not declare %s, which the body consumes from the enclosing query; the header must declare every outer requirement, including predicate-only inputs", n.JoinVars, sym)
		}
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

// headerScope computes the scope of an explicit-join form: Provides is the
// declared header restricted to what the branch combination produces —
// branch locals never escape — and header variables the branches cannot
// produce correlate with the enclosing scope, alongside branch externals.
func headerScope(
	joinVars []Symbol,
	branches [][]Clause,
	combine func([][]Symbol) []Symbol,
	rules *RuleSet,
) ClauseScope {
	produced, externals, _ := branchInterfaces(branches, combine, rules)

	var provides []Symbol
	correlates := externals
	for _, jv := range joinVars {
		if ContainsSymbol(produced, jv) {
			provides = append(provides, jv)
		} else if !ContainsSymbol(correlates, jv) {
			correlates = append(correlates, jv)
		}
	}
	return ClauseScope{Provides: provides, Correlates: correlates, Mandatory: correlates}
}

// intersectSymbolSets returns the symbols present in every set, in the first
// set's order.
func intersectSymbolSets(sets [][]Symbol) []Symbol {
	if len(sets) == 0 {
		return nil
	}
	var result []Symbol
	for _, sym := range sets[0] {
		inAll := true
		for _, other := range sets[1:] {
			if !ContainsSymbol(other, sym) {
				inAll = false
				break
			}
		}
		if inAll {
			result = append(result, sym)
		}
	}
	return result
}
