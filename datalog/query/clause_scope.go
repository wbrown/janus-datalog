package query

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// ClauseScope is a clause's interface to its enclosing scope. Provides are
// the variables the clause can bind there; Correlates are its free variables
// that unify with the enclosing scope, which binds every one of them before
// the clause runs: a predicate's, an expression's or a subquery's inputs, a
// header form's required run, and an invocation's required positions. One
// the query cannot bind is a query error the planner surfaces. Variables
// scoped inside a clause, a header form's branch locals and a subquery's
// inner variables, appear in neither. This is the canonical scoping
// definition; the planner's scheduling, the executor's anti-join keys, and
// the algebra bridge's inference all derive from it.
type ClauseScope struct {
	Provides   []Symbol
	Correlates []Symbol
}

// ScopeOf returns the scope interface of a clause under the rule set the
// query runs with. A rule invocation's interface is declared by its
// predicate: the variables at the positions the predicate requires bound are
// correlates, and the rest are provided. Read with no rule set, an
// invocation provides every variable argument. The clause taxonomy is
// closed; a form without a case here panics rather than silently exposing
// nothing.
func ScopeOf(clause Clause, rules *RuleSet) ClauseScope {
	switch c := clause.(type) {
	case *DataPattern:
		return ClauseScope{Provides: c.Symbols()}

	case *Expression:
		return ClauseScope{Provides: c.BoundVariables(), Correlates: c.Function.RequiredSymbols()}

	case *SubqueryPattern:
		var inputs []Symbol
		for _, input := range c.Inputs {
			if v, ok := input.(Variable); ok && !v.Name.IsSource() {
				inputs = append(inputs, v.Name)
			}
		}
		return ClauseScope{Provides: c.Binding.BoundVariables(), Correlates: inputs}

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
		return ClauseScope{Provides: provides, Correlates: correlates}

	case *NotJoinClause:
		// The header is the complete interface by language contract.
		return ClauseScope{Correlates: c.RequiredVars}

	case *OrJoinClause:
		// The header is the complete interface: the required run correlates
		// and the outputs are provided. Every other variable of a branch is
		// the branch's own.
		return ClauseScope{Provides: c.OutputVars, Correlates: c.RequiredVars}

	case *OrDefaultJoinClause:
		// The header is the complete interface: the required run correlates
		// and quantifies the per-group fallback decision, and the outputs are
		// provided, every branch binding them, which Validate checks at the
		// text boundaries and InferHeaders under the rule set. Branch locals
		// never escape.
		return ClauseScope{Provides: c.OutputVars, Correlates: c.RequiredVars}

	case Predicate:
		// Every predicate form: consumes, never binds.
		return ClauseScope{Correlates: c.RequiredSymbols()}

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
// provide, and the symbols they correlate on, which the branch or the
// enclosing header binds before the clause runs. Each list deduplicates in
// first-appearance order, and the lists overlap where one clause provides a
// symbol another reads.
func branchInterface(branch []Clause, rules *RuleSet) (provided, correlates []Symbol) {
	for _, clause := range branch {
		scope := ScopeOf(clause, rules)
		for _, sym := range scope.Provides {
			if !ContainsSymbol(provided, sym) {
				provided = append(provided, sym)
			}
		}
		for _, sym := range scope.Correlates {
			if !ContainsSymbol(correlates, sym) {
				correlates = append(correlates, sym)
			}
		}
	}
	return provided, correlates
}

// Validate reads the declared interface at a text boundary: at least one
// variable in the header, at least one branch, required and output sets
// disjoint, and the header against the branches through
// branchesUnderHeader with no rule set, under which an invocation provides
// every variable argument, so the boundary refuses a subset of what Prepare
// refuses under the rule set, with the same message. The parser, the qb
// builder, and the executor call this at their boundaries; ScopeOf reads
// the declaration without re-checking.
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
	_, _, err := branchesUnderHeader(o, o.RequiredVars, o.OutputVars, o.Branches, nil)
	return err
}

// Validate reads the declared interface at a text boundary: at least one
// variable in the header, at least two branches, required and output sets
// disjoint, and the header against the branches through
// branchesUnderHeader with no rule set, under which an invocation provides
// every variable argument, so the boundary refuses a subset of what Prepare
// refuses under the rule set, with the same message. The parser, the qb
// builder, and the executor call this at their boundaries; ScopeOf reads
// the declaration without re-checking.
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
	_, _, err := branchesUnderHeader(o, o.RequiredVars, o.OutputVars, o.Branches, nil)
	return err
}

// Validate enforces the subquery clause's static shape: the inner query
// passes ValidateQuery whole, its refusal wrapped with this clause, so an
// inner query is checked as the outer query is, at every depth; the inner
// query carries no :limit, since the scope's evaluation applies neither
// :order-by nor :limit, which are the result boundary's own steps, and a cap
// the inner query carried would be silently ignored
// (docs/bugs/resolved/BUG_QUERY_LIMIT_CLAUSE_UNSUPPORTED.md); its :find holds
// no pull, since pull output is result presentation and no relational value,
// where the subquery's result enters the scope's joins and deduplication,
// which operate on values (docs/bugs/resolved/BUG_PULL_WITH_ORDER_BY_PANICS.md);
// the call passes one input per position of the inner :in; and the binding
// form's arity matches the inner :find's. Each is a property of the clause
// text alone, so the parser, the qb builder and Prepare enforce it through
// ValidateQuery with one message
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md goal 5 and §6).
func (p *SubqueryPattern) Validate() error {
	if p.Query == nil {
		return fmt.Errorf("subquery has no inner query")
	}
	if p.Binding == nil {
		return fmt.Errorf("subquery has no binding form")
	}
	if err := ValidateQuery(p.Query); err != nil {
		return fmt.Errorf("subquery %s: %w", p, err)
	}
	if p.Query.Limit != nil {
		return fmt.Errorf(":limit is not supported inside a subquery; cap tuples in the enclosing query, or use the (max ?x)/(min ?x) aggregate idiom for top-1-per-group")
	}
	for _, elem := range p.Query.Find {
		if _, isPull := elem.(FindPull); isPull {
			return fmt.Errorf("(pull ...) is not supported inside a subquery find: pull output is result presentation, not a relational value; return the entity from the subquery and pull it in the enclosing query's :find")
		}
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
	case RelationBinding:
		if len(b.Variables) != arity {
			return fmt.Errorf("subquery relation binding declares %d symbol(s), but the inner :find has %d element(s)", len(b.Variables), arity)
		}
	default:
		return fmt.Errorf("subquery has unknown binding form %T", p.Binding)
	}
	return nil
}

// Validate reads the not-join at a text boundary: the header names at least
// one variable, the body's nested shapes hold, and the body reads nothing
// outside the header, through branchesUnderHeader with no rule set, under
// which an invocation provides every variable argument, so the boundary
// refuses a subset of what Prepare refuses under the rule set, with the
// same message. Enforced at the same boundaries as SubqueryPattern above.
// Whether the header names a variable the body never touches is
// InferHeaders' check.
func (n *NotJoinClause) Validate() error {
	if len(n.RequiredVars) == 0 {
		// The parser rejects empty headers at parse; this is the backstop
		// for constructed ASTs.
		return fmt.Errorf("not-join header cannot be empty")
	}
	if err := ValidateStaticClauseShapes(n.Clauses); err != nil {
		return err
	}
	_, _, err := branchesUnderHeader(n, n.RequiredVars, nil, [][]Clause{n.Clauses}, nil)
	return err
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
		case *Expression:
			validateErr = v.Validate()
		}
		return validateErr == nil
	})
	return validateErr
}

// Validate decides a ground's shape against its binding from the constant
// the text holds, so every boundary refuses the same shapes naming the
// repair over the author's own symbols: a ground under the tuple form is a
// vector of the binding's length, one under the collection form is a vector,
// and one under the relation form is a vector of inner vectors, each of the
// binding's arity. One value under the tuple or relation form is bound by
// the scalar form over the first symbol. A vector of values under the
// relation form is bound by the collection form when the binding names one
// symbol and by the tuple form when it names as many symbols as the vector
// holds values; otherwise the refusal states both counts. A tuple or
// relation binding names at least one symbol, as the parser requires of the
// text. A function's result is read at run time and is checked nowhere here.
func (e *Expression) Validate() error {
	var value interface{}
	var grounded bool
	switch f := e.Function.(type) {
	case GroundFunction:
		value, grounded = f.Value, true
	case *GroundFunction:
		value, grounded = f.Value, true
	}
	switch b := e.Binding.(type) {
	case TupleBinding:
		if len(b.Variables) == 0 {
			return fmt.Errorf("tuple binding cannot be empty")
		}
		if !grounded {
			return nil
		}
		values, ok := VectorElements(value)
		if !ok {
			return fmt.Errorf("%s is one value; bind it with the scalar form %s", e.Function, b.Variables[0])
		}
		if len(values) != len(b.Variables) {
			return fmt.Errorf("tuple ground mismatch: %d values, %d binding variables", len(values), len(b.Variables))
		}
	case CollectionBinding:
		if !grounded {
			return nil
		}
		if _, ok := VectorElements(value); !ok {
			return fmt.Errorf("%s is one value; bind it with the scalar form %s", e.Function, b.Variable)
		}
	case RelationBinding:
		if len(b.Variables) == 0 {
			return fmt.Errorf("relation binding cannot be empty")
		}
		if !grounded {
			return nil
		}
		vectors, ok := VectorElements(value)
		if !ok {
			return fmt.Errorf("%s is one value; bind it with the scalar form %s", e.Function, b.Variables[0])
		}
		for i, vector := range vectors {
			elements, ok := VectorElements(vector)
			if !ok {
				switch {
				case len(b.Variables) == 1:
					return fmt.Errorf("%s is one tuple; bind it with the collection form %s", e.Function, CollectionBinding{Variable: b.Variables[0]})
				case len(vectors) == len(b.Variables):
					return fmt.Errorf("%s is one tuple; bind it with the tuple form %s", e.Function, TupleBinding{Variables: b.Variables})
				default:
					return fmt.Errorf("%s is one tuple of %d values, and the binding %s names %d symbols", e.Function, len(vectors), b, len(b.Variables))
				}
			}
			if len(elements) != len(b.Variables) {
				return fmt.Errorf("%s tuple %d has %d values, and the binding %s names %d", e.Function, i+1, len(elements), b, len(b.Variables))
			}
		}
	}
	return nil
}

// InputPosition is one call position of an :in clause: the entry at it and
// the symbol the call's value there binds, nil for a source and for the
// rule set.
type InputPosition struct {
	Spec   InputSpec
	Symbol Symbol
}

// InputPositions is the call positions of an :in clause in call order: one
// per source, rules, scalar and collection input, one per symbol of a tuple
// or relation input, and the one of the $ an omitted :in defaults to. A
// subquery's call is checked against their count as a static shape, the
// executor binds a call's values by them, and a subquery's frame and the
// attribute roster read a call's substitution from their symbols against
// the call's arguments.
func InputPositions(in []InputSpec) []InputPosition {
	if len(in) == 0 {
		return []InputPosition{{Spec: DatabaseInput{Name: datalog.SymDollar}}}
	}
	positions := make([]InputPosition, 0, len(in))
	for _, spec := range in {
		switch entry := spec.(type) {
		case ScalarInput:
			positions = append(positions, InputPosition{Spec: spec, Symbol: entry.Symbol})
		case CollectionInput:
			positions = append(positions, InputPosition{Spec: spec, Symbol: entry.Symbol})
		case RelationInput:
			for _, sym := range entry.Symbols {
				positions = append(positions, InputPosition{Spec: spec, Symbol: sym})
			}
		case TupleInput:
			for _, sym := range entry.Symbols {
				positions = append(positions, InputPosition{Spec: spec, Symbol: sym})
			}
		default:
			positions = append(positions, InputPosition{Spec: spec})
		}
	}
	return positions
}

// InputArity is the number of values a call supplies for an :in clause, the
// count of its call positions.
func InputArity(in []InputSpec) int {
	return len(InputPositions(in))
}

// InputSymbols is the symbol a call's value at each call position of an :in
// clause binds, nil at a source's or the rule set's position.
func InputSymbols(in []InputSpec) []Symbol {
	positions := InputPositions(in)
	symbols := make([]Symbol, len(positions))
	for i, position := range positions {
		symbols[i] = position.Symbol
	}
	return symbols
}

// BindingInputs is the entries of an :in clause that bind variables, in
// order: each scalar, collection, tuple or relation input. A source and the
// rule set bind none. The executor's entry takes one relation per entry.
func BindingInputs(in []InputSpec) []InputSpec {
	var binding []InputSpec
	for _, spec := range in {
		switch spec.(type) {
		case ScalarInput, CollectionInput, TupleInput, RelationInput:
			binding = append(binding, spec)
		}
	}
	return binding
}

// ValidateInputShapes validates every static input-shape rule — the checks
// that depend on the :in clause alone: a query declares % at most once, and
// a tuple or relation input names at least one variable. The user
// boundaries share it beside ValidateStaticClauseShapes, so a hand-built
// query gets the same rejection as parsed text.
func ValidateInputShapes(in []InputSpec) error {
	declared := false
	for _, spec := range in {
		switch entry := spec.(type) {
		case RulesInput:
			if declared {
				return fmt.Errorf("a query declares %% at most once")
			}
			declared = true
		case TupleInput:
			if len(entry.Symbols) == 0 {
				return fmt.Errorf("%s names no variable; a tuple input names at least one, as [?x]", entry)
			}
		case RelationInput:
			if len(entry.Symbols) == 0 {
				return fmt.Errorf("%s names no variable; a relation input names at least one, as [[?x] ...]", entry)
			}
		}
	}
	return nil
}

// ValidateRulesInput refuses the first clause of where that uses the query's
// % in the query's own scope, as RulesUse finds it, when in declares no %
// input: a rule invocation names the rule set it resolves against, and a
// subquery call passing % through names the rule set it passes. A subquery
// body's invocations use the inner query's %, checked when that query is.
// ParseQuery, qb.Build and Prepare each run it, so the three boundaries
// refuse the shape with one message.
func ValidateRulesInput(in []InputSpec, where []Clause) error {
	for _, spec := range in {
		if _, ok := spec.(RulesInput); ok {
			return nil
		}
	}
	switch use := RulesUse(where).(type) {
	case *RuleInvocation:
		return fmt.Errorf("rule invocation %s requires %% in :in, the rule set the invocation resolves against", use)
	case *SubqueryPattern:
		return fmt.Errorf("subquery %s passes %% through and requires %% in :in, the rule set it passes", use)
	}
	return nil
}
