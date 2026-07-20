package algebra

import (
	"fmt"
	"strings"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// Compile converts a query's WHERE clauses into a relational algebra tree.
// The resulting tree can be optimized via transform passes before execution.
func Compile(q *query.Query) (*Node, error) {
	if len(q.Where) == 0 {
		return nil, fmt.Errorf("empty WHERE clause")
	}
	return compileClauses(q.Where)
}

// compileClauses builds an algebra tree from an ordered list of clauses.
// Each clause either produces a new leaf or wraps/joins with the existing tree.
func compileClauses(clauses []query.Clause) (*Node, error) {
	return compileClausesFrom(clauses, nil)
}

// compileClausesFrom builds an algebra tree starting with an initial relation.
// Used by OR branch compilation to thread the outer relation into branches
// so that NOT clauses have a relation to anti-join against.
func compileClausesFrom(clauses []query.Clause, initial *Node) (*Node, error) {
	current := initial

	for _, clause := range clauses {
		node, err := compileClause(clause, current)
		if err != nil {
			return nil, err
		}
		current = node
	}

	if current == nil {
		return nil, fmt.Errorf("no clauses produced a relation")
	}
	return current, nil
}

// compileClause compiles a single clause, potentially joining it with
// the current accumulated relation.
func compileClause(clause query.Clause, current *Node) (*Node, error) {
	switch c := clause.(type) {
	case *query.DataPattern:
		return compileDataPattern(c, current), nil

	case *query.Expression:
		return compileExpression(c, current), nil

	case *query.SubqueryPattern:
		return compileSubquery(c, current), nil

	case *query.NotClause:
		return compileNot(c, current)

	case *query.NotJoinClause:
		return compileNotJoin(c, current)

	case *query.OrClause:
		return compileOr(c, current)

	case *query.OrJoinClause:
		return compileOrJoin(c, current)

	case *query.OrDefaultClause:
		return compileOrDefault(c, current)

	case *query.OrDefaultJoinClause:
		return compileOrDefaultJoin(c, current)

	case *query.Comparison:
		return compilePredicate(c, current), nil

	case *query.ChainedComparison:
		return compilePredicate(c, current), nil

	case *query.GroundPredicate:
		return compilePredicate(c, current), nil

	case *query.DatabaseFunctionPredicate:
		return compileDatabaseFunctionPredicate(c, current)

	case query.Predicate:
		return compilePredicate(c, current), nil

	default:
		return nil, fmt.Errorf("unsupported clause type: %T", clause)
	}
}

// compileDataPattern produces a Scan node and joins it with current.
func compileDataPattern(p *query.DataPattern, current *Node) *Node {
	output := patternVariables(p)
	scan := &Node{
		Op: RuleScan,
		Data: &Scan{
			Source:  p.Source,
			Pattern: p,
			Output:  output,
		},
	}
	return joinWith(current, scan)
}

// compileExpression wraps current with a Map node.
func compileExpression(expr *query.Expression, current *Node) *Node {
	required := expr.Function.RequiredSymbols()
	bindingSyms := bindingSymbols(expr.Binding)

	// Output = current symbols + new binding symbols
	var output []query.Symbol
	if current != nil {
		output = append(output, current.Symbols()...)
	}
	for _, bs := range bindingSyms {
		if !query.ContainsSymbol(output, bs) {
			output = append(output, bs)
		}
	}

	mapNode := &Node{
		Op: RuleMap,
		Data: &Map{
			Expression: expr,
			Required:   required,
			Output:     output,
		},
	}
	if current != nil {
		mapNode.Children = []*Node{current}
	}
	return mapNode
}

// compileSubquery produces a LateralJoin (correlated) or Join (uncorrelated).
func compileSubquery(sp *query.SubqueryPattern, current *Node) *Node {
	bindingSyms := sp.Binding.BoundVariables()

	// Determine correlation variables: inputs that are variable symbols
	// (excluding source symbols like $ which are always available)
	var correlationVars []query.Symbol
	for _, input := range sp.Inputs {
		if v, ok := input.(query.Variable); ok {
			if !strings.HasPrefix(v.Name.String(), "$") {
				correlationVars = append(correlationVars, v.Name)
			}
		}
	}

	// Output = current symbols + correlation vars + binding symbols.
	// Correlation vars are always in the output — they're the join keys
	// that connect the subquery result to the outer relation, even when
	// the subquery is compiled without an outer context (e.g., inside
	// a union OR branch).
	var output []query.Symbol
	if current != nil {
		output = append(output, current.Symbols()...)
	}
	for _, cv := range correlationVars {
		if !query.ContainsSymbol(output, cv) {
			output = append(output, cv)
		}
	}
	for _, bs := range bindingSyms {
		if !query.ContainsSymbol(output, bs) {
			output = append(output, bs)
		}
	}

	if len(correlationVars) > 0 {
		// Correlated subquery → LateralJoin
		lj := &Node{
			Op: RuleLateralJoin,
			Data: &LateralJoin{
				CorrelationVars: correlationVars,
				InnerQuery:      sp.Query,
				Binding:         sp.Binding,
				Output:          output,
			},
		}
		if current != nil {
			lj.Children = []*Node{current}
		}
		return lj
	}

	// Uncorrelated subquery → LateralJoin with no correlation vars.
	// The decompiler handles LateralJoin → SubqueryPattern correctly.
	lj := &Node{
		Op: RuleLateralJoin,
		Data: &LateralJoin{
			InnerQuery: sp.Query,
			Binding:    sp.Binding,
			Output:     bindingSyms,
		},
	}
	if current != nil {
		lj.Children = []*Node{current}
	}
	return lj
}

// compileNot produces an AntiJoin.
func compileNot(nc *query.NotClause, current *Node) (*Node, error) {
	if current == nil {
		return nil, fmt.Errorf("NOT clause requires prior relation")
	}

	inner, err := compileClauses(nc.Clauses)
	if err != nil {
		return nil, fmt.Errorf("NOT inner clauses: %w", err)
	}

	// Join symbols include right-produced equality keys and predicate-only
	// requirements supplied by the outer relation. The algebra bridge resolves
	// NOT's context-dependent join variables statically and emits NotJoinClause
	// so the executor needs no runtime inference.
	joinSyms := sharedSymbols(current.Symbols(), inner.Symbols())
	analysis, err := Analyze(inner)
	if err != nil {
		return nil, fmt.Errorf("NOT inner analysis: %w", err)
	}
	var required []query.Symbol
	for _, symbol := range analysis[inner].Required {
		if symbol.IsSource() {
			continue
		}
		if !query.ContainsSymbol(current.Symbols(), symbol) {
			return nil, fmt.Errorf("NOT body requires unbound outer symbol %s", symbol)
		}
		if !query.ContainsSymbol(joinSyms, symbol) {
			joinSyms = append(joinSyms, symbol)
		}
		if !query.ContainsSymbol(inner.Symbols(), symbol) {
			required = append(required, symbol)
		}
	}
	// Zero join symbols means the anti-join has no keys: the NOT does not
	// unify with the outer relation, and the not-join this node decompiles
	// to would have an empty header — a form the language rejects. Error
	// here rather than emit an inexpressible clause.
	if len(joinSyms) == 0 {
		return nil, fmt.Errorf(
			"NOT clause %s shares no variable with the outer relation; a NOT body must unify with the enclosing query through at least one variable",
			nc,
		)
	}

	return &Node{
		Op: RuleAntiJoin,
		Data: &AntiJoin{
			JoinSymbols:  joinSyms,
			Required:     required,
			Output:       current.Symbols(),
			ExplicitJoin: true,
		},
		Children: []*Node{current, inner},
	}, nil
}

// compileNotJoin produces an AntiJoin with explicit join variables.
func compileNotJoin(nj *query.NotJoinClause, current *Node) (*Node, error) {
	if current == nil {
		return nil, fmt.Errorf("NOT-JOIN clause requires prior relation")
	}

	inner, err := compileClauses(nj.Clauses)
	if err != nil {
		return nil, fmt.Errorf("NOT-JOIN inner clauses: %w", err)
	}
	analysis, err := Analyze(inner)
	if err != nil {
		return nil, fmt.Errorf("NOT-JOIN inner analysis: %w", err)
	}
	right := analysis[inner]
	for _, symbol := range nj.JoinVars {
		if !query.ContainsSymbol(current.Symbols(), symbol) {
			return nil, fmt.Errorf("not-join header symbol %s is not bound by the outer relation", symbol)
		}
	}
	for _, symbol := range right.Required {
		if symbol.IsSource() {
			continue
		}
		if !query.ContainsSymbol(current.Symbols(), symbol) {
			return nil, fmt.Errorf("NOT-JOIN body requires unbound outer symbol %s", symbol)
		}
		if !query.ContainsSymbol(nj.JoinVars, symbol) {
			return nil, fmt.Errorf(
				"not-join header must declare outer requirement %s used by the body",
				symbol,
			)
		}
	}
	var required []query.Symbol
	for _, symbol := range nj.JoinVars {
		if query.ContainsSymbol(inner.Symbols(), symbol) {
			continue
		}
		if !query.ContainsSymbol(right.Required, symbol) {
			return nil, fmt.Errorf(
				"not-join header symbol %s is neither produced nor consumed by the body",
				symbol,
			)
		}
		required = append(required, symbol)
	}

	return &Node{
		Op: RuleAntiJoin,
		Data: &AntiJoin{
			JoinSymbols:  nj.JoinVars,
			Required:     required,
			Output:       current.Symbols(),
			ExplicitJoin: true, // NotJoinClause — user specified join vars
		},
		Children: []*Node{current, inner},
	}, nil
}

// compileOr handles OR clauses — union semantics.
// When branches contain correlated predicates (NOT, missing?) that require
// outer context, independent union branch compilation can't give them a
// relation to anti-join against; the correlated route compiles branches
// against the outer schema and the clause round-trips unchanged.
func compileOr(oc *query.OrClause, current *Node) (*Node, error) {
	if branchesRequireOuterContext(oc.Branches) {
		return compileOrUnionCorrelated(oc.Branches, nil, query.ScopeOf(oc), current)
	}
	return compileOrUnion(oc.Branches, current)
}

// compileOrJoin handles OR-JOIN clauses — union semantics with join vars.
// Same correlated-predicate detection as compileOr; the correlated route
// preserves the declared header verbatim.
func compileOrJoin(oj *query.OrJoinClause, current *Node) (*Node, error) {
	if branchesRequireOuterContext(oj.Branches) {
		return compileOrUnionCorrelated(oj.Branches, oj.JoinVars, query.ScopeOf(oj), current)
	}
	return compileOrUnionWithJoinVars(oj.Branches, oj.JoinVars, current)
}

// branchesRequireOuterContext returns true if any branch contains (at any
// nesting depth) predicates that are inherently correlated — they reference
// the outer relation and cannot be compiled as independent union branches.
func branchesRequireOuterContext(branches [][]query.Clause) bool {
	for _, branch := range branches {
		if clausesRequireOuterContext(branch) {
			return true
		}
	}
	return false
}

func clausesRequireOuterContext(clauses []query.Clause) bool {
	for _, c := range clauses {
		switch cl := c.(type) {
		case *query.NotClause, *query.NotJoinClause:
			return true
		case *query.MissingPredicate:
			return true
		case *query.OrClause:
			if branchesRequireOuterContext(cl.Branches) {
				return true
			}
		case *query.OrJoinClause:
			if branchesRequireOuterContext(cl.Branches) {
				return true
			}
		case *query.OrDefaultClause:
			if branchesRequireOuterContext(cl.Branches) {
				return true
			}
		case *query.OrDefaultJoinClause:
			if branchesRequireOuterContext(cl.Branches) {
				return true
			}
		}
	}
	return false
}

// compileOrDefault handles OR-DEFAULT clauses — fallback semantics.
func compileOrDefault(oc *query.OrDefaultClause, current *Node) (*Node, error) {
	return compileOrFallback(oc.Branches, current)
}

// compileOrDefaultJoin handles OR-DEFAULT-JOIN clauses — fallback with a
// declared required/output interface.
func compileOrDefaultJoin(oj *query.OrDefaultJoinClause, current *Node) (*Node, error) {
	return compileOrFallbackWithVars(oj.Branches, oj.RequiredVars, oj.OutputVars, current)
}

// compileOrUnion compiles each branch and unions the results.
// Infers join variables from shared symbols between current and branches,
// so the decompiler emits OrJoinClause with explicit join vars (same
// pattern as not → not-join).
func compileOrUnion(branches [][]query.Clause, current *Node) (*Node, error) {
	return compileOrUnionWithJoinVars(branches, nil, current)
}

// compileOrUnionWithJoinVars compiles OR branches into a Union node,
// optionally preserving explicit join variables from or-join.
func compileOrUnionWithJoinVars(branches [][]query.Clause, joinVars []query.Symbol, current *Node) (*Node, error) {
	children := make([]*Node, 0, len(branches))
	for i, branch := range branches {
		compiled, err := compileClauses(branch)
		if err != nil {
			return nil, fmt.Errorf("OR branch %d: %w", i, err)
		}
		children = append(children, compiled)
	}

	explicitJoin := len(joinVars) > 0
	required := make([]query.Symbol, 0)
	if current != nil {
		for _, child := range children {
			for _, symbol := range sharedSymbols(current.Symbols(), child.Symbols()) {
				if (!explicitJoin || query.ContainsSymbol(joinVars, symbol)) &&
					!query.ContainsSymbol(required, symbol) {
					required = append(required, symbol)
				}
			}
		}
	}
	var output []query.Symbol
	effectiveJoinVars := append([]query.Symbol(nil), joinVars...)
	if explicitJoin {
		output = append([]query.Symbol(nil), joinVars...)
	} else {
		output = normalizedBranchSymbols(children, required)
		if len(required) > 0 {
			effectiveJoinVars = append([]query.Symbol(nil), output...)
		}
	}

	union := &Node{
		Op: RuleUnion,
		Data: &Union{
			Output:   output,
			JoinVars: effectiveJoinVars,
			Required: required,
		},
		Children: children,
	}
	return joinWith(current, union), nil
}

// compileOrUnionCorrelated compiles or/or-join whose branches contain
// correlated predicates (NOT, missing?). Branches compile against a schema
// placeholder so anti-joins see the outer symbols without embedding outer
// scans, and the node's interface comes from the clause's canonical scope —
// never inferred from the placeholder-inflated children, which would leak
// outer symbols into the header. The fallback machinery is not a valid
// target here: fallback short-circuits per group, union does not, and
// encoding union through it drops rows (see
// docs/bugs/resolved/BUG_CORRELATED_ORJOIN_GLOBAL_FALLBACK_DROPS_ROWS.md).
// Correlated union has no semantics-preserving rewrite, so the node
// decompiles back to the original clause and execution uses the executor's
// or/or-join path.
func compileOrUnionCorrelated(branches [][]query.Clause, joinVars []query.Symbol, scope query.ClauseScope, current *Node) (*Node, error) {
	if len(branches) < 2 {
		return nil, fmt.Errorf("OR requires at least 2 branches")
	}

	var initial *Node
	if current != nil {
		initial = &Node{
			Op:   RuleProject,
			Data: &Project{Symbols: current.Symbols()},
		}
	}
	compiled := make([]*Node, len(branches))
	for i, branch := range branches {
		node, err := compileClausesFrom(branch, initial)
		if err != nil {
			return nil, fmt.Errorf("OR branch %d: %w", i, err)
		}
		compiled[i] = node
	}

	// Header vars the branches cannot produce ride Required (they unify
	// with the outer relation); everything the clause provides is output.
	output := append([]query.Symbol(nil), joinVars...)
	for _, sym := range scope.Provides {
		if !query.ContainsSymbol(output, sym) {
			output = append(output, sym)
		}
	}

	union := &Node{
		Op: RuleUnion,
		Data: &Union{
			Output:   output,
			JoinVars: joinVars,
			Required: append([]query.Symbol(nil), scope.Correlates...),
		},
		Children: compiled,
	}
	return joinWith(current, union), nil
}

// compileOrFallback handles OR-fallback: subquery branch + ground default branch.
// This is the pattern that produces LateralJoin with defaults.
func compileOrFallback(branches [][]query.Clause, current *Node) (*Node, error) {
	return compileOrFallbackWithVars(branches, nil, nil, current)
}

// compileOrFallbackWithVars handles OR-fallback, preserving or-default-join's
// declared required/output interface through the IR.
func compileOrFallbackWithVars(branches [][]query.Clause, requiredVars, outputVars []query.Symbol, current *Node) (*Node, error) {
	if len(branches) < 2 {
		return nil, fmt.Errorf("OR fallback requires at least 2 branches")
	}

	// Look for the pattern: branch 0 has a SubqueryPattern, branch 1 has ground/constants
	var subqueryBranch []query.Clause
	var defaultValues []interface{}
	var defaultSymbols []query.Symbol
	isSubqueryFallback := false

	if sp := findSubqueryInBranch(branches[0]); sp != nil {
		subqueryBranch = branches[0]
		defaultValues, defaultSymbols = extractGroundDefaults(branches[1])
		if defaultValues != nil {
			isSubqueryFallback = true
		}
	}

	if !isSubqueryFallback {
		// General OR-fallback: "try branch 1, else branch 2 per tuple."
		// Algebraically: Union(B1(R), B2(R - B1_entities))
		// Branch 1 sees the full outer relation. Branch 2 sees only tuples
		// that branch 1 did NOT match (anti-join complement).
		// compileOrFallbackExclusive returns a Union node (not joined with
		// current). We join it here so the decompiler emits outer clauses
		// separately from the OR.
		union, err := compileOrFallbackExclusive(branches, requiredVars, outputVars, current)
		if err != nil {
			return nil, err
		}
		return joinWith(current, union), nil
	}

	// Compile the subquery branch
	subNode, err := compileClauses(subqueryBranch)
	if err != nil {
		return nil, fmt.Errorf("OR fallback subquery branch: %w", err)
	}

	// Attach defaults based on subquery type
	if lj := findLateralJoin(subNode); lj != nil {
		// Correlated subquery → LateralJoin with defaults
		ljData := lj.Data.(*LateralJoin)
		ljData.DefaultValues = defaultValues
		for _, ds := range defaultSymbols {
			if !query.ContainsSymbol(ljData.Output, ds) {
				ljData.Output = append(ljData.Output, ds)
			}
		}
	} else if len(defaultValues) > 0 {
		// Uncorrelated subquery → wrap in LeftOuterJoin with defaults.
		// The subquery returns results for some outer tuples; non-matching
		// outer tuples get default values via the LeftOuterJoin.
		joinSyms := sharedSymbols(symbolsOf(current), subNode.Symbols())
		subNode = &Node{
			Op: RuleJoin,
			Data: &Join{
				Kind:          LeftOuterJoin,
				JoinSymbols:   joinSyms,
				Output:        mergeSymbols(symbolsOf(current), subNode.Symbols()),
				DefaultValues: defaultValues,
			},
			Children: []*Node{current, subNode},
		}
		return subNode, nil
	}

	// The subNode is already joined with current (compileSubquery does this)
	if current != nil && subNode.Op != RuleLateralJoin {
		return joinWith(current, subNode), nil
	}

	// For LateralJoin at top level, attach current as left child if not already
	if subNode.Op == RuleLateralJoin && len(subNode.Children) == 0 && current != nil {
		subNode.Children = []*Node{current}
	}

	return subNode, nil
}

// compileOrFallbackExclusive compiles OR-fallback with exclusive branch semantics:
// Branch 1 sees the full outer relation. Branch 2 sees only tuples that
// branch 1 did NOT produce (the complement). Results are unioned.
//
// Algebraically: Union(B1(R), B2(R ▷ B1_entities))
//
// This correctly implements "try branch 1, else branch 2 per tuple" because
// branch 2 only runs on tuples where branch 1 produced no results.
func compileOrFallbackExclusive(branches [][]query.Clause, requiredVars, outputVars []query.Symbol, current *Node) (*Node, error) {
	if current == nil {
		// Uncorrelated global-fallback shape: an or-default opening :where
		// has no prior relation, which means the outer group is the unit
		// relation — the join identity — so branch evaluation is one global
		// pass with first-non-empty-branch-wins semantics and an empty
		// correlation interface. The unit is expressed as the same
		// childless Project the schema placeholder below uses (zero
		// symbols; decompiles to nothing), and the caller's
		// joinWith(nil, union) returns the union directly.
		current = &Node{Op: RuleProject, Data: &Project{}}
	}
	if len(branches) < 2 {
		return nil, fmt.Errorf("OR fallback requires at least 2 branches")
	}

	// Schema placeholder: provides outer symbols for NOT/missing? compilation
	// without embedding the outer's scan nodes. Decompiles to nothing (Project
	// with no children → nil clauses).
	outerSchema := &Node{
		Op:   RuleProject,
		Data: &Project{Symbols: current.Symbols()},
	}

	// Compile each branch with the schema placeholder as context.
	// NOT and missing? get the symbols they need for anti-join, but
	// the decompiler won't emit the outer scan patterns.
	compiled := make([]*Node, len(branches))
	for i, branch := range branches {
		node, err := compileClausesFrom(branch, outerSchema)
		if err != nil {
			return nil, fmt.Errorf("OR fallback branch %d: %w", i, err)
		}
		compiled[i] = node
	}

	output := normalizedBranchSymbols(compiled, current.Symbols())

	lateralUnion := &Node{
		Op: RuleLateralUnion,
		Data: &LateralUnion{
			Output:       output,
			RequiredVars: requiredVars,
			OutputVars:   outputVars,
			Required:     append([]query.Symbol(nil), current.Symbols()...),
		},
		Children: compiled,
	}
	return lateralUnion, nil
}

func normalizedBranchSymbols(branches []*Node, joinVars []query.Symbol) []query.Symbol {
	output := append([]query.Symbol(nil), joinVars...)
	if len(branches) == 0 {
		return output
	}
	for _, symbol := range branches[0].Symbols() {
		if query.ContainsSymbol(joinVars, symbol) {
			continue
		}
		common := true
		for _, branch := range branches[1:] {
			if !query.ContainsSymbol(branch.Symbols(), symbol) {
				common = false
				break
			}
		}
		if common && !query.ContainsSymbol(output, symbol) {
			output = append(output, symbol)
		}
	}
	return output
}

// compilePredicate produces a Select node for any predicate type.
func compilePredicate(p query.Predicate, current *Node) *Node {
	return &Node{
		Op: RuleSelect,
		Data: &Select{
			Predicate: p,
			Required:  p.RequiredSymbols(),
			Output:    symbolsOf(current),
		},
		Children: childrenOf(current),
	}
}

// compileDatabaseFunctionPredicate compiles database function predicates.
// missing?($ ?e :attr) compiles to AntiJoin(current, Scan([?e :attr _])).
// Other database function predicates fall back to Select.
func compileDatabaseFunctionPredicate(p *query.DatabaseFunctionPredicate, current *Node) (*Node, error) {
	if current == nil {
		return nil, fmt.Errorf("database function predicate requires prior relation")
	}

	// Compile all database function predicates (missing?, get-some, etc.)
	// as Select nodes. The executor handles them via EvalWithLookup.
	// The predicate is preserved as-is through the round-trip.
	return compilePredicate(p, current), nil
}

// --- internal functions ---

// joinWith combines two nodes with a natural join.
// If left is nil, returns right directly.
func joinWith(left, right *Node) *Node {
	if left == nil {
		return right
	}

	shared := sharedSymbols(left.Symbols(), right.Symbols())
	output := mergeSymbols(left.Symbols(), right.Symbols())

	return &Node{
		Op: RuleJoin,
		Data: &Join{
			Kind:        InnerJoin,
			JoinSymbols: shared,
			Output:      output,
		},
		Children: []*Node{left, right},
	}
}

// patternVariables extracts variable symbols from a DataPattern.
func patternVariables(p *query.DataPattern) []query.Symbol {
	var vars []query.Symbol
	for _, elem := range p.Elements {
		if v, ok := elem.(query.Variable); ok {
			vars = append(vars, v.Name)
		}
	}
	return vars
}

// bindingSymbols extracts symbols from an Expression's Binding field.
func bindingSymbols(binding interface{}) []query.Symbol {
	switch b := binding.(type) {
	case query.Symbol:
		if b != nil {
			return []query.Symbol{b}
		}
	case query.TupleBinding:
		return b.Variables
	}
	return nil
}

// collectBranchSymbols collects all variable symbols from OR branch clauses.
func collectBranchSymbols(branches [][]query.Clause) []query.Symbol {
	seen := make(map[query.Symbol]bool)
	var syms []query.Symbol
	for _, branch := range branches {
		for _, clause := range branch {
			for _, sym := range clauseSymbols(clause) {
				if !seen[sym] {
					seen[sym] = true
					syms = append(syms, sym)
				}
			}
		}
	}
	return syms
}

// clauseSymbols extracts all variable symbols from a clause.
func clauseSymbols(clause query.Clause) []query.Symbol {
	switch c := clause.(type) {
	case *query.DataPattern:
		return patternVariables(c)
	case *query.Expression:
		var syms []query.Symbol
		syms = append(syms, c.Function.RequiredSymbols()...)
		syms = append(syms, bindingSymbols(c.Binding)...)
		return syms
	case *query.SubqueryPattern:
		var syms []query.Symbol
		// Include input variables (correlation vars that link to outer scope)
		for _, input := range c.Inputs {
			if v, ok := input.(query.Variable); ok {
				if !strings.HasPrefix(v.Name.String(), "$") {
					syms = append(syms, v.Name)
				}
			}
		}
		syms = append(syms, c.Binding.BoundVariables()...)
		return syms
	default:
		return nil
	}
}

// sharedSymbols returns symbols present in both a and b.
func sharedSymbols(a, b []query.Symbol) []query.Symbol {
	set := make(map[query.Symbol]bool, len(b))
	for _, s := range b {
		set[s] = true
	}
	var shared []query.Symbol
	for _, s := range a {
		if set[s] {
			shared = append(shared, s)
		}
	}
	return shared
}

// mergeSymbols returns the union of two symbol slices, preserving order.
func mergeSymbols(a, b []query.Symbol) []query.Symbol {
	result := make([]query.Symbol, len(a))
	copy(result, a)
	seen := make(map[query.Symbol]bool, len(a))
	for _, s := range a {
		seen[s] = true
	}
	for _, s := range b {
		if !seen[s] {
			result = append(result, s)
			seen[s] = true
		}
	}
	return result
}

// symbolsOf returns the output symbols of a node, or nil if node is nil.
func symbolsOf(n *Node) []query.Symbol {
	if n == nil {
		return nil
	}
	return n.Symbols()
}

// childrenOf returns a single-element children slice, or nil.
func childrenOf(n *Node) []*Node {
	if n == nil {
		return nil
	}
	return []*Node{n}
}

// findSubqueryInBranch returns the first SubqueryPattern in a branch, or nil.
func findSubqueryInBranch(branch []query.Clause) *query.SubqueryPattern {
	for _, c := range branch {
		if sp, ok := c.(*query.SubqueryPattern); ok {
			return sp
		}
	}
	return nil
}

// extractGroundDefaults extracts constant values and symbols from a ground-only branch.
// Returns nil if the branch contains non-ground clauses.
// Ground values come from Expression clauses with GroundFunction.
func extractGroundDefaults(branch []query.Clause) ([]interface{}, []query.Symbol) {
	var values []interface{}
	var symbols []query.Symbol

	for _, c := range branch {
		switch g := c.(type) {
		case *query.Expression:
			if gf, ok := g.Function.(*query.GroundFunction); ok {
				// Flatten tuple ground values: (ground [0 0]) has Value=[0,0]
				// which should produce individual defaults [0, 0], not [[0, 0]].
				if slice, ok := gf.Value.([]interface{}); ok {
					values = append(values, slice...)
				} else {
					values = append(values, gf.Value)
				}
				symbols = append(symbols, bindingSymbols(g.Binding)...)
			} else {
				return nil, nil // Non-ground expression
			}
		case *query.GroundPredicate:
			// GroundPredicate checks if variables are bound, not a value producer.
			continue
		case *query.NotClause, *query.NotJoinClause, *query.DatabaseFunctionPredicate:
			// NOT and missing? in default branches are guard conditions
			// ("entity has no children", "entity lacks attribute"). The
			// LeftOuterJoin default semantics handle non-matching tuples,
			// making these guards redundant. Skip them so this branch
			// routes through the SubqueryPattern+defaults path.
			continue
		default:
			return nil, nil // Non-ground clause
		}
	}

	return values, symbols
}

// findLateralJoin finds the first LateralJoin node in a tree (DFS).
func findLateralJoin(n *Node) *Node {
	if n == nil {
		return nil
	}
	if n.Op == RuleLateralJoin {
		return n
	}
	for _, child := range n.Children {
		if found := findLateralJoin(child); found != nil {
			return found
		}
	}
	return nil
}
