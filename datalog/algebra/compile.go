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
		if !containsSymbol(output, bs) {
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
	bindingSyms := bindingFormSymbols(sp.Binding)

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

	// Output = current symbols + binding symbols
	var output []query.Symbol
	if current != nil {
		output = append(output, current.Symbols()...)
	}
	for _, bs := range bindingSyms {
		if !containsSymbol(output, bs) {
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

	// Join symbols = variables shared between current and inner
	joinSyms := sharedSymbols(current.Symbols(), inner.Symbols())

	return &Node{
		Op: RuleAntiJoin,
		Data: &AntiJoin{
			JoinSymbols:  joinSyms,
			Output:       current.Symbols(),
			ExplicitJoin: false, // NotClause — executor infers join vars
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

	return &Node{
		Op: RuleAntiJoin,
		Data: &AntiJoin{
			JoinSymbols:  nj.JoinVars,
			Output:       current.Symbols(),
			ExplicitJoin: true, // NotJoinClause — user specified join vars
		},
		Children: []*Node{current, inner},
	}, nil
}

// compileOr handles OR clauses. Union semantics produce a Union node.
// Fallback semantics (OR with subquery + ground default) produce a
// LateralJoin with default values.
func compileOr(oc *query.OrClause, current *Node) (*Node, error) {
	if !query.OrHasExpressions(oc.Branches) {
		// Union semantics
		return compileOrUnion(oc.Branches, current)
	}

	// Fallback semantics — check for the correlated subquery + ground pattern
	return compileOrFallback(oc.Branches, current)
}

// compileOrJoin is like compileOr but with explicit join variables.
// The join variables are preserved on the Union node so the decompiler
// can emit OrJoinClause (not OrClause) to maintain round-trip fidelity.
func compileOrJoin(oj *query.OrJoinClause, current *Node) (*Node, error) {
	if !query.OrHasExpressions(oj.Branches) {
		return compileOrUnionWithJoinVars(oj.Branches, oj.JoinVars, current)
	}
	return compileOrFallbackWithJoinVars(oj.Branches, oj.JoinVars, current)
}

// compileOrUnion compiles each branch and unions the results.
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

	// Output symbols = union of all branch symbols
	var output []query.Symbol
	if len(children) > 0 {
		output = children[0].Symbols()
	}

	union := &Node{
		Op:       RuleUnion,
		Data:     &Union{Output: output, JoinVars: joinVars},
		Children: children,
	}
	return joinWith(current, union), nil
}

// compileOrFallback handles OR-fallback: subquery branch + ground default branch.
// This is the pattern that produces LateralJoin with defaults.
func compileOrFallback(branches [][]query.Clause, current *Node) (*Node, error) {
	return compileOrFallbackWithJoinVars(branches, nil, current)
}

// compileOrFallbackWithJoinVars handles OR-fallback, preserving explicit join
// variables from or-join on Union nodes.
func compileOrFallbackWithJoinVars(branches [][]query.Clause, joinVars []query.Symbol, current *Node) (*Node, error) {
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
		union, err := compileOrFallbackExclusive(branches, joinVars, current)
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
			if !containsSymbol(ljData.Output, ds) {
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
func compileOrFallbackExclusive(branches [][]query.Clause, joinVars []query.Symbol, current *Node) (*Node, error) {
	if current == nil {
		return nil, fmt.Errorf("OR fallback requires prior relation")
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

	output := mergeSymbols(compiled[0].Symbols(), compiled[1].Symbols())

	union := &Node{
		Op:       RuleUnion,
		Data:     &Union{Output: output, JoinVars: joinVars},
		Children: compiled,
	}
	return union, nil
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

// --- helpers ---

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

// bindingFormSymbols extracts symbols from a SubqueryPattern's BindingForm.
func bindingFormSymbols(binding query.BindingForm) []query.Symbol {
	switch b := binding.(type) {
	case query.TupleBinding:
		return b.Variables
	case query.ScalarBinding:
		return []query.Symbol{b.Variable}
	case query.CollectionBinding:
		return []query.Symbol{b.Variable}
	case query.RelationBinding:
		return b.Variables
	}
	return nil
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

// containsSymbol checks if a symbol is in a slice.
func containsSymbol(syms []query.Symbol, s query.Symbol) bool {
	for _, sym := range syms {
		if sym == s {
			return true
		}
	}
	return false
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

