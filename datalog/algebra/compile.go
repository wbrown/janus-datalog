package algebra

import (
	"fmt"
	"sort"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// Compile converts a prepared query's WHERE clauses into a relational algebra
// tree. The resulting tree can be optimized via transform passes before
// execution. Every nested form of the query states its interface in its
// header, so each compiles to one operator keyed on that header; the engine
// infers nothing, and its output is a function of the text alone. Clauses
// fold in dependency-honoring order, not source order — clause order in
// query text carries no meaning, and the fold requires binders before their
// consumers (see orderClausesForCompile).
//
// The query's :in symbols are the bound environment: an input-bound
// correlate is bindable. rules is the rule set % is bound to, under which an
// invocation consumes the variables at its predicate's required positions,
// so the fold orders it after their binders.
func Compile(q *query.Query, rules *query.RuleSet) (*Node, error) {
	if len(q.Where) == 0 {
		return nil, fmt.Errorf("empty WHERE clause")
	}
	bound := query.ConstantInputSymbols(q.In)
	for sym := range query.MultiValuedInputSymbols(q.In) {
		bound[sym] = true
	}
	return compileClausesFrom(q.Where, nil, query.Bindings{Symbols: bound, Rules: rules})
}

// compileClausesFrom builds an algebra tree starting with an initial relation.
// Used by branch compilation to thread a schema placeholder into branches so
// that NOT clauses have a relation to anti-join against.
//
// The clauses fold in the order orderClausesForCompile returns. scope's
// symbols are what is bound around the list: the query's :in symbols and the
// caller's at the top level, a header's bound variables inside a header form.
// They feed ordering, and — when the clause list contains no generator at
// all — they become the prior relation the fold starts from: a clause list
// that is consumer-only filters the bound environment, so the fold seeds a
// childless Project carrying those symbols — the schema-placeholder idiom,
// which decompiles to nothing. Ordering prefers generators for the first
// pick, so the placeholder appears only for genuinely generator-free lists;
// the folded tree never embeds environment scans. An initial relation's
// symbols are within scope's.
func compileClausesFrom(clauses []query.Clause, initial *Node, scope query.Bindings) (*Node, error) {
	ordered, err := orderClausesForCompile(clauses, scope)
	if err != nil {
		return nil, err
	}

	current := initial
	if current == nil && len(ordered) > 0 && len(query.ScopeOf(ordered[0], scope.Rules).Provides) == 0 {
		if env := environmentSymbols(scope.Symbols); len(env) > 0 {
			current = &Node{Op: RuleProject, Data: &Project{Symbols: env}}
		}
	}
	for _, clause := range ordered {
		node, err := compileClause(clause, current, scope)
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

// environmentSymbols lists the symbols bound around a clause list in a
// deterministic order (map iteration must not leak into the tree).
func environmentSymbols(bound map[query.Symbol]bool) []query.Symbol {
	env := make([]query.Symbol, 0, len(bound))
	for sym := range bound {
		env = append(env, sym)
	}
	sort.Slice(env, func(i, j int) bool { return env[i].Compare(env[j]) < 0 })
	return env
}

// bodyScope is the scope a header form's body compiles in: the header
// variables bound before the form runs, and the rule set. Nothing else of
// the enclosing scope reaches the body, the query's inputs included.
func bodyScope(bound []query.Symbol, scope query.Bindings) query.Bindings {
	symbols := make(map[query.Symbol]bool, len(bound))
	for _, sym := range bound {
		symbols[sym] = true
	}
	return query.Bindings{Symbols: symbols, Rules: scope.Rules}
}

// compileClause compiles a single clause, potentially joining it with
// the current accumulated relation. scope threads to the compound forms,
// whose bodies order their own clause lists.
func compileClause(clause query.Clause, current *Node, scope query.Bindings) (*Node, error) {
	switch c := clause.(type) {
	case *query.DataPattern:
		return compileDataPattern(c, current), nil

	case *query.Expression:
		return compileExpression(c, current), nil

	case *query.SubqueryPattern:
		return compileSubquery(c, current, scope), nil

	case *query.RuleInvocation:
		return joinWith(current, &Node{
			Op:   RuleDerived,
			Data: &Derived{Invocation: c, Output: c.Variables()},
		}), nil

	case *query.NotJoinClause:
		return compileNotJoin(c, current, scope)

	case *query.OrJoinClause:
		return compileOrJoin(c, current, scope)

	case *query.OrDefaultJoinClause:
		return compileOrDefaultJoin(c, current, scope)

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
	bindingSyms := expr.BoundVariables()

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
// A call passing % carries the rule set % is bound to in scope, so a pass
// that compiles the inner query compiles it under that rule set.
func compileSubquery(sp *query.SubqueryPattern, current *Node, scope query.Bindings) *Node {
	bindingSyms := sp.Binding.BoundVariables()

	var rules *query.RuleSet
	if query.RulesUse([]query.Clause{sp}) != nil {
		rules = scope.Rules
	}

	// Determine correlation variables: inputs that are variable symbols
	// (excluding source symbols like $ which are always available)
	var correlationVars []query.Symbol
	for _, input := range sp.Inputs {
		if v, ok := input.(query.Variable); ok {
			if !v.Name.IsSource() {
				correlationVars = append(correlationVars, v.Name)
			}
		}
	}

	output := lateralJoinOutput(symbolsOf(current), current != nil, correlationVars, bindingSyms)

	if len(correlationVars) > 0 {
		// Correlated subquery → LateralJoin
		lj := &Node{
			Op: RuleLateralJoin,
			Data: &LateralJoin{
				CorrelationVars: correlationVars,
				Inputs:          sp.Inputs,
				InnerQuery:      sp.Query,
				Binding:         sp.Binding,
				Output:          output,
				Rules:           rules,
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
			Inputs:     sp.Inputs,
			InnerQuery: sp.Query,
			Binding:    sp.Binding,
			Output:     output,
			Rules:      rules,
		},
	}
	if current != nil {
		lj.Children = []*Node{current}
	}
	return lj
}

// compileNotJoin produces an AntiJoin keyed on the not-join's header. The
// body compiles under the header alone, so a body whose first clause binds
// nothing starts from the header's variables.
func compileNotJoin(nj *query.NotJoinClause, current *Node, scope query.Bindings) (*Node, error) {
	if current == nil {
		return nil, fmt.Errorf("NOT-JOIN clause requires prior relation")
	}

	inner, err := compileClausesFrom(nj.Clauses, nil, bodyScope(nj.RequiredVars, scope))
	if err != nil {
		return nil, fmt.Errorf("NOT-JOIN inner clauses: %w", err)
	}
	if _, err := Analyze(inner); err != nil {
		return nil, fmt.Errorf("NOT-JOIN inner analysis: %w", err)
	}
	// A header symbol the body does not produce is a correlation input the
	// body consumes.
	var required []query.Symbol
	for _, symbol := range nj.RequiredVars {
		if query.ContainsSymbol(inner.Symbols(), symbol) {
			continue
		}
		required = append(required, symbol)
	}

	return &Node{
		Op: RuleAntiJoin,
		Data: &AntiJoin{
			JoinSymbols: nj.RequiredVars,
			Required:    required,
			Output:      current.Symbols(),
		},
		Children: []*Node{current, inner},
	}, nil
}

// compileOrJoin handles OR-JOIN clauses — union semantics keyed on the
// header. Branches compile under the header's required run, the variables
// bound before the form runs, so a branch variable outside the header is
// local. When branches contain correlated predicates (NOT, missing?), the
// correlated route compiles them against a schema placeholder over the
// required run, so anti-joins have a relation without embedding outer scans.
func compileOrJoin(oj *query.OrJoinClause, current *Node, scope query.Bindings) (*Node, error) {
	branchScope := bodyScope(oj.RequiredVars, scope)
	if branchesRequireOuterContext(oj.Branches) {
		return compileOrUnionCorrelated(oj, current, branchScope)
	}
	return compileOrUnionDirect(oj, current, branchScope)
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
		case *query.NotJoinClause:
			return true
		case *query.MissingPredicate:
			return true
		case *query.OrJoinClause:
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

// compileOrDefaultJoin handles OR-DEFAULT-JOIN clauses — fallback with a
// declared required/output interface.
func compileOrDefaultJoin(oj *query.OrDefaultJoinClause, current *Node, scope query.Bindings) (*Node, error) {
	return compileOrFallbackWithVars(oj, current, scope)
}

// orJoinUnion is the Union an or-join's compiled branches form. Its output
// is the whole header, it carries the header's two halves, the required run
// and the outputs, and it requires the required run and the outputs the
// outer relation already binds.
func orJoinUnion(oj *query.OrJoinClause, current *Node, children []*Node) *Node {
	header := mergeSymbols(oj.RequiredVars, oj.OutputVars)
	required := cloneSymbols(oj.RequiredVars)
	for _, symbol := range oj.OutputVars {
		if query.ContainsSymbol(symbolsOf(current), symbol) && !query.ContainsSymbol(required, symbol) {
			required = append(required, symbol)
		}
	}
	return &Node{
		Op: RuleUnion,
		Data: &Union{
			Output:       header,
			Required:     required,
			RequiredVars: cloneSymbols(oj.RequiredVars),
			OutputVars:   cloneSymbols(oj.OutputVars),
		},
		Children: children,
	}
}

// compileOrUnionDirect compiles an or-join whose branches hold no not-join
// and no missing? at any depth: each branch compiles from nothing under the
// required run, and the branches form the Union orJoinUnion builds.
// compileOrUnionCorrelated is the route for the other branches.
func compileOrUnionDirect(oj *query.OrJoinClause, current *Node, branchScope query.Bindings) (*Node, error) {
	children := make([]*Node, 0, len(oj.Branches))
	for i, branch := range oj.Branches {
		compiled, err := compileClausesFrom(branch, nil, branchScope)
		if err != nil {
			return nil, fmt.Errorf("OR branch %d: %w", i+1, err)
		}
		children = append(children, compiled)
	}
	return joinWith(current, orJoinUnion(oj, current, children)), nil
}

// compileOrUnionCorrelated compiles an or-join whose branches contain
// correlated predicates (NOT, missing?). Branches compile against a schema
// placeholder over the required run, so anti-joins see the variables bound
// before the form without embedding outer scans. The fallback machinery is
// not a valid target here: fallback short-circuits per group, union does
// not, and encoding union through it drops tuples (see
// docs/bugs/resolved/BUG_CORRELATED_ORJOIN_GLOBAL_FALLBACK_DROPS_TUPLES.md).
// Correlated union has no semantics-preserving rewrite, so the node
// decompiles back to the original clause and execution uses the executor's
// or-join path.
func compileOrUnionCorrelated(oj *query.OrJoinClause, current *Node, branchScope query.Bindings) (*Node, error) {
	var initial *Node
	if len(oj.RequiredVars) > 0 {
		initial = &Node{
			Op:   RuleProject,
			Data: &Project{Symbols: cloneSymbols(oj.RequiredVars)},
		}
	}
	compiled := make([]*Node, len(oj.Branches))
	for i, branch := range oj.Branches {
		node, err := compileClausesFrom(branch, initial, branchScope)
		if err != nil {
			return nil, fmt.Errorf("OR branch %d: %w", i+1, err)
		}
		compiled[i] = node
	}
	return joinWith(current, orJoinUnion(oj, current, compiled)), nil
}

// compileOrFallbackWithVars handles OR-fallback, preserving or-default-join's
// declared required/output interface through the IR. Branches compile under
// the header's required run.
//
// The subquery lowering applies when branch 1 is one subquery and branch 2 is
// grounds alone: the fallback is then the subquery's result or, where it
// yields nothing, the grounds, a LateralJoin with defaults. Every other shape
// falls back per tuple through a LateralUnion, since a clause beside the
// subquery or the grounds would otherwise be applied outside the fallback or
// not at all.
func compileOrFallbackWithVars(oj *query.OrDefaultJoinClause, current *Node, scope query.Bindings) (*Node, error) {
	branches := oj.Branches
	if len(branches) < 2 {
		return nil, fmt.Errorf("OR fallback requires at least 2 branches")
	}
	branchScope := bodyScope(oj.RequiredVars, scope)

	var defaults []Default
	isSubqueryFallback := false
	if len(branches) == 2 && len(branches[0]) == 1 {
		if _, isSubquery := branches[0][0].(*query.SubqueryPattern); isSubquery {
			defaults = extractGroundDefaults(branches[1])
			isSubqueryFallback = defaults != nil
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
		union, err := compileOrFallbackExclusive(oj, current, branchScope)
		if err != nil {
			return nil, err
		}
		return joinWith(current, union), nil
	}

	// Compile the subquery branch
	subNode, err := compileClausesFrom(branches[0], nil, branchScope)
	if err != nil {
		return nil, fmt.Errorf("OR fallback subquery branch: %w", err)
	}

	// Attach defaults based on subquery type
	if lj := findLateralJoin(subNode); lj != nil {
		// Correlated subquery → LateralJoin with defaults
		ljData := lj.Data.(*LateralJoin)
		ljData.Defaults = defaults
		for _, d := range defaults {
			if !query.ContainsSymbol(ljData.Output, d.Symbol) {
				ljData.Output = append(ljData.Output, d.Symbol)
			}
		}
	} else if len(defaults) > 0 {
		// Uncorrelated subquery → wrap in LeftOuterJoin with defaults.
		// The subquery returns results for some outer tuples; non-matching
		// outer tuples get default values via the LeftOuterJoin.
		joinSyms := sharedSymbols(symbolsOf(current), subNode.Symbols())
		subNode = &Node{
			Op: RuleJoin,
			Data: &Join{
				Kind:        LeftOuterJoin,
				JoinSymbols: joinSyms,
				Output:      mergeSymbols(symbolsOf(current), subNode.Symbols()),
				Defaults:    defaults,
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
//
// The outer schema is the outer relation's symbols and the required run,
// which the outer relation or the environment binds. Branches see the
// required run alone.
func compileOrFallbackExclusive(oj *query.OrDefaultJoinClause, current *Node, branchScope query.Bindings) (*Node, error) {
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
	if len(oj.Branches) < 2 {
		return nil, fmt.Errorf("OR fallback requires at least 2 branches")
	}
	outer := mergeSymbols(current.Symbols(), oj.RequiredVars)

	// Schema placeholder: provides the required run for NOT/missing?
	// compilation without embedding the outer's scan nodes. Decompiles to
	// nothing (Project with no children → nil clauses).
	branchSchema := &Node{
		Op:   RuleProject,
		Data: &Project{Symbols: cloneSymbols(oj.RequiredVars)},
	}

	// Compile each branch with the schema placeholder as context.
	// NOT and missing? get the symbols they need for anti-join, but
	// the decompiler won't emit the outer scan patterns.
	compiled := make([]*Node, len(oj.Branches))
	for i, branch := range oj.Branches {
		node, err := compileClausesFrom(branch, branchSchema, branchScope)
		if err != nil {
			return nil, fmt.Errorf("OR fallback branch %d: %w", i+1, err)
		}
		compiled[i] = node
	}

	output := normalizedBranchSymbols(compiled, outer)

	lateralUnion := &Node{
		Op: RuleLateralUnion,
		Data: &LateralUnion{
			Output:       output,
			RequiredVars: oj.RequiredVars,
			OutputVars:   oj.OutputVars,
			Required:     outer,
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

// lateralJoinOutput is the output of a lateral join: the outer relation's
// symbols and the binding's symbols. A correlation variable the outer
// relation does not carry is bound by the enclosing scope and is the join's
// free requirement, not its output; with no outer relation the correlation
// variables are the output's, since nothing else carries them into the
// result. The compiler, the analyzer and the schema refresh read this one
// rule.
func lateralJoinOutput(outer []query.Symbol, hasOuter bool, correlation, bound []query.Symbol) []query.Symbol {
	output := cloneSymbols(outer)
	if !hasOuter {
		output = mergeSymbols(output, correlation)
	}
	return mergeSymbols(output, bound)
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
		syms = append(syms, c.BoundVariables()...)
		return syms
	case *query.SubqueryPattern:
		var syms []query.Symbol
		// Include input variables (correlation vars that link to outer scope)
		for _, input := range c.Inputs {
			if v, ok := input.(query.Variable); ok {
				if !v.Name.IsSource() {
					syms = append(syms, v.Name)
				}
			}
		}
		syms = append(syms, c.Binding.BoundVariables()...)
		return syms
	case *query.RuleInvocation:
		return c.Variables()
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

// extractGroundDefaults is the defaults a branch of ground expressions alone
// states, one per symbol the grounds bind, each with its value. Returns nil
// when the branch holds any other clause, a ground under the collection or
// relation form, or a tuple-form ground whose value is not a vector of its
// arity: a guard or filter beside the grounds decides which tuples take the
// default, and a ground yielding a default tuple per element states no one
// default per symbol.
func extractGroundDefaults(branch []query.Clause) []Default {
	var defaults []Default

	for _, c := range branch {
		g, isExpression := c.(*query.Expression)
		if !isExpression {
			return nil
		}
		gf, isGround := g.Function.(*query.GroundFunction)
		if !isGround {
			return nil
		}
		// Each ground states one default per symbol its binding names: the
		// whole value under a scalar binding, one element per variable under
		// the tuple form. The collection and relation forms yield a default
		// tuple per element, which one default per symbol cannot state.
		switch binding := g.Binding.(type) {
		case query.Symbol:
			defaults = append(defaults, Default{Symbol: binding, Value: gf.Value})
		case query.TupleBinding:
			values, isVector := query.VectorElements(gf.Value)
			if !isVector || len(values) != len(binding.Variables) {
				return nil
			}
			for i, sym := range binding.Variables {
				defaults = append(defaults, Default{Symbol: sym, Value: values[i]})
			}
		default:
			return nil
		}
	}

	return defaults
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
