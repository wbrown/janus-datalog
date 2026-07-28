package algebra

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Decompile converts an algebra tree back to a list of Datalog clauses.
// This is the final step: clauses → algebra → optimize → algebra → clauses.
// The existing executor consumes the rewritten clauses unchanged.
func Decompile(root *Node) ([]query.Clause, error) {
	if root == nil {
		return nil, nil
	}
	return decompileNode(root)
}

func decompileNode(n *Node) ([]query.Clause, error) {
	switch n.Op {
	case RuleScan:
		return decompileScan(n)
	case RuleSelect:
		return decompileSelect(n)
	case RuleMap:
		return decompileMap(n)
	case RuleJoin:
		return decompileJoin(n)
	case RuleAntiJoin:
		return decompileAntiJoin(n)
	case RuleUnion:
		return decompileUnion(n)
	case RuleLateralUnion:
		return decompileLateralUnion(n)
	case RuleLateralJoin:
		return decompileLateralJoin(n)
	case RuleAggregate:
		return decompileAggregate(n)
	case RuleConstant:
		return decompileConstant(n)
	case RuleProject:
		return decompileProject(n)
	default:
		return nil, fmt.Errorf("unknown algebra node: %s", n.Op)
	}
}

func decompileProject(n *Node) ([]query.Clause, error) {
	project := n.Data.(*Project)
	if len(n.Children) == 0 {
		return nil, nil
	}
	if len(n.Children) != 1 {
		return nil, fmt.Errorf("Project requires 1 child")
	}
	where, err := decompileNode(n.Children[0])
	if err != nil {
		return nil, err
	}
	find := make([]query.FindElement, len(project.Symbols))
	for i, symbol := range project.Symbols {
		find[i] = query.FindVariable{Symbol: symbol}
	}
	return []query.Clause{&query.SubqueryPattern{
		Query: &query.Query{
			Find:  find,
			In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
			Where: where,
		},
		Inputs: []query.PatternElement{query.Constant{Value: datalog.SymDollar}},
		Binding: query.RelationBinding{
			Variables: append([]query.Symbol(nil), project.Symbols...),
		},
	}}, nil
}

// decompileScan emits the original DataPattern.
func decompileScan(n *Node) ([]query.Clause, error) {
	scan := n.Data.(*Scan)
	if scan.Pattern == nil {
		return nil, nil // placeholder node, no clause
	}
	return []query.Clause{scan.Pattern}, nil
}

// decompileSelect emits child clauses followed by the predicate.
func decompileSelect(n *Node) ([]query.Clause, error) {
	sel := n.Data.(*Select)
	var clauses []query.Clause
	for _, child := range n.Children {
		childClauses, err := decompileNode(child)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, childClauses...)
	}
	clauses = append(clauses, sel.Predicate)
	return clauses, nil
}

// decompileMap emits child clauses followed by the expression.
func decompileMap(n *Node) ([]query.Clause, error) {
	m := n.Data.(*Map)
	var clauses []query.Clause
	for _, child := range n.Children {
		childClauses, err := decompileNode(child)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, childClauses...)
	}
	clauses = append(clauses, m.Expression)
	return clauses, nil
}

// decompileJoin emits clauses from both sides.
// Inner join = both sides' clauses concatenated (the executor joins on shared symbols).
// Left outer join = OR-fallback clause wrapping both sides.
func decompileJoin(n *Node) ([]query.Clause, error) {
	join := n.Data.(*Join)

	if join.Kind == LeftOuterJoin {
		return decompileLeftOuterJoin(n)
	}

	// Inner join: emit all children's clauses in order
	var clauses []query.Clause
	for _, child := range n.Children {
		childClauses, err := decompileNode(child)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, childClauses...)
	}
	return clauses, nil
}

// decompileLeftOuterJoin emits left clauses followed by an OR-fallback.
//
// Per ALGEBRA.md Rule 1, a LeftOuterJoin is produced by decorrelation:
//
//	LeftOuterJoin(on=x, defaults=D) [R, Aggregate(x, F)(S)]
//
// Decompiles to:
//
//	<R clauses>
//	(or <Aggregate → SubqueryPattern>
//	    <ground D>)
//
// The SubqueryPattern runs once (uncorrelated). The OR-fallback evaluates
// per outer tuple: filterBranchToOuterTuple matches on shared symbols,
// non-matching tuples get defaults.
//
// LeftOuterJoin ALWAYS has defaults (the decorrelation transform produces
// InnerJoin when there are no defaults). If defaults are missing, fall
// back to emitting both sides as an OR clause.
func decompileLeftOuterJoin(n *Node) ([]query.Clause, error) {
	join := n.Data.(*Join)
	if len(n.Children) < 2 {
		return nil, fmt.Errorf("LeftOuterJoin requires 2 children")
	}

	// Left child = outer relation (emitted as prefix clauses)
	leftClauses, err := decompileNode(n.Children[0])
	if err != nil {
		return nil, err
	}

	// Right child = inner relation (becomes branch 1 of OR-fallback)
	rightClauses, err := decompileNode(n.Children[1])
	if err != nil {
		return nil, err
	}

	if len(join.DefaultValues) == 0 {
		return nil, fmt.Errorf("LeftOuterJoin has no defaults (decorrelation produces InnerJoin when no defaults)")
	}

	// Build ground default branch from Join.DefaultValues.
	// The defaults correspond to the original LateralJoin's binding symbols —
	// the aggregate output symbols minus the GROUP BY keys. These are the
	// symbols that the OR-fallback's ground branch must provide.
	//
	// The right child (Aggregate) decompiles to a SubqueryPattern with
	// RelationBinding [[?joinKey ?agg1 ?agg2 ...]]. The default branch must
	// provide the non-join-key symbols: [?agg1, ?agg2, ...].
	rightSyms := n.Children[1].Symbols()
	joinSet := make(map[query.Symbol]bool, len(join.JoinSymbols))
	for _, s := range join.JoinSymbols {
		joinSet[s] = true
	}
	var defaultSyms []query.Symbol
	for _, s := range rightSyms {
		if !joinSet[s] {
			defaultSyms = append(defaultSyms, s)
		}
	}

	var defaultBranch []query.Clause
	if join.DefaultAttr != nil && len(join.DefaultValues) == 1 && len(defaultSyms) == 1 && len(join.JoinSymbols) > 0 {
		// Get-else rewrite origin: emit get-else in the default branch so
		// TypedDefaulter can convert the default to the schema type (e.g.,
		// []interface{} → []string). The default branch only runs for entities
		// that the Scan didn't match, so get-else returns the typed default.
		defaultBranch = []query.Clause{
			&query.Expression{
				Function: &query.GetElseFunction{
					Entity:  query.VariableTerm{Symbol: join.JoinSymbols[0]},
					Attr:    *join.DefaultAttr,
					Default: join.DefaultValues[0],
				},
				Binding: defaultSyms[0],
			},
		}
	} else if len(join.DefaultValues) == 1 && len(defaultSyms) == 1 {
		defaultBranch = []query.Clause{
			&query.Expression{
				Function: &query.GroundFunction{Value: join.DefaultValues[0]},
				Binding:  defaultSyms[0],
			},
		}
	} else {
		defaultBranch = []query.Clause{
			&query.Expression{
				Function: &query.GroundFunction{Value: join.DefaultValues},
				Binding:  query.TupleBinding{Variables: defaultSyms},
			},
		}
	}

	// Declare the interface: the join symbols are the per-group correlation
	// keys the outer relation must bind (so the phaser schedules this after
	// the DataPattern that provides them), and the default symbols are the
	// outputs every branch binds.
	orDefaultJoinClause := &query.OrDefaultJoinClause{
		RequiredVars: join.JoinSymbols,
		OutputVars:   defaultSyms,
		Branches:     [][]query.Clause{rightClauses, defaultBranch},
	}

	return append(leftClauses, orDefaultJoinClause), nil
}

// decompileAntiJoin emits child clauses from left, then a NOT clause wrapping right.
func decompileAntiJoin(n *Node) ([]query.Clause, error) {
	aj := n.Data.(*AntiJoin)
	if len(n.Children) < 2 {
		return nil, fmt.Errorf("AntiJoin requires 2 children")
	}

	leftClauses, err := decompileNode(n.Children[0])
	if err != nil {
		return nil, err
	}

	rightClauses, err := decompileNode(n.Children[1])
	if err != nil {
		return nil, err
	}

	var notClause query.Clause
	if aj.ExplicitJoin {
		notClause = &query.NotJoinClause{
			JoinVars: aj.JoinSymbols,
			Clauses:  rightClauses,
		}
	} else {
		notClause = &query.NotClause{
			Clauses: rightClauses,
		}
	}

	return append(leftClauses, notClause), nil
}

// decompileUnion emits an OR clause (or OR-join clause if join vars present).
// Union nodes always decompile to OrClause/OrJoinClause (independent union).
func decompileUnion(n *Node) ([]query.Clause, error) {
	u := n.Data.(*Union)
	var branches [][]query.Clause
	for _, child := range n.Children {
		branch, err := decompileNode(child)
		if err != nil {
			return nil, err
		}
		branches = append(branches, branch)
	}
	if len(u.JoinVars) > 0 {
		return []query.Clause{&query.OrJoinClause{
			JoinVars: u.JoinVars,
			Branches: branches,
		}}, nil
	}
	return []query.Clause{&query.OrClause{Branches: branches}}, nil
}

// decompileLateralUnion emits an OR-DEFAULT clause (or OR-DEFAULT-JOIN if join vars present).
// LateralUnion nodes represent correlated per-tuple branch evaluation.
func decompileLateralUnion(n *Node) ([]query.Clause, error) {
	lu := n.Data.(*LateralUnion)
	var branches [][]query.Clause
	for _, child := range n.Children {
		branch, err := decompileNode(child)
		if err != nil {
			return nil, err
		}
		branches = append(branches, branch)
	}
	if len(lu.RequiredVars) > 0 || len(lu.OutputVars) > 0 {
		return []query.Clause{&query.OrDefaultJoinClause{
			RequiredVars: lu.RequiredVars,
			OutputVars:   lu.OutputVars,
			Branches:     branches,
		}}, nil
	}
	return []query.Clause{&query.OrDefaultClause{Branches: branches}}, nil
}

// decompileLateralJoin emits a SubqueryPattern clause.
// If the LateralJoin has defaults (from OR-fallback), wraps in an OR clause
// with a ground fallback branch.
func decompileLateralJoin(n *Node) ([]query.Clause, error) {
	lj := n.Data.(*LateralJoin)

	// Emit child clauses first (the outer relation)
	var clauses []query.Clause
	for _, child := range n.Children {
		childClauses, err := decompileNode(child)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, childClauses...)
	}

	// Rebuild the SubqueryPattern from the preserved call-site arguments —
	// they carry constants and named sources that CorrelationVars cannot
	// express. Fabricate [$ ...correlation vars] only for nodes constructed
	// without an original call site.
	inputs := lj.Inputs
	if inputs == nil {
		inputs = make([]query.PatternElement, 0, len(lj.CorrelationVars)+1)
		inputs = append(inputs, query.Constant{Value: datalog.SymDollar})
		for _, cv := range lj.CorrelationVars {
			inputs = append(inputs, query.Variable{Name: cv})
		}
	}

	sp := &query.SubqueryPattern{
		Query:   lj.InnerQuery,
		Inputs:  inputs,
		Binding: bindingAsForm(lj.Binding),
	}

	if len(lj.DefaultValues) == 0 {
		// No defaults — bare subquery
		clauses = append(clauses, sp)
		return clauses, nil
	}

	// Has defaults — wrap in OR-fallback
	// Branch 1: the subquery
	subqueryBranch := []query.Clause{sp}

	// Compute join vars FIRST — needed to determine default binding symbols.
	// Join vars = symbols shared between the subquery binding and the outer
	// relation. The default branch only provides NON-join symbols; the join
	// keys come from the outer relation via or-join semantics.
	bindingSyms := sp.Binding.BoundVariables()

	var joinVars []query.Symbol
	if len(lj.CorrelationVars) > 0 {
		joinVars = lj.CorrelationVars
	} else {
		// Uncorrelated: use symbols shared between binding and outer
		var outerSyms []query.Symbol
		for _, child := range n.Children {
			outerSyms = append(outerSyms, child.Symbols()...)
		}
		for _, bs := range bindingSyms {
			for _, os := range outerSyms {
				if bs == os {
					joinVars = append(joinVars, bs)
				}
			}
		}
	}

	// Default symbols = binding symbols minus join vars.
	// The join keys come from the outer relation; the default branch
	// only needs to provide values for the non-join symbols.
	joinSet := make(map[query.Symbol]bool, len(joinVars))
	for _, jv := range joinVars {
		joinSet[jv] = true
	}
	var defaultSyms []query.Symbol
	for _, bs := range bindingSyms {
		if !joinSet[bs] {
			defaultSyms = append(defaultSyms, bs)
		}
	}

	// Branch 2: ground defaults using only non-join symbols
	var defaultBranch []query.Clause
	if len(lj.DefaultValues) == 1 && len(defaultSyms) == 1 {
		// Scalar ground: [(ground val) ?sym]
		defaultBranch = []query.Clause{
			&query.Expression{
				Function: &query.GroundFunction{Value: lj.DefaultValues[0]},
				Binding:  defaultSyms[0],
			},
		}
	} else if len(defaultSyms) > 0 {
		// Tuple ground: [(ground [v1 v2 ...]) [[?a ?b ...]]]
		defaultBranch = []query.Clause{
			&query.Expression{
				Function: &query.GroundFunction{Value: lj.DefaultValues},
				Binding:  query.TupleBinding{Variables: defaultSyms},
			},
		}
	}

	if len(joinVars) > 0 {
		orDefaultJoinClause := &query.OrDefaultJoinClause{
			RequiredVars: joinVars,
			OutputVars:   defaultSyms,
			Branches:     [][]query.Clause{subqueryBranch, defaultBranch},
		}
		clauses = append(clauses, orDefaultJoinClause)
	} else {
		orDefaultClause := &query.OrDefaultClause{
			Branches: [][]query.Clause{subqueryBranch, defaultBranch},
		}
		clauses = append(clauses, orDefaultClause)
	}
	return clauses, nil
}

// decompileAggregate emits a decorrelated SubqueryPattern.
// This is the output of the decorrelation transform — a subquery that
// runs once with GROUP BY, producing all groups in one pass.
func decompileAggregate(n *Node) ([]query.Clause, error) {
	agg := n.Data.(*Aggregate)

	// The Aggregate node wraps a child that produces the ungrouped relation.
	// Decompile the child into the subquery's WHERE clauses.
	var innerClauses []query.Clause
	for _, child := range n.Children {
		childClauses, err := decompileNode(child)
		if err != nil {
			return nil, err
		}
		innerClauses = append(innerClauses, childClauses...)
	}

	// Build find elements: GroupBy keys as variables, then aggregate functions
	var findElems []query.FindElement
	for _, key := range agg.GroupBy {
		findElems = append(findElems, query.FindVariable{Symbol: key})
	}
	for _, fn := range agg.Functions {
		findElems = append(findElems, fn)
	}

	// Build the decorrelated query — must include :in $ for the database source
	innerQuery := &query.Query{
		Find:  findElems,
		In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
		Where: innerClauses,
	}

	// Build binding with the outer names corresponding positionally to the
	// grouped keys and aggregate results.
	// Uses Constant($) for the database source marker (not Variable).
	// RelationBinding (not TupleBinding) because the decorrelated query
	// returns multiple tuples (one per distinct group key).
	sp := &query.SubqueryPattern{
		Query:   innerQuery,
		Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}},
		Binding: query.RelationBinding{Variables: agg.Bindings},
	}

	return []query.Clause{sp}, nil
}

// decompileConstant emits a ground expression.
func decompileConstant(n *Node) ([]query.Clause, error) {
	c := n.Data.(*Constant)

	if len(c.Values) == 1 && len(c.Symbols) == 1 {
		return []query.Clause{
			&query.Expression{
				Function: &query.GroundFunction{Value: c.Values[0]},
				Binding:  c.Symbols[0],
			},
		}, nil
	}

	return []query.Clause{
		&query.Expression{
			Function: &query.GroundFunction{Value: c.Values},
			Binding:  query.TupleBinding{Variables: c.Symbols},
		},
	}, nil
}

// bindingAsForm safely converts a Binding interface{} to BindingForm.
func bindingAsForm(b interface{}) query.BindingForm {
	if bf, ok := b.(query.BindingForm); ok {
		return bf
	}
	return nil
}
