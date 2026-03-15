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
	case RuleLateralJoin:
		return decompileLateralJoin(n)
	case RuleAggregate:
		return decompileAggregate(n)
	case RuleConstant:
		return decompileConstant(n)
	case RuleProject:
		// Project doesn't produce clauses — pass through child
		if len(n.Children) > 0 {
			return decompileNode(n.Children[0])
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown algebra node: %s", n.Op)
	}
}

// decompileScan emits the original DataPattern, or a decorrelated SubqueryPattern.
func decompileScan(n *Node) ([]query.Clause, error) {
	// Check for decorrelated scan (produced by decorrelation pass)
	if ds, ok := n.Data.(*decorrelatedScan); ok {
		return decompileDecorrelatedScan(ds)
	}

	scan := n.Data.(*Scan)
	if scan.Pattern == nil {
		return nil, nil // placeholder node, no clause
	}
	return []query.Clause{scan.Pattern}, nil
}

// decompileDecorrelatedScan emits a decorrelated SubqueryPattern wrapped in
// an OR-fallback clause with ground defaults.
//
// The decorrelated subquery has no correlation inputs (only $), so it runs
// once and produces all matching groups via RelationBinding. The OR-fallback
// preserves "try subquery, else use default" per outer tuple — for tuples
// where the subquery produced no matching group, the default fires.
//
// This is semantically identical to the original correlated OR-fallback,
// but the subquery execution is O(1) (runs once, result cached) instead of
// O(N) (runs per outer tuple).
func decompileDecorrelatedScan(ds *decorrelatedScan) ([]query.Clause, error) {
	if len(ds.DefaultValues) == 0 {
		// No defaults — bare decorrelated subquery
		return []query.Clause{ds.SubqueryPattern}, nil
	}

	// Branch 1: the decorrelated subquery
	subqueryBranch := []query.Clause{ds.SubqueryPattern}

	// Branch 2: ground defaults for original binding symbols
	var defaultBranch []query.Clause
	for i, sym := range ds.OriginalBinding {
		var val interface{}
		if i < len(ds.DefaultValues) {
			val = ds.DefaultValues[i]
		}
		defaultBranch = append(defaultBranch, &query.Expression{
			Function: query.GroundFunction{Value: val},
			Binding:  sym,
		})
	}

	return []query.Clause{
		&query.OrClause{
			Branches: [][]query.Clause{subqueryBranch, defaultBranch},
		},
	}, nil
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

// decompileLeftOuterJoin emits an OR clause with fallback semantics.
func decompileLeftOuterJoin(n *Node) ([]query.Clause, error) {
	if len(n.Children) < 2 {
		return nil, fmt.Errorf("LeftOuterJoin requires 2 children")
	}

	var branches [][]query.Clause
	for _, child := range n.Children {
		branch, err := decompileNode(child)
		if err != nil {
			return nil, err
		}
		branches = append(branches, branch)
	}

	return []query.Clause{&query.OrClause{Branches: branches}}, nil
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

// decompileUnion emits an OR clause with union semantics.
func decompileUnion(n *Node) ([]query.Clause, error) {
	var branches [][]query.Clause
	for _, child := range n.Children {
		branch, err := decompileNode(child)
		if err != nil {
			return nil, err
		}
		branches = append(branches, branch)
	}
	return []query.Clause{&query.OrClause{Branches: branches}}, nil
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

	// Build the SubqueryPattern
	inputs := make([]query.PatternElement, 0, len(lj.CorrelationVars)+1)
	inputs = append(inputs, query.Variable{Name: datalog.SymDollar})
	for _, cv := range lj.CorrelationVars {
		inputs = append(inputs, query.Variable{Name: cv})
	}

	sp := &query.SubqueryPattern{
		Query:   lj.InnerQuery,
		Inputs:  inputs,
		Binding: lj.Binding.(query.BindingForm),
	}

	if len(lj.DefaultValues) == 0 {
		// No defaults — bare subquery
		clauses = append(clauses, sp)
		return clauses, nil
	}

	// Has defaults — wrap in OR-fallback
	// Branch 1: the subquery
	subqueryBranch := []query.Clause{sp}

	// Branch 2: ground defaults
	bindingSyms := bindingFormSymbols(sp.Binding)
	var defaultBranch []query.Clause
	if len(lj.DefaultValues) == 1 && len(bindingSyms) == 1 {
		// Scalar ground: [(ground val) ?sym]
		defaultBranch = []query.Clause{
			&query.Expression{
				Function: query.GroundFunction{Value: lj.DefaultValues[0]},
				Binding:  bindingSyms[0],
			},
		}
	} else {
		// Tuple ground: [(ground [v1 v2 ...]) [[?a ?b ...]]]
		defaultBranch = []query.Clause{
			&query.Expression{
				Function: query.GroundFunction{Value: lj.DefaultValues},
				Binding:  query.TupleBinding{Variables: bindingSyms},
			},
		}
	}

	orClause := &query.OrClause{
		Branches: [][]query.Clause{subqueryBranch, defaultBranch},
	}
	clauses = append(clauses, orClause)
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

	// Build the decorrelated query
	innerQuery := &query.Query{
		Find:  findElems,
		Where: innerClauses,
	}

	// Build binding: tuple binding with GroupBy keys + aggregate output symbols
	sp := &query.SubqueryPattern{
		Query:   innerQuery,
		Inputs:  []query.PatternElement{query.Variable{Name: datalog.SymDollar}},
		Binding: query.TupleBinding{Variables: agg.Output},
	}

	return []query.Clause{sp}, nil
}

// decompileConstant emits a ground expression.
func decompileConstant(n *Node) ([]query.Clause, error) {
	c := n.Data.(*Constant)

	if len(c.Values) == 1 && len(c.Symbols) == 1 {
		return []query.Clause{
			&query.Expression{
				Function: query.GroundFunction{Value: c.Values[0]},
				Binding:  c.Symbols[0],
			},
		}, nil
	}

	return []query.Clause{
		&query.Expression{
			Function: query.GroundFunction{Value: c.Values},
			Binding:  query.TupleBinding{Variables: c.Symbols},
		},
	}, nil
}
