package algebra

import (
	"fmt"

	"github.com/wbrown/ebnf/parse"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// DecorrelationPass returns a transform pass that rewrites LateralJoin nodes
// into regular Join + decorrelated subquery.
//
// Algebraic rule: R ⋈_L S(r.x) → R ⋈ (S GROUP BY x)
//
// The inner query is rewritten:
//   - Correlation variable removed from :in
//   - Correlation variable added to :find as a grouping key
//   - Binding updated to include the correlation variable
//   - The subquery runs once for all values, producing groups
//   - Results joined back on the correlation variable
func DecorrelationPass(handler annotations.Handler) Pass {
	emit := func(name string, data map[string]interface{}) {
		if handler != nil {
			handler(annotations.Event{Name: name, Data: data})
		}
	}

	return Pass{
		Name: "decorrelation",
		Transforms: parse.TransformMap{
			RuleLateralJoin: makeDecorrelateTransform(emit),
		},
	}
}

// decorrelateTransform is the transform function for LateralJoin nodes.
// It receives the node's children as already-transformed values.
// If decorrelation is possible, it returns a rewritten algebra Node.
// Otherwise it returns the original node unchanged.
type emitFn func(name string, data map[string]interface{})

func makeDecorrelateTransform(emit emitFn) parse.TransformFunc {
	return func(ctx *parse.TransformContext, node *parse.Node, children ...interface{}) interface{} {
		return decorrelateTransform(ctx, node, emit, children...)
	}
}

func decorrelateTransform(ctx *parse.TransformContext, node *parse.Node, emit emitFn, children ...interface{}) interface{} {
	if node.TransformedValue == nil {
		return node
	}
	algNode, ok := node.TransformedValue.(*Node)
	if !ok {
		return node
	}
	lj, ok := algNode.Data.(*LateralJoin)
	if !ok {
		return node
	}

	// Decorrelation inside a Union is semantically incorrect: it moves the
	// correlation variable from input to output, creating a schema mismatch
	// with the ground branch. The ground branch lacks the correlation var,
	// causing a cross-product when the Union result is joined with the outer.
	if ctx.Parent != nil && ctx.Parent.Rule == RuleUnion {
		return rebuildWithChildren(node, children)
	}

	emit("algebra/decorrelate-check", map[string]interface{}{
		"correlation_vars": fmt.Sprintf("%v", lj.CorrelationVars),
		"has_aggregates":   hasAggregates(lj.InnerQuery),
		"has_defaults":     len(lj.DefaultValues) > 0,
		"should":           shouldDecorrelate(lj),
		"inner_query":      lj.InnerQuery.String(),
	})

	if !shouldDecorrelate(lj) {
		emit("algebra/decorrelate-skip", map[string]interface{}{
			"reason": "pure DataPattern query — indexed lookup is faster",
		})
		return rebuildWithChildren(node, children)
	}

	if len(lj.CorrelationVars) == 0 {
		emit("algebra/decorrelate-skip", map[string]interface{}{
			"reason": "no correlation variables",
		})
		return rebuildWithChildren(node, children)
	}

	innerParams := mapCorrelationToInnerParams(lj.InnerQuery, lj.CorrelationVars)
	if len(innerParams) == 0 {
		emit("algebra/decorrelate-skip", map[string]interface{}{
			"reason": "cannot map correlation to inner params",
		})
		return node
	}

	emit("algebra/decorrelate-apply", map[string]interface{}{
		"correlation_vars": fmt.Sprintf("%v", lj.CorrelationVars),
		"inner_params":     fmt.Sprintf("%v", innerParams),
		"has_aggregates":   hasAggregates(lj.InnerQuery),
	})

	// Decorrelate: remove correlation params from :in, add to :find.
	decorrelated := decorrelateQuery(lj.InnerQuery, innerParams)
	if decorrelated == nil {
		return node
	}

	// Output symbols: correlation vars (OUTER names) + original binding vars.
	originalBindingSyms := bindingFormSymbols(lj.Binding.(query.BindingForm))
	output := make([]query.Symbol, 0, len(lj.CorrelationVars)+len(originalBindingSyms))
	output = append(output, lj.CorrelationVars...)
	output = append(output, originalBindingSyms...)

	// Recover the outer relation from children.
	var outerNode *Node
	if len(children) > 0 {
		outerNode = recoverChild(children[0])
	}

	var innerResultNode *Node

	if hasAggregates(lj.InnerQuery) {
		// Rule 1: aggregate decorrelation — compile inner, wrap in Aggregate
		var groupBy []query.Symbol
		var aggFns []query.FindAggregate
		for _, elem := range decorrelated.Find {
			switch e := elem.(type) {
			case query.FindVariable:
				groupBy = append(groupBy, e.Symbol)
			case query.FindAggregate:
				aggFns = append(aggFns, e)
			}
		}

		innerNode, compileErr := compileClauses(decorrelated.Where)
		if compileErr != nil || innerNode == nil {
			return node
		}

		innerResultNode = &Node{
			Op: RuleAggregate,
			Data: &Aggregate{
				GroupBy:   groupBy,
				Functions: aggFns,
				Output:    output,
			},
			Children: []*Node{innerNode},
		}
	} else {
		// Rule 4: non-aggregate decorrelation.
		// Compile the inner WHERE, recursively optimize to decorrelate nested
		// subqueries (e.g., the max subquery in the argmax pattern), then
		// decompile back to produce optimized WHERE clauses.
		innerNode, compileErr := compileClauses(decorrelated.Where)
		if compileErr != nil || innerNode == nil {
			return rebuildWithChildren(node, children)
		}

		// Recursively optimize the inner tree — decorrelates nested LateralJoins
		innerOptimizer := NewOptimizer(DecorrelationPass(nil))
		optimizedInner, optErr := innerOptimizer.Optimize(innerNode)
		if optErr == nil && optimizedInner != nil {
			innerNode = optimizedInner
		}

		// Decompile the optimized inner tree back to WHERE clauses
		optimizedWhere, decompErr := Decompile(innerNode)
		if decompErr != nil {
			return rebuildWithChildren(node, children)
		}

		// Log the optimized inner WHERE for debugging
		emit("algebra/decorrelate-inner-optimized", map[string]interface{}{
			"clause_count": len(optimizedWhere),
			"clauses":      fmt.Sprintf("%v", optimizedWhere),
		})

		// Build the decorrelated query with optimized WHERE
		optimizedDecorrelated := &query.Query{
			Find:  decorrelated.Find,
			In:    decorrelated.In,
			Where: optimizedWhere,
		}

		innerResultNode = &Node{
			Op: RuleLateralJoin,
			Data: &LateralJoin{
				CorrelationVars: nil,
				InnerQuery:      optimizedDecorrelated,
				Binding:         query.RelationBinding{Variables: output},
				Output:          output,
				DefaultValues:   nil,
			},
		}
	}

	if outerNode == nil {
		return wrapAsParseNode(innerResultNode)
	}

	joinKind := InnerJoin
	if len(lj.DefaultValues) > 0 {
		joinKind = LeftOuterJoin
	}

	joinNode := &Node{
		Op: RuleJoin,
		Data: &Join{
			Kind:          joinKind,
			JoinSymbols:   lj.CorrelationVars,
			Output:        mergeSymbols(outerNode.Symbols(), output),
			DefaultValues: lj.DefaultValues,
		},
		Children: []*Node{outerNode, innerResultNode},
	}

	return wrapAsParseNode(joinNode)
}

// rebuildWithChildren returns a parse node with the same rule and data as the
// original but with potentially-rewritten children. This ensures that child
// transforms propagate through parents that skip their own transformation.
func rebuildWithChildren(node *parse.Node, children []interface{}) *parse.Node {
	rebuilt := &parse.Node{
		Rule:             node.Rule,
		Value:            node.Value,
		TransformedValue: node.TransformedValue,
	}
	for _, child := range children {
		if pn, ok := child.(*parse.Node); ok {
			rebuilt.Children = append(rebuilt.Children, pn)
		}
	}
	return rebuilt
}

// shouldDecorrelate determines whether a LateralJoin should be decorrelated
// based on structural analysis of the inner query. Per ALGEBRA.md Rule 4.
func shouldDecorrelate(lj *LateralJoin) bool {
	if hasAggregates(lj.InnerQuery) {
		return true // Rule 1: aggregate decorrelation
	}
	if hasNestedCorrelatedSubquery(lj.InnerQuery) {
		return true // Rule 4: argmax pattern
	}
	if hasFilteringClauses(lj.InnerQuery) {
		return true // Rule 4: selective inner query
	}
	return false // Pure DataPatterns — indexed lookup is faster
}

// hasNestedCorrelatedSubquery returns true if the query's WHERE contains
// a SubqueryPattern with variable (non-$) inputs — a correlated subquery.
func hasNestedCorrelatedSubquery(q *query.Query) bool {
	for _, c := range q.Where {
		if sp, ok := c.(*query.SubqueryPattern); ok {
			for _, input := range sp.Inputs {
				if v, ok := input.(query.Variable); ok && !v.Name.IsSource() {
					return true
				}
			}
		}
	}
	return false
}

// hasFilteringClauses returns true if the query's WHERE contains predicates,
// expressions, NOT clauses, or nested subqueries — anything beyond bare
// DataPatterns that reduces the result set.
func hasFilteringClauses(q *query.Query) bool {
	for _, c := range q.Where {
		switch c.(type) {
		case *query.DataPattern:
			continue // Bare pattern — not filtering
		default:
			return true // Predicate, expression, NOT, subquery, etc.
		}
	}
	return false
}

// decorrelateQuery rewrites a correlated query into a decorrelated one.
// The correlation variables are the INNER parameter names (from :in),
// not the outer variable names. They get removed from :in and added to
// :find as grouping keys.
//
// Input:  [:find (count ?t) :in $ ?s :where [?t :task/root ?s] ...]
// Output: [:find ?s (count ?t) :in $ :where [?t :task/root ?s] ...]
//
// The result runs once for all values of ?s, grouped by ?s.
func decorrelateQuery(q *query.Query, innerParamNames []query.Symbol) *query.Query {
	// Build new :in — remove correlation parameters, keep $ and other inputs
	var newIn []query.InputSpec
	paramSet := make(map[query.Symbol]bool, len(innerParamNames))
	for _, p := range innerParamNames {
		paramSet[p] = true
	}

	for _, in := range q.In {
		switch inp := in.(type) {
		case query.ScalarInput:
			if paramSet[inp.Symbol] {
				continue // Remove correlation parameter from :in
			}
			newIn = append(newIn, in)
		case query.DatabaseInput:
			newIn = append(newIn, in)
		default:
			newIn = append(newIn, in)
		}
	}

	// Build new :find — prepend inner parameter names as grouping keys
	newFind := make([]query.FindElement, 0, len(innerParamNames)+len(q.Find))
	for _, p := range innerParamNames {
		newFind = append(newFind, query.FindVariable{Symbol: p})
	}
	newFind = append(newFind, q.Find...)

	return &query.Query{
		Find:  newFind,
		In:    newIn,
		Where: q.Where, // WHERE clauses unchanged — they still reference the inner params
	}
}

// mapCorrelationToInnerParams finds the inner parameter names that correspond
// to the outer correlation variables. The mapping is positional:
//   SubqueryPattern.Inputs: [$ ?e]  maps to  Query.In: [$ ?s]
// So outer ?e corresponds to inner ?s.
func mapCorrelationToInnerParams(innerQuery *query.Query, outerVars []query.Symbol) []query.Symbol {
	// Match outer correlation vars to inner parameter names by position.
	// Both appear as ScalarInput entries in :in (after $), in the same order.
	// Only return as many inner params as there are outer vars — additional
	// scalar inputs (e.g., constant thresholds) are NOT correlation params.
	var innerParams []query.Symbol
	for _, in := range innerQuery.In {
		if si, ok := in.(query.ScalarInput); ok {
			innerParams = append(innerParams, si.Symbol)
			if len(innerParams) == len(outerVars) {
				break
			}
		}
	}
	return innerParams
}

// hasAggregates returns true if the query's :find clause contains aggregates.
func hasAggregates(q *query.Query) bool {
	for _, elem := range q.Find {
		if elem.IsAggregate() {
			return true
		}
	}
	return false
}

// recoverChild extracts an algebra Node from a transform result.
func recoverChild(v interface{}) *Node {
	switch c := v.(type) {
	case *Node:
		return c
	case *parse.Node:
		return fromParseNode(c)
	}
	return nil
}

// wrapAsParseNode wraps an algebra Node as a parse.Node for the transform framework.
func wrapAsParseNode(n *Node) *parse.Node {
	return toParseNode(n)
}
