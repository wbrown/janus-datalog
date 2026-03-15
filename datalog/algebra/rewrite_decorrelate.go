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
	return func(node *parse.Node, children ...interface{}) interface{} {
		return decorrelateTransform(node, emit, children...)
	}
}

func decorrelateTransform(node *parse.Node, emit emitFn, children ...interface{}) interface{} {
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

	emit("algebra/decorrelate-check", map[string]interface{}{
		"correlation_vars": fmt.Sprintf("%v", lj.CorrelationVars),
		"has_aggregates":   hasAggregates(lj.InnerQuery),
		"has_defaults":     len(lj.DefaultValues) > 0,
		"inner_query":      lj.InnerQuery.String(),
	})

	// Only decorrelate if the inner query has aggregates in :find
	if !hasAggregates(lj.InnerQuery) {
		emit("algebra/decorrelate-skip", map[string]interface{}{
			"reason": "no aggregates in :find",
		})
		// Rebuild node with potentially-rewritten children so child
		// decorrelations propagate through non-decorrelatable parents.
		return rebuildWithChildren(node, children)
	}

	// Only decorrelate scalar correlation (single variable)
	// Multi-variable correlation is possible but more complex
	if len(lj.CorrelationVars) == 0 {
		emit("algebra/decorrelate-skip", map[string]interface{}{
			"reason": "no correlation variables",
		})
		return rebuildWithChildren(node, children)
	}

	// Map outer correlation vars to inner parameter names.
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
	})

	// Build the decorrelated inner query: remove correlation params from :in,
	// add them to :find as GROUP BY keys.
	decorrelated := decorrelateQuery(lj.InnerQuery, innerParams)
	if decorrelated == nil {
		return node
	}

	// Extract GROUP BY keys and aggregate functions from the decorrelated :find.
	// The :find is [?s (count ?t) ...] where ?s is the GROUP BY key.
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

	// Output symbols: correlation vars (OUTER names) + original binding vars.
	// The Aggregate's output uses outer names because the decompiler maps them
	// to the SubqueryPattern's RelationBinding. The inner :find clause has the
	// inner names; the binding maps inner→outer positionally.
	originalBindingSyms := bindingFormSymbols(lj.Binding.(query.BindingForm))
	output := make([]query.Symbol, 0, len(lj.CorrelationVars)+len(originalBindingSyms))
	output = append(output, lj.CorrelationVars...)
	output = append(output, originalBindingSyms...)

	// Compile the decorrelated WHERE clauses to algebra nodes.
	innerNode, compileErr := compileClauses(decorrelated.Where)
	if compileErr != nil || innerNode == nil {
		return node
	}

	// Build Aggregate(groupBy, fns) over the inner scans.
	// This runs ONCE, producing one row per distinct GROUP BY value.
	// Output uses outer names so the decompiler can build correct bindings.
	aggregateNode := &Node{
		Op: RuleAggregate,
		Data: &Aggregate{
			GroupBy:   groupBy,
			Functions: aggFns,
			Output:    output,
		},
		Children: []*Node{innerNode},
	}

	// Recover the outer relation from children.
	var outerNode *Node
	if len(children) > 0 {
		outerNode = recoverChild(children[0])
	}

	if outerNode == nil {
		// No outer relation — return just the Aggregate.
		return wrapAsParseNode(aggregateNode)
	}

	// Rule 1: LateralJoin(x, defaults) → LeftOuterJoin(x, defaults) [R, Aggregate]
	// LeftOuterJoin preserves all outer tuples; non-matching get defaults.
	// InnerJoin would drop outer tuples without matches.
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
		Children: []*Node{outerNode, aggregateNode},
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
