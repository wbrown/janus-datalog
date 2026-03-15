package algebra

import (
	"github.com/wbrown/ebnf/parse"
	"github.com/wbrown/janus-datalog/datalog"
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
func DecorrelationPass() Pass {
	return Pass{
		Name: "decorrelation",
		Transforms: parse.TransformMap{
			RuleLateralJoin: decorrelateTransform,
			RuleJoin:        collapseLeftOuterJoinTransform,
		},
	}
}

// decorrelateTransform is the transform function for LateralJoin nodes.
// It receives the node's children as already-transformed values.
// If decorrelation is possible, it returns a rewritten algebra Node.
// Otherwise it returns the original node unchanged.
func decorrelateTransform(node *parse.Node, children ...interface{}) interface{} {
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

	// Only decorrelate if the inner query has aggregates in :find
	if !hasAggregates(lj.InnerQuery) {
		return node // Keep as correlated LateralJoin
	}

	// Only decorrelate scalar correlation (single variable)
	// Multi-variable correlation is possible but more complex
	if len(lj.CorrelationVars) == 0 {
		return node
	}

	// Map outer correlation vars to inner parameter names.
	// SubqueryPattern.Inputs maps positionally to Query.In:
	//   Inputs: [$ ?e]  →  In: [$ ?s]
	// So ?e (outer) corresponds to ?s (inner).
	innerParams := mapCorrelationToInnerParams(lj.InnerQuery, lj.CorrelationVars)
	if len(innerParams) == 0 {
		return node // Can't determine inner parameter names
	}

	// Build the decorrelated inner query using inner parameter names
	decorrelated := decorrelateQuery(lj.InnerQuery, innerParams)
	if decorrelated == nil {
		return node // Decorrelation failed
	}

	// Build the new binding: outer correlation vars (join keys) + original binding vars.
	// The decorrelated :find outputs [?s (count ?t)] but the binding maps positionally
	// to [?e ?count], so ?s in the output becomes ?e in the binding — enabling the
	// join with the outer relation on ?e.
	originalBindingSyms := bindingFormSymbols(lj.Binding.(query.BindingForm))
	newBindingVars := make([]query.Symbol, 0, len(lj.CorrelationVars)+len(originalBindingSyms))
	newBindingVars = append(newBindingVars, lj.CorrelationVars...)
	newBindingVars = append(newBindingVars, originalBindingSyms...)

	// Build the decorrelated SubqueryPattern.
	// Uses RelationBinding [[?e ?count] ...] because the decorrelated query
	// returns multiple rows (one per group). TupleBinding [[?e ?count]] only
	// handles single-row results.
	sp := &query.SubqueryPattern{
		Query:   decorrelated,
		Inputs:  []query.PatternElement{query.Variable{Name: datalog.SymDollar}},
		Binding: query.RelationBinding{Variables: newBindingVars},
	}

	// Output symbols: correlation vars + original binding vars
	output := make([]query.Symbol, len(newBindingVars))
	copy(output, newBindingVars)

	// If there's a left child (outer relation), produce a Join
	var resultChildren []*Node
	if len(children) > 0 {
		if childNode := recoverChild(children[0]); childNode != nil {
			resultChildren = []*Node{childNode}
		}
	}

	// The decorrelated subquery is now a Scan-like leaf that produces
	// all groups in one pass. We represent it as a new LateralJoin-free node.
	scanNode := &Node{
		Op: RuleScan,
		Data: &Scan{
			Pattern: nil, // Signals this is a subquery, not a storage scan
			Output:  output,
		},
	}
	// Attach the SubqueryPattern as metadata for the decompiler
	scanNode.Data = &decorrelatedScan{
		SubqueryPattern:  sp,
		Output:           output,
		DefaultValues:    lj.DefaultValues,
		OriginalBinding:  originalBindingSyms,
		CorrelationVars:  lj.CorrelationVars,
		OriginalBindForm: lj.Binding.(query.BindingForm),
	}

	if len(resultChildren) == 0 {
		result := wrapAsParseNode(scanNode)
		return result
	}

	// Join the outer relation with the decorrelated subquery on correlation vars
	joinNode := &Node{
		Op: RuleJoin,
		Data: &Join{
			Kind:        InnerJoin,
			JoinSymbols: lj.CorrelationVars,
			Output:      mergeSymbols(resultChildren[0].Symbols(), output),
		},
		Children: []*Node{resultChildren[0], scanNode},
	}

	// If there are default values, wrap in LeftOuterJoin instead
	if len(lj.DefaultValues) > 0 {
		joinNode.Data.(*Join).Kind = LeftOuterJoin
	}

	return wrapAsParseNode(joinNode)
}

// collapseLeftOuterJoinTransform handles Join(LeftOuter) nodes after
// decorrelation. When a LeftOuterJoin has a decorrelatedScan child (from
// a successfully decorrelated LateralJoin) and a ground/Map child (the
// default values), it absorbs the defaults into the decorrelatedScan
// and replaces the entire LeftOuterJoin with just the decorrelatedScan.
//
// Before: Join(LeftOuter) → [decorrelatedScan, Map(ground defaults)]
// After:  decorrelatedScan (with DefaultValues set)
func collapseLeftOuterJoinTransform(node *parse.Node, children ...interface{}) interface{} {
	algNode := node.TransformedValue.(*Node)
	join, ok := algNode.Data.(*Join)
	if !ok {
		return node
	}
	if join.Kind != LeftOuterJoin {
		// Not a LeftOuterJoin — but children may have been transformed.
		// Rebuild the node with updated children.
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

	// Need exactly 2 children
	if len(children) != 2 {
		return node
	}

	// Find the decorrelatedScan child and the defaults child
	var dsChild *Node
	var defaultValues []interface{}
	var defaultSymbols []query.Symbol

	for _, child := range children {
		childNode := recoverChild(child)
		if childNode == nil {
			continue
		}

		if ds, ok := childNode.Data.(*decorrelatedScan); ok {
			dsChild = childNode
			_ = ds
		} else {
			// Walk the node tree to collect all ground/constant defaults.
			// Multiple ground expressions compile to a chain of Map nodes:
			//   Map(ground 0, ?b) → Map(ground 0, ?a) → ...
			collectDefaults(childNode, &defaultValues, &defaultSymbols)
		}
	}

	if dsChild == nil {
		return node // No decorrelatedScan child — leave unchanged
	}

	// Absorb defaults into the decorrelatedScan
	if len(defaultValues) > 0 {
		ds := dsChild.Data.(*decorrelatedScan)
		ds.DefaultValues = defaultValues
		ds.OriginalBinding = defaultSymbols
	}

	result := wrapAsParseNode(dsChild)
	return result
}

// collectDefaults walks an algebra node tree to gather all ground/constant
// default values. Multiple ground expressions compile to chained Map nodes.
func collectDefaults(n *Node, values *[]interface{}, symbols *[]query.Symbol) {
	if n == nil {
		return
	}
	switch d := n.Data.(type) {
	case *Map:
		switch gf := d.Expression.Function.(type) {
		case query.GroundFunction:
			*values = append(*values, extractDefaultValues(gf.Value)...)
			*symbols = append(*symbols, bindingSymbols(d.Expression.Binding)...)
		case *query.GroundFunction:
			*values = append(*values, extractDefaultValues(gf.Value)...)
			*symbols = append(*symbols, bindingSymbols(d.Expression.Binding)...)
		}
	case *Constant:
		*values = append(*values, d.Values...)
		*symbols = append(*symbols, d.Symbols...)
	}
	// Recurse into children to find nested defaults
	for _, child := range n.Children {
		collectDefaults(child, values, symbols)
	}
}

// extractDefaultValues normalizes a ground value into a slice.
func extractDefaultValues(v interface{}) []interface{} {
	if slice, ok := v.([]interface{}); ok {
		return slice
	}
	return []interface{}{v}
}

// decorrelatedScan is a special Scan variant that holds a decorrelated
// SubqueryPattern. The decompiler emits this as a SubqueryPattern clause
// that LOOKS correlated (same Inputs/Binding as original) but has a
// rewritten inner query that can run uncorrelated.
type decorrelatedScan struct {
	SubqueryPattern  *query.SubqueryPattern  // Decorrelated subquery (Inputs: [$], RelationBinding)
	Output           []query.Symbol
	DefaultValues    []interface{}
	OriginalBinding  []query.Symbol          // Original binding symbols (for defaults)
	CorrelationVars  []query.Symbol          // Outer variable names (e.g., ?scenario)
	OriginalBindForm query.BindingForm       // Original binding form (e.g., TupleBinding [[?count]])
}

func (d *decorrelatedScan) OutputSymbols() []query.Symbol { return d.Output }
func (d *decorrelatedScan) String() string {
	return "decorrelated(" + d.SubqueryPattern.Query.String() + ")"
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
	// Build a set of outer correlation vars for quick lookup
	outerSet := make(map[query.Symbol]bool, len(outerVars))
	for _, v := range outerVars {
		outerSet[v] = true
	}

	// The inner query's :in clause has the parameter names.
	// Non-database scalar inputs that aren't in outerVars are unrelated params.
	// We need to identify which :in entries ARE the correlation params.
	// Since we only know the outer var names, we match by position:
	// the correlation vars appear as ScalarInput entries in :in (after $).
	var innerParams []query.Symbol
	for _, in := range innerQuery.In {
		if si, ok := in.(query.ScalarInput); ok {
			// Every scalar input in a correlated subquery IS a correlation param
			// (the subquery was identified as correlated because it had variable inputs)
			innerParams = append(innerParams, si.Symbol)
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
