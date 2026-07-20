package algebra

import (
	"fmt"

	"github.com/wbrown/ebnf/parse"
	"github.com/wbrown/janus-datalog/datalog"
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

	// Classify every correlation parameter before transforming: the rewrite
	// fires only where its preconditions hold; shapes with no
	// semantics-preserving grouped form decline it and keep per-combination
	// execution — a lost optimization, never a wrong answer or a late error.
	plans, declineReason := classifyCorrelationParams(lj.InnerQuery, innerParams)
	if declineReason != "" {
		emit("algebra/decorrelate-skip", map[string]interface{}{
			"reason": declineReason,
		})
		return rebuildWithChildren(node, children)
	}

	emit("algebra/decorrelate-apply", map[string]interface{}{
		"correlation_vars": fmt.Sprintf("%v", lj.CorrelationVars),
		"inner_params":     fmt.Sprintf("%v", innerParams),
		"has_aggregates":   hasAggregates(lj.InnerQuery),
	})

	// Decorrelate: remove correlation params from :in, add the classified
	// group-by columns to :find, consume translated correlation equalities.
	decorrelated := decorrelateQuery(lj.InnerQuery, plans)
	if decorrelated == nil {
		return node
	}

	// Output symbols: correlation vars (OUTER names) + original binding vars.
	originalBindingSyms := lj.Binding.(query.BindingForm).BoundVariables()
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

		innerNode, compileErr := Compile(decorrelated)
		if compileErr != nil || innerNode == nil {
			return node
		}

		innerResultNode = &Node{
			Op: RuleAggregate,
			Data: &Aggregate{
				GroupBy:   groupBy,
				Functions: aggFns,
				Bindings:  cloneSymbols(output),
				Output:    cloneSymbols(output),
			},
			Children: []*Node{innerNode},
		}
	} else {
		// Rule 4: non-aggregate decorrelation.
		// Compile the inner WHERE, recursively optimize to decorrelate nested
		// subqueries (e.g., the max subquery in the argmax pattern), then
		// decompile back to produce optimized WHERE clauses.
		innerNode, compileErr := Compile(decorrelated)
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

		// The rewrapped call site passes only the original source markers
		// (Constant $ or Variable $name forms): the correlation variables
		// moved into :find, and classification guarantees no constant
		// arguments reach this path.
		var sourceInputs []query.PatternElement
		for _, in := range lj.Inputs {
			if v, isVar := in.(query.Variable); !isVar || v.Name.IsSource() {
				sourceInputs = append(sourceInputs, in)
			}
		}

		innerResultNode = &Node{
			Op: RuleLateralJoin,
			Data: &LateralJoin{
				CorrelationVars: nil,
				Inputs:          sourceInputs,
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

// paramPlan is one correlation parameter's ruled treatment under
// decorrelation, produced by classifyCorrelationParams.
type paramPlan struct {
	// param is the inner :in parameter this plan covers; it is removed
	// from the decorrelated query's :in.
	param query.Symbol
	// groupCol is the inner column that becomes the group-by key: the
	// parameter itself when a pattern binds it (data-bound), or the
	// inner-provided side of its single equality predicate
	// (equality-bound) — the correlation predicate IS the join condition,
	// so the grouped result joins the outer on that column, positionally
	// renamed to the outer correlation name by the binding.
	groupCol query.Symbol
	// dropClause is the consumed correlation equality to remove from the
	// inner :where (nil for data-bound parameters — their patterns stay).
	dropClause query.Clause
}

// classifyCorrelationParams decides, before any transformation, whether the
// rewrite's preconditions hold for every correlation parameter. The
// taxonomy, with a conservative default:
//
//   - data-bound: a DataPattern position binds the parameter — the pattern
//     provides the group-by column when the input is freed (today's rule).
//   - equality-bound: the parameter's only consumption is one
//     [(= ?inner ?param)] comparison whose other side the body provides —
//     the predicate is the join condition; group by the inner side and
//     drop the predicate.
//   - anything else (inequalities, expression operands, compound-clause
//     use, multiple equalities, extra non-correlation inputs): the rewrite
//     has no semantics-preserving grouped form, so the subquery declines
//     decorrelation and keeps per-combination execution — a lost
//     optimization, never a wrong answer.
//
// See docs/bugs/BUG_DECORRELATION_PREDICATE_ONLY_INPUT_SYMBOLS.md.
func classifyCorrelationParams(q *query.Query, innerParams []query.Symbol) ([]paramPlan, string) {
	// Extra scalar inputs beyond the correlation parameters (e.g. call-site
	// constants) have no outer column to join on, and the rewrite discards
	// their bindings; their presence declines the rewrite.
	scalarInputs := 0
	for _, in := range q.In {
		if _, ok := in.(query.ScalarInput); ok {
			scalarInputs++
		}
	}
	if scalarInputs > len(innerParams) {
		return nil, "inner query has scalar inputs beyond the correlation parameters"
	}

	// The inner-provided symbols — the only candidates for equality-
	// translated group-by columns. Provides only: a symbol merely consumed
	// elsewhere (including the parameter itself, which every correlation
	// equality correlates on) is not a column the grouped result can carry.
	var provided []query.Symbol
	for _, c := range q.Where {
		for _, sym := range query.ScopeOf(c).Provides {
			if !query.ContainsSymbol(provided, sym) {
				provided = append(provided, sym)
			}
		}
	}

	plans := make([]paramPlan, 0, len(innerParams))
	for _, param := range innerParams {
		dataBound := false
		var equalities []*query.Comparison
		otherUse := false

		for _, c := range q.Where {
			switch v := c.(type) {
			case *query.DataPattern:
				for _, elem := range v.Elements {
					if variable, ok := elem.(query.Variable); ok && variable.Name == param {
						dataBound = true
					}
				}
			case *query.Comparison:
				left, leftIsVar := v.Left.(query.VariableTerm)
				right, rightIsVar := v.Right.(query.VariableTerm)
				mentionsParam := (leftIsVar && left.Symbol == param) || (rightIsVar && right.Symbol == param)
				if !mentionsParam {
					continue
				}
				if v.Op == datalog.SymEQ {
					equalities = append(equalities, v)
				} else {
					otherUse = true
				}
			default:
				// Compound clauses, expressions, chained comparisons,
				// nested subqueries, database functions: any mention is a
				// consumption shape the translation does not cover.
				if query.ContainsSymbol(query.ScopeOf(c).Correlates, param) {
					otherUse = true
				}
			}
		}

		switch {
		case dataBound:
			// The pattern provides the column regardless of other uses;
			// remaining predicates stay as ordinary filters.
			plans = append(plans, paramPlan{param: param, groupCol: param})
		case otherUse:
			return nil, fmt.Sprintf("correlation parameter %s is consumed outside data patterns and single equality predicates", param)
		case len(equalities) == 1:
			eq := equalities[0]
			inner, ok := equalityInnerSide(eq, param, provided)
			if !ok {
				return nil, fmt.Sprintf("correlation parameter %s equates with a variable the inner body does not provide", param)
			}
			plans = append(plans, paramPlan{param: param, groupCol: inner, dropClause: eq})
		case len(equalities) > 1:
			return nil, fmt.Sprintf("correlation parameter %s is consumed by multiple equality predicates", param)
		default:
			return nil, fmt.Sprintf("correlation parameter %s is not consumed by the inner query", param)
		}
	}
	return plans, ""
}

// equalityInnerSide returns the non-parameter side of a correlation
// equality, requiring it to be a variable the inner body provides.
func equalityInnerSide(eq *query.Comparison, param query.Symbol, provided []query.Symbol) (query.Symbol, bool) {
	var other query.Term
	if l, ok := eq.Left.(query.VariableTerm); ok && l.Symbol == param {
		other = eq.Right
	} else {
		other = eq.Left
	}
	v, ok := other.(query.VariableTerm)
	if !ok {
		return nil, false
	}
	if !query.ContainsSymbol(provided, v.Symbol) {
		return nil, false
	}
	return v.Symbol, true
}

// decorrelateQuery rewrites a correlated query into a decorrelated one
// according to the classified plans. Each correlation parameter is removed
// from :in and its group-by column prepended to :find:
//
//	data-bound:     [:find (count ?t) :in $ ?s :where [?t :task/root ?s] ...]
//	             →  [:find ?s (count ?t) :in $ :where [?t :task/root ?s] ...]
//	equality-bound: [:find (max ?h) :in $ ?d :where ... [(day ?t) ?pd] [(= ?pd ?d)] ...]
//	             →  [:find ?pd (max ?h) :in $ :where ... [(day ?t) ?pd] ...]
//
// For the equality-bound form the correlation predicate is consumed — it
// becomes the join condition (group by the inner side, join the outer on
// it, positionally renamed to the outer name by the binding).
func decorrelateQuery(q *query.Query, plans []paramPlan) *query.Query {
	paramSet := make(map[query.Symbol]bool, len(plans))
	dropSet := make(map[query.Clause]bool)
	for _, p := range plans {
		paramSet[p.param] = true
		if p.dropClause != nil {
			dropSet[p.dropClause] = true
		}
	}

	// Build new :in — remove correlation parameters, keep $ and other inputs
	var newIn []query.InputSpec
	for _, in := range q.In {
		if si, ok := in.(query.ScalarInput); ok && paramSet[si.Symbol] {
			continue // Remove correlation parameter from :in
		}
		newIn = append(newIn, in)
	}

	// Build new :find — prepend the group-by columns in parameter order
	newFind := make([]query.FindElement, 0, len(plans)+len(q.Find))
	for _, p := range plans {
		newFind = append(newFind, query.FindVariable{Symbol: p.groupCol})
	}
	newFind = append(newFind, q.Find...)

	// Build new :where — consumed correlation equalities become the outer
	// join; everything else is unchanged and still references inner names.
	newWhere := make([]query.Clause, 0, len(q.Where))
	for _, c := range q.Where {
		if dropSet[c] {
			continue
		}
		newWhere = append(newWhere, c)
	}

	return &query.Query{
		Find:  newFind,
		In:    newIn,
		Where: newWhere,
	}
}

// mapCorrelationToInnerParams finds the inner parameter names that correspond
// to the outer correlation variables. The mapping is positional:
//
//	SubqueryPattern.Inputs: [$ ?e]  maps to  Query.In: [$ ?s]
//
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
