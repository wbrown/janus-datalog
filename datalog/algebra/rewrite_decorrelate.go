package algebra

import (
	"fmt"

	"github.com/wbrown/ebnf/parse"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// decorrelationPassName names the pass in rewrite records and event data.
const decorrelationPassName = "decorrelation"

// DecorrelationPass returns a transform pass that rewrites LateralJoin nodes
// into regular Join + decorrelated subquery. Every decision — considered,
// applied, declined with the failed precondition — goes to the sink as a
// typed RewriteRecord and as its annotation-event form (a nil sink records
// nothing).
//
// Algebraic rule: R ⋈_L S(r.x) → R ⋈ (S GROUP BY x)
//
// The inner query is rewritten:
//   - Correlation variable removed from :in
//   - Correlation variable added to :find as a grouping key
//   - Binding updated to include the correlation variable
//   - The subquery runs once for all values, producing groups
//   - Results joined back on the correlation variable
func DecorrelationPass(sink *RewriteSink) Pass {
	return Pass{
		Name: decorrelationPassName,
		Transforms: parse.TransformMap{
			RuleLateralJoin: makeDecorrelateTransform(sink),
		},
	}
}

// decorrelateTransform is the transform function for LateralJoin nodes.
// It receives the node's children as already-transformed values.
// If decorrelation is possible, it returns a rewritten algebra Node.
// Otherwise it returns the original node unchanged.
func makeDecorrelateTransform(sink *RewriteSink) parse.TransformFunc {
	return func(ctx *parse.TransformContext, node *parse.Node, children ...interface{}) interface{} {
		return decorrelateTransform(ctx, node, sink, children...)
	}
}

func decorrelateTransform(ctx *parse.TransformContext, node *parse.Node, sink *RewriteSink, children ...interface{}) interface{} {
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

	// Ask the sink before building the payload. The subject and the correlation
	// vars are carried, so they cost nothing, but the map itself and the two
	// walks of the inner query inside it (hasAggregates, shouldDecorrelate) are
	// paid per LateralJoin this pass visits, and a guard inside Record cannot
	// prevent that — Go evaluates arguments before the call. On the normal query
	// path — no handler, no explanation — nothing consumes any of it.
	//
	observing := sink.Recording()
	if observing {
		sink.Record(RewriteRecord{
			Pass:    decorrelationPassName,
			Action:  RewriteConsidered,
			Subject: lj.InnerQuery,
		}, annotations.AlgebraDecorrelateCheck, map[string]interface{}{
			"correlation_vars": lj.CorrelationVars,
			"has_aggregates":   lj.InnerQuery.Aggregates(),
			"has_defaults":     len(lj.Defaults) > 0,
			"should":           shouldDecorrelate(lj),
			"inner_query":      lj.InnerQuery,
		})
	}

	decline := func(reason string) {
		if !observing {
			return
		}
		sink.Record(RewriteRecord{
			Pass:    decorrelationPassName,
			Action:  RewriteDeclined,
			Reason:  reason,
			Subject: lj.InnerQuery,
		}, annotations.AlgebraDecorrelateSkip, map[string]interface{}{
			"reason": reason,
		})
	}

	if !shouldDecorrelate(lj) {
		decline("pure DataPattern query — indexed lookup is faster")
		return rebuildWithChildren(node, children)
	}

	if len(lj.CorrelationVars) == 0 {
		decline("no correlation variables")
		return rebuildWithChildren(node, children)
	}

	// The outer relation, recovered from the children. The decorrelated
	// result joins it on the correlation variables, and a join keys only on
	// symbols both sides produce, so a correlation variable the outer
	// relation does not carry, one the enclosing scope binds, keeps the
	// subquery on per-combination execution.
	var outerNode *Node
	if len(children) > 0 {
		outerNode = recoverChild(children[0])
	}
	if outerNode != nil {
		for _, cv := range lj.CorrelationVars {
			if !query.ContainsSymbol(outerNode.Symbols(), cv) {
				decline(fmt.Sprintf("correlation variable %s is bound by the enclosing scope, not the outer relation", cv))
				return rebuildWithChildren(node, children)
			}
		}
	}

	correlations, declineReason := correlationParameters(lj.InnerQuery, lj.Inputs)
	if declineReason != "" {
		decline(declineReason)
		return rebuildWithChildren(node, children)
	}
	innerParams := make([]query.Symbol, len(correlations))
	for i, c := range correlations {
		innerParams[i] = c.param
	}

	// Classify every correlation parameter before transforming: the rewrite
	// fires only where its preconditions hold; shapes with no
	// semantics-preserving grouped form decline it and keep per-combination
	// execution — a lost optimization, never a wrong answer or a late error.
	plans, declineReason := classifyCorrelationParams(lj.InnerQuery, innerParams)
	if declineReason != "" {
		decline(declineReason)
		return rebuildWithChildren(node, children)
	}

	if observing {
		sink.Record(RewriteRecord{
			Pass:    decorrelationPassName,
			Action:  RewriteApplied,
			Subject: lj.InnerQuery,
		}, annotations.AlgebraDecorrelateApply, map[string]interface{}{
			"correlation_vars": lj.CorrelationVars,
			"inner_params":     innerParams,
			"has_aggregates":   lj.InnerQuery.Aggregates(),
		})
	}

	// Decorrelate: remove correlation params from :in, add the classified
	// group-by symbols to :find, consume translated correlation equalities.
	decorrelated := decorrelateQuery(lj.InnerQuery, plans, correlations)
	if decorrelated == nil {
		return node
	}

	// Output symbols: correlation vars (OUTER names) + original binding vars.
	originalBindingSyms := lj.Binding.(query.BindingForm).BoundVariables()
	output := make([]query.Symbol, 0, len(lj.CorrelationVars)+len(originalBindingSyms))
	output = append(output, lj.CorrelationVars...)
	output = append(output, originalBindingSyms...)

	var innerResultNode *Node

	if lj.InnerQuery.Aggregates() {
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

		// The inner query compiles in its own scope: its :in, and the rule set
		// the call's % binds, which the lateral join carries.
		innerNode, compileErr := Compile(decorrelated, lj.Rules)
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
				In:        decorrelated.In,
				Inputs:    callSourceArguments(lj.Inputs, decorrelated.In),
			},
			Children: []*Node{innerNode},
		}
	} else {
		// Rule 4: non-aggregate decorrelation.
		// Compile the inner WHERE, recursively optimize to decorrelate nested
		// subqueries (e.g., the max subquery in the argmax pattern), then
		// decompile back to produce optimized WHERE clauses.
		innerNode, compileErr := Compile(decorrelated, lj.Rules)
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

		// Diagnostic detail accompanying the applied rewrite: the optimized
		// inner WHERE. Event-only — the applied record above is the decision —
		// so the guard is Emitting, not Recording: a collect-only sink discards
		// this payload, and rendering the clause list to build it is the argument
		// preparation the guard exists to skip.
		if sink.Emitting() {
			sink.Emit(annotations.AlgebraDecorrelateInnerOptimized, map[string]interface{}{
				"clause_count": len(optimizedWhere),
				"clauses":      optimizedWhere,
			})
		}

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
				Inputs:          callSourceArguments(lj.Inputs, decorrelated.In),
				InnerQuery:      optimizedDecorrelated,
				Binding:         query.RelationBinding{Variables: output},
				Output:          output,
				Defaults:        nil,
				Rules:           lj.Rules,
			},
		}
	}

	if outerNode == nil {
		return wrapAsParseNode(innerResultNode)
	}

	joinKind := InnerJoin
	if len(lj.Defaults) > 0 {
		joinKind = LeftOuterJoin
	}

	joinNode := &Node{
		Op: RuleJoin,
		Data: &Join{
			Kind:        joinKind,
			JoinSymbols: lj.CorrelationVars,
			Output:      mergeSymbols(outerNode.Symbols(), output),
			Defaults:    lj.Defaults,
		},
		Children: []*Node{outerNode, innerResultNode},
	}

	return wrapAsParseNode(joinNode)
}

// callSourceArguments is what a decorrelated call passes: the written call's
// source and rule-set arguments, in call order, with the correlation
// variables left out, since they moved into the inner :find. A decorrelated
// :in left with no entry defaults to $ (query.InputPositions), the one
// position of such a call, and the call passes $ there.
func callSourceArguments(inputs []query.PatternElement, in []query.InputSpec) []query.PatternElement {
	if len(in) == 0 {
		return []query.PatternElement{query.Constant{Value: datalog.SymDollar}}
	}
	var sourceInputs []query.PatternElement
	for _, input := range inputs {
		if v, isVar := input.(query.Variable); !isVar || v.Name.IsSource() {
			sourceInputs = append(sourceInputs, input)
		}
	}
	return sourceInputs
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
	if lj.InnerQuery.Aggregates() {
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
	// groupSym is the inner symbol that becomes the group-by key: the
	// parameter itself when a pattern binds it (data-bound), or the
	// inner-provided side of its single equality predicate
	// (equality-bound) — the correlation predicate IS the join condition,
	// so the grouped result joins the outer on that symbol, positionally
	// renamed to the outer correlation name by the binding.
	groupSym query.Symbol
	// dropClause is the consumed correlation equality to remove from the
	// inner :where (nil for data-bound parameters — their patterns stay).
	dropClause query.Clause
}

// classifyCorrelationParams decides, before any transformation, whether the
// rewrite's preconditions hold for every correlation parameter. The
// taxonomy, with a conservative default:
//
//   - data-bound: a DataPattern position binds the parameter — the pattern
//     provides the group-by symbol when the input is freed (today's rule).
//   - equality-bound: the parameter's only consumption is one
//     [(= ?inner ?param)] comparison whose other side the body provides —
//     the predicate is the join condition; group by the inner side and
//     drop the predicate.
//   - anything else (inequalities, expression operands, compound-clause
//     use, multiple equalities): the rewrite has no semantics-preserving
//     grouped form, so the subquery declines decorrelation and keeps
//     per-combination execution — a lost optimization, never a wrong answer.
//
// See docs/bugs/BUG_DECORRELATION_PREDICATE_ONLY_INPUT_SYMBOLS.md.
func classifyCorrelationParams(q *query.Query, innerParams []query.Symbol) ([]paramPlan, string) {
	// The inner-provided symbols — the only candidates for equality-
	// translated group-by symbols. Provides only: a symbol merely consumed
	// elsewhere (including the parameter itself, which every correlation
	// equality correlates on) is not a symbol the grouped result can carry.
	var provided []query.Symbol
	for _, c := range q.Where {
		for _, sym := range query.ScopeOf(c, nil).Provides {
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
				if query.ContainsSymbol(query.ScopeOf(c, nil).Correlates, param) {
					otherUse = true
				}
			}
		}

		switch {
		case dataBound:
			// The pattern provides the symbol regardless of other uses;
			// remaining predicates stay as ordinary filters.
			plans = append(plans, paramPlan{param: param, groupSym: param})
		case otherUse:
			return nil, fmt.Sprintf("correlation parameter %s is consumed outside data patterns and single equality predicates", param)
		case len(equalities) == 1:
			eq := equalities[0]
			inner, ok := equalityInnerSide(eq, param, provided)
			if !ok {
				return nil, fmt.Sprintf("correlation parameter %s equates with a variable the inner body does not provide", param)
			}
			plans = append(plans, paramPlan{param: param, groupSym: inner, dropClause: eq})
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
// according to the classified plans, one per correlation in order. The
// binding entries leave :in, since every one is a correlation, and the
// sources and the rule set stay. One group-by symbol per outer variable is
// prepended to :find, the first plan's where the call passes one outer
// variable at several positions, and each later plan's group symbol is
// equated with it by a predicate appended to :where, so the grouped result
// carries the outer variable once and keeps the tuples the per-combination
// run kept:
//
//	data-bound:     [:find (count ?t) :in $ ?s :where [?t :task/root ?s] ...]
//	             →  [:find ?s (count ?t) :in $ :where [?t :task/root ?s] ...]
//	equality-bound: [:find (max ?h) :in $ ?d :where ... [(day ?t) ?pd] [(= ?pd ?d)] ...]
//	             →  [:find ?pd (max ?h) :in $ :where ... [(day ?t) ?pd] ...]
//	one outer variable at two positions, passed $ ?a ?a:
//	                [:find (sum ?v) :in $ ?n ?m :where [?x :p/n ?n] [?x :p/m ?m] ...]
//	             →  [:find ?n (sum ?v) :in $ :where [?x :p/n ?n] [?x :p/m ?m] ... [(= ?n ?m)]]
//
// For the equality-bound form the correlation predicate is consumed — it
// becomes the join condition (group by the inner side, join the outer on
// it, positionally renamed to the outer name by the binding).
func decorrelateQuery(q *query.Query, plans []paramPlan, correlations []correlation) *query.Query {
	dropSet := make(map[query.Clause]bool)
	for _, p := range plans {
		if p.dropClause != nil {
			dropSet[p.dropClause] = true
		}
	}

	var newIn []query.InputSpec
	for _, in := range q.In {
		switch in.(type) {
		case query.DatabaseInput, query.RulesInput:
			newIn = append(newIn, in)
		}
	}

	keyFor := make(map[query.Symbol]query.Symbol, len(plans))
	newFind := make([]query.FindElement, 0, len(plans)+len(q.Find))
	var equalities []query.Clause
	for i, p := range plans {
		outer := correlations[i].outer
		if key, seen := keyFor[outer]; seen {
			equalities = append(equalities, &query.Comparison{
				Op:    datalog.SymEQ,
				Left:  query.VariableTerm{Symbol: key},
				Right: query.VariableTerm{Symbol: p.groupSym},
			})
			continue
		}
		keyFor[outer] = p.groupSym
		newFind = append(newFind, query.FindVariable{Symbol: p.groupSym})
	}
	newFind = append(newFind, q.Find...)

	// Consumed correlation equalities become the outer join; everything else
	// is unchanged and still references inner names.
	newWhere := make([]query.Clause, 0, len(q.Where)+len(equalities))
	for _, c := range q.Where {
		if dropSet[c] {
			continue
		}
		newWhere = append(newWhere, c)
	}
	newWhere = append(newWhere, equalities...)

	return &query.Query{
		Find:  newFind,
		In:    newIn,
		Where: newWhere,
	}
}

// correlation is one inner :in symbol the call binds and the outer variable
// the call passes at its position.
type correlation struct {
	param query.Symbol
	outer query.Symbol
}

// correlationParameters pairs each inner :in symbol the call binds with the
// outer variable the call passes at its position, in call order, each inner
// symbol once: a scalar entry's symbol, and each symbol of a tuple or a
// relation entry, is bound by the value at its own position
// (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.5). It declines, with the
// reason, the shapes with no grouped form: a collection entry, whose symbol
// ranges over the elements of the value at its position and so is no key
// the grouped result can join the outer on; a constant at a binding
// position, which has no outer symbol to join on; and an inner symbol bound
// at two positions by different outer variables.
func correlationParameters(inner *query.Query, inputs []query.PatternElement) ([]correlation, string) {
	var params []correlation
	for position, at := range query.InputPositions(inner.In) {
		if at.Symbol == nil {
			continue
		}
		if _, isCollection := at.Spec.(query.CollectionInput); isCollection {
			return nil, fmt.Sprintf("collection entry %s has no grouped form", at.Spec)
		}
		arg, isVar := inputs[position].(query.Variable)
		if !isVar || arg.Name.IsSource() {
			return nil, fmt.Sprintf("argument %v at the position of %s has no outer symbol to join on", inputs[position], at.Symbol)
		}
		bound := false
		for _, p := range params {
			if p.param != at.Symbol {
				continue
			}
			bound = true
			if p.outer != arg.Name {
				return nil, fmt.Sprintf("inner symbol %s is bound by %s and by %s", at.Symbol, p.outer, arg.Name)
			}
		}
		if !bound {
			params = append(params, correlation{param: at.Symbol, outer: arg.Name})
		}
	}
	return params, ""
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
