package algebra

import (
	"github.com/wbrown/ebnf/parse"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// getElsePassName names the pass in rewrite records and event data.
const getElsePassName = "get-else-scan-rewrite"

// MatchAnswersLookup reports whether a match of attr on the source a query
// names answers, per entity, the one value the source's lookup answers. A
// source states it through the executor's AttributeFetchFusable, and the
// planner hands the pass the executing sources' answer.
type MatchAnswersLookup func(source query.Symbol, attr datalog.Keyword) bool

// GetElseScanRewritePass returns a transform pass that rewrites
// Map(get-else) nodes into LeftOuterJoin + Scan where answers says a match
// on the source the get-else reads answers its lookup's one value per entity
// (docs/proposals/GET_ELSE_SCAN_REWRITE_PROOF.md). Every decision on a
// get-else candidate — applied, or declined with the failed precondition —
// goes to the sink as a typed RewriteRecord and as its annotation-event form
// (a nil sink records nothing). Map nodes that are not get-else expressions
// are not candidates and produce no record.
//
// Rule 5 (ALGEBRA.md): get-else is a disguised left-outer-join.
// Replace N per-tuple point lookups with 1 index scan + hash join.
func GetElseScanRewritePass(sink *RewriteSink, answers MatchAnswersLookup) Pass {
	return Pass{
		Name: getElsePassName,
		Transforms: parse.TransformMap{
			RuleMap: makeGetElseScanRewriteTransform(sink, answers),
		},
	}
}

func makeGetElseScanRewriteTransform(sink *RewriteSink, answers MatchAnswersLookup) parse.TransformFunc {
	return func(node *parse.Node, children ...interface{}) interface{} {
		return getElseScanRewriteTransform(node, sink, answers, children...)
	}
}

// getElseScanRewriteTransform detects Map(get-else) and rewrites to
// LeftOuterJoin(child, Scan([E A ?result])) with default.
func getElseScanRewriteTransform(node *parse.Node, sink *RewriteSink, answers MatchAnswersLookup, children ...interface{}) interface{} {
	if node.TransformedValue == nil {
		return node
	}
	algNode, ok := node.TransformedValue.(*Node)
	if !ok || algNode == nil {
		return node
	}
	mapData, ok := algNode.Data.(*Map)
	if !ok {
		return node
	}

	// Only rewrite get-else, not other expressions (arithmetic, comparisons)
	ge, ok := mapData.Expression.Function.(*query.GetElseFunction)
	if !ok {
		return rebuildWithChildren(node, children)
	}

	// Ask before building the payload map, which is paid once per get-else this
	// pass visits; a guard inside Record cannot prevent it, because Go evaluates
	// arguments before the call. The subject is carried rather than rendered, so
	// it costs nothing either way.
	observing := sink.Recording()
	decline := func(reason string) {
		if !observing {
			return
		}
		sink.Record(RewriteRecord{
			Pass:    getElsePassName,
			Action:  RewriteDeclined,
			Reason:  reason,
			Subject: mapData.Expression,
		}, annotations.AlgebraGetElseScanSkip, map[string]interface{}{
			"reason":     reason,
			"expression": mapData.Expression,
		})
	}

	// Extract the entity variable from the get-else
	entityVar, ok := ge.Entity.(query.VariableTerm)
	if !ok {
		decline("get-else entity is not a variable")
		return rebuildWithChildren(node, children)
	}

	// Extract the binding symbol (output variable)
	var bindingSym query.Symbol
	switch b := mapData.Expression.Binding.(type) {
	case query.Symbol:
		bindingSym = b
	default:
		decline("get-else binds a tuple form")
		return rebuildWithChildren(node, children)
	}

	// Recover the child (the relation the get-else operates on)
	var childNode *Node
	if len(children) > 0 {
		childNode = recoverChild(children[0])
	}
	if childNode == nil {
		decline("no child relation to join against")
		return node
	}

	// Skip rewrite if entity variable isn't provided by the child relation
	// (e.g., it's an input parameter from :in, not a pattern variable).
	if !query.ContainsSymbol(childNode.Symbols(), entityVar.Symbol) {
		decline("entity variable is not provided by the child relation")
		return rebuildWithChildren(node, children)
	}

	// A get-else onto a variable the child relation already provides is a
	// unification the Map evaluates per tuple; the rewrite's scan would
	// output that variable beside the entity, and its default would stand
	// in for a value the child already holds.
	if query.ContainsSymbol(childNode.Symbols(), bindingSym) {
		decline("binding variable is already provided by the child relation")
		return rebuildWithChildren(node, children)
	}

	// The default branch reads the default's variables, so the child relation
	// provides each, as it provides the entity.
	for _, sym := range ge.Default.RequiredSymbols() {
		if !query.ContainsSymbol(childNode.Symbols(), sym) {
			decline("default variable is not provided by the child relation")
			return rebuildWithChildren(node, children)
		}
	}

	// A scope's required run is the scope's input, as :in is a query's, so
	// an entity it alone provides is an input parameter too. The default
	// branch the rewrite's own join decompiles to runs as a scope over the
	// entities the scan did not match, and its get-else is such a candidate.
	if originatesAtRequiredRunAlone(childNode, entityVar.Symbol) {
		decline("entity variable is provided by the scope's required run alone")
		return rebuildWithChildren(node, children)
	}

	// The scan stands in for the lookup only where a match on the source the
	// get-else reads answers, per entity, the one value the source's lookup
	// answers. The source answers for itself.
	if !answers(ge.ReadsFrom(), ge.Attr) {
		decline("the source's match does not answer its lookup's one value per entity")
		return rebuildWithChildren(node, children)
	}

	// Build: Scan([?entity :attr ?result]) on the source the get-else reads, a
	// $ source leaving the pattern's Source nil, the pattern's spelling of $.
	var scanSource query.Symbol
	if source := ge.ReadsFrom(); source != datalog.SymDollar {
		scanSource = source
	}
	scanPattern := &query.DataPattern{
		Source: scanSource,
		Elements: []query.PatternElement{
			query.Variable{Name: entityVar.Symbol},
			query.Constant{Value: ge.Attr},
			query.Variable{Name: bindingSym},
		},
	}
	scanNode := &Node{
		Op: RuleScan,
		Data: &Scan{
			Pattern: scanPattern,
			Output:  []query.Symbol{entityVar.Symbol, bindingSym},
		},
	}

	if observing {
		sink.Record(RewriteRecord{
			Pass:    getElsePassName,
			Action:  RewriteApplied,
			Subject: mapData.Expression,
		}, annotations.AlgebraGetElseScanApply, map[string]interface{}{
			"expression": mapData.Expression,
			"scan":       scanPattern,
			"entity_var": entityVar.Symbol,
			"binding":    bindingSym,
			"attribute":  ge.Attr,
		})
	}

	// Build: LeftOuterJoin(on=[?entity], default=[?result], get-else=ge)
	// The DefaultGetElse is the get-else the decompiler emits in the default
	// branch instead of plain ground. It reads the default from each tuple's
	// bindings and types it by the schema of the source it reads (e.g.,
	// []string vs []interface{} for a vector default); the Defaults entry
	// names the symbol it binds.
	joinNode := &Node{
		Op: RuleJoin,
		Data: &Join{
			Kind:           LeftOuterJoin,
			JoinSymbols:    []query.Symbol{entityVar.Symbol},
			Output:         mergeSymbols(childNode.Symbols(), []query.Symbol{bindingSym}),
			Defaults:       []Default{{Symbol: bindingSym}},
			DefaultGetElse: ge,
		},
		Children: []*Node{childNode, scanNode},
	}

	return wrapAsParseNode(joinNode)
}

// originatesAtRequiredRunAlone reports whether every node of the tree at
// which sym originates, outputting it while no child of the node does, is
// the scope's required run: the scope/required invocation, or the childless
// placeholder a header form's branch is compiled over, which provides the
// form's required run. root outputs sym.
func originatesAtRequiredRunAlone(root *Node, sym query.Symbol) bool {
	fromChild := false
	for _, child := range root.Children {
		if query.ContainsSymbol(child.Symbols(), sym) {
			fromChild = true
			if !originatesAtRequiredRunAlone(child, sym) {
				return false
			}
		}
	}
	if fromChild {
		return true
	}
	if _, placeholder := root.Data.(*Project); placeholder && len(root.Children) == 0 {
		return true
	}
	derived, ok := root.Data.(*Derived)
	return ok && derived.Invocation.Name == query.ScopeRequired
}
