package algebra

import (
	"github.com/wbrown/ebnf/parse"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// getElsePassName names the pass in rewrite records and event data.
const getElsePassName = "get-else-scan-rewrite"

// GetElseScanRewritePass returns a transform pass that rewrites
// Map(get-else) nodes into LeftOuterJoin + Scan. Every decision on a
// get-else candidate — applied, or declined with the failed precondition —
// goes to the sink as a typed RewriteRecord and as its annotation-event form
// (a nil sink records nothing). Map nodes that are not get-else expressions
// are not candidates and produce no record.
//
// Rule 5 (ALGEBRA.md): get-else is a disguised left-outer-join.
// Replace N per-tuple point lookups with 1 index scan + hash join.
func GetElseScanRewritePass(sink *RewriteSink) Pass {
	return Pass{
		Name: getElsePassName,
		Transforms: parse.TransformMap{
			RuleMap: makeGetElseScanRewriteTransform(sink),
		},
	}
}

func makeGetElseScanRewriteTransform(sink *RewriteSink) parse.TransformFunc {
	return func(node *parse.Node, children ...interface{}) interface{} {
		return getElseScanRewriteTransform(node, sink, children...)
	}
}

// getElseScanRewriteTransform detects Map(get-else) and rewrites to
// LeftOuterJoin(child, Scan([E A ?result])) with default.
func getElseScanRewriteTransform(node *parse.Node, sink *RewriteSink, children ...interface{}) interface{} {
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
	// it costs nothing either way. The sink may be nil: several callers build
	// this pass with GetElseScanRewritePass(nil).
	observing := sink != nil && (sink.Collect || sink.Handler != nil)
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

	// Build: Scan([?entity :attr ?result])
	scanPattern := &query.DataPattern{
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

	// Build: LeftOuterJoin(on=[?entity], default=[ge.Default], attr=ge.Attr)
	// The DefaultAttr enables the decompiler to emit a get-else expression
	// in the default branch instead of plain ground, preserving schema type
	// information for vector defaults (e.g., []string vs []interface{}).
	defaultAttr := ge.Attr
	joinNode := &Node{
		Op: RuleJoin,
		Data: &Join{
			Kind:          LeftOuterJoin,
			JoinSymbols:   []query.Symbol{entityVar.Symbol},
			Output:        mergeSymbols(childNode.Symbols(), []query.Symbol{bindingSym}),
			DefaultValues: []interface{}{ge.Default},
			DefaultAttr:   &defaultAttr,
		},
		Children: []*Node{childNode, scanNode},
	}

	return wrapAsParseNode(joinNode)
}
