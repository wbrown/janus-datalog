package algebra

import (
	"reflect"

	"github.com/wbrown/ebnf/parse"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// GetElseScanRewritePass returns a transform pass that rewrites
// Map(get-else) nodes into LeftOuterJoin + Scan.
//
// Rule 5 (ALGEBRA.md): get-else is a disguised left-outer-join.
// Replace N per-tuple point lookups with 1 index scan + hash join.
func GetElseScanRewritePass() Pass {
	return Pass{
		Name: "get-else-scan-rewrite",
		Transforms: parse.TransformMap{
			RuleMap: getElseScanRewriteTransform,
		},
	}
}

// getElseScanRewriteTransform detects Map(get-else) and rewrites to
// LeftOuterJoin(child, Scan([E A ?result])) with default.
func getElseScanRewriteTransform(node *parse.Node, children ...interface{}) interface{} {
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

	// Skip rewrite when default is a slice/vector — the ground expression
	// loses schema type information ([]interface{} vs []string).
	if ge.Default != nil && reflect.TypeOf(ge.Default).Kind() == reflect.Slice {
		return rebuildWithChildren(node, children)
	}

	// Extract the entity variable from the get-else
	entityVar, ok := ge.Entity.(query.VariableTerm)
	if !ok {
		return rebuildWithChildren(node, children)
	}

	// Extract the binding symbol (output variable)
	var bindingSym query.Symbol
	switch b := mapData.Expression.Binding.(type) {
	case query.Symbol:
		bindingSym = b
	default:
		return rebuildWithChildren(node, children) // Can't handle tuple bindings for get-else
	}

	// Recover the child (the relation the get-else operates on)
	var childNode *Node
	if len(children) > 0 {
		childNode = recoverChild(children[0])
	}
	if childNode == nil {
		return node
	}

	// Skip rewrite if entity variable isn't provided by the child relation
	// (e.g., it's an input parameter from :in, not a pattern variable).
	if !containsSymbol(childNode.Symbols(), entityVar.Symbol) {
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

	// Build: LeftOuterJoin(on=[?entity], default=[ge.Default])
	joinNode := &Node{
		Op: RuleJoin,
		Data: &Join{
			Kind:          LeftOuterJoin,
			JoinSymbols:   []query.Symbol{entityVar.Symbol},
			Output:        mergeSymbols(childNode.Symbols(), []query.Symbol{bindingSym}),
			DefaultValues: []interface{}{ge.Default},
		},
		Children: []*Node{childNode, scanNode},
	}

	return wrapAsParseNode(joinNode)
}
