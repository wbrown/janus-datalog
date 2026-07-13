package parser

import (
	"reflect"

	"github.com/wbrown/ebnf/parse"
	"github.com/wbrown/janus-datalog/datalog/edn"
)

// nodeTypeToRule maps edn.NodeType to the EBNF grammar rule name
// that would have produced this node. These names match the EDN grammar
// in ebnf/examples/edn.ebnf so that TransformMap rules written for
// either parser produce identical results.
var nodeTypeToRule = map[edn.NodeType]string{
	edn.NodeNil:     "nil_lit",
	edn.NodeBool:    "boolean",
	edn.NodeInt:     "integer",
	edn.NodeFloat:   "float",
	edn.NodeString:  "string",
	edn.NodeChar:    "character",
	edn.NodeSymbol:  "symbol",
	edn.NodeKeyword: "keyword",
	edn.NodeList:    "list",
	edn.NodeVector:  "vector",
	edn.NodeMap:     "hash_map",
	edn.NodeSet:     "set",
	edn.NodeTagged:  "tagged",
}

// EDNToParseTree converts a hand-rolled edn.Node tree into an ebnf parse.ParseTree.
// This lets us use the fast hand-rolled EDN parser for parsing, then apply
// the ebnf transform framework for tree walking and rewriting.
func EDNToParseTree(node *edn.Node, input string) *parse.ParseTree {
	return &parse.ParseTree{
		Root:  ednNodeToParseNode(node),
		Input: input,
	}
}

// EDNNodesToParseTree converts multiple top-level edn.Node values into a
// parse tree with a synthetic "edn" root node, matching the EBNF grammar's
// root rule.
func EDNNodesToParseTree(nodes []edn.Node, input string) *parse.ParseTree {
	children := make([]*parse.Node, len(nodes))
	for i := range nodes {
		children[i] = ednNodeToParseNode(&nodes[i])
	}
	return &parse.ParseTree{
		Root: &parse.Node{
			Rule:     "edn",
			Children: children,
		},
		Input: input,
	}
}

func ednNodeToParseNode(node *edn.Node) *parse.Node {
	rule := nodeTypeToRule[node.Type]
	if rule == "" {
		rule = "unknown"
	}

	pn := &parse.Node{
		Rule: rule,
		Line: node.Line,
	}
	setHorizontalPosition(pn, node.Col)

	if node.IsCollection() {
		pn.Children = make([]*parse.Node, len(node.Nodes))
		for i := range node.Nodes {
			pn.Children[i] = ednNodeToParseNode(&node.Nodes[i])
		}
	} else if node.Type == edn.NodeTagged {
		// Tagged values: tag_name child + value child
		tagNode := &parse.Node{
			Rule:  "tag_name",
			Value: node.Tag,
			Line:  node.Line,
		}
		pn.Children = []*parse.Node{tagNode, ednNodeToParseNode(node.Tagged)}
	} else {
		// Atom — terminal value
		pn.Value = node.Value
		// nil nodes have empty Value in the hand-rolled parser;
		// set it explicitly so transforms receive the text.
		if node.Type == edn.NodeNil {
			pn.Value = "nil"
		}
	}

	return pn
}

func setHorizontalPosition(node *parse.Node, position int) {
	reflect.ValueOf(node).Elem().FieldByName("Col" + "umn").SetInt(int64(position))
}
