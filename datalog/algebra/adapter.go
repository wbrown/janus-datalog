package algebra

import (
	"github.com/wbrown/ebnf/parse"
)

// ToParseTree converts an algebra Node tree into an EBNF parse.ParseTree.
// This enables the EBNF transform framework (Transform, TransformMultiPass,
// TransformTopDown) to apply algebraic equivalence rules.
//
// Each algebra.Node maps to a parse.Node where:
//   - Rule = the algebra operator name (Scan, Join, LateralJoin, etc.)
//   - TransformedValue = the algebra.Node itself (preserves typed data)
//   - Children = recursively adapted child nodes
func ToParseTree(root *Node) *parse.ParseTree {
	return &parse.ParseTree{
		Root: toParseNode(root),
	}
}

// FromParseTree converts an EBNF parse.ParseTree back to an algebra Node tree.
// Used after transform passes to recover the (possibly rewritten) algebra.
func FromParseTree(tree *parse.ParseTree) *Node {
	if tree == nil || tree.Root == nil {
		return nil
	}
	return fromParseNode(tree.Root)
}

func toParseNode(n *Node) *parse.Node {
	if n == nil {
		return nil
	}

	pn := &parse.Node{
		Rule:             n.Op,
		TransformedValue: n,
		Value:            n.Data.String(),
	}

	if len(n.Children) > 0 {
		pn.Children = make([]*parse.Node, len(n.Children))
		for i, child := range n.Children {
			pn.Children[i] = toParseNode(child)
		}
	}

	return pn
}

func fromParseNode(pn *parse.Node) *Node {
	if pn == nil {
		return nil
	}

	// Recover children first (they may have been rewritten by transforms)
	var children []*Node
	if len(pn.Children) > 0 {
		children = make([]*Node, len(pn.Children))
		for i, child := range pn.Children {
			children[i] = fromParseNode(child)
		}
	}

	// If the node carries an algebra.Node in TransformedValue, use it
	// with recovered children.
	if n, ok := pn.TransformedValue.(*Node); ok {
		// Clone the node so we don't mutate the original
		result := &Node{
			Op:       n.Op,
			Data:     n.Data,
			Children: children,
		}
		return result
	}

	// Fallback: node was created by a transform or preserved without
	// TransformedValue. Use the rule name as the Op.
	return &Node{
		Op:       pn.Rule,
		Children: children,
	}
}
