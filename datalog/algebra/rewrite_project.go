package algebra

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// InsertJoinProjects inserts materialized projections on inner-join children
// when symbols are no longer live above that join. Compound operators remain
// conservative until their branch-local liveness laws are proven separately.
func InsertJoinProjects(root *Node, terminal []query.Symbol) (*Node, error) {
	if _, err := Analyze(root); err != nil {
		return nil, fmt.Errorf("insert join projects: %w", err)
	}

	var rewrite func(*Node, []query.Symbol) (*Node, error)
	rewrite = func(node *Node, live []query.Symbol) (*Node, error) {
		switch data := node.Data.(type) {
		case *Select:
			if len(node.Children) != 1 {
				return nil, fmt.Errorf("Select requires one child")
			}
			childLive := append([]query.Symbol(nil), live...)
			for _, symbol := range data.Required {
				if !query.ContainsSymbol(childLive, symbol) {
					childLive = append(childLive, symbol)
				}
			}
			child, err := rewrite(node.Children[0], childLive)
			if err != nil {
				return nil, err
			}
			return &Node{Op: node.Op, Data: node.Data, Children: []*Node{child}}, nil

		case *Map:
			if len(node.Children) == 0 {
				return node, nil
			}
			if len(node.Children) != 1 {
				return nil, fmt.Errorf("Map requires at most one child")
			}
			childLive := make([]query.Symbol, 0, len(live)+len(data.Required))
			for _, symbol := range node.Children[0].Symbols() {
				if query.ContainsSymbol(live, symbol) {
					childLive = append(childLive, symbol)
				}
			}
			for _, symbol := range data.Required {
				if !query.ContainsSymbol(childLive, symbol) {
					childLive = append(childLive, symbol)
				}
			}
			child, err := rewrite(node.Children[0], childLive)
			if err != nil {
				return nil, err
			}
			return &Node{Op: node.Op, Data: node.Data, Children: []*Node{child}}, nil

		case *Project:
			if len(node.Children) == 0 {
				return node, nil
			}
			if len(node.Children) != 1 {
				return nil, fmt.Errorf("Project requires at most one child")
			}
			child, err := rewrite(node.Children[0], data.Symbols)
			if err != nil {
				return nil, err
			}
			return &Node{Op: node.Op, Data: node.Data, Children: []*Node{child}}, nil

		case *Join:
			if data.Kind != InnerJoin || len(node.Children) != 2 {
				return node, nil
			}
			needed := func(child *Node) []query.Symbol {
				symbols := make([]query.Symbol, 0, len(child.Symbols()))
				for _, symbol := range child.Symbols() {
					if query.ContainsSymbol(live, symbol) || query.ContainsSymbol(data.JoinSymbols, symbol) {
						symbols = append(symbols, symbol)
					}
				}
				return symbols
			}
			project := func(child *Node, symbols []query.Symbol) *Node {
				if len(symbols) == 0 || len(symbols) == len(child.Symbols()) {
					return child
				}
				return &Node{
					Op:       RuleProject,
					Data:     &Project{Symbols: symbols},
					Children: []*Node{child},
				}
			}

			leftNeeded := needed(node.Children[0])
			left, err := rewrite(node.Children[0], leftNeeded)
			if err != nil {
				return nil, err
			}
			rightNeeded := needed(node.Children[1])
			right, err := rewrite(node.Children[1], rightNeeded)
			if err != nil {
				return nil, err
			}
			return &Node{
				Op:   node.Op,
				Data: node.Data,
				Children: []*Node{
					project(left, leftNeeded),
					project(right, rightNeeded),
				},
			}, nil

		default:
			return node, nil
		}
	}

	rewritten, err := rewrite(root, terminal)
	if err != nil {
		return nil, fmt.Errorf("insert join projects: %w", err)
	}
	refreshed, err := RefreshSchemas(rewritten)
	if err != nil {
		return nil, fmt.Errorf("insert join projects: %w", err)
	}
	return refreshed, nil
}
