package algebra

import (
	"github.com/wbrown/ebnf/parse"
	"github.com/wbrown/janus-datalog/datalog/annotations"
)

// Optimizer applies algebraic equivalence rules to an algebra tree.
// Each pass is a parse.TransformMap that matches on algebra node rule names
// (Scan, Join, LateralJoin, etc.) and rewrites them.
type Optimizer struct {
	passes  []Pass
	handler annotations.Handler
}

// Pass is a named transform pass with an enablement predicate.
type Pass struct {
	Name       string
	Transforms parse.TransformMap
}

// NewOptimizer creates an optimizer with the given passes.
func NewOptimizer(passes ...Pass) *Optimizer {
	return &Optimizer{passes: passes}
}

// WithHandler sets the annotation handler for optimization observability.
func (o *Optimizer) WithHandler(h annotations.Handler) *Optimizer {
	o.handler = h
	return o
}

// Optimize applies all passes to an algebra tree and returns the rewritten tree.
// Each pass is a bottom-up transform over the parse.Node representation.
func (o *Optimizer) Optimize(root *Node) (*Node, error) {
	if root == nil || len(o.passes) == 0 {
		return root, nil
	}

	// Rebuild passes with handler if set, to enable annotations inside transforms
	passes := o.passes
	if o.handler != nil {
		passes = []Pass{DecorrelationPass(o.handler)}
	}

	tree := ToParseTree(root)

	for _, pass := range passes {
		result, err := parse.TransformPreserveStructure(tree, pass.Transforms)
		if err != nil {
			return nil, err
		}

		switch v := result.(type) {
		case *parse.Node:
			tree = &parse.ParseTree{Root: v}
		case *parse.ParseTree:
			tree = v
		case *Node:
			tree = ToParseTree(v)
		default:
			if tree.Root == nil {
				return root, nil
			}
		}
	}

	return FromParseTree(tree), nil
}

// DefaultPasses returns the standard optimization passes.
func DefaultPasses() []Pass {
	return []Pass{
		DecorrelationPass(nil),
	}
}
