package algebra

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// Analysis describes the physical schema produced by an algebra node and any
// symbols its subtree requires from an enclosing correlated context.
type Analysis struct {
	Output   []query.Symbol
	Required []query.Symbol
}

// RefreshSchemas immutably rebuilds a transformed algebra tree and derives
// every operator output that is determined by its rewritten children.
func RefreshSchemas(root *Node) (*Node, error) {
	var refresh func(*Node) (*Node, error)
	refresh = func(node *Node) (*Node, error) {
		if node == nil {
			return nil, fmt.Errorf("refresh algebra schemas: nil node")
		}
		children := make([]*Node, len(node.Children))
		for i, child := range node.Children {
			refreshed, err := refresh(child)
			if err != nil {
				return nil, err
			}
			children[i] = refreshed
		}

		refreshed := &Node{Op: node.Op, Children: children}
		switch data := node.Data.(type) {
		case *Scan:
			copy := *data
			copy.Output = cloneSymbols(data.Output)
			refreshed.Data = &copy
		case *Select:
			if len(children) > 1 {
				return nil, fmt.Errorf("refresh Select: expected at most one child, got %d", len(children))
			}
			copy := *data
			copy.Required = cloneSymbols(data.Required)
			if len(children) == 1 {
				copy.Output = cloneSymbols(children[0].Symbols())
			} else {
				copy.Output = cloneSymbols(data.Output)
			}
			refreshed.Data = &copy
		case *Project:
			copy := *data
			copy.Symbols = cloneSymbols(data.Symbols)
			refreshed.Data = &copy
		case *Map:
			if len(children) > 1 {
				return nil, fmt.Errorf("refresh Map: expected at most one child, got %d", len(children))
			}
			copy := *data
			copy.Required = cloneSymbols(data.Required)
			var output []query.Symbol
			if len(children) == 1 {
				output = cloneSymbols(children[0].Symbols())
			}
			if data.Expression != nil {
				output = mergeSymbols(output, bindingSymbols(data.Expression.Binding))
			}
			copy.Output = output
			refreshed.Data = &copy
		case *Join:
			if len(children) != 2 {
				return nil, fmt.Errorf("refresh Join: expected two children, got %d", len(children))
			}
			copy := *data
			copy.JoinSymbols = cloneSymbols(data.JoinSymbols)
			copy.Output = mergeSymbols(children[0].Symbols(), children[1].Symbols())
			copy.DefaultValues = append([]interface{}(nil), data.DefaultValues...)
			refreshed.Data = &copy
		case *AntiJoin:
			if len(children) != 2 {
				return nil, fmt.Errorf("refresh AntiJoin: expected two children, got %d", len(children))
			}
			copy := *data
			copy.JoinSymbols = cloneSymbols(data.JoinSymbols)
			copy.Required = cloneSymbols(data.Required)
			copy.Output = cloneSymbols(children[0].Symbols())
			refreshed.Data = &copy
		case *Union:
			// Single-branch unions are legal IR (single-branch or/or-join);
			// see analyzeUnionBranches.
			if len(children) < 1 {
				return nil, fmt.Errorf("refresh Union: expected at least one child, got %d", len(children))
			}
			copy := *data
			copy.Output = cloneSymbols(data.Output)
			copy.JoinVars = cloneSymbols(data.JoinVars)
			copy.Required = cloneSymbols(data.Required)
			refreshed.Data = &copy
		case *LateralUnion:
			// Arity parity with Union: clause-level branch minimums (e.g.
			// or-default's two branches) are language rules enforced by the
			// clause's Validate at the boundaries, not IR invariants.
			if len(children) < 1 {
				return nil, fmt.Errorf("refresh LateralUnion: expected at least one child, got %d", len(children))
			}
			copy := *data
			copy.Output = cloneSymbols(data.Output)
			copy.RequiredVars = cloneSymbols(data.RequiredVars)
			copy.OutputVars = cloneSymbols(data.OutputVars)
			copy.Required = cloneSymbols(data.Required)
			refreshed.Data = &copy
		case *LateralJoin:
			if len(children) > 1 {
				return nil, fmt.Errorf("refresh LateralJoin: expected at most one child, got %d", len(children))
			}
			copy := *data
			copy.CorrelationVars = cloneSymbols(data.CorrelationVars)
			copy.DefaultValues = append([]interface{}(nil), data.DefaultValues...)
			var output []query.Symbol
			if len(children) == 1 {
				output = cloneSymbols(children[0].Symbols())
			} else {
				output = cloneSymbols(data.CorrelationVars)
			}
			binding, ok := data.Binding.(query.BindingForm)
			if !ok {
				return nil, fmt.Errorf("refresh LateralJoin: binding has type %T", data.Binding)
			}
			copy.Output = mergeSymbols(output, binding.BoundVariables())
			refreshed.Data = &copy
		case *Aggregate:
			copy := *data
			copy.GroupBy = cloneSymbols(data.GroupBy)
			copy.Functions = append([]query.FindAggregate(nil), data.Functions...)
			copy.Bindings = cloneSymbols(data.Bindings)
			output, err := aggregateOutputSchema(data)
			if err != nil {
				return nil, fmt.Errorf("refresh Aggregate: %w", err)
			}
			copy.Output = output
			refreshed.Data = &copy
		case *Constant:
			copy := *data
			copy.Symbols = cloneSymbols(data.Symbols)
			copy.Values = append([]interface{}(nil), data.Values...)
			refreshed.Data = &copy
		default:
			return nil, fmt.Errorf("refresh %s: unsupported operator data %T", node.Op, node.Data)
		}
		return refreshed, nil
	}

	refreshed, err := refresh(root)
	if err != nil {
		return nil, err
	}
	if _, err := Analyze(refreshed); err != nil {
		return nil, fmt.Errorf("refresh algebra schemas: %w", err)
	}
	return refreshed, nil
}

// Analyze validates an algebra tree and returns the exact schema and free
// requirements of every node.
func Analyze(root *Node) (map[*Node]Analysis, error) {
	if root == nil {
		return nil, fmt.Errorf("analyze algebra: nil root")
	}
	result := make(map[*Node]Analysis)
	if _, err := analyzeNode(root, result); err != nil {
		return nil, err
	}
	return result, nil
}

func analyzeNode(node *Node, result map[*Node]Analysis) (Analysis, error) {
	if node == nil {
		return Analysis{}, fmt.Errorf("analyze algebra: nil node")
	}
	if existing, ok := result[node]; ok {
		return existing, nil
	}

	children := make([]Analysis, len(node.Children))
	for i, child := range node.Children {
		analysis, err := analyzeNode(child, result)
		if err != nil {
			return Analysis{}, fmt.Errorf("%s child %d: %w", node.Op, i, err)
		}
		children[i] = analysis
	}

	var analysis Analysis
	var err error
	switch node.Op {
	case RuleScan:
		analysis, err = analyzeScan(node, children)
	case RuleSelect:
		analysis, err = analyzeSelect(node, children)
	case RuleProject:
		analysis, err = analyzeProject(node, children)
	case RuleMap:
		analysis, err = analyzeMap(node, children)
	case RuleJoin:
		analysis, err = analyzeJoin(node, children)
	case RuleAntiJoin:
		analysis, err = analyzeAntiJoin(node, children)
	case RuleUnion:
		analysis, err = analyzeUnion(node, children)
	case RuleLateralUnion:
		analysis, err = analyzeLateralUnion(node, children)
	case RuleLateralJoin:
		analysis, err = analyzeLateralJoin(node, children)
	case RuleAggregate:
		analysis, err = analyzeAggregate(node, children)
	case RuleConstant:
		analysis, err = analyzeConstant(node, children)
	default:
		err = fmt.Errorf("unknown algebra operator %q", node.Op)
	}
	if err != nil {
		return Analysis{}, fmt.Errorf("%s: %w", node.Op, err)
	}
	if err := validateUniqueSymbols("output", analysis.Output); err != nil {
		return Analysis{}, err
	}
	analysis.Output = cloneSymbols(analysis.Output)
	analysis.Required = uniqueSymbols(analysis.Required)
	result[node] = analysis
	return analysis, nil
}

func analyzeScan(node *Node, children []Analysis) (Analysis, error) {
	if err := requireChildCount(node.Op, children, 0); err != nil {
		return Analysis{}, err
	}
	data, ok := node.Data.(*Scan)
	if !ok {
		return Analysis{}, dataTypeError(node, "*algebra.Scan")
	}
	return Analysis{Output: data.Output}, nil
}

func analyzeSelect(node *Node, children []Analysis) (Analysis, error) {
	if len(children) > 1 {
		return Analysis{}, fmt.Errorf("expected at most one child, got %d", len(children))
	}
	data, ok := node.Data.(*Select)
	if !ok {
		return Analysis{}, dataTypeError(node, "*algebra.Select")
	}
	var output []query.Symbol
	var required []query.Symbol
	if len(children) == 1 {
		output = children[0].Output
		required = append(required, children[0].Required...)
	} else {
		output = data.Output
	}
	for _, symbol := range data.Required {
		if !query.ContainsSymbol(output, symbol) {
			required = append(required, symbol)
		}
	}
	if !sameSymbolsInOrder(data.Output, output) {
		return Analysis{}, fmt.Errorf("select output %v does not match child output %v", data.Output, output)
	}
	return Analysis{Output: data.Output, Required: required}, nil
}

func analyzeProject(node *Node, children []Analysis) (Analysis, error) {
	if len(children) > 1 {
		return Analysis{}, fmt.Errorf("expected zero or one child, got %d", len(children))
	}
	data, ok := node.Data.(*Project)
	if !ok {
		return Analysis{}, dataTypeError(node, "*algebra.Project")
	}
	if len(children) == 0 {
		return Analysis{Output: data.Symbols, Required: data.Symbols}, nil
	}
	for _, symbol := range data.Symbols {
		if !query.ContainsSymbol(children[0].Output, symbol) {
			return Analysis{}, fmt.Errorf("project symbol %s is not produced by its child", symbol)
		}
	}
	return Analysis{Output: data.Symbols, Required: children[0].Required}, nil
}

func analyzeMap(node *Node, children []Analysis) (Analysis, error) {
	if len(children) > 1 {
		return Analysis{}, fmt.Errorf("expected at most one child, got %d", len(children))
	}
	data, ok := node.Data.(*Map)
	if !ok {
		return Analysis{}, dataTypeError(node, "*algebra.Map")
	}
	var childOutput []query.Symbol
	var required []query.Symbol
	if len(children) == 1 {
		childOutput = children[0].Output
		required = append(required, children[0].Required...)
	}
	for _, symbol := range data.Required {
		if !query.ContainsSymbol(childOutput, symbol) {
			required = append(required, symbol)
		}
	}
	expected := cloneSymbols(childOutput)
	if data.Expression != nil {
		expected = mergeSymbols(expected, bindingSymbols(data.Expression.Binding))
		if !sameSymbolsInOrder(data.Output, expected) {
			return Analysis{}, fmt.Errorf("map output %v does not match expected output %v", data.Output, expected)
		}
	} else if !containsAllSymbols(data.Output, childOutput) {
		return Analysis{}, fmt.Errorf("map output %v omits child symbols %v", data.Output, childOutput)
	}
	return Analysis{Output: data.Output, Required: required}, nil
}

func analyzeJoin(node *Node, children []Analysis) (Analysis, error) {
	if err := requireChildCount(node.Op, children, 2); err != nil {
		return Analysis{}, err
	}
	data, ok := node.Data.(*Join)
	if !ok {
		return Analysis{}, dataTypeError(node, "*algebra.Join")
	}
	for _, symbol := range data.JoinSymbols {
		if !query.ContainsSymbol(children[0].Output, symbol) || !query.ContainsSymbol(children[1].Output, symbol) {
			return Analysis{}, fmt.Errorf("join symbol %s must be produced by both children", symbol)
		}
	}
	expected := mergeSymbols(children[0].Output, children[1].Output)
	if !sameSymbolsInOrder(data.Output, expected) {
		return Analysis{}, fmt.Errorf("join output %v does not match combined child output %v", data.Output, expected)
	}
	return Analysis{
		Output:   data.Output,
		Required: combineFreeRequirements(children[0], children[1]),
	}, nil
}

func analyzeAntiJoin(node *Node, children []Analysis) (Analysis, error) {
	if err := requireChildCount(node.Op, children, 2); err != nil {
		return Analysis{}, err
	}
	data, ok := node.Data.(*AntiJoin)
	if !ok {
		return Analysis{}, dataTypeError(node, "*algebra.AntiJoin")
	}
	// A declared symbol the left child does not produce is a free requirement
	// of the anti-join node itself: the enclosing context — ultimately the
	// query's environment — supplies it at evaluation. Select and Map treat
	// their unproduced Required the same way; Union models the same outer
	// supply as outerRequired. What a symbol *means* (correlation demand of
	// the right subtree, equality key, declared interface) stays validated
	// below; only who supplies it widens.
	var free []query.Symbol
	for _, symbol := range data.Required {
		if !query.ContainsSymbol(data.JoinSymbols, symbol) {
			return Analysis{}, fmt.Errorf("correlation requirement %s is not declared as an anti-join symbol", symbol)
		}
		if query.ContainsSymbol(children[1].Output, symbol) {
			return Analysis{}, fmt.Errorf("correlation requirement %s is already produced by the right child", symbol)
		}
		if !query.ContainsSymbol(children[1].Required, symbol) {
			return Analysis{}, fmt.Errorf("correlation requirement %s is not a free requirement of the right child", symbol)
		}
		if !query.ContainsSymbol(children[0].Output, symbol) {
			free = append(free, symbol)
		}
	}
	for _, symbol := range data.JoinSymbols {
		if !query.ContainsSymbol(children[1].Output, symbol) && !query.ContainsSymbol(data.Required, symbol) {
			return Analysis{}, fmt.Errorf(
				"anti-join symbol %s must be produced by the right child or declared as a correlation requirement",
				symbol,
			)
		}
		if !query.ContainsSymbol(children[0].Output, symbol) {
			free = append(free, symbol)
		}
	}
	for _, symbol := range children[1].Required {
		if symbol.IsSource() || query.ContainsSymbol(children[1].Output, symbol) {
			continue
		}
		if query.ContainsSymbol(children[0].Output, symbol) && !query.ContainsSymbol(data.Required, symbol) {
			return Analysis{}, fmt.Errorf(
				"right child requires outer symbol %s but the anti-join does not declare it as a correlation requirement",
				symbol,
			)
		}
	}
	if !sameSymbolsInOrder(data.Output, children[0].Output) {
		return Analysis{}, fmt.Errorf("anti-join output %v does not match left child output %v", data.Output, children[0].Output)
	}
	return Analysis{
		Output:   data.Output,
		Required: uniqueSymbols(append(antiJoinFreeRequirements(children[0], children[1]), free...)),
	}, nil
}

func analyzeUnion(node *Node, children []Analysis) (Analysis, error) {
	data, ok := node.Data.(*Union)
	if !ok {
		return Analysis{}, dataTypeError(node, "*algebra.Union")
	}
	return analyzeUnionBranches(data.Output, data.JoinVars, data.Required, children)
}

func analyzeLateralUnion(node *Node, children []Analysis) (Analysis, error) {
	data, ok := node.Data.(*LateralUnion)
	if !ok {
		return Analysis{}, dataTypeError(node, "*algebra.LateralUnion")
	}
	return analyzeUnionBranches(data.Output, data.RequiredVars, data.Required, children)
}

func analyzeUnionBranches(output, joinVars, outerRequired []query.Symbol, children []Analysis) (Analysis, error) {
	// A single-branch union is the honest IR image of a legal single-branch
	// or/or-join: the branch restricted to the declared header interface.
	// The IR must accept every shape the language accepts; clause-level
	// arity rules (e.g. or-default's two-branch minimum) are enforced at
	// the language boundaries by the clause's own Validate, not here.
	if len(children) == 0 {
		return Analysis{}, fmt.Errorf("union requires at least one branch")
	}
	var required []query.Symbol
	for i, child := range children {
		for _, symbol := range output {
			if query.ContainsSymbol(outerRequired, symbol) {
				continue
			}
			if !query.ContainsSymbol(child.Output, symbol) {
				if len(joinVars) > 0 && query.ContainsSymbol(joinVars, symbol) {
					return Analysis{}, fmt.Errorf(
						"or-join header declares %s, but branch %d schema %v does not bind it; every branch must bind every header variable; if %s is an input filter rather than an output, remove it from the header",
						symbol,
						i,
						child.Output,
						symbol,
					)
				}
				return Analysis{}, fmt.Errorf("union branch %d schema %v does not provide output symbol %s", i, child.Output, symbol)
			}
		}
		required = append(required, child.Required...)
	}
	for _, symbol := range joinVars {
		if !query.ContainsSymbol(output, symbol) {
			return Analysis{}, fmt.Errorf("union join variable %s is not in union output", symbol)
		}
	}
	required = append(required, outerRequired...)
	return Analysis{Output: output, Required: required}, nil
}

func analyzeLateralJoin(node *Node, children []Analysis) (Analysis, error) {
	if len(children) > 1 {
		return Analysis{}, fmt.Errorf("expected at most one child, got %d", len(children))
	}
	data, ok := node.Data.(*LateralJoin)
	if !ok {
		return Analysis{}, dataTypeError(node, "*algebra.LateralJoin")
	}
	if data.InnerQuery == nil {
		return Analysis{}, fmt.Errorf("lateral join has nil inner query")
	}
	var outer []query.Symbol
	var required []query.Symbol
	if len(children) == 1 {
		outer = children[0].Output
		required = append(required, children[0].Required...)
	}
	for _, symbol := range data.CorrelationVars {
		if !query.ContainsSymbol(outer, symbol) {
			required = append(required, symbol)
		}
	}
	binding, ok := data.Binding.(query.BindingForm)
	if !ok {
		return Analysis{}, fmt.Errorf("lateral binding has type %T", data.Binding)
	}
	bound := binding.BoundVariables()
	if len(bound) != len(data.InnerQuery.Find) {
		return Analysis{}, fmt.Errorf("lateral binding arity %d does not match inner find arity %d", len(bound), len(data.InnerQuery.Find))
	}
	expected := cloneSymbols(outer)
	if len(children) == 0 {
		expected = mergeSymbols(expected, data.CorrelationVars)
	}
	expected = mergeSymbols(expected, bound)
	if !sameSymbolsInOrder(data.Output, expected) {
		return Analysis{}, fmt.Errorf("lateral output %v does not match outer and binding output %v", data.Output, expected)
	}
	return Analysis{Output: data.Output, Required: required}, nil
}

func analyzeAggregate(node *Node, children []Analysis) (Analysis, error) {
	if err := requireChildCount(node.Op, children, 1); err != nil {
		return Analysis{}, err
	}
	data, ok := node.Data.(*Aggregate)
	if !ok {
		return Analysis{}, dataTypeError(node, "*algebra.Aggregate")
	}
	required := append([]query.Symbol(nil), children[0].Required...)
	for _, symbol := range data.GroupBy {
		if !query.ContainsSymbol(children[0].Output, symbol) {
			required = append(required, symbol)
		}
	}
	for _, function := range data.Functions {
		if !query.ContainsSymbol(children[0].Output, function.Arg) {
			required = append(required, function.Arg)
		}
		if function.Predicate != nil && !query.ContainsSymbol(children[0].Output, function.Predicate) {
			required = append(required, function.Predicate)
		}
	}
	expected, err := aggregateOutputSchema(data)
	if err != nil {
		return Analysis{}, err
	}
	if !sameSymbolsInOrder(data.Output, expected) {
		return Analysis{}, fmt.Errorf("aggregate output %v does not match group keys and result bindings %v",
			data.Output, expected)
	}
	return Analysis{Output: data.Output, Required: required}, nil
}

func analyzeConstant(node *Node, children []Analysis) (Analysis, error) {
	if err := requireChildCount(node.Op, children, 0); err != nil {
		return Analysis{}, err
	}
	data, ok := node.Data.(*Constant)
	if !ok {
		return Analysis{}, dataTypeError(node, "*algebra.Constant")
	}
	if len(data.Symbols) != len(data.Values) {
		return Analysis{}, fmt.Errorf("constant symbol arity %d does not match value arity %d", len(data.Symbols), len(data.Values))
	}
	return Analysis{Output: data.Symbols}, nil
}

func requireChildCount(operator string, children []Analysis, expected int) error {
	if len(children) != expected {
		return fmt.Errorf("expected %d children, got %d", expected, len(children))
	}
	return nil
}

func dataTypeError(node *Node, expected string) error {
	return fmt.Errorf("operator data has type %T, expected %s", node.Data, expected)
}

func combineFreeRequirements(left, right Analysis) []query.Symbol {
	required := cloneSymbols(left.Required)
	for _, symbol := range right.Required {
		if !query.ContainsSymbol(left.Output, symbol) {
			required = append(required, symbol)
		}
	}
	for _, symbol := range left.Required {
		if query.ContainsSymbol(right.Output, symbol) {
			required = removeSymbol(required, symbol)
		}
	}
	return uniqueSymbols(required)
}

func antiJoinFreeRequirements(left, right Analysis) []query.Symbol {
	required := cloneSymbols(left.Required)
	for _, symbol := range right.Required {
		if !query.ContainsSymbol(left.Output, symbol) {
			required = append(required, symbol)
		}
	}
	return uniqueSymbols(required)
}

func aggregateOutputSchema(data *Aggregate) ([]query.Symbol, error) {
	if len(data.Bindings) != len(data.GroupBy)+len(data.Functions) {
		return nil, fmt.Errorf("aggregate binding arity %d does not match %d group keys and %d functions",
			len(data.Bindings), len(data.GroupBy), len(data.Functions))
	}
	for i, binding := range data.Bindings {
		if binding == nil {
			return nil, fmt.Errorf("aggregate output binding %d is nil", i)
		}
	}
	return cloneSymbols(data.Bindings), nil
}

func validateUniqueSymbols(name string, symbols []query.Symbol) error {
	seen := make(map[query.Symbol]struct{}, len(symbols))
	for _, symbol := range symbols {
		if _, ok := seen[symbol]; ok {
			return fmt.Errorf("duplicate %s symbol %s", name, symbol)
		}
		seen[symbol] = struct{}{}
	}
	return nil
}

func sameSymbolsInOrder(left, right []query.Symbol) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func containsAllSymbols(have, required []query.Symbol) bool {
	for _, symbol := range required {
		if !query.ContainsSymbol(have, symbol) {
			return false
		}
	}
	return true
}

func uniqueSymbols(symbols []query.Symbol) []query.Symbol {
	result := make([]query.Symbol, 0, len(symbols))
	for _, symbol := range symbols {
		if !query.ContainsSymbol(result, symbol) {
			result = append(result, symbol)
		}
	}
	return result
}

func removeSymbol(symbols []query.Symbol, remove query.Symbol) []query.Symbol {
	result := symbols[:0]
	for _, symbol := range symbols {
		if symbol != remove {
			result = append(result, symbol)
		}
	}
	return result
}

func cloneSymbols(symbols []query.Symbol) []query.Symbol {
	return append([]query.Symbol(nil), symbols...)
}
