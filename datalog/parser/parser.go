package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/codec"
	"github.com/wbrown/janus-datalog/datalog/edn"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// ParseQuery parses a Datalog query from EDN format
func ParseQuery(input string) (*query.Query, error) {
	// Parse as EDN first
	node, err := edn.Parse(input)
	if err != nil {
		return nil, fmt.Errorf("EDN parse error: %w", err)
	}

	// Must be a vector
	if node.Type != edn.NodeVector {
		return nil, fmt.Errorf("query must be a vector, got %v", node.Type)
	}

	q, err := parseQueryVector(node)
	if err != nil {
		return nil, err
	}
	// Every static check the text admits runs through ValidateQuery, which
	// reaches every inner query.
	if err := query.ValidateQuery(q); err != nil {
		return nil, err
	}
	return q, nil
}

// parseQueryVector parses a query from an EDN vector node
func parseQueryVector(node *edn.Node) (*query.Query, error) {
	q := &query.Query{}

	i := 0
	for i < len(node.Nodes) {
		if node.Nodes[i].Type != edn.NodeKeyword {
			return nil, fmt.Errorf("expected keyword at position %d, got %v", i, node.Nodes[i].Type)
		}

		keyword := node.Nodes[i].Value
		i++

		switch keyword {
		case ":find":
			// Parse find elements (variables or aggregates)
			// Also check for scalar find spec: ":find (max ?v) ." where . indicates scalar return
			for i < len(node.Nodes) && node.Nodes[i].Type != edn.NodeKeyword {
				// Check for scalar find spec marker "."
				if node.Nodes[i].Type == edn.NodeSymbol && node.Nodes[i].Value == "." {
					q.ScalarReturn = true
					i++
					continue
				}

				elem, err := parseFindElement(&node.Nodes[i])
				if err != nil {
					return nil, fmt.Errorf("error parsing find element: %w", err)
				}
				q.Find = append(q.Find, elem)
				i++
			}

		case ":in":
			// Parse input specifications
			for i < len(node.Nodes) && node.Nodes[i].Type != edn.NodeKeyword {
				input, err := parseInputSpec(&node.Nodes[i])
				if err != nil {
					return nil, fmt.Errorf("error parsing input spec: %w", err)
				}
				q.In = append(q.In, input)
				i++
			}

		case ":where":
			// Parse where patterns
			for i < len(node.Nodes) && node.Nodes[i].Type != edn.NodeKeyword {
				var clause query.Clause
				var err error

				switch node.Nodes[i].Type {
				case edn.NodeVector:
					// Standard pattern: [?e :attr ?v] or [(fn ...) ?binding]
					clause, err = parsePattern(&node.Nodes[i])
				case edn.NodeList:
					// List form: (not ...), (or ...), (not-join ...), (or-join ...)
					clause, err = parseListClause(&node.Nodes[i])
				default:
					return nil, fmt.Errorf("expected vector or list in :where clause, got %v", node.Nodes[i].Type)
				}

				if err != nil {
					return nil, fmt.Errorf("error parsing pattern: %w", err)
				}
				q.Where = append(q.Where, clause)
				i++
			}

		case ":order-by":
			// Parse order-by clauses
			// :order-by expects a vector of clauses
			if i >= len(node.Nodes) {
				return nil, fmt.Errorf(":order-by requires a vector")
			}

			if node.Nodes[i].Type != edn.NodeVector {
				return nil, fmt.Errorf(":order-by must be followed by a vector, got %v", node.Nodes[i].Type)
			}

			orderByVector := &node.Nodes[i]
			for j := 0; j < len(orderByVector.Nodes); j++ {
				clause, err := parseOrderByClause(&orderByVector.Nodes[j])
				if err != nil {
					return nil, fmt.Errorf("error parsing order-by clause: %w", err)
				}
				q.OrderBy = append(q.OrderBy, clause)
			}
			i++

		case ":limit":
			// :limit expects exactly one non-negative integer.
			if i >= len(node.Nodes) {
				return nil, fmt.Errorf(":limit requires a non-negative integer")
			}
			if node.Nodes[i].Type != edn.NodeInt {
				return nil, fmt.Errorf(":limit must be a non-negative integer, got %v", node.Nodes[i].Type)
			}
			n, err := strconv.ParseInt(node.Nodes[i].Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf(":limit must be a valid integer: %w", err)
			}
			if n < 0 {
				return nil, fmt.Errorf(":limit must be non-negative, got %d", n)
			}
			limit := int(n)
			q.Limit = &limit
			i++

		default:
			return nil, fmt.Errorf("unknown query clause: %s", keyword)
		}
	}

	return q, nil
}

// parseOrderByClause parses an order-by clause element
func parseOrderByClause(node *edn.Node) (query.OrderByClause, error) {
	switch node.Type {
	case edn.NodeSymbol:
		// Simple variable (defaults to ascending)
		sym := datalog.NewSymbol(node.Value)
		if !sym.IsVariable() {
			return query.OrderByClause{}, fmt.Errorf("order-by must use variables, got %s", sym)
		}
		return query.OrderByClause{Variable: sym}, nil

	case edn.NodeVector:
		// [?var :direction] format
		if len(node.Nodes) != 2 {
			return query.OrderByClause{}, fmt.Errorf("order-by vector must have exactly 2 elements: [variable direction]")
		}

		if node.Nodes[0].Type != edn.NodeSymbol {
			return query.OrderByClause{}, fmt.Errorf("order-by variable must be a symbol")
		}

		sym := datalog.NewSymbol(node.Nodes[0].Value)
		if !sym.IsVariable() {
			return query.OrderByClause{}, fmt.Errorf("order-by must use variables, got %s", sym)
		}

		if node.Nodes[1].Type != edn.NodeKeyword {
			return query.OrderByClause{}, fmt.Errorf("order-by direction must be a keyword (:asc or :desc)")
		}

		var descending bool
		switch node.Nodes[1].Value {
		case ":asc":
			descending = false
		case ":desc":
			descending = true
		default:
			return query.OrderByClause{}, fmt.Errorf("order-by direction must be :asc or :desc, got %s", node.Nodes[1].Value)
		}

		return query.OrderByClause{
			Variable:   sym,
			Descending: descending,
		}, nil

	default:
		return query.OrderByClause{}, fmt.Errorf("order-by element must be a symbol or vector, got %v", node.Type)
	}
}

// parseFindElement parses a find element (variable or aggregate)
func parseFindElement(node *edn.Node) (query.FindElement, error) {
	switch node.Type {
	case edn.NodeSymbol:
		// Simple variable
		sym := datalog.NewSymbol(node.Value)
		if !sym.IsVariable() {
			return nil, fmt.Errorf("find clause must contain variables, got %s", sym)
		}
		return query.FindVariable{Symbol: sym}, nil

	case edn.NodeList:
		// Could be aggregate function or pull expression
		if len(node.Nodes) < 2 {
			return nil, fmt.Errorf("find list expression must have at least 2 elements")
		}

		if node.Nodes[0].Type != edn.NodeSymbol {
			return nil, fmt.Errorf("find list expression must start with a function name")
		}

		fn := node.Nodes[0].Value

		// Check for pull expression: (pull ?e [...])
		if fn == "pull" {
			return parseFindPull(node)
		}

		// Otherwise, it's an aggregate function (sum ?x), (count ?x), etc.
		if len(node.Nodes) != 2 {
			return nil, fmt.Errorf("aggregate function must have exactly 2 elements: function and argument")
		}

		if node.Nodes[1].Type != edn.NodeSymbol {
			return nil, fmt.Errorf("aggregate argument must be a symbol")
		}

		argSym := datalog.NewSymbol(node.Nodes[1].Value)

		if !argSym.IsVariable() {
			return nil, fmt.Errorf("aggregate argument must be a variable, got %s", argSym)
		}

		// Resolve the function name to its interned symbol here, once; all
		// downstream dispatch is pointer equality against the pre-interned set.
		var fnSym query.Symbol
		switch fn {
		case "sum":
			fnSym = datalog.SymSum
		case "avg":
			fnSym = datalog.SymAvg
		case "count":
			fnSym = datalog.SymCount
		case "min":
			fnSym = datalog.SymMin
		case "max":
			fnSym = datalog.SymMax
		default:
			return nil, fmt.Errorf("unknown aggregate function: %s", fn)
		}

		return query.FindAggregate{
			Function: fnSym,
			Arg:      argSym,
		}, nil

	default:
		return nil, fmt.Errorf("find element must be a symbol or list, got %v", node.Type)
	}
}

// parsePattern parses a pattern from an EDN vector
func parsePattern(node *edn.Node) (query.Clause, error) {
	if node.Type != edn.NodeVector {
		return nil, fmt.Errorf("pattern must be a vector")
	}

	// Check if this is a function/expression pattern [(fn ...) ...]
	// Also handle [[(fn ...)] binding] where a vector wraps the function call
	// (produced by the query builder for comparison bindings like Gt().As())
	firstNode := &node.Nodes[0]
	if firstNode.Type == edn.NodeVector && len(firstNode.Nodes) == 1 && firstNode.Nodes[0].Type == edn.NodeList {
		firstNode = &firstNode.Nodes[0]
	}
	if len(node.Nodes) >= 1 && firstNode.Type == edn.NodeList {
		list := firstNode

		// Check if it's a subquery pattern [(q ...) binding]
		if len(list.Nodes) >= 2 && list.Nodes[0].Type == edn.NodeSymbol && list.Nodes[0].Value == "q" {
			if len(node.Nodes) != 2 {
				return nil, fmt.Errorf("subquery pattern must have exactly 2 elements: [(q ...) binding]")
			}
			return parseSubqueryPattern(list, &node.Nodes[1])
		}

		// An expression binds its result under Datomic's forms: the scalar
		// [(fn ...) ?x], the tuple form [(fn ...) [?a ?b]], which destructures
		// one tuple, the collection form [(fn ...) [?a ...]], which binds one
		// tuple per element, and the relation form [(fn ...) [[?a ?b]]], which
		// binds the relation over a collection of tuples.
		if len(node.Nodes) == 2 {
			if node.Nodes[1].Type == edn.NodeSymbol {
				sym := datalog.NewSymbol(node.Nodes[1].Value)
				if sym.IsVariable() {
					return parseExpression(list, sym)
				}
			}
			if node.Nodes[1].Type == edn.NodeVector {
				bindingNode := &node.Nodes[1]
				if len(bindingNode.Nodes) == 2 && bindingNode.Nodes[1].Type == edn.NodeSymbol && bindingNode.Nodes[1].Value == "..." {
					collectionBinding, err := parseBindingForm(bindingNode)
					if err != nil {
						return nil, fmt.Errorf("error parsing collection binding: %w", err)
					}
					return parseExpression(list, collectionBinding)
				}
				if len(bindingNode.Nodes) == 1 && bindingNode.Nodes[0].Type == edn.NodeVector {
					relationBinding, err := parseRelationBinding(&bindingNode.Nodes[0])
					if err != nil {
						return nil, fmt.Errorf("error parsing relation binding: %w", err)
					}
					return parseExpression(list, relationBinding)
				}
				tupleBinding, err := parseTupleBinding(bindingNode)
				if err != nil {
					return nil, fmt.Errorf("error parsing tuple binding: %w", err)
				}
				return parseExpression(list, tupleBinding)
			}
		}
		// Otherwise it's a predicate function pattern [(fn ...)]
		if len(node.Nodes) == 1 {
			// Parse as a concrete predicate
			pred, err := tryParsePredicate(&node.Nodes[0])
			if err != nil {
				return nil, fmt.Errorf("error parsing predicate: %w", err)
			}
			return pred, nil
		}
	}

	// Otherwise it's a data pattern
	// Check if first element is a source marker ($name)
	var sourceOffset int
	var source query.Symbol
	if len(node.Nodes) >= 1 && node.Nodes[0].Type == edn.NodeSymbol {
		if strings.HasPrefix(node.Nodes[0].Value, "$") {
			source = datalog.NewSymbol(node.Nodes[0].Value)
			sourceOffset = 1
		}
	}

	// Data pattern elements (after source marker):
	// 3 elements: [e a v]
	// 4 elements: [e a v tx]
	elemCount := len(node.Nodes) - sourceOffset
	if elemCount < 3 || elemCount > 4 {
		return nil, fmt.Errorf("data pattern must have 3 or 4 elements, got %d", elemCount)
	}

	pattern := &query.DataPattern{
		Source:   source,
		Elements: make([]query.PatternElement, elemCount),
	}

	for i := 0; i < elemCount; i++ {
		patternElem, err := parsePatternElement(&node.Nodes[i+sourceOffset])
		if err != nil {
			return nil, fmt.Errorf("error parsing pattern element %d: %w", i, err)
		}
		pattern.Elements[i] = patternElem
	}

	return pattern, nil
}

// tryParsePredicate attempts to parse a node as a concrete Predicate type
func tryParsePredicate(node *edn.Node) (query.Predicate, error) {
	if node.Type != edn.NodeList {
		return nil, fmt.Errorf("predicate must be a list")
	}

	if len(node.Nodes) < 2 {
		return nil, fmt.Errorf("predicate must have at least function name and one argument")
	}

	// First element must be the function name (symbol)
	if node.Nodes[0].Type != edn.NodeSymbol {
		return nil, fmt.Errorf("function name must be a symbol, got %v", node.Nodes[0].Type)
	}

	fn := node.Nodes[0].Value

	// Parse arguments as PatternElements first
	args := make([]query.PatternElement, len(node.Nodes)-1)
	for i := 1; i < len(node.Nodes); i++ {
		arg, err := parsePatternElement(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing predicate argument %d: %w", i, err)
		}
		args[i-1] = arg
	}

	// Try to create a concrete predicate
	return parsePredicate(fn, args)
}

// parseExpression parses an expression from a list node under its binding:
// a query.Symbol, a query.TupleBinding or a query.RelationBinding. The
// binding's shape against the function's result is Expression.Validate's
// check, run by ValidateStaticClauseShapes at every boundary.
func parseExpression(node *edn.Node, binding interface{}) (*query.Expression, error) {
	if node.Type != edn.NodeList {
		return nil, fmt.Errorf("expression must be a list")
	}

	if len(node.Nodes) < 2 {
		return nil, fmt.Errorf("expression must have at least function name and one argument")
	}

	// First element must be the function name (symbol)
	if node.Nodes[0].Type != edn.NodeSymbol {
		return nil, fmt.Errorf("function name must be a symbol, got %v", node.Nodes[0].Type)
	}

	fn := node.Nodes[0].Value

	// Parse arguments as PatternElements
	args := make([]query.PatternElement, len(node.Nodes)-1)
	for i := 1; i < len(node.Nodes); i++ {
		arg, err := parsePatternElement(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing expression argument %d: %w", i, err)
		}
		args[i-1] = arg
	}

	// Try to create a concrete Function
	function, err := parseFunction(fn, args)
	if err != nil {
		return nil, fmt.Errorf("error parsing function: %w", err)
	}

	return &query.Expression{
		Function: function,
		Binding:  binding,
	}, nil
}

// parseSubqueryPattern parses a subquery pattern from (q <query> <inputs...>) and binding form
func parseSubqueryPattern(list *edn.Node, bindingNode *edn.Node) (*query.SubqueryPattern, error) {
	if list.Type != edn.NodeList {
		return nil, fmt.Errorf("subquery must be a list")
	}

	if len(list.Nodes) < 2 {
		return nil, fmt.Errorf("subquery must have at least 'q' and a query form")
	}

	// First element must be 'q'
	if list.Nodes[0].Type != edn.NodeSymbol || list.Nodes[0].Value != "q" {
		return nil, fmt.Errorf("subquery must start with 'q' symbol")
	}

	// Second element must be the query (a vector)
	if list.Nodes[1].Type != edn.NodeVector {
		return nil, fmt.Errorf("subquery query form must be a vector, got %v", list.Nodes[1].Type)
	}

	// Parse the nested query
	nestedQuery, err := parseQueryVector(&list.Nodes[1])
	if err != nil {
		return nil, fmt.Errorf("error parsing nested query: %w", err)
	}

	// Parse inputs (everything between query and end)
	inputs := make([]query.PatternElement, 0, len(list.Nodes)-2)
	for i := 2; i < len(list.Nodes); i++ {
		input, err := parsePatternElement(&list.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing subquery input %d: %w", i-2, err)
		}
		inputs = append(inputs, input)
	}

	// Parse binding form
	binding, err := parseBindingForm(bindingNode)
	if err != nil {
		return nil, fmt.Errorf("error parsing binding form: %w", err)
	}

	return &query.SubqueryPattern{
		Query:   nestedQuery,
		Inputs:  inputs,
		Binding: binding,
	}, nil
}

// parseBindingForm parses a subquery's binding form, Datomic's grammar for
// a binding (query-data-reference, "Binding Forms"):
//   - ?var          = bind-scalar, the relation over one symbol
//   - [?var ...]    = bind-coll, the relation over one symbol
//   - [[?a ?b]]     = bind-rel, the relation over the named symbols
//   - [[?a ?b] ...] = the same relation binding, a spelling of janus's own
//
// The tuple form [?a ?b] is an expression's destructuring (parseExpression)
// and is not a subquery binding.
func parseBindingForm(node *edn.Node) (query.BindingForm, error) {
	switch node.Type {
	case edn.NodeSymbol:
		// Scalar binding: ?var (expects one tuple, one symbol)
		sym := datalog.NewSymbol(node.Value)
		if !sym.IsVariable() {
			return nil, fmt.Errorf("scalar binding must be a variable, got %s", sym)
		}
		return query.ScalarBinding{Variable: sym}, nil

	case edn.NodeVector:
		if len(node.Nodes) == 0 {
			return nil, fmt.Errorf("binding form cannot be empty vector")
		}

		// Check if it's a collection binding [?var ...]
		if len(node.Nodes) == 2 &&
			node.Nodes[0].Type == edn.NodeSymbol &&
			node.Nodes[1].Type == edn.NodeSymbol && node.Nodes[1].Value == "..." {
			sym := datalog.NewSymbol(node.Nodes[0].Value)
			if !sym.IsVariable() {
				return nil, fmt.Errorf("collection binding must be a variable, got %s", sym)
			}
			return query.CollectionBinding{Variable: sym}, nil
		}

		// A nested vector is the relation binding, written [[?a ?b]] or
		// [[?a ?b] ...]
		if node.Nodes[0].Type == edn.NodeVector {
			if len(node.Nodes) == 1 {
				return parseRelationBinding(&node.Nodes[0])
			} else if len(node.Nodes) == 2 && node.Nodes[1].Type == edn.NodeSymbol && node.Nodes[1].Value == "..." {
				return parseRelationBinding(&node.Nodes[0])
			}
		}

		// A vector of variables is the tuple form, an expression's
		// destructuring and no subquery binding; the repair is the relation
		// form over the same variables.
		if written, err := parseTupleBinding(node); err == nil {
			return nil, fmt.Errorf("%s is the tuple form, an expression's destructuring and no subquery binding; bind the inner :find with the relation form %s", written, query.RelationBinding{Variables: written.Variables})
		}

		return nil, fmt.Errorf("invalid binding form: expected ?var, [?var ...], [[?vars]], or [[?vars] ...]")

	default:
		return nil, fmt.Errorf("binding form must be a symbol or vector, got %v", node.Type)
	}
}

// parseTupleBinding parses the tuple form [?a ?b] an expression destructures
// its one tuple by
func parseTupleBinding(node *edn.Node) (query.TupleBinding, error) {
	if node.Type != edn.NodeVector {
		return query.TupleBinding{}, fmt.Errorf("tuple binding must be a vector")
	}

	if len(node.Nodes) == 0 {
		return query.TupleBinding{}, fmt.Errorf("tuple binding cannot be empty")
	}

	vars := make([]query.Symbol, 0, len(node.Nodes))
	for i, elem := range node.Nodes {
		if elem.Type != edn.NodeSymbol {
			return query.TupleBinding{}, fmt.Errorf("tuple binding element %d must be a symbol", i)
		}
		sym := datalog.NewSymbol(elem.Value)
		if !sym.IsVariable() {
			return query.TupleBinding{}, fmt.Errorf("tuple binding element %d must be a variable, got %s", i, sym)
		}
		vars = append(vars, sym)
	}

	return query.TupleBinding{Variables: vars}, nil
}

// parseRelationBinding parses the inner vector of a relation binding, the
// [?a ?b] of [[?a ?b]] or [[?a ?b] ...]
func parseRelationBinding(node *edn.Node) (query.RelationBinding, error) {
	if node.Type != edn.NodeVector {
		return query.RelationBinding{}, fmt.Errorf("relation binding must be a vector")
	}

	if len(node.Nodes) == 0 {
		return query.RelationBinding{}, fmt.Errorf("relation binding cannot be empty")
	}

	vars := make([]query.Symbol, 0, len(node.Nodes))
	for i, elem := range node.Nodes {
		if elem.Type != edn.NodeSymbol {
			return query.RelationBinding{}, fmt.Errorf("relation binding element %d must be a symbol", i)
		}
		sym := datalog.NewSymbol(elem.Value)
		if !sym.IsVariable() {
			return query.RelationBinding{}, fmt.Errorf("relation binding element %d must be a variable, got %s", i, sym)
		}
		vars = append(vars, sym)
	}

	return query.RelationBinding{Variables: vars}, nil
}

// parseInputSpec parses an input specification from the :in clause under
// Datomic's grammar: $name a source, % the rule set, ?x the scalar input,
// [?x ...] the collection input, [?x ?y] the tuple input, [[?x ?y]] the
// relation input, and [[?x ?y] ...], janus's own spelling, the relation
// input as well. [] is the tuple input naming no variable, which
// ValidateQuery refuses at every boundary.
func parseInputSpec(node *edn.Node) (query.InputSpec, error) {
	switch node.Type {
	case edn.NodeSymbol:
		if strings.HasPrefix(node.Value, "$") {
			return query.DatabaseInput{Name: datalog.NewSymbol(node.Value)}, nil
		}
		if node.Value == "%" {
			return query.RulesInput{}, nil
		}
		sym := datalog.NewSymbol(node.Value)
		if !sym.IsVariable() {
			return nil, fmt.Errorf("input must be $name, %% or a variable, got %s", node.Value)
		}
		return query.ScalarInput{Symbol: sym}, nil

	case edn.NodeVector:
		if len(node.Nodes) == 0 {
			return query.TupleInput{}, nil
		}
		trailing := len(node.Nodes) == 2 && node.Nodes[1].Type == edn.NodeSymbol && node.Nodes[1].Value == "..."

		if node.Nodes[0].Type == edn.NodeVector {
			if len(node.Nodes) != 1 && !trailing {
				return nil, fmt.Errorf("invalid input specification format")
			}
			vars, err := inputVariables(node.Nodes[0].Nodes, "relation")
			if err != nil {
				return nil, err
			}
			return query.RelationInput{Symbols: vars}, nil
		}

		if trailing {
			if node.Nodes[0].Type != edn.NodeSymbol {
				return nil, fmt.Errorf("collection input must contain a variable")
			}
			sym := datalog.NewSymbol(node.Nodes[0].Value)
			if !sym.IsVariable() {
				return nil, fmt.Errorf("collection input must contain a variable, got %s", sym)
			}
			return query.CollectionInput{Symbol: sym}, nil
		}

		vars, err := inputVariables(node.Nodes, "tuple")
		if err != nil {
			return nil, err
		}
		return query.TupleInput{Symbols: vars}, nil

	default:
		return nil, fmt.Errorf("input spec must be a symbol or vector, got %v", node.Type)
	}
}

// inputVariables is the variables a tuple or relation input's vector names,
// in order; an element that is not a variable is refused by its position,
// naming the input's kind.
func inputVariables(nodes []edn.Node, kind string) ([]query.Symbol, error) {
	vars := make([]query.Symbol, 0, len(nodes))
	for i, elem := range nodes {
		if elem.Type != edn.NodeSymbol {
			return nil, fmt.Errorf("%s input element %d must be a symbol", kind, i)
		}
		sym := datalog.NewSymbol(elem.Value)
		if !sym.IsVariable() {
			return nil, fmt.Errorf("%s input element %d must be a variable, got %s", kind, i, sym)
		}
		vars = append(vars, sym)
	}
	return vars, nil
}

// parsePatternElement parses a single pattern element
func parsePatternElement(node *edn.Node) (query.PatternElement, error) {
	switch node.Type {
	case edn.NodeSymbol:
		sym := datalog.NewSymbol(node.Value)
		if sym.IsVariable() {
			return query.Variable{Name: sym}, nil
		} else if node.Value == "_" {
			return query.Blank{}, nil
		} else if node.Value == "$" {
			// Database marker - treat as a special constant
			return query.Constant{Value: datalog.SymDollar}, nil
		} else {
			// Bare symbols are Symbol values (function references, rule names, etc.)
			return query.Constant{Value: datalog.NewSymbol(node.Value)}, nil
		}

	case edn.NodeKeyword:
		// Keywords are attributes
		return query.Constant{Value: datalog.NewKeyword(node.Value)}, nil

	case edn.NodeString:
		// String values
		return query.Constant{Value: node.Value}, nil

	case edn.NodeInt:
		// Integer values
		val, err := strconv.ParseInt(node.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer: %w", err)
		}
		return query.Constant{Value: val}, nil

	case edn.NodeFloat:
		// Float values
		val, err := strconv.ParseFloat(node.Value, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float: %w", err)
		}
		return query.Constant{Value: val}, nil

	case edn.NodeBool:
		// Boolean values
		val := node.Value == "true"
		return query.Constant{Value: val}, nil

	case edn.NodeVector:
		// Vector of constants - used for tuple ground: [(ground [1 2 3]) [?a ?b ?c]]
		values := make([]interface{}, len(node.Nodes))
		for i, elem := range node.Nodes {
			switch elem.Type {
			case edn.NodeInt:
				val, err := strconv.ParseInt(elem.Value, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid integer in vector: %w", err)
				}
				values[i] = val
			case edn.NodeFloat:
				val, err := strconv.ParseFloat(elem.Value, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid float in vector: %w", err)
				}
				values[i] = val
			case edn.NodeString:
				values[i] = elem.Value
			case edn.NodeBool:
				values[i] = elem.Value == "true"
			case edn.NodeKeyword:
				values[i] = datalog.NewKeyword(elem.Value)
			case edn.NodeSymbol:
				values[i] = datalog.NewSymbol(elem.Value)
			case edn.NodeTagged:
				val, err := parseTaggedLiteral(&elem)
				if err != nil {
					return nil, fmt.Errorf("invalid tagged literal in vector: %w", err)
				}
				values[i] = val.(query.Constant).Value
			case edn.NodeVector:
				nested, err := parsePatternElement(&elem)
				if err != nil {
					return nil, fmt.Errorf("invalid vector in vector: %w", err)
				}
				values[i] = nested.(query.VectorConstant).Values
			default:
				return nil, fmt.Errorf("unsupported type in vector constant: %v", elem.Type)
			}
		}
		return query.VectorConstant{Values: values}, nil

	case edn.NodeTagged:
		return parseTaggedLiteral(node)

	default:
		return nil, fmt.Errorf("unsupported pattern element type: %v", node.Type)
	}
}

// parseTaggedLiteral converts an EDN tagged literal into a query Constant.
func parseTaggedLiteral(node *edn.Node) (query.PatternElement, error) {
	value, err := TaggedLiteralValue(node)
	if err != nil {
		return nil, err
	}
	return query.Constant{Value: value}, nil
}

// TaggedLiteralValue converts an EDN tagged literal into its datalog value.
// This is the query dialect's one vocabulary — #identity, #id, #inst,
// #bytes — shared by query-text parsing and CLI input conversion so the two
// entry points cannot drift. (The export/import dump format keeps its own
// dialect in the storage package: it carries #lzj and deliberately omits the
// #id sugar, since the formatter only emits canonical #identity.)
func TaggedLiteralValue(node *edn.Node) (interface{}, error) {
	if node.Tagged == nil {
		return nil, fmt.Errorf("tagged literal missing value")
	}
	val := node.Tagged
	switch node.Tag {
	case "identity":
		if val.Type != edn.NodeString {
			return nil, fmt.Errorf("#identity requires string value")
		}
		hash, err := codec.DecodeFixed20(val.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid L85 in #identity: %w", err)
		}
		return datalog.NewIdentityFromHash(hash), nil

	case "id":
		// #id "seed" constructs the identity by hashing the seed — NewIdentity
		// in literal form. Input-only sugar: the formatter always emits the
		// canonical #identity "L85" (seed→hash is one-way).
		if val.Type != edn.NodeString {
			return nil, fmt.Errorf("#id requires string value")
		}
		return datalog.NewIdentity(val.Value), nil

	case "inst":
		if val.Type != edn.NodeString {
			return nil, fmt.Errorf("#inst requires string value")
		}
		t, err := time.Parse(time.RFC3339Nano, val.Value)
		if err != nil {
			t, err = time.Parse(time.RFC3339, val.Value)
			if err != nil {
				return nil, fmt.Errorf("invalid instant: %w", err)
			}
		}
		return t.UTC(), nil

	case "bytes":
		if val.Type != edn.NodeString {
			return nil, fmt.Errorf("#bytes requires string value")
		}
		if val.Value == "" {
			return []byte{}, nil
		}
		decoded, err := codec.DecodeL85(val.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid L85 in #bytes: %w", err)
		}
		return decoded, nil

	default:
		return nil, fmt.Errorf("unsupported tagged literal: #%s", node.Tag)
	}
}

// ParseMultipleQueries parses multiple queries from a single input
func ParseMultipleQueries(input string) ([]*query.Query, error) {
	lexer := edn.NewLexer(input)
	if err := lexer.Lex(); err != nil {
		return nil, fmt.Errorf("EDN lex error: %w", err)
	}

	parser := edn.NewParser(lexer)
	nodes, err := parser.ParseAll()
	if err != nil {
		return nil, fmt.Errorf("EDN parse error: %w", err)
	}

	var queries []*query.Query
	for i, node := range nodes {
		if node.Type != edn.NodeVector {
			return nil, fmt.Errorf("query %d must be a vector, got %v", i, node.Type)
		}
		q, err := parseQueryVector(&node)
		if err != nil {
			return nil, fmt.Errorf("error parsing query %d: %w", i, err)
		}
		queries = append(queries, q)
	}

	return queries, nil
}

// parseListClause parses a list-form clause: (not ...), (not-join ...),
// (or ...), (or-join ...), (or-default ...), (or-default-join ...), or a rule
// invocation (name args...) for any other head symbol.
func parseListClause(node *edn.Node) (query.Clause, error) {
	if node.Type != edn.NodeList {
		return nil, fmt.Errorf("list clause must be a list")
	}

	if len(node.Nodes) == 0 {
		return nil, fmt.Errorf("list clause must not be empty")
	}

	if node.Nodes[0].Type != edn.NodeSymbol {
		return nil, fmt.Errorf("list clause must start with a symbol, got %v", node.Nodes[0].Type)
	}

	switch node.Nodes[0].Value {
	case "not":
		return parseNotClause(node)
	case "not-join":
		return parseNotJoinClause(node)
	case "or":
		return parseOrClause(node)
	case "or-join":
		return parseOrJoinClause(node)
	case "or-default":
		return parseOrDefaultClause(node)
	case "or-default-join":
		return parseOrDefaultJoinClause(node)
	default:
		return parseRuleInvocation(node)
	}
}

// parseNotClause parses (not [clause1] [clause2] ...)
func parseNotClause(node *edn.Node) (*query.NotClause, error) {
	// First element is "not" symbol, remaining are clauses
	var clauses []query.Clause
	for i := 1; i < len(node.Nodes); i++ {
		clause, err := parseWhereElement(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing not clause element %d: %w", len(clauses)+1, err)
		}
		clauses = append(clauses, clause)
	}

	if len(clauses) == 0 {
		return nil, fmt.Errorf("not clause must have at least one inner clause")
	}

	return &query.NotClause{Clauses: clauses}, nil
}

// parseNotJoinClause parses (not-join [?x ?y] [clause1] [clause2] ...)
func parseNotJoinClause(node *edn.Node) (*query.NotJoinClause, error) {
	if len(node.Nodes) < 3 {
		return nil, fmt.Errorf("not-join clause must have a header and at least one clause")
	}

	if node.Nodes[1].Type != edn.NodeVector {
		return nil, fmt.Errorf("not-join second element must be a header vector, got %v", node.Nodes[1].Type)
	}

	// A not-join's header is all required, and parseRequiredRun refuses an
	// empty one.
	required, err := parseRequiredRun(&node.Nodes[1])
	if err != nil {
		return nil, fmt.Errorf("error parsing not-join header: %w", err)
	}

	// Parse remaining elements as clauses
	var clauses []query.Clause
	for i := 2; i < len(node.Nodes); i++ {
		clause, err := parseWhereElement(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing not-join clause element %d: %w", len(clauses)+1, err)
		}
		clauses = append(clauses, clause)
	}

	if len(clauses) == 0 {
		return nil, fmt.Errorf("not-join clause must have at least one inner clause")
	}

	return &query.NotJoinClause{RequiredVars: required, Clauses: clauses}, nil
}

// parseOrClause parses (or branch1 branch2 ...)
func parseOrClause(node *edn.Node) (*query.OrClause, error) {
	// First element is "or" symbol, remaining are branches
	var branches [][]query.Clause
	for i := 1; i < len(node.Nodes); i++ {
		branch, err := parseBranch(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing or branch %d: %w", len(branches)+1, err)
		}
		branches = append(branches, branch)
	}

	if len(branches) == 0 {
		return nil, fmt.Errorf("or clause must have at least one branch")
	}

	return &query.OrClause{Branches: branches}, nil
}

// parseOrJoinClause parses an or-join with a declared interface:
//
//	(or-join [[?required ...] ?output ...] branch1 branch2 ...)
//	(or-join [?output ...] branch1 branch2 ...)
//
// A nested vector as the header's first element declares the variables the
// enclosing query binds before the clause runs; the remaining symbols are the
// outputs every branch binds. A flat header declares outputs only. A union
// of one branch is that branch over the header.
func parseOrJoinClause(node *edn.Node) (*query.OrJoinClause, error) {
	if len(node.Nodes) < 3 {
		return nil, fmt.Errorf("or-join clause must have a header vector and at least one branch")
	}

	requiredVars, joinVars, err := parseInterfaceHeader(&node.Nodes[1], "or-join")
	if err != nil {
		return nil, err
	}

	// Parse remaining elements as branches
	var branches [][]query.Clause
	for i := 2; i < len(node.Nodes); i++ {
		branch, err := parseBranch(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing or-join branch %d: %w", len(branches)+1, err)
		}
		branches = append(branches, branch)
	}

	return &query.OrJoinClause{RequiredVars: requiredVars, OutputVars: joinVars, Branches: branches}, nil
}

// parseInterfaceHeader parses the header of an or-join or an or-default-join:
// an optional nested vector of required variables as its first element, then
// the output variables, every one a symbol starting with ?. form names the
// clause in the errors.
func parseInterfaceHeader(header *edn.Node, form string) (required, outputs []query.Symbol, err error) {
	if header.Type != edn.NodeVector {
		return nil, nil, fmt.Errorf("%s second element must be a header vector, got %v", form, header.Type)
	}
	if len(header.Nodes) == 0 {
		return nil, nil, fmt.Errorf("%s header cannot be empty", form)
	}

	outputStart := 0
	if header.Nodes[0].Type == edn.NodeVector {
		required, err = parseRequiredRun(&header.Nodes[0])
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing %s required run: %w", form, err)
		}
		outputStart = 1
	}

	for i := outputStart; i < len(header.Nodes); i++ {
		elem := &header.Nodes[i]
		if elem.Type == edn.NodeVector {
			return nil, nil, fmt.Errorf("%s header may declare required vars only as its first element", form)
		}
		if elem.Type != edn.NodeSymbol {
			return nil, nil, fmt.Errorf("%s output variable %d must be a symbol, got %v", form, i, elem.Type)
		}
		sym := datalog.NewSymbol(elem.Value)
		if !sym.IsVariable() {
			return nil, nil, fmt.Errorf("%s output variable %d must start with ?, got %s", form, i, sym)
		}
		outputs = append(outputs, sym)
	}
	return required, outputs, nil
}

// parseOrDefaultClause parses (or-default branch1 branch2 ...)
func parseOrDefaultClause(node *edn.Node) (*query.OrDefaultClause, error) {
	var branches [][]query.Clause
	for i := 1; i < len(node.Nodes); i++ {
		branch, err := parseBranch(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing or-default branch %d: %w", len(branches)+1, err)
		}
		branches = append(branches, branch)
	}

	if len(branches) < 2 {
		return nil, fmt.Errorf("or-default clause must have at least two branches")
	}

	return &query.OrDefaultClause{Branches: branches}, nil
}

// parseOrDefaultJoinClause parses an or-default-join with a declared interface:
//
//	(or-default-join [[?required ...] ?output ...] branch1 branch2 ...)
//	(or-default-join [?output ...] branch1 branch2 ...)   ; global fallback
//
// A nested vector as the header's first element declares the per-group
// correlation keys; the remaining symbols are the outputs every branch must
// bind. A flat header declares outputs only — the fallback decision is
// global. or-default is non-monotone, so the correlation keys are syntax,
// never inferred.
func parseOrDefaultJoinClause(node *edn.Node) (*query.OrDefaultJoinClause, error) {
	if len(node.Nodes) < 4 {
		return nil, fmt.Errorf("or-default-join clause must have a header vector and at least two branches")
	}

	requiredVars, outputVars, err := parseInterfaceHeader(&node.Nodes[1], "or-default-join")
	if err != nil {
		return nil, err
	}

	var branches [][]query.Clause
	for i := 2; i < len(node.Nodes); i++ {
		branch, err := parseBranch(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing or-default-join branch %d: %w", len(branches)+1, err)
		}
		branches = append(branches, branch)
	}

	return &query.OrDefaultJoinClause{
		RequiredVars: requiredVars,
		OutputVars:   outputVars,
		Branches:     branches,
	}, nil
}

// parseRequiredRun parses a header's required run, a vector of variables
// [?x ?y ...] each a symbol starting with ?: a not-join's whole header, or
// the nested first element of an or-join's or an or-default-join's header.
func parseRequiredRun(node *edn.Node) ([]query.Symbol, error) {
	if node.Type != edn.NodeVector {
		return nil, fmt.Errorf("required run must be a vector")
	}

	if len(node.Nodes) == 0 {
		return nil, fmt.Errorf("required run cannot be empty")
	}

	var vars []query.Symbol
	for i, elem := range node.Nodes {
		if elem.Type != edn.NodeSymbol {
			return nil, fmt.Errorf("required variable %d must be a symbol, got %v", i+1, elem.Type)
		}
		sym := datalog.NewSymbol(elem.Value)
		if !sym.IsVariable() {
			return nil, fmt.Errorf("required variable %d must start with ?, got %s", i+1, sym)
		}
		vars = append(vars, sym)
	}

	return vars, nil
}

// parseBranch parses a single branch of an or clause
// Can be a single clause [?e :attr ?v] or an (and ...) form for multiple clauses
func parseBranch(node *edn.Node) ([]query.Clause, error) {
	switch node.Type {
	case edn.NodeVector:
		// Single clause: [?e :attr ?v]
		clause, err := parsePattern(node)
		if err != nil {
			return nil, err
		}
		return []query.Clause{clause}, nil

	case edn.NodeList:
		// Check for (and ...) form
		if len(node.Nodes) >= 1 && node.Nodes[0].Type == edn.NodeSymbol && node.Nodes[0].Value == "and" {
			var clauses []query.Clause
			for i := 1; i < len(node.Nodes); i++ {
				clause, err := parseWhereElement(&node.Nodes[i])
				if err != nil {
					return nil, fmt.Errorf("error parsing and clause %d: %w", i, err)
				}
				clauses = append(clauses, clause)
			}
			if len(clauses) == 0 {
				return nil, fmt.Errorf("and branch must have at least one clause")
			}
			return clauses, nil
		}
		// Otherwise it's a single list clause like a predicate
		clause, err := parseListClause(node)
		if err != nil {
			return nil, err
		}
		return []query.Clause{clause}, nil

	default:
		return nil, fmt.Errorf("or branch must be a vector or list, got %v", node.Type)
	}
}

// parseWhereElement parses a single element that can appear in a where clause
// This handles both vectors (data patterns, expressions) and lists (not, or, predicates)
func parseWhereElement(node *edn.Node) (query.Clause, error) {
	switch node.Type {
	case edn.NodeVector:
		return parsePattern(node)
	case edn.NodeList:
		return parseListClause(node)
	default:
		return nil, fmt.Errorf("where element must be a vector or list, got %v", node.Type)
	}
}
