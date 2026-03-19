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

	return parseQueryVector(node)
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

			// Validate: scalar find spec requires exactly one find element
			if q.ScalarReturn && len(q.Find) != 1 {
				return nil, fmt.Errorf("scalar find spec (.) requires exactly one find element, got %d", len(q.Find))
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

		default:
			return nil, fmt.Errorf("unknown query clause: %s", keyword)
		}
	}

	// Validate query
	if len(q.Find) == 0 {
		return nil, fmt.Errorf("query must have at least one find variable")
	}
	if len(q.Where) == 0 {
		return nil, fmt.Errorf("query must have at least one where pattern")
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
		return query.OrderByClause{
			Variable:  sym,
			Direction: query.OrderAsc,
		}, nil

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

		var direction query.OrderDirection
		switch node.Nodes[1].Value {
		case ":asc":
			direction = query.OrderAsc
		case ":desc":
			direction = query.OrderDesc
		default:
			return query.OrderByClause{}, fmt.Errorf("order-by direction must be :asc or :desc, got %s", node.Nodes[1].Value)
		}

		return query.OrderByClause{
			Variable:  sym,
			Direction: direction,
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

		// Validate function name
		switch fn {
		case "sum", "avg", "count", "min", "max":
			// Valid aggregate functions
		default:
			return nil, fmt.Errorf("unknown aggregate function: %s", fn)
		}

		return query.FindAggregate{
			Function: fn,
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

		// Check if it's an expression [(fn ...) ?binding] or [(fn ...) [?a ?b ?c]]
		if len(node.Nodes) == 2 {
			// Scalar binding: [(fn ...) ?x]
			if node.Nodes[1].Type == edn.NodeSymbol {
				sym := datalog.NewSymbol(node.Nodes[1].Value)
				if sym.IsVariable() {
					return parseExpression(list, sym)
				}
			}
			// Tuple binding: [(fn ...) [?a ?b ?c]] or [(fn ...) [[?a ?b ?c]]]
			// The double-bracket form [[...]] is the Datomic-style tuple binding
			// produced by the query builder's TupleGround().As() method.
			if node.Nodes[1].Type == edn.NodeVector {
				bindingNode := &node.Nodes[1]
				// Unwrap [[?a ?b ?c]] → [?a ?b ?c]
				if len(bindingNode.Nodes) == 1 && bindingNode.Nodes[0].Type == edn.NodeVector {
					bindingNode = &bindingNode.Nodes[0]
				}
				tupleBinding, err := parseTupleBinding(bindingNode)
				if err != nil {
					return nil, fmt.Errorf("error parsing tuple binding: %w", err)
				}
				return parseExpressionWithTupleBinding(list, tupleBinding)
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

// parseExpression parses an expression from a list node and binding variable
func parseExpression(node *edn.Node, binding query.Symbol) (*query.Expression, error) {
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

// parseExpressionWithTupleBinding parses an expression with a tuple binding
// Example: [(ground [1 2 3]) [?a ?b ?c]]
func parseExpressionWithTupleBinding(node *edn.Node, binding query.TupleBinding) (*query.Expression, error) {
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

	// For ground function with tuple binding, validate length match
	if fn == "ground" {
		if gf, ok := function.(*query.GroundFunction); ok {
			if values, ok := gf.Value.([]interface{}); ok {
				if len(values) != len(binding.Variables) {
					return nil, fmt.Errorf("tuple ground mismatch: %d values, %d binding variables",
						len(values), len(binding.Variables))
				}
			}
		}
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

// parseBindingForm parses a binding form for subqueries
// Datomic binding forms:
//   - ?var         = Scalar binding (expects single tuple, single symbol)
//   - [?var ...]   = Collection binding (collects all values from single symbol)
//   - [[?a ?b]]    = Tuple binding (expects single tuple, multiple symbols)
//   - [[?a ?b] ...] = Relation binding (multiple tuples and symbols)
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

		// Check if it's a nested vector (tuple or relation binding)
		if node.Nodes[0].Type == edn.NodeVector {
			if len(node.Nodes) == 1 {
				// [[?a ?b]] - tuple binding
				return parseTupleBinding(&node.Nodes[0])
			} else if len(node.Nodes) == 2 && node.Nodes[1].Type == edn.NodeSymbol && node.Nodes[1].Value == "..." {
				// [[?a ?b] ...] - relation binding
				return parseRelationBinding(&node.Nodes[0])
			}
		}

		return nil, fmt.Errorf("invalid binding form: expected ?var, [?var ...], [[?vars]], or [[?vars] ...]")

	default:
		return nil, fmt.Errorf("binding form must be a symbol or vector, got %v", node.Type)
	}
}

// parseTupleBinding parses a tuple binding form [[?a ?b]]
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

// parseRelationBinding parses a relation binding form [[?a ?b] ...]
func parseRelationBinding(node *edn.Node) (query.RelationBinding, error) {
	if node.Type != edn.NodeVector {
		return query.RelationBinding{}, fmt.Errorf("relation binding must be a vector")
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

// parseInputSpec parses an input specification from the :in clause
func parseInputSpec(node *edn.Node) (query.InputSpec, error) {
	switch node.Type {
	case edn.NodeSymbol:
		// Either $name (database source) or ?var (scalar input)
		if strings.HasPrefix(node.Value, "$") {
			return query.DatabaseInput{Name: datalog.NewSymbol(node.Value)}, nil
		}
		sym := datalog.NewSymbol(node.Value)
		if !sym.IsVariable() {
			return nil, fmt.Errorf("input must be $name or a variable, got %s", node.Value)
		}
		return query.ScalarInput{Symbol: sym}, nil

	case edn.NodeVector:
		// Could be [?x ...] collection or [[?x ?y]] tuple or [[?x ?y] ...] relation
		if len(node.Nodes) == 0 {
			return nil, fmt.Errorf("input vector cannot be empty")
		}

		// Check for tuple or relation input first (they start with a vector)
		if node.Nodes[0].Type == edn.NodeVector {
			// It's a tuple [[?x ?y]] or relation [[?x ?y] ...]
			tupleNode := &node.Nodes[0]
			vars := make([]query.Symbol, 0, len(tupleNode.Nodes))

			for i, elem := range tupleNode.Nodes {
				if elem.Type != edn.NodeSymbol {
					return nil, fmt.Errorf("tuple input element %d must be a symbol", i)
				}
				sym := datalog.NewSymbol(elem.Value)
				if !sym.IsVariable() {
					return nil, fmt.Errorf("tuple input element %d must be a variable, got %s", i, sym)
				}
				vars = append(vars, sym)
			}

			// Check if it's a relation binding [[?x ?y] ...]
			if len(node.Nodes) == 2 && node.Nodes[1].Type == edn.NodeSymbol && node.Nodes[1].Value == "..." {
				return query.RelationInput{Symbols: vars}, nil
			}

			// Otherwise it's a tuple binding [[?x ?y]]
			if len(node.Nodes) == 1 {
				return query.TupleInput{Symbols: vars}, nil
			}
		}

		// Check for collection input [?x ...]
		if len(node.Nodes) == 2 && node.Nodes[1].Type == edn.NodeSymbol && node.Nodes[1].Value == "..." {
			if node.Nodes[0].Type != edn.NodeSymbol {
				return nil, fmt.Errorf("collection input must contain a variable")
			}
			sym := datalog.NewSymbol(node.Nodes[0].Value)
			if !sym.IsVariable() {
				return nil, fmt.Errorf("collection input must contain a variable, got %s", sym)
			}
			return query.CollectionInput{Symbol: sym}, nil
		}

		return nil, fmt.Errorf("invalid input specification format")

	default:
		return nil, fmt.Errorf("input spec must be a symbol or vector, got %v", node.Type)
	}
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
		return query.Constant{Value: datalog.NewIdentityFromHash(hash)}, nil

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
		return query.Constant{Value: t.UTC()}, nil

	case "bytes":
		if val.Type != edn.NodeString {
			return nil, fmt.Errorf("#bytes requires string value")
		}
		if val.Value == "" {
			return query.Constant{Value: []byte{}}, nil
		}
		decoded, err := codec.DecodeL85(val.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid L85 in #bytes: %w", err)
		}
		return query.Constant{Value: decoded}, nil

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

// parseListClause parses a list-form clause: (not ...), (or ...), (not-join ...), (or-join ...)
func parseListClause(node *edn.Node) (query.Clause, error) {
	if node.Type != edn.NodeList {
		return nil, fmt.Errorf("list clause must be a list")
	}

	if len(node.Nodes) < 2 {
		return nil, fmt.Errorf("list clause must have at least a keyword and one element")
	}

	// First element must be a symbol (not, or, not-join, or-join)
	if node.Nodes[0].Type != edn.NodeSymbol {
		return nil, fmt.Errorf("list clause must start with a symbol, got %v", node.Nodes[0].Type)
	}

	keyword := node.Nodes[0].Value

	switch keyword {
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
		return nil, fmt.Errorf("unknown list clause type: %s", keyword)
	}
}

// parseNotClause parses (not [clause1] [clause2] ...)
func parseNotClause(node *edn.Node) (*query.NotClause, error) {
	// First element is "not" symbol, remaining are clauses
	var clauses []query.Clause
	for i := 1; i < len(node.Nodes); i++ {
		clause, err := parseWhereElement(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing not clause element %d: %w", i, err)
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
		return nil, fmt.Errorf("not-join clause must have join vars and at least one clause")
	}

	// Second element must be join vars vector
	if node.Nodes[1].Type != edn.NodeVector {
		return nil, fmt.Errorf("not-join second element must be a vector of join variables, got %v", node.Nodes[1].Type)
	}

	// Parse join variables
	joinVars, err := parseJoinVars(&node.Nodes[1])
	if err != nil {
		return nil, fmt.Errorf("error parsing not-join vars: %w", err)
	}

	// Parse remaining elements as clauses
	var clauses []query.Clause
	for i := 2; i < len(node.Nodes); i++ {
		clause, err := parseWhereElement(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing not-join clause element %d: %w", i, err)
		}
		clauses = append(clauses, clause)
	}

	if len(clauses) == 0 {
		return nil, fmt.Errorf("not-join clause must have at least one inner clause")
	}

	return &query.NotJoinClause{JoinVars: joinVars, Clauses: clauses}, nil
}

// parseOrClause parses (or branch1 branch2 ...)
func parseOrClause(node *edn.Node) (*query.OrClause, error) {
	// First element is "or" symbol, remaining are branches
	var branches [][]query.Clause
	for i := 1; i < len(node.Nodes); i++ {
		branch, err := parseBranch(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing or branch %d: %w", i, err)
		}
		branches = append(branches, branch)
	}

	if len(branches) < 2 {
		return nil, fmt.Errorf("or clause must have at least two branches")
	}

	return &query.OrClause{Branches: branches}, nil
}

// parseOrJoinClause parses (or-join [?x] branch1 branch2 ...)
func parseOrJoinClause(node *edn.Node) (*query.OrJoinClause, error) {
	if len(node.Nodes) < 4 {
		return nil, fmt.Errorf("or-join clause must have join vars and at least two branches")
	}

	// Second element must be join vars vector
	if node.Nodes[1].Type != edn.NodeVector {
		return nil, fmt.Errorf("or-join second element must be a vector of join variables, got %v", node.Nodes[1].Type)
	}

	// Parse join variables
	joinVars, err := parseJoinVars(&node.Nodes[1])
	if err != nil {
		return nil, fmt.Errorf("error parsing or-join vars: %w", err)
	}

	// Parse remaining elements as branches
	var branches [][]query.Clause
	for i := 2; i < len(node.Nodes); i++ {
		branch, err := parseBranch(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing or-join branch %d: %w", i, err)
		}
		branches = append(branches, branch)
	}

	if len(branches) < 2 {
		return nil, fmt.Errorf("or-join clause must have at least two branches")
	}

	return &query.OrJoinClause{JoinVars: joinVars, Branches: branches}, nil
}

// parseOrDefaultClause parses (or-default branch1 branch2 ...)
func parseOrDefaultClause(node *edn.Node) (*query.OrDefaultClause, error) {
	var branches [][]query.Clause
	for i := 1; i < len(node.Nodes); i++ {
		branch, err := parseBranch(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing or-default branch %d: %w", i, err)
		}
		branches = append(branches, branch)
	}

	if len(branches) < 2 {
		return nil, fmt.Errorf("or-default clause must have at least two branches")
	}

	return &query.OrDefaultClause{Branches: branches}, nil
}

// parseOrDefaultJoinClause parses (or-default-join [?x] branch1 branch2 ...)
func parseOrDefaultJoinClause(node *edn.Node) (*query.OrDefaultJoinClause, error) {
	if len(node.Nodes) < 4 {
		return nil, fmt.Errorf("or-default-join clause must have join vars and at least two branches")
	}

	if node.Nodes[1].Type != edn.NodeVector {
		return nil, fmt.Errorf("or-default-join second element must be a vector of join variables, got %v", node.Nodes[1].Type)
	}

	joinVars, err := parseJoinVars(&node.Nodes[1])
	if err != nil {
		return nil, fmt.Errorf("error parsing or-default-join vars: %w", err)
	}

	var branches [][]query.Clause
	for i := 2; i < len(node.Nodes); i++ {
		branch, err := parseBranch(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("error parsing or-default-join branch %d: %w", i, err)
		}
		branches = append(branches, branch)
	}

	if len(branches) < 2 {
		return nil, fmt.Errorf("or-default-join clause must have at least two branches")
	}

	return &query.OrDefaultJoinClause{JoinVars: joinVars, Branches: branches}, nil
}

// parseJoinVars parses a vector of join variables [?x ?y ...]
func parseJoinVars(node *edn.Node) ([]query.Symbol, error) {
	if node.Type != edn.NodeVector {
		return nil, fmt.Errorf("join vars must be a vector")
	}

	if len(node.Nodes) == 0 {
		return nil, fmt.Errorf("join vars cannot be empty")
	}

	var vars []query.Symbol
	for i, elem := range node.Nodes {
		if elem.Type != edn.NodeSymbol {
			return nil, fmt.Errorf("join variable %d must be a symbol, got %v", i, elem.Type)
		}
		sym := datalog.NewSymbol(elem.Value)
		if !sym.IsVariable() {
			return nil, fmt.Errorf("join variable %d must start with ?, got %s", i, sym)
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

// Utility functions

// ExtractVariables returns all unique variables from patterns
func ExtractVariables(clauses []query.Clause) []query.Symbol {
	seen := make(map[query.Symbol]bool)
	var vars []query.Symbol

	for _, clause := range clauses {
		switch p := clause.(type) {
		case *query.DataPattern:
			for _, elem := range p.Elements {
				if elem.IsVariable() {
					if v, ok := elem.(query.Variable); ok {
						if !seen[v.Name] {
							seen[v.Name] = true
							vars = append(vars, v.Name)
						}
					}
				}
			}
		case *query.SubqueryPattern:
			// Add variables from binding form - these are PROVIDED by the subquery
			switch b := p.Binding.(type) {
			case query.TupleBinding:
				for _, v := range b.Variables {
					if !seen[v] {
						seen[v] = true
						vars = append(vars, v)
					}
				}
			case query.CollectionBinding:
				if !seen[b.Variable] {
					seen[b.Variable] = true
					vars = append(vars, b.Variable)
				}
			case query.RelationBinding:
				for _, v := range b.Variables {
					if !seen[v] {
						seen[v] = true
						vars = append(vars, v)
					}
				}
			}
			// Note: Input variables are consumed, not provided

		case *query.NotClause:
			// NOT doesn't provide new variables, but recursively extract from inner clauses
			// (to check they're all bound before NOT executes)
			innerVars := ExtractVariables(p.Clauses)
			for _, v := range innerVars {
				if !seen[v] {
					seen[v] = true
					vars = append(vars, v)
				}
			}

		case *query.NotJoinClause:
			// NOT-JOIN only exposes join vars
			for _, v := range p.JoinVars {
				if !seen[v] {
					seen[v] = true
					vars = append(vars, v)
				}
			}

		case *query.OrClause:
			// OR provides intersection of all branches
			if len(p.Branches) > 0 {
				// Get vars from first branch
				firstBranchVars := make(map[query.Symbol]bool)
				for _, v := range ExtractVariables(p.Branches[0]) {
					firstBranchVars[v] = true
				}

				// Intersect with remaining branches
				for _, branch := range p.Branches[1:] {
					branchVars := make(map[query.Symbol]bool)
					for _, v := range ExtractVariables(branch) {
						branchVars[v] = true
					}
					// Keep only vars in both
					for v := range firstBranchVars {
						if !branchVars[v] {
							delete(firstBranchVars, v)
						}
					}
				}

				// Add intersection to result
				for v := range firstBranchVars {
					if !seen[v] {
						seen[v] = true
						vars = append(vars, v)
					}
				}
			}

		case *query.OrJoinClause:
			// OR-JOIN only exposes join vars
			for _, v := range p.JoinVars {
				if !seen[v] {
					seen[v] = true
					vars = append(vars, v)
				}
			}
		}
	}

	return vars
}

// ValidateQuery performs semantic validation on a query
func ValidateQuery(q *query.Query) error {
	// Check that all find variables appear in where clause
	whereVars := ExtractVariables(q.Where)
	whereVarSet := make(map[query.Symbol]bool)
	for _, v := range whereVars {
		whereVarSet[v] = true
	}

	// Check each find element
	for _, elem := range q.Find {
		switch e := elem.(type) {
		case query.FindVariable:
			if !whereVarSet[e.Symbol] {
				return fmt.Errorf("find variable %s not bound in where clause", e.Symbol)
			}
		case query.FindAggregate:
			if !whereVarSet[e.Arg] {
				return fmt.Errorf("aggregate variable %s not bound in where clause", e.Arg)
			}
		}
	}

	return nil
}

// FormatQuery formats a query as a readable string in EDN format
func FormatQuery(q *query.Query) string {
	return formatQueryWithIndent(q, "")
}

// formatQueryWithIndent formats a query with a given indentation prefix
func formatQueryWithIndent(q *query.Query, indent string) string {
	var sb strings.Builder

	sb.WriteString("[:find")
	for _, v := range q.Find {
		sb.WriteString(" ")
		sb.WriteString(v.String())
	}

	// Add :in clause if present
	if len(q.In) > 0 {
		sb.WriteString("\n")
		sb.WriteString(indent)
		sb.WriteString(" :in")
		for _, input := range q.In {
			sb.WriteString(" ")
			sb.WriteString(input.String())
		}
	}

	sb.WriteString("\n")
	sb.WriteString(indent)
	sb.WriteString(" :where")

	// For patterns, we want them aligned under :where
	// The first pattern can go on the same line as :where
	patternIndent := indent + "        " // 8 spaces to align with :where text

	for i, p := range q.Where {
		if i == 0 {
			sb.WriteString(" ")
		} else {
			sb.WriteString("\n")
			sb.WriteString(patternIndent)
		}
		formatPatternWithIndent(&sb, p, indent)
	}

	// Add :order-by clause if present
	if len(q.OrderBy) > 0 {
		sb.WriteString("\n")
		sb.WriteString(indent)
		sb.WriteString(" :order-by [")
		for i, clause := range q.OrderBy {
			if i > 0 {
				sb.WriteString(" ")
			}
			if clause.Direction == query.OrderDesc {
				sb.WriteString("[")
				sb.WriteString(clause.Variable.String())
				sb.WriteString(" :desc]")
			} else {
				// For ascending (default), just write the variable
				sb.WriteString(clause.Variable.String())
			}
		}
		sb.WriteString("]")
	}

	sb.WriteString("]")

	return sb.String()
}

// formatPattern formats a single pattern in proper EDN format
func formatPattern(sb *strings.Builder, pattern query.Pattern) {
	formatPatternWithIndent(sb, pattern, "")
}

// formatPatternWithIndent formats a pattern with proper indentation
func formatPatternWithIndent(sb *strings.Builder, pattern query.Pattern, indent string) {
	// Special handling for complex nested structures that need indentation
	switch p := pattern.(type) {
	case *query.SubqueryPattern:
		// Subqueries need special formatting with indentation
		formatSubqueryPattern(sb, p, indent)
		return

	case *query.DataPattern:
		// DataPattern needs custom formatting for pattern elements
		sb.WriteString("[")
		for i, elem := range p.Elements {
			if i > 0 {
				sb.WriteString(" ")
			}
			formatPatternElement(sb, elem)
		}
		sb.WriteString("]")
		return

	}

	// For everything else (new types with String() methods), just use String()
	sb.WriteString(pattern.String())
}

// formatSubqueryPattern formats a subquery with proper indentation
func formatSubqueryPattern(sb *strings.Builder, p *query.SubqueryPattern, indent string) {
	sb.WriteString("[(q ")
	// Format the nested query with proper indentation
	// The current pattern is indented by 'indent' + 8 spaces (for alignment under :where)
	// We've written "[(q " which is 4 characters
	// So the nested query should be indented by: current line indent + 4 spaces for "[(q "
	baseIndent := indent + "        "   // 8 spaces to match main pattern alignment
	nestedIndent := baseIndent + "    " // 4 more spaces for "[(q "
	formattedNested := formatQueryWithIndent(p.Query, nestedIndent)
	sb.WriteString(formattedNested)

	// Add newline and indent for inputs and binding
	// They should align with the [ of the query vector
	sb.WriteString("\n")
	sb.WriteString(nestedIndent)

	// Format inputs
	for i, input := range p.Inputs {
		if i > 0 {
			sb.WriteString(" ")
		}
		formatPatternElement(sb, input)
	}
	sb.WriteString(") ")

	// Format binding
	sb.WriteString(p.Binding.String())
	sb.WriteString("]")
}

// formatPatternElement formats a single pattern element in EDN format
func formatPatternElement(sb *strings.Builder, elem query.PatternElement) {
	switch e := elem.(type) {
	case query.Variable:
		sb.WriteString(e.Name.String())

	case query.Blank:
		sb.WriteString("_")

	case query.Constant:
		formatValue(sb, e.Value)
	}
}

// formatValue formats a value in proper EDN format
func formatValue(sb *strings.Builder, v interface{}) {
	switch val := v.(type) {
	case datalog.Keyword:
		sb.WriteString(val.String())

	case datalog.Identity:
		// For entity references in queries, use the original string representation
		// wrapped in a custom reader tag for clarity
		sb.WriteString("#db/id \"")
		sb.WriteString(val.String())
		sb.WriteString("\"")

	case string:
		// Properly escape strings for EDN
		sb.WriteString(`"`)
		for _, r := range val {
			switch r {
			case '"':
				sb.WriteString(`\"`)
			case '\\':
				sb.WriteString(`\\`)
			case '\n':
				sb.WriteString(`\n`)
			case '\r':
				sb.WriteString(`\r`)
			case '\t':
				sb.WriteString(`\t`)
			default:
				sb.WriteRune(r)
			}
		}
		sb.WriteString(`"`)

	case int64:
		sb.WriteString(strconv.FormatInt(val, 10))

	case int:
		sb.WriteString(strconv.Itoa(val))

	case float64:
		sb.WriteString(strconv.FormatFloat(val, 'g', -1, 64))

	case bool:
		if val {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}

	default:
		// Fallback to string representation
		sb.WriteString(fmt.Sprintf("%v", v))
	}
}
