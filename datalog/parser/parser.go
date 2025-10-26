package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/wbrown/janus-datalog/datalog"
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
			for i < len(node.Nodes) && node.Nodes[i].Type != edn.NodeKeyword {
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
				if node.Nodes[i].Type != edn.NodeVector {
					return nil, fmt.Errorf("expected vector in :where clause, got %v", node.Nodes[i].Type)
				}
				pattern, err := parsePattern(&node.Nodes[i])
				if err != nil {
					return nil, fmt.Errorf("error parsing pattern: %w", err)
				}
				q.Where = append(q.Where, pattern)
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
		sym := query.Symbol(node.Value)
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

		sym := query.Symbol(node.Nodes[0].Value)
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
		sym := query.Symbol(node.Value)
		if !sym.IsVariable() {
			return nil, fmt.Errorf("find clause must contain variables, got %s", sym)
		}
		return query.FindVariable{Symbol: sym}, nil

	case edn.NodeList:
		// Aggregate function (sum ?x), (count ?x), etc.
		if len(node.Nodes) != 2 {
			return nil, fmt.Errorf("aggregate function must have exactly 2 elements: function and argument")
		}

		if node.Nodes[0].Type != edn.NodeSymbol {
			return nil, fmt.Errorf("aggregate function name must be a symbol")
		}

		if node.Nodes[1].Type != edn.NodeSymbol {
			return nil, fmt.Errorf("aggregate argument must be a symbol")
		}

		fn := node.Nodes[0].Value
		argSym := query.Symbol(node.Nodes[1].Value)

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
	if len(node.Nodes) >= 1 && node.Nodes[0].Type == edn.NodeList {
		list := &node.Nodes[0]

		// Check if it's a subquery pattern [(q ...) binding]
		if len(list.Nodes) >= 2 && list.Nodes[0].Type == edn.NodeSymbol && list.Nodes[0].Value == "q" {
			if len(node.Nodes) != 2 {
				return nil, fmt.Errorf("subquery pattern must have exactly 2 elements: [(q ...) binding]")
			}
			return parseSubqueryPattern(list, &node.Nodes[1])
		}

		// Check if it's an expression [(fn ...) ?binding]
		if len(node.Nodes) == 2 && node.Nodes[1].Type == edn.NodeSymbol {
			sym := query.Symbol(node.Nodes[1].Value)
			if sym.IsVariable() {
				return parseExpression(&node.Nodes[0], sym)
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
	if len(node.Nodes) < 3 || len(node.Nodes) > 4 {
		return nil, fmt.Errorf("data pattern must have 3 or 4 elements, got %d", len(node.Nodes))
	}

	pattern := &query.DataPattern{
		Elements: make([]query.PatternElement, len(node.Nodes)),
	}

	for i, elem := range node.Nodes {
		patternElem, err := parsePatternElement(&elem)
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
func parseBindingForm(node *edn.Node) (query.BindingForm, error) {
	switch node.Type {
	case edn.NodeSymbol:
		// Collection binding: ?coll
		sym := query.Symbol(node.Value)
		if !sym.IsVariable() {
			return nil, fmt.Errorf("collection binding must be a variable, got %s", sym)
		}
		return query.CollectionBinding{Variable: sym}, nil

	case edn.NodeVector:
		// Could be [[?a ?b]] tuple binding or [[?a ?b] ...] relation binding
		if len(node.Nodes) == 0 {
			return nil, fmt.Errorf("binding form cannot be empty vector")
		}

		// Check if it's a nested vector (tuple binding)
		if node.Nodes[0].Type == edn.NodeVector {
			if len(node.Nodes) == 1 {
				// [[?a ?b]] - tuple binding
				return parseTupleBinding(&node.Nodes[0])
			} else if len(node.Nodes) == 2 && node.Nodes[1].Type == edn.NodeSymbol && node.Nodes[1].Value == "..." {
				// [[?a ?b] ...] - relation binding
				return parseRelationBinding(&node.Nodes[0])
			}
		}

		return nil, fmt.Errorf("invalid binding form: expected [[?vars]], [[?vars] ...], or ?coll")

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
		sym := query.Symbol(elem.Value)
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
		sym := query.Symbol(elem.Value)
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
		// Either $ (database) or ?var (scalar input)
		if node.Value == "$" {
			return query.DatabaseInput{}, nil
		}
		sym := query.Symbol(node.Value)
		if !sym.IsVariable() {
			return nil, fmt.Errorf("input must be $ or a variable, got %s", node.Value)
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
				sym := query.Symbol(elem.Value)
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
			sym := query.Symbol(node.Nodes[0].Value)
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
		sym := query.Symbol(node.Value)
		if sym.IsVariable() {
			return query.Variable{Name: sym}, nil
		} else if node.Value == "_" {
			return query.Blank{}, nil
		} else if node.Value == "$" {
			// Database marker - treat as a special constant
			return query.Constant{Value: query.Symbol("$")}, nil
		} else {
			return nil, fmt.Errorf("invalid symbol in pattern: %s", node.Value)
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

	default:
		return nil, fmt.Errorf("unsupported pattern element type: %v", node.Type)
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
				sb.WriteString(string(clause.Variable))
				sb.WriteString(" :desc]")
			} else {
				// For ascending (default), just write the variable
				sb.WriteString(string(clause.Variable))
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
		sb.WriteString(string(e.Name))

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
