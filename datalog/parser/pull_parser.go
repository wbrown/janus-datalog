package parser

import (
	"fmt"
	"strconv"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/edn"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// parseFindPull parses a pull expression in the find clause: (pull ?e [...])
func parseFindPull(node *edn.Node) (query.FindPull, error) {
	// Validate: (pull ?var [...])
	if len(node.Nodes) != 3 {
		return query.FindPull{}, fmt.Errorf("pull requires exactly 2 arguments: variable and pattern, got %d", len(node.Nodes)-1)
	}

	// First element is "pull" (already validated)

	// Parse variable (second element)
	if node.Nodes[1].Type != edn.NodeSymbol {
		return query.FindPull{}, fmt.Errorf("pull first argument must be a variable, got %v", node.Nodes[1].Type)
	}
	varSym := query.Symbol(node.Nodes[1].Value)
	if !varSym.IsVariable() {
		return query.FindPull{}, fmt.Errorf("pull argument must be a variable (starting with ?), got %s", varSym)
	}

	// Parse pattern (third element)
	if node.Nodes[2].Type != edn.NodeVector {
		return query.FindPull{}, fmt.Errorf("pull pattern must be a vector [...], got %v", node.Nodes[2].Type)
	}
	pattern, err := parsePullPattern(&node.Nodes[2])
	if err != nil {
		return query.FindPull{}, fmt.Errorf("invalid pull pattern: %w", err)
	}

	return query.FindPull{
		Variable: varSym,
		Pattern:  pattern,
	}, nil
}

// parsePullPattern parses a pull pattern vector: [:attr1 :attr2 {:ref [...]}]
func parsePullPattern(node *edn.Node) (*query.PullPattern, error) {
	if node.Type != edn.NodeVector {
		return nil, fmt.Errorf("pull pattern must be a vector, got %v", node.Type)
	}

	specs := make([]query.PullAttrSpec, 0, len(node.Nodes))

	for i := range node.Nodes {
		spec, err := parsePullAttrSpec(&node.Nodes[i])
		if err != nil {
			return nil, fmt.Errorf("invalid pull spec at position %d: %w", i, err)
		}
		specs = append(specs, spec)
	}

	return &query.PullPattern{Specs: specs}, nil
}

// parsePullAttrSpec parses a single attribute specification in a pull pattern
func parsePullAttrSpec(node *edn.Node) (query.PullAttrSpec, error) {
	switch node.Type {
	case edn.NodeKeyword:
		// Simple attribute: :entity/name
		return &query.PullAttribute{
			Attr: datalog.NewKeyword(node.Value),
		}, nil

	case edn.NodeSymbol:
		if node.Value == "*" {
			// Wildcard: *
			return &query.PullWildcard{}, nil
		}
		return nil, fmt.Errorf("invalid pull spec symbol: %s (expected * for wildcard)", node.Value)

	case edn.NodeMap:
		// Map spec: {:entity/region [...]}
		return parsePullMapSpec(node)

	case edn.NodeList:
		// Function expression: (limit ...) or (default ...)
		return parsePullExpr(node)

	default:
		return nil, fmt.Errorf("invalid pull spec type: %v (expected keyword, *, map, or expression)", node.Type)
	}
}

// parsePullMapSpec parses a map specification: {:attr [...pattern...]}
func parsePullMapSpec(node *edn.Node) (*query.PullMapSpec, error) {
	// Map nodes have key-value pairs in Nodes slice: [key1, val1, key2, val2, ...]
	// We expect exactly one key-value pair
	if len(node.Nodes) != 2 {
		return nil, fmt.Errorf("pull map spec must have exactly one attribute, got %d elements", len(node.Nodes)/2)
	}

	// Key must be keyword
	if node.Nodes[0].Type != edn.NodeKeyword {
		return nil, fmt.Errorf("pull map key must be a keyword, got %v", node.Nodes[0].Type)
	}
	attr := datalog.NewKeyword(node.Nodes[0].Value)

	// Value must be pattern vector
	if node.Nodes[1].Type != edn.NodeVector {
		return nil, fmt.Errorf("pull map value must be a pattern vector [...], got %v", node.Nodes[1].Type)
	}
	pattern, err := parsePullPattern(&node.Nodes[1])
	if err != nil {
		return nil, fmt.Errorf("invalid nested pattern for %s: %w", attr.String(), err)
	}

	return &query.PullMapSpec{
		Attr:    attr,
		Pattern: pattern,
	}, nil
}

// parsePullExpr parses a pull expression: (limit ...) or (default ...)
func parsePullExpr(node *edn.Node) (query.PullAttrSpec, error) {
	if len(node.Nodes) < 2 {
		return nil, fmt.Errorf("pull expression requires at least 2 arguments")
	}

	if node.Nodes[0].Type != edn.NodeSymbol {
		return nil, fmt.Errorf("pull expression must start with a function name, got %v", node.Nodes[0].Type)
	}

	fn := node.Nodes[0].Value
	switch fn {
	case "limit":
		// (limit :attr N)
		if len(node.Nodes) != 3 {
			return nil, fmt.Errorf("limit requires exactly 2 arguments: attribute and count, got %d", len(node.Nodes)-1)
		}
		if node.Nodes[1].Type != edn.NodeKeyword {
			return nil, fmt.Errorf("limit first argument must be a keyword, got %v", node.Nodes[1].Type)
		}
		if node.Nodes[2].Type != edn.NodeInt {
			return nil, fmt.Errorf("limit second argument must be an integer, got %v", node.Nodes[2].Type)
		}
		limit, err := strconv.Atoi(node.Nodes[2].Value)
		if err != nil {
			return nil, fmt.Errorf("invalid limit value: %w", err)
		}
		if limit < 0 {
			return nil, fmt.Errorf("limit must be non-negative, got %d", limit)
		}
		return &query.PullLimitExpr{
			Attr:  datalog.NewKeyword(node.Nodes[1].Value),
			Limit: limit,
		}, nil

	case "default":
		// (default :attr value)
		if len(node.Nodes) != 3 {
			return nil, fmt.Errorf("default requires exactly 2 arguments: attribute and value, got %d", len(node.Nodes)-1)
		}
		if node.Nodes[1].Type != edn.NodeKeyword {
			return nil, fmt.Errorf("default first argument must be a keyword, got %v", node.Nodes[1].Type)
		}
		defaultVal, err := parseDefaultValue(&node.Nodes[2])
		if err != nil {
			return nil, fmt.Errorf("invalid default value: %w", err)
		}
		return &query.PullDefaultExpr{
			Attr:    datalog.NewKeyword(node.Nodes[1].Value),
			Default: defaultVal,
		}, nil

	default:
		return nil, fmt.Errorf("unknown pull expression: %s (expected 'limit' or 'default')", fn)
	}
}

// parseDefaultValue parses a default value for (default ...) expressions
func parseDefaultValue(node *edn.Node) (interface{}, error) {
	switch node.Type {
	case edn.NodeString:
		return node.Value, nil
	case edn.NodeInt:
		val, err := strconv.ParseInt(node.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer: %w", err)
		}
		return val, nil
	case edn.NodeFloat:
		val, err := strconv.ParseFloat(node.Value, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float: %w", err)
		}
		return val, nil
	case edn.NodeBool:
		return node.Value == "true", nil
	case edn.NodeKeyword:
		return datalog.NewKeyword(node.Value), nil
	case edn.NodeNil:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported default value type: %v", node.Type)
	}
}

// ParsePullPattern parses a standalone pull pattern string
// Example: `[:entity/code :entity/name {:entity/region [...]}]`
func ParsePullPattern(input string) (*query.PullPattern, error) {
	node, err := edn.Parse(input)
	if err != nil {
		return nil, fmt.Errorf("EDN parse error: %w", err)
	}

	if node.Type != edn.NodeVector {
		return nil, fmt.Errorf("pull pattern must be a vector [...], got %v", node.Type)
	}

	return parsePullPattern(node)
}
