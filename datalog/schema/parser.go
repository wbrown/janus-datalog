package schema

import (
	"fmt"
	"os"
	"strings"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/edn"
)

// ParseSchema parses an EDN schema definition string
//
// Example input:
//
//	{:person/name   {:db/valueType   :db.type/string
//	                 :db/cardinality :db.cardinality/one}
//	 :person/friends {:db/valueType   :db.type/ref
//	                  :db/cardinality :db.cardinality/many}}
func ParseSchema(input string) (*Schema, error) {
	node, err := edn.Parse(input)
	if err != nil {
		return nil, fmt.Errorf("EDN parse error: %w", err)
	}

	return parseSchemaNode(node)
}

// ParseSchemaFile parses a schema from a file
func ParseSchemaFile(path string) (*Schema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}
	return ParseSchema(string(data))
}

// parseSchemaNode parses a schema from an EDN node
func parseSchemaNode(node *edn.Node) (*Schema, error) {
	if node.Type != edn.NodeMap {
		return nil, fmt.Errorf("schema must be a map, got %v", node.Type)
	}

	schema := NewSchema()

	// Map nodes have alternating key-value pairs in Nodes slice
	if len(node.Nodes)%2 != 0 {
		return nil, fmt.Errorf("schema map has odd number of elements")
	}

	for i := 0; i < len(node.Nodes); i += 2 {
		keyNode := &node.Nodes[i]
		valueNode := &node.Nodes[i+1]

		if keyNode.Type != edn.NodeKeyword {
			return nil, fmt.Errorf("attribute ident must be keyword at line %d, got %v",
				keyNode.Line, keyNode.Type)
		}

		def, err := parseAttributeDefinition(keyNode.Value, valueNode)
		if err != nil {
			return nil, fmt.Errorf("invalid attribute %s at line %d: %w",
				keyNode.Value, keyNode.Line, err)
		}

		schema.Add(def)
	}

	return schema, nil
}

// parseAttributeDefinition parses a single attribute definition from EDN
func parseAttributeDefinition(ident string, node *edn.Node) (*AttributeDefinition, error) {
	if node.Type != edn.NodeMap {
		return nil, fmt.Errorf("attribute definition must be a map, got %v", node.Type)
	}

	def := &AttributeDefinition{
		Ident:       datalog.NewKeyword(ident),
		Cardinality: CardinalityOne, // Default
	}

	// Parse map entries
	if len(node.Nodes)%2 != 0 {
		return nil, fmt.Errorf("attribute definition map has odd number of elements")
	}

	for i := 0; i < len(node.Nodes); i += 2 {
		keyNode := &node.Nodes[i]
		valueNode := &node.Nodes[i+1]

		if keyNode.Type != edn.NodeKeyword {
			continue // Skip non-keyword keys
		}

		switch keyNode.Value {
		case ":db/valueType":
			vt, err := parseValueType(valueNode)
			if err != nil {
				return nil, fmt.Errorf(":db/valueType error: %w", err)
			}
			def.ValueType = vt

		case ":db/cardinality":
			card, err := parseCardinality(valueNode)
			if err != nil {
				return nil, fmt.Errorf(":db/cardinality error: %w", err)
			}
			def.Cardinality = card

		case ":db/unique":
			uniq, err := parseUnique(valueNode)
			if err != nil {
				return nil, fmt.Errorf(":db/unique error: %w", err)
			}
			def.Unique = uniq

		case ":db/doc":
			if valueNode.Type != edn.NodeString {
				return nil, fmt.Errorf(":db/doc must be string, got %v", valueNode.Type)
			}
			def.Doc = valueNode.Value
		}
	}

	return def, nil
}

// parseValueType parses a :db/valueType keyword
func parseValueType(node *edn.Node) (ValueType, error) {
	if node.Type != edn.NodeKeyword {
		return "", fmt.Errorf("must be keyword, got %v", node.Type)
	}

	// Remove leading colon and convert to our format
	val := strings.TrimPrefix(node.Value, ":")

	switch val {
	case "db.type/string":
		return TypeString, nil
	case "db.type/long":
		return TypeLong, nil
	case "db.type/double":
		return TypeDouble, nil
	case "db.type/boolean":
		return TypeBoolean, nil
	case "db.type/instant":
		return TypeInstant, nil
	case "db.type/bytes":
		return TypeBytes, nil
	case "db.type/ref":
		return TypeRef, nil
	case "db.type/keyword":
		return TypeKeyword, nil
	default:
		return "", fmt.Errorf("unknown value type: %s", node.Value)
	}
}

// parseCardinality parses a :db/cardinality keyword
func parseCardinality(node *edn.Node) (Cardinality, error) {
	if node.Type != edn.NodeKeyword {
		return "", fmt.Errorf("must be keyword, got %v", node.Type)
	}

	val := strings.TrimPrefix(node.Value, ":")

	switch val {
	case "db.cardinality/one":
		return CardinalityOne, nil
	case "db.cardinality/many":
		return CardinalityMany, nil
	default:
		return "", fmt.Errorf("unknown cardinality: %s", node.Value)
	}
}

// parseUnique parses a :db/unique keyword
func parseUnique(node *edn.Node) (Unique, error) {
	if node.Type != edn.NodeKeyword {
		return "", fmt.Errorf("must be keyword, got %v", node.Type)
	}

	val := strings.TrimPrefix(node.Value, ":")

	switch val {
	case "db.unique/value":
		return UniqueValue, nil
	case "db.unique/identity":
		return UniqueIdentity, nil
	default:
		return "", fmt.Errorf("unknown unique constraint: %s", node.Value)
	}
}
