package schema

import (
	"fmt"
	"os"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/edn"
)

// Pre-interned keywords for schema EDN parsing. Using interned
// Keyword pointers (not stripped strings) lets us compare parse input
// by pointer equality — the idiomatic comparison in this codebase —
// and avoids a TrimPrefix allocation per call.

// Attribute definition map keys.
var (
	kwDBValueType       = datalog.NewKeyword(":db/valueType")
	kwDBCardinality     = datalog.NewKeyword(":db/cardinality")
	kwDBUnique          = datalog.NewKeyword(":db/unique")
	kwDBDoc             = datalog.NewKeyword(":db/doc")
	kwDBUniqueElements  = datalog.NewKeyword(":db/unique-elements")
)

// Value-type keywords.
var (
	kwTypeString  = datalog.NewKeyword(":db.type/string")
	kwTypeLong    = datalog.NewKeyword(":db.type/long")
	kwTypeDouble  = datalog.NewKeyword(":db.type/double")
	kwTypeBoolean = datalog.NewKeyword(":db.type/boolean")
	kwTypeInstant = datalog.NewKeyword(":db.type/instant")
	kwTypeBytes   = datalog.NewKeyword(":db.type/bytes")
	kwTypeRef     = datalog.NewKeyword(":db.type/ref")
	kwTypeKeyword = datalog.NewKeyword(":db.type/keyword")
	kwTypeTx      = datalog.NewKeyword(":db.type/tx")
)

// Cardinality keywords.
var (
	kwCardOne    = datalog.NewKeyword(":db.cardinality/one")
	kwCardMany   = datalog.NewKeyword(":db.cardinality/many")
	kwCardVector = datalog.NewKeyword(":db.cardinality/vector")
)

// Unique-constraint keywords.
var (
	kwUniqueValue    = datalog.NewKeyword(":db.unique/value")
	kwUniqueIdentity = datalog.NewKeyword(":db.unique/identity")
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

		// Intern the map key once and compare against the pre-interned
		// constants by pointer equality — idiomatic and O(1).
		key := datalog.NewKeyword(keyNode.Value)
		switch key {
		case kwDBValueType:
			vt, err := parseValueType(valueNode)
			if err != nil {
				return nil, fmt.Errorf(":db/valueType error: %w", err)
			}
			def.ValueType = vt

		case kwDBCardinality:
			card, err := parseCardinality(valueNode)
			if err != nil {
				return nil, fmt.Errorf(":db/cardinality error: %w", err)
			}
			def.Cardinality = card

		case kwDBUnique:
			uniq, err := parseUnique(valueNode)
			if err != nil {
				return nil, fmt.Errorf(":db/unique error: %w", err)
			}
			def.Unique = uniq

		case kwDBDoc:
			if valueNode.Type != edn.NodeString {
				return nil, fmt.Errorf(":db/doc must be string, got %v", valueNode.Type)
			}
			def.Doc = valueNode.Value

		case kwDBUniqueElements:
			if valueNode.Type != edn.NodeBool {
				return nil, fmt.Errorf(":db/unique-elements must be boolean, got %v", valueNode.Type)
			}
			def.UniqueElements = valueNode.Value == "true"
		}
	}

	return def, nil
}

// parseValueType parses a :db/valueType keyword via pointer-equality
// comparison against the pre-interned kwType* constants.
func parseValueType(node *edn.Node) (ValueType, error) {
	if node.Type != edn.NodeKeyword {
		return "", fmt.Errorf("must be keyword, got %v", node.Type)
	}

	kw := datalog.NewKeyword(node.Value)
	switch kw {
	case kwTypeString:
		return TypeString, nil
	case kwTypeLong:
		return TypeLong, nil
	case kwTypeDouble:
		return TypeDouble, nil
	case kwTypeBoolean:
		return TypeBoolean, nil
	case kwTypeInstant:
		return TypeInstant, nil
	case kwTypeBytes:
		return TypeBytes, nil
	case kwTypeRef:
		return TypeRef, nil
	case kwTypeKeyword:
		return TypeKeyword, nil
	case kwTypeTx:
		return TypeTx, nil
	default:
		return "", fmt.Errorf("unknown value type: %s", node.Value)
	}
}

// parseCardinality parses a :db/cardinality keyword via
// pointer-equality comparison against the pre-interned kwCard* constants.
func parseCardinality(node *edn.Node) (Cardinality, error) {
	if node.Type != edn.NodeKeyword {
		return "", fmt.Errorf("must be keyword, got %v", node.Type)
	}

	kw := datalog.NewKeyword(node.Value)
	switch kw {
	case kwCardOne:
		return CardinalityOne, nil
	case kwCardMany:
		return CardinalityMany, nil
	case kwCardVector:
		return CardinalityVector, nil
	default:
		return "", fmt.Errorf("unknown cardinality: %s", node.Value)
	}
}

// parseUnique parses a :db/unique keyword via pointer-equality
// comparison against the pre-interned kwUnique* constants.
func parseUnique(node *edn.Node) (Unique, error) {
	if node.Type != edn.NodeKeyword {
		return "", fmt.Errorf("must be keyword, got %v", node.Type)
	}

	kw := datalog.NewKeyword(node.Value)
	switch kw {
	case kwUniqueValue:
		return UniqueValue, nil
	case kwUniqueIdentity:
		return UniqueIdentity, nil
	default:
		return "", fmt.Errorf("unknown unique constraint: %s", node.Value)
	}
}
