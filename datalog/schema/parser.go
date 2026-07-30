package schema

import (
	"fmt"
	"os"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/edn"
)

// Attribute definition map keys, interned so the parse loop compares by
// pointer. Registered well-known: these are package variables compared by
// identity, so a ClearInterns that re-interned rather than restored them would
// leave every parse silently failing to recognise its own map keys.
//
// Only the map keys. The value vocabularies — :db.type/string and the rest —
// are the constants in types.go, and parsing compares against those directly:
// interning makes the parsed keyword the same instance, so there is nothing to
// translate and no second copy to keep in step.
var (
	kwDBValueType      = datalog.WellKnownKeyword(":db/valueType")
	kwDBCardinality    = datalog.WellKnownKeyword(":db/cardinality")
	kwDBUnique         = datalog.WellKnownKeyword(":db/unique")
	kwDBDoc            = datalog.WellKnownKeyword(":db/doc")
	kwDBUniqueElements = datalog.WellKnownKeyword(":db/unique-elements")
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

// parseValueType parses a :db/valueType keyword. The parsed keyword is the
// value — interning makes it the same instance as the TypeString constant —
// so all this does is admit it or reject it against the declared set.
func parseValueType(node *edn.Node) (datalog.Keyword, error) {
	if node.Type != edn.NodeKeyword {
		return nil, fmt.Errorf("must be keyword, got %v", node.Type)
	}
	kw := datalog.NewKeyword(node.Value)
	if _, ok := valueTypes[kw]; !ok {
		return nil, fmt.Errorf("unknown value type: %s", node.Value)
	}
	return kw, nil
}

// parseCardinality parses a :db/cardinality keyword. CardinalityUnknown is not
// in the admitted set: it marks an attribute no schema defines, so a schema
// declaring it would be declaring that it had declared nothing.
func parseCardinality(node *edn.Node) (datalog.Keyword, error) {
	if node.Type != edn.NodeKeyword {
		return nil, fmt.Errorf("must be keyword, got %v", node.Type)
	}
	kw := datalog.NewKeyword(node.Value)
	if _, ok := cardinalities[kw]; !ok {
		return nil, fmt.Errorf("unknown cardinality: %s", node.Value)
	}
	return kw, nil
}

// parseUnique parses a :db/unique keyword. There is no keyword for "not
// unique" — omitting :db/unique is how a definition says it.
func parseUnique(node *edn.Node) (datalog.Keyword, error) {
	if node.Type != edn.NodeKeyword {
		return nil, fmt.Errorf("must be keyword, got %v", node.Type)
	}
	kw := datalog.NewKeyword(node.Value)
	if _, ok := uniques[kw]; !ok {
		return nil, fmt.Errorf("unknown unique constraint: %s", node.Value)
	}
	return kw, nil
}
