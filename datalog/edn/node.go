package edn

import (
	"fmt"
	"strconv"
	"strings"
)

// NodeType represents the type of EDN node
type NodeType int

const (
	NodeNil NodeType = iota
	NodeBool
	NodeInt
	NodeFloat
	NodeString
	NodeChar
	NodeSymbol
	NodeKeyword
	NodeList
	NodeVector
	NodeMap
	NodeSet
	NodeTagged
)

// Node represents an EDN value
type Node struct {
	Type   NodeType
	Line   int
	Col    int
	Value  string // For atoms
	Nodes  []Node // For collections
	Tag    string // For tagged values
	Tagged *Node  // For tagged values
}

// String returns a string representation of the node
func (n Node) String() string {
	switch n.Type {
	case NodeNil:
		return "nil"
	case NodeBool, NodeInt, NodeFloat, NodeString, NodeChar, NodeSymbol, NodeKeyword:
		return n.Value
	case NodeList:
		parts := make([]string, len(n.Nodes))
		for i, node := range n.Nodes {
			parts[i] = node.String()
		}
		return "(" + strings.Join(parts, " ") + ")"
	case NodeVector:
		parts := make([]string, len(n.Nodes))
		for i, node := range n.Nodes {
			parts[i] = node.String()
		}
		return "[" + strings.Join(parts, " ") + "]"
	case NodeMap:
		parts := make([]string, len(n.Nodes))
		for i, node := range n.Nodes {
			parts[i] = node.String()
		}
		return "{" + strings.Join(parts, " ") + "}"
	case NodeSet:
		parts := make([]string, len(n.Nodes))
		for i, node := range n.Nodes {
			parts[i] = node.String()
		}
		return "#{" + strings.Join(parts, " ") + "}"
	case NodeTagged:
		return "#" + n.Tag + " " + n.Tagged.String()
	default:
		return fmt.Sprintf("Unknown[%v]", n.Value)
	}
}

// AsString returns the string value of a string node
func (n Node) AsString() (string, error) {
	if n.Type != NodeString {
		return "", fmt.Errorf("node is not a string")
	}
	return n.Value, nil
}

// AsInt returns the int value of an int node
func (n Node) AsInt() (int64, error) {
	if n.Type != NodeInt {
		return 0, fmt.Errorf("node is not an int")
	}
	return strconv.ParseInt(n.Value, 10, 64)
}

// AsFloat returns the float value of a float node
func (n Node) AsFloat() (float64, error) {
	if n.Type != NodeFloat {
		return 0, fmt.Errorf("node is not a float")
	}
	return strconv.ParseFloat(n.Value, 64)
}

// AsBool returns the bool value of a bool node
func (n Node) AsBool() (bool, error) {
	if n.Type != NodeBool {
		return false, fmt.Errorf("node is not a bool")
	}
	return n.Value == "true", nil
}

// AsSymbol returns the symbol value
func (n Node) AsSymbol() (string, error) {
	if n.Type != NodeSymbol {
		return "", fmt.Errorf("node is not a symbol")
	}
	return n.Value, nil
}

// AsKeyword returns the keyword value
func (n Node) AsKeyword() (string, error) {
	if n.Type != NodeKeyword {
		return "", fmt.Errorf("node is not a keyword")
	}
	return n.Value, nil
}

// IsNil returns true if the node is nil
func (n Node) IsNil() bool {
	return n.Type == NodeNil
}

// IsCollection returns true if the node is a collection type
func (n Node) IsCollection() bool {
	return n.Type == NodeList || n.Type == NodeVector || n.Type == NodeMap || n.Type == NodeSet
}
