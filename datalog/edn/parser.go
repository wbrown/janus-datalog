package edn

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

var (
	// Character validation for symbols
	symbolChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.*+!-_?$%&=<>/#"

	// Regex patterns for validation
	intPattern   = regexp.MustCompile(`^[+-]?\d+[MN]?$`)
	floatPattern = regexp.MustCompile(`^[+-]?\d+(\.\d+)?([eE][+-]?\d+)?M?$`)
)

// Parser parses EDN tokens into an AST
type Parser struct {
	lexer *Lexer
}

// NewParser creates a new parser
func NewParser(lexer *Lexer) *Parser {
	return &Parser{lexer: lexer}
}

// Parse parses the tokens into a Node
func Parse(input string) (*Node, error) {
	lexer := NewLexer(input)
	if err := lexer.Lex(); err != nil {
		return nil, err
	}

	parser := NewParser(lexer)
	return parser.Parse()
}

// Parse reads a single value
func (p *Parser) Parse() (*Node, error) {
	return p.readNode()
}

// ParseAll reads all values until EOF
func (p *Parser) ParseAll() ([]Node, error) {
	var nodes []Node

	for {
		token := p.lexer.PeekToken()
		if token.Type == TokenEOF {
			break
		}

		node, err := p.readNode()
		if err != nil {
			return nil, err
		}

		// Skip discarded forms (#_)
		if node == nil {
			continue
		}

		nodes = append(nodes, *node)
	}

	return nodes, nil
}

// readNode reads a single node
func (p *Parser) readNode() (*Node, error) {
	token := p.lexer.PeekToken()

	switch token.Type {
	case TokenEOF:
		return nil, fmt.Errorf("unexpected EOF at %d:%d", token.Line, token.Col)

	case TokenString:
		p.lexer.NextToken()
		return &Node{
			Type:  NodeString,
			Value: token.Value,
			Line:  token.Line,
			Col:   token.Col,
		}, nil

	case TokenAtom:
		return p.readAtom()

	case TokenLeftParen:
		return p.readList()

	case TokenLeftBracket:
		return p.readVector()

	case TokenLeftBrace:
		return p.readMap()

	default:
		return nil, fmt.Errorf("unexpected token %v at %d:%d", token.Type, token.Line, token.Col)
	}
}

// readAtom reads and classifies an atom
func (p *Parser) readAtom() (*Node, error) {
	token := p.lexer.NextToken()
	value := token.Value

	// Handle special atoms
	switch value {
	case "nil":
		return &Node{Type: NodeNil, Line: token.Line, Col: token.Col}, nil
	case "true", "false":
		return &Node{Type: NodeBool, Value: value, Line: token.Line, Col: token.Col}, nil
	}

	// Handle discard (#_)
	if strings.HasPrefix(value, "#_") {
		// Consume and discard the next form
		_, err := p.readNode()
		if err != nil {
			return nil, err
		}
		return nil, nil // Return nil to indicate discarded form
	}

	// Handle sets (#{...})
	if value == "#{" {
		return p.readSet()
	}

	// Handle tagged values (#tag value)
	if strings.HasPrefix(value, "#") && len(value) > 1 && value != "#_" {
		tag := value[1:]
		taggedNode, err := p.readNode()
		if err != nil {
			return nil, err
		}
		return &Node{
			Type:   NodeTagged,
			Tag:    tag,
			Tagged: taggedNode,
			Line:   token.Line,
			Col:    token.Col,
		}, nil
	}

	// Handle character literals
	if strings.HasPrefix(value, "\\") {
		if len(value) == 2 {
			return &Node{Type: NodeChar, Value: value, Line: token.Line, Col: token.Col}, nil
		}
		// Named characters
		switch value {
		case "\\newline":
			return &Node{Type: NodeChar, Value: "\\n", Line: token.Line, Col: token.Col}, nil
		case "\\return":
			return &Node{Type: NodeChar, Value: "\\r", Line: token.Line, Col: token.Col}, nil
		case "\\space":
			return &Node{Type: NodeChar, Value: "\\ ", Line: token.Line, Col: token.Col}, nil
		case "\\tab":
			return &Node{Type: NodeChar, Value: "\\t", Line: token.Line, Col: token.Col}, nil
		default:
			return nil, fmt.Errorf("invalid character literal %s at %d:%d", value, token.Line, token.Col)
		}
	}

	// Handle keywords
	if strings.HasPrefix(value, ":") {
		if err := validateKeyword(value); err != nil {
			return nil, fmt.Errorf("%v at %d:%d", err, token.Line, token.Col)
		}
		return &Node{Type: NodeKeyword, Value: value, Line: token.Line, Col: token.Col}, nil
	}

	// Check for integer
	if isValidInt(value) {
		return &Node{Type: NodeInt, Value: value, Line: token.Line, Col: token.Col}, nil
	}

	// Check for float
	if isValidFloat(value) {
		return &Node{Type: NodeFloat, Value: value, Line: token.Line, Col: token.Col}, nil
	}

	// Must be a symbol
	if err := validateSymbol(value); err != nil {
		return nil, fmt.Errorf("%v at %d:%d", err, token.Line, token.Col)
	}

	return &Node{Type: NodeSymbol, Value: value, Line: token.Line, Col: token.Col}, nil
}

// readList reads a list (...)
func (p *Parser) readList() (*Node, error) {
	startToken := p.lexer.NextToken() // consume (

	var nodes []Node
	for {
		token := p.lexer.PeekToken()
		if token.Type == TokenRightParen {
			p.lexer.NextToken() // consume )
			break
		}
		if token.Type == TokenEOF {
			return nil, fmt.Errorf("unterminated list starting at %d:%d", startToken.Line, startToken.Col)
		}

		node, err := p.readNode()
		if err != nil {
			return nil, err
		}
		if node != nil { // Skip discarded forms
			nodes = append(nodes, *node)
		}
	}

	return &Node{
		Type:  NodeList,
		Nodes: nodes,
		Line:  startToken.Line,
		Col:   startToken.Col,
	}, nil
}

// readVector reads a vector [...]
func (p *Parser) readVector() (*Node, error) {
	startToken := p.lexer.NextToken() // consume [

	var nodes []Node
	for {
		token := p.lexer.PeekToken()
		if token.Type == TokenRightBracket {
			p.lexer.NextToken() // consume ]
			break
		}
		if token.Type == TokenEOF {
			return nil, fmt.Errorf("unterminated vector starting at %d:%d", startToken.Line, startToken.Col)
		}

		node, err := p.readNode()
		if err != nil {
			return nil, err
		}
		if node != nil { // Skip discarded forms
			nodes = append(nodes, *node)
		}
	}

	return &Node{
		Type:  NodeVector,
		Nodes: nodes,
		Line:  startToken.Line,
		Col:   startToken.Col,
	}, nil
}

// readMap reads a map {...}
func (p *Parser) readMap() (*Node, error) {
	startToken := p.lexer.NextToken() // consume {

	var nodes []Node
	for {
		token := p.lexer.PeekToken()
		if token.Type == TokenRightBrace {
			p.lexer.NextToken() // consume }
			break
		}
		if token.Type == TokenEOF {
			return nil, fmt.Errorf("unterminated map starting at %d:%d", startToken.Line, startToken.Col)
		}

		// Read key
		key, err := p.readNode()
		if err != nil {
			return nil, err
		}
		if key == nil { // Skip discarded forms
			continue
		}

		// Read value
		token = p.lexer.PeekToken()
		if token.Type == TokenRightBrace || token.Type == TokenEOF {
			return nil, fmt.Errorf("map missing value for key at %d:%d", key.Line, key.Col)
		}

		value, err := p.readNode()
		if err != nil {
			return nil, err
		}
		if value == nil { // Skip discarded forms
			continue
		}

		nodes = append(nodes, *key, *value)
	}

	if len(nodes)%2 != 0 {
		return nil, fmt.Errorf("map must have even number of elements at %d:%d", startToken.Line, startToken.Col)
	}

	return &Node{
		Type:  NodeMap,
		Nodes: nodes,
		Line:  startToken.Line,
		Col:   startToken.Col,
	}, nil
}

// readSet reads a set #{...}
func (p *Parser) readSet() (*Node, error) {
	// The #{ has already been consumed as an atom
	startToken := p.lexer.tokens[p.lexer.current-1]

	var nodes []Node
	for {
		token := p.lexer.PeekToken()
		if token.Type == TokenRightBrace {
			p.lexer.NextToken() // consume }
			break
		}
		if token.Type == TokenEOF {
			return nil, fmt.Errorf("unterminated set starting at %d:%d", startToken.Line, startToken.Col)
		}

		node, err := p.readNode()
		if err != nil {
			return nil, err
		}
		if node != nil { // Skip discarded forms
			nodes = append(nodes, *node)
		}
	}

	return &Node{
		Type:  NodeSet,
		Nodes: nodes,
		Line:  startToken.Line,
		Col:   startToken.Col,
	}, nil
}

// Validation functions

func validateSymbol(s string) error {
	if s == "" {
		return fmt.Errorf("empty symbol")
	}

	// First character can't be a digit
	if unicode.IsDigit(rune(s[0])) {
		return fmt.Errorf("symbol cannot start with digit: %s", s)
	}

	// Convert to uppercase for validation (following C++ impl)
	upper := strings.ToUpper(s)

	// Check all characters are valid
	for _, ch := range upper {
		if !strings.ContainsRune(symbolChars, ch) {
			return fmt.Errorf("invalid character '%c' in symbol: %s", ch, s)
		}
	}

	return nil
}

func validateKeyword(s string) error {
	if !strings.HasPrefix(s, ":") {
		return fmt.Errorf("keyword must start with colon: %s", s)
	}

	if len(s) == 1 {
		return fmt.Errorf("empty keyword")
	}

	// Validate the rest as a symbol
	return validateSymbol(s[1:])
}

func isValidInt(s string) bool {
	return intPattern.MatchString(s)
}

func isValidFloat(s string) bool {
	// Check it's not an int first
	if isValidInt(s) {
		return false
	}
	return floatPattern.MatchString(s)
}
