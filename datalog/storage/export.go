package storage

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/codec"
	"github.com/wbrown/janus-datalog/datalog/edn"
)

// Export writes all datoms in the database to EDN format.
// Each line contains a single EDN vector: [#identity "L85" :attribute value tx-id]
//
// The format preserves all type information for round-trip fidelity:
//   - Entities: #identity "L85hash" (always L85 encoded, 25 chars)
//   - Attributes: keyword (e.g., :person/name)
//   - Values: EDN representation with type tags where needed
//   - Transaction IDs: integers
//
// Example output:
//
//	[#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" :person/name "Alice" 1]
//	[#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" :person/age 30 1]
//	[#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" :person/created #inst "2025-01-15T10:30:00Z" 1]
//
// The output is suitable for Import() to restore the database.
func (d *Database) Export(w io.Writer) error {
	// Scan all datoms from EAVT index only
	// Keys are prefixed with index type byte, so we scan from EAVT prefix to AEVT prefix
	start := []byte{byte(EAVT)}
	end := []byte{byte(AEVT)} // AEVT is the next index after EAVT
	iter, err := d.store.Scan(EAVT, start, end)
	if err != nil {
		return fmt.Errorf("failed to scan database: %w", err)
	}
	defer iter.Close()

	bw := bufio.NewWriter(w)

	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return fmt.Errorf("failed to read datom: %w", err)
		}

		// Format as EDN vector
		line := FormatDatomEDN(datom)
		if _, err := bw.WriteString(line); err != nil {
			return fmt.Errorf("failed to write datom: %w", err)
		}
		if _, err := bw.WriteString("\n"); err != nil {
			return fmt.Errorf("failed to write newline: %w", err)
		}
	}

	if err := bw.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

	return nil
}

// Import reads datoms from EDN format and loads them into the database.
// Each line should contain a single EDN vector: [#identity "L85" :attribute value tx-id]
//
// The transaction IDs from the file are preserved, allowing exact restoration
// of the database state. Empty lines and lines starting with ; are ignored.
//
// Import uses batched transactions for efficiency (5000 datoms per batch).
func (d *Database) Import(r io.Reader) error {
	scanner := bufio.NewScanner(r)
	// Increase buffer size for long lines
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	const batchSize = 5000
	datoms := make([]datalog.Datom, 0, batchSize)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, ";") {
			continue
		}

		datom, err := ParseDatomEDN(line)
		if err != nil {
			return fmt.Errorf("line %d: %w", lineNum, err)
		}

		datoms = append(datoms, *datom)

		// Batch commit
		if len(datoms) >= batchSize {
			if err := d.store.Assert(datoms); err != nil {
				return fmt.Errorf("failed to assert batch at line %d: %w", lineNum, err)
			}
			datoms = datoms[:0]
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read input: %w", err)
	}

	// Commit remaining datoms
	if len(datoms) > 0 {
		if err := d.store.Assert(datoms); err != nil {
			return fmt.Errorf("failed to assert final batch: %w", err)
		}
	}

	return nil
}

// FormatDatomEDN formats a single datom as an EDN vector string.
// The format is: [#identity "L85" :attribute value tx-id]
func FormatDatomEDN(d *datalog.Datom) string {
	var sb strings.Builder
	sb.WriteString("[")
	sb.WriteString(FormatIdentityEDN(d.E))
	sb.WriteString(" ")
	sb.WriteString(d.A.String())
	sb.WriteString(" ")
	sb.WriteString(FormatValueEDN(d.V))
	sb.WriteString(" ")
	sb.WriteString(strconv.FormatUint(d.Tx, 10))
	sb.WriteString("]")
	return sb.String()
}

// FormatIdentityEDN formats an entity Identity as EDN.
// Always uses L85 encoding for consistency.
func FormatIdentityEDN(id datalog.Identity) string {
	if id == nil {
		return "nil"
	}
	return fmt.Sprintf("#identity %s", formatStringEDN(id.L85()))
}

// FormatValueEDN formats a value as EDN with proper type representation.
func FormatValueEDN(v interface{}) string {
	if v == nil {
		return "nil"
	}

	switch val := v.(type) {
	case string:
		return formatStringEDN(val)

	case int64:
		return strconv.FormatInt(val, 10)

	case int:
		return strconv.Itoa(val)

	case float64:
		// Use %g for compact representation, but ensure it has a decimal point
		s := strconv.FormatFloat(val, 'g', -1, 64)
		if !strings.Contains(s, ".") && !strings.Contains(s, "e") && !strings.Contains(s, "E") {
			s += ".0"
		}
		return s

	case bool:
		if val {
			return "true"
		}
		return "false"

	case time.Time:
		// EDN instant format - always UTC
		return fmt.Sprintf("#inst %s", formatStringEDN(val.UTC().Format(time.RFC3339Nano)))

	case []byte:
		// L85 encoded bytes
		if len(val) == 0 {
			return `#bytes ""`
		}
		return fmt.Sprintf("#bytes %s", formatStringEDN(codec.EncodeL85(val)))

	case datalog.Identity:
		// Entity reference - use #identity tag
		return FormatIdentityEDN(val)

	case datalog.Keyword:
		return val.String()

	case datalog.Symbol:
		return val.String()

	case datalog.ElementID:
		// ElementID as #eid [lamport replica-id]
		return fmt.Sprintf("#eid [%d %d]", val.Lamport, val.ReplicaID)

	default:
		// Fallback to string representation
		return formatStringEDN(fmt.Sprintf("%v", val))
	}
}

// formatStringEDN formats a string with proper EDN escaping.
func formatStringEDN(s string) string {
	var sb strings.Builder
	sb.WriteString("\"")
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString("\\\"")
		case '\\':
			sb.WriteString("\\\\")
		case '\n':
			sb.WriteString("\\n")
		case '\r':
			sb.WriteString("\\r")
		case '\t':
			sb.WriteString("\\t")
		default:
			sb.WriteRune(r)
		}
	}
	sb.WriteString("\"")
	return sb.String()
}

// ParseDatomEDN parses a single EDN vector into a Datom.
// Expected format: [#identity "L85" :attribute value tx-id]
func ParseDatomEDN(s string) (*datalog.Datom, error) {
	// Parse as EDN
	node, err := edn.Parse(s)
	if err != nil {
		return nil, fmt.Errorf("invalid EDN: %w", err)
	}

	if node.Type != edn.NodeVector {
		return nil, fmt.Errorf("expected vector, got %s", nodeTypeName(node.Type))
	}

	if len(node.Nodes) != 4 {
		return nil, fmt.Errorf("expected 4 elements [e a v tx], got %d", len(node.Nodes))
	}

	// Parse entity
	entity, err := ParseIdentityNode(&node.Nodes[0])
	if err != nil {
		return nil, fmt.Errorf("invalid entity: %w", err)
	}

	// Parse attribute
	attr, err := parseKeywordNode(&node.Nodes[1])
	if err != nil {
		return nil, fmt.Errorf("invalid attribute: %w", err)
	}

	// Parse value
	value, err := ParseValueNode(&node.Nodes[2])
	if err != nil {
		return nil, fmt.Errorf("invalid value: %w", err)
	}

	// Parse transaction ID
	txID, err := parseIntNode(&node.Nodes[3])
	if err != nil {
		return nil, fmt.Errorf("invalid tx: %w", err)
	}

	return &datalog.Datom{
		E:  entity,
		A:  attr,
		V:  value,
		Tx: uint64(txID),
	}, nil
}

// ParseIdentityNode parses an identity from an EDN node.
// Expects #identity "L85string" format.
func ParseIdentityNode(node *edn.Node) (datalog.Identity, error) {
	switch node.Type {
	case edn.NodeTagged:
		if node.Tag != "identity" {
			return nil, fmt.Errorf("expected #identity tag, got #%s", node.Tag)
		}
		if node.Tagged == nil || node.Tagged.Type != edn.NodeString {
			return nil, fmt.Errorf("#identity requires string value")
		}
		// Decode L85 to get the hash
		l85Str := node.Tagged.Value
		hash, err := codec.DecodeFixed20(l85Str)
		if err != nil {
			return nil, fmt.Errorf("invalid L85 in #identity: %w", err)
		}
		return datalog.NewIdentityFromHash(hash), nil

	case edn.NodeSymbol:
		if node.Value == "nil" {
			return nil, nil
		}
		return nil, fmt.Errorf("expected #identity tag, got symbol %s", node.Value)

	default:
		return nil, fmt.Errorf("expected #identity tag, got %s", nodeTypeName(node.Type))
	}
}

// ParseValueNode parses a value from an EDN node.
func ParseValueNode(node *edn.Node) (interface{}, error) {
	switch node.Type {
	case edn.NodeString:
		return node.Value, nil

	case edn.NodeInt:
		return strconv.ParseInt(node.Value, 10, 64)

	case edn.NodeFloat:
		return strconv.ParseFloat(node.Value, 64)

	case edn.NodeBool:
		return node.Value == "true", nil

	case edn.NodeNil:
		return nil, nil

	case edn.NodeKeyword:
		return datalog.NewKeyword(node.Value), nil

	case edn.NodeSymbol:
		if node.Value == "nil" {
			return nil, nil
		}
		return datalog.NewSymbol(node.Value), nil

	case edn.NodeTagged:
		return parseTaggedValue(node)

	default:
		return nil, fmt.Errorf("unsupported value type: %s", nodeTypeName(node.Type))
	}
}

// parseTaggedValue parses a tagged literal value.
func parseTaggedValue(node *edn.Node) (interface{}, error) {
	if node.Tagged == nil {
		return nil, fmt.Errorf("tagged literal missing value")
	}

	tag := node.Tag
	valueNode := node.Tagged

	switch tag {
	case "inst":
		// Parse instant
		if valueNode.Type != edn.NodeString {
			return nil, fmt.Errorf("#inst requires string value")
		}
		t, err := time.Parse(time.RFC3339Nano, valueNode.Value)
		if err != nil {
			// Try without nanoseconds
			t, err = time.Parse(time.RFC3339, valueNode.Value)
			if err != nil {
				return nil, fmt.Errorf("invalid instant: %w", err)
			}
		}
		return t.UTC(), nil

	case "bytes":
		// Parse L85 encoded bytes
		if valueNode.Type != edn.NodeString {
			return nil, fmt.Errorf("#bytes requires string value")
		}
		if valueNode.Value == "" {
			return []byte{}, nil
		}
		decoded, err := codec.DecodeL85(valueNode.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid L85 in #bytes: %w", err)
		}
		return decoded, nil

	case "identity":
		// Parse entity reference
		if valueNode.Type != edn.NodeString {
			return nil, fmt.Errorf("#identity requires string value")
		}
		hash, err := codec.DecodeFixed20(valueNode.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid L85 in #identity: %w", err)
		}
		return datalog.NewIdentityFromHash(hash), nil

	case "eid":
		// Parse ElementID: #eid [lamport replica-id]
		if valueNode.Type != edn.NodeVector {
			return nil, fmt.Errorf("#eid requires [lamport replica-id] vector")
		}
		if len(valueNode.Nodes) != 2 {
			return nil, fmt.Errorf("#eid requires exactly 2 elements [lamport replica-id], got %d", len(valueNode.Nodes))
		}

		// Parse Lamport
		lamportNode := valueNode.Nodes[0]
		if lamportNode.Type != edn.NodeInt {
			return nil, fmt.Errorf("#eid lamport must be integer, got %s", nodeTypeName(lamportNode.Type))
		}
		lamport, err := strconv.ParseInt(lamportNode.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("#eid invalid lamport value: %w", err)
		}
		if lamport < 0 {
			return nil, fmt.Errorf("#eid lamport must be non-negative, got %d", lamport)
		}

		// Parse ReplicaID
		replicaNode := valueNode.Nodes[1]
		if replicaNode.Type != edn.NodeInt {
			return nil, fmt.Errorf("#eid replica-id must be integer, got %s", nodeTypeName(replicaNode.Type))
		}
		replicaID, err := strconv.ParseInt(replicaNode.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("#eid invalid replica-id value: %w", err)
		}
		if replicaID < 0 {
			return nil, fmt.Errorf("#eid replica-id must be non-negative, got %d", replicaID)
		}

		return datalog.ElementID{Lamport: uint64(lamport), ReplicaID: uint64(replicaID)}, nil

	default:
		return nil, fmt.Errorf("unknown tag: #%s", tag)
	}
}

// parseKeywordNode parses a keyword from an EDN node.
func parseKeywordNode(node *edn.Node) (datalog.Keyword, error) {
	if node.Type != edn.NodeKeyword {
		return nil, fmt.Errorf("expected keyword, got %s", nodeTypeName(node.Type))
	}
	return datalog.NewKeyword(node.Value), nil
}

// parseIntNode parses an integer from an EDN node.
func parseIntNode(node *edn.Node) (int64, error) {
	if node.Type != edn.NodeInt {
		return 0, fmt.Errorf("expected int, got %s", nodeTypeName(node.Type))
	}
	return strconv.ParseInt(node.Value, 10, 64)
}

// nodeTypeName returns a human-readable name for an EDN node type.
func nodeTypeName(t edn.NodeType) string {
	switch t {
	case edn.NodeNil:
		return "nil"
	case edn.NodeBool:
		return "bool"
	case edn.NodeInt:
		return "int"
	case edn.NodeFloat:
		return "float"
	case edn.NodeString:
		return "string"
	case edn.NodeChar:
		return "char"
	case edn.NodeSymbol:
		return "symbol"
	case edn.NodeKeyword:
		return "keyword"
	case edn.NodeList:
		return "list"
	case edn.NodeVector:
		return "vector"
	case edn.NodeMap:
		return "map"
	case edn.NodeSet:
		return "set"
	case edn.NodeTagged:
		return "tagged"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}
