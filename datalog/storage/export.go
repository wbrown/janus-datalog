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
	// Keys are prefixed with index type byte, so we scan from EAVT prefix to EATV prefix
	start := []byte{byte(EAVT)}
	end := []byte{byte(EATV)} // EATV is the next index after EAVT
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

	const batchSize = 1000
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

// FormatCRDTOpEDN formats a CRDTOp as an EDN keyword string.
func FormatCRDTOpEDN(op datalog.CRDTOp) string {
	switch op {
	case datalog.OpNone:
		return ":op/none"
	case datalog.OpCRDTAdd:
		return ":op/add"
	case datalog.OpCRDTRemove:
		return ":op/remove"
	case datalog.OpRGAInsert:
		return ":op/rga-insert"
	case datalog.OpRGATombstone:
		return ":op/rga-tombstone"
	default:
		return fmt.Sprintf(":op/unknown-%d", op)
	}
}

// ParseCRDTOpNode parses a CRDTOp from an EDN keyword node.
func ParseCRDTOpNode(node *edn.Node) (datalog.CRDTOp, error) {
	if node.Type != edn.NodeKeyword {
		return datalog.OpNone, fmt.Errorf("expected keyword for op, got %s", nodeTypeName(node.Type))
	}
	switch node.Value {
	case ":op/none":
		return datalog.OpNone, nil
	case ":op/add":
		return datalog.OpCRDTAdd, nil
	case ":op/remove":
		return datalog.OpCRDTRemove, nil
	case ":op/rga-insert":
		return datalog.OpRGAInsert, nil
	case ":op/rga-tombstone":
		return datalog.OpRGATombstone, nil
	default:
		return datalog.OpNone, fmt.Errorf("unknown op keyword: %s", node.Value)
	}
}

// FormatDatomEDN formats a single datom as an EDN vector string.
// The format is: [#identity "L85" :attribute value tx-id op [after-ref]]
func FormatDatomEDN(d *datalog.Datom) string {
	nodes := []edn.Node{
		IdentityNode(d.E),
		{Type: edn.NodeKeyword, Value: d.A.String()},
		ValueNode(d.V),
		ElementIDNode(d.Tx),
		{Type: edn.NodeKeyword, Value: FormatCRDTOpEDN(d.Op)},
	}
	if d.Op.HasAfterRef() {
		nodes = append(nodes, ElementIDNode(d.AfterRef))
	}
	vec := edn.Node{Type: edn.NodeVector, Nodes: nodes}
	return vec.String()
}

// IdentityNode builds an EDN node for an entity Identity.
func IdentityNode(id datalog.Identity) edn.Node {
	if id == nil {
		return edn.Node{Type: edn.NodeNil}
	}
	return edn.Node{
		Type: edn.NodeTagged,
		Tag:  "identity",
		Tagged: &edn.Node{
			Type:  edn.NodeString,
			Value: id.L85(),
		},
	}
}

// FormatIdentityEDN formats an entity Identity as EDN string.
func FormatIdentityEDN(id datalog.Identity) string {
	return IdentityNode(id).String()
}

// ElementIDNode builds an EDN [lamport replica] vector node.
func ElementIDNode(eid datalog.ElementID) edn.Node {
	return edn.Node{
		Type: edn.NodeVector,
		Nodes: []edn.Node{
			{Type: edn.NodeInt, Value: strconv.FormatUint(eid.Lamport, 10)},
			{Type: edn.NodeInt, Value: strconv.FormatUint(eid.ReplicaID, 10)},
		},
	}
}

// ValueNode builds an EDN node for a value.
func ValueNode(v interface{}) edn.Node {
	if v == nil {
		return edn.Node{Type: edn.NodeNil}
	}

	switch val := v.(type) {
	case string:
		return edn.Node{Type: edn.NodeString, Value: val}

	case int64:
		return edn.Node{Type: edn.NodeInt, Value: strconv.FormatInt(val, 10)}

	case int:
		return edn.Node{Type: edn.NodeInt, Value: strconv.Itoa(val)}

	case float64:
		s := strconv.FormatFloat(val, 'g', -1, 64)
		if !strings.Contains(s, ".") && !strings.Contains(s, "e") && !strings.Contains(s, "E") {
			s += ".0"
		}
		return edn.Node{Type: edn.NodeFloat, Value: s}

	case bool:
		if val {
			return edn.Node{Type: edn.NodeBool, Value: "true"}
		}
		return edn.Node{Type: edn.NodeBool, Value: "false"}

	case time.Time:
		return edn.Node{
			Type: edn.NodeTagged,
			Tag:  "inst",
			Tagged: &edn.Node{
				Type:  edn.NodeString,
				Value: val.UTC().Format(time.RFC3339Nano),
			},
		}

	case []byte:
		encoded := ""
		if len(val) > 0 {
			encoded = codec.EncodeL85(val)
		}
		return edn.Node{
			Type: edn.NodeTagged,
			Tag:  "bytes",
			Tagged: &edn.Node{
				Type:  edn.NodeString,
				Value: encoded,
			},
		}

	case datalog.Identity:
		return IdentityNode(val)

	case datalog.Keyword:
		return edn.Node{Type: edn.NodeKeyword, Value: val.String()}

	case datalog.Symbol:
		return edn.Node{Type: edn.NodeSymbol, Value: val.String()}

	case datalog.ElementID:
		return edn.Node{
			Type: edn.NodeTagged,
			Tag:  "eid",
			Tagged: &edn.Node{
				Type: edn.NodeVector,
				Nodes: []edn.Node{
					{Type: edn.NodeInt, Value: strconv.FormatUint(val.Lamport, 10)},
					{Type: edn.NodeInt, Value: strconv.FormatUint(val.ReplicaID, 10)},
				},
			},
		}

	default:
		return edn.Node{Type: edn.NodeString, Value: fmt.Sprintf("%v", val)}
	}
}

// FormatValueEDN formats a value as EDN string.
func FormatValueEDN(v interface{}) string {
	return ValueNode(v).String()
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

	nElems := len(node.Nodes)
	if nElems < 4 || nElems > 6 {
		return nil, fmt.Errorf("expected 4-6 elements [e a v tx [op [after-ref]]], got %d", nElems)
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

	// Parse transaction ID (ElementID)
	// Supports both old format (integer) and new format ([lamport replica] vector)
	txNode := &node.Nodes[3]
	var elemID datalog.ElementID
	if txNode.Type == edn.NodeVector && len(txNode.Nodes) == 2 {
		// New format: [lamport replica]
		lamport, err := parseUintNode(&txNode.Nodes[0])
		if err != nil {
			return nil, fmt.Errorf("invalid tx lamport: %w", err)
		}
		replica, err := parseUintNode(&txNode.Nodes[1])
		if err != nil {
			return nil, fmt.Errorf("invalid tx replica: %w", err)
		}
		elemID = datalog.ElementID{Lamport: lamport, ReplicaID: replica}
	} else {
		// Old format: integer (backward compatibility)
		txID, err := parseUintNode(txNode)
		if err != nil {
			return nil, fmt.Errorf("invalid tx: %w", err)
		}
		elemID = datalog.ElementID{Lamport: txID, ReplicaID: 0}
	}

	// Parse Op (5th element, optional for backward compatibility)
	var op datalog.CRDTOp
	if nElems >= 5 {
		parsedOp, err := ParseCRDTOpNode(&node.Nodes[4])
		if err != nil {
			return nil, fmt.Errorf("invalid op: %w", err)
		}
		op = parsedOp
	}

	// Parse AfterRef (6th element, only present when Op requires it)
	var afterRef datalog.ElementID
	if nElems >= 6 {
		arNode := &node.Nodes[5]
		if arNode.Type != edn.NodeVector || len(arNode.Nodes) != 2 {
			return nil, fmt.Errorf("invalid after-ref: expected [lamport replica] vector")
		}
		lamport, err := parseUintNode(&arNode.Nodes[0])
		if err != nil {
			return nil, fmt.Errorf("invalid after-ref lamport: %w", err)
		}
		replica, err := parseUintNode(&arNode.Nodes[1])
		if err != nil {
			return nil, fmt.Errorf("invalid after-ref replica: %w", err)
		}
		afterRef = datalog.ElementID{Lamport: lamport, ReplicaID: replica}
	}

	return &datalog.Datom{
		E:        entity,
		A:        attr,
		V:        value,
		Tx:       elemID,
		Op:       op,
		AfterRef: afterRef,
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
		lamport, err := strconv.ParseUint(lamportNode.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("#eid invalid lamport value: %w", err)
		}

		// Parse ReplicaID
		replicaNode := valueNode.Nodes[1]
		if replicaNode.Type != edn.NodeInt {
			return nil, fmt.Errorf("#eid replica-id must be integer, got %s", nodeTypeName(replicaNode.Type))
		}
		replicaID, err := strconv.ParseUint(replicaNode.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("#eid invalid replica-id value: %w", err)
		}

		return datalog.ElementID{Lamport: lamport, ReplicaID: replicaID}, nil

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

// parseUintNode parses an unsigned integer from an EDN node.
// Used for uint64 values like Lamport clocks and ReplicaIDs.
func parseUintNode(node *edn.Node) (uint64, error) {
	if node.Type != edn.NodeInt {
		return 0, fmt.Errorf("expected int, got %s", nodeTypeName(node.Type))
	}
	return strconv.ParseUint(node.Value, 10, 64)
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
