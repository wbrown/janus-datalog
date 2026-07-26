package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// BinaryKeyEncoder builds and parses the binary keys used by every physical index.
type BinaryKeyEncoder struct {
	CompressionThreshold int // 0 = disabled; values >= this size are compressed
}

// Key geometry. Every index key is [index][components in that index's
// order][AfterRef?][Op]. Every component except V is fixed width, and Op — the
// last byte — says whether the AfterRef block precedes it. Three readers depend
// on this arithmetic: DecodeKey recovers a variable-length V from it,
// extractElementIDFromKey locates Tx from it, and EncodedRun decides from a
// key's own length whether it belongs to a V-bound run.
const (
	prefixSize   = 1
	entitySize   = 20
	attrSize     = 32
	txSize       = 16
	afterRefSize = 16
	opSize       = 1
)

// keyTailSize is the width of what follows the last ordered component of a key:
// the Op byte, and the AfterRef block when Op announces one. It reads Op from
// the key's last byte, so it works on a key whose V length is not yet known.
func keyTailSize(key []byte) int {
	if len(key) == 0 {
		return 0
	}
	if datalog.CRDTOp(key[len(key)-1]).HasAfterRef() {
		return afterRefSize + opSize
	}
	return opSize
}

// componentKeySize is the encoded width of a fixed-width key component.
// componentV has none — a value is as long as it is, which is the whole reason
// a V-bound byte range is a prefix range.
func componentKeySize(c keyComponent) (int, bool) {
	switch c {
	case componentE:
		return entitySize, true
	case componentA:
		return attrSize, true
	case componentTx:
		return txSize, true
	case componentV:
		return 0, false
	default:
		return 0, false
	}
}

// txToDescending applies bitwise NOT to Tx bytes for descending sort order.
// This ensures highest ElementID sorts first in forward scans, enabling O(1)
// current value lookup (first entry = highest Tx = current value).
// Returns [16]byte to avoid heap allocation - use result[:] when slice needed.
func txToDescending(tx [16]byte) [16]byte {
	var result [16]byte
	// Use uint64 NOT operations (2 ops instead of 16 byte ops)
	binary.BigEndian.PutUint64(result[0:8], ^binary.BigEndian.Uint64(tx[0:8]))
	binary.BigEndian.PutUint64(result[8:16], ^binary.BigEndian.Uint64(tx[8:16]))
	return result
}

// txFromDescending reverses bitwise NOT to recover original Tx bytes.
func txFromDescending(encoded []byte) [16]byte {
	var tx [16]byte
	for i := 0; i < 16 && i < len(encoded); i++ {
		tx[i] = ^encoded[i]
	}
	return tx
}

// EncodeKey creates a binary index key from a datom
// Tx is encoded with bitwise NOT for descending sort order (highest Tx first).
// Op is always the LAST byte of every key. This enables deterministic decoding:
// op = key[len(key)-1]. If Op.HasAfterRef(), AfterRef is the 16 bytes before Op.
//
// Key formats (Op always last — see docs/reference/OP_POSITION_PROOF.md):
//
//	EAVT: [prefix][E][A][type][value][Tx↓][AfterRef?][Op]  - groups by value for add-wins
//	EATV: [prefix][E][A][Tx↓][type][value][AfterRef?][Op]  - first entry is current (E-primary CRDT)
//	AEVT: [prefix][A][E][type][value][Tx↓][AfterRef?][Op]  - by attribute (V before Tx)
//	AETV: [prefix][A][E][Tx↓][type][value][AfterRef?][Op]  - first entry is current (A-primary CRDT)
//	ATEV: [prefix][A][Tx↓][E][type][value][AfterRef?][Op]  - Tx↓ ahead of E: AsOf-by-attribute seeks straight to the transaction
//	AVET: [prefix][A][type][value][E][Tx↓][AfterRef?][Op]  - value lookup
//	VAET: [prefix][type][value][A][E][Tx↓][AfterRef?][Op]  - reverse refs
//	TAEV: [prefix][Tx↓][A][E][type][value][AfterRef?][Op]  - transaction log
//
// AfterRef? = 16 bytes present only if Op ∈ {OpRGAInsert(3), OpRGATombstone(4)}
// EncodeValueBytes computes the V run a key holds for a value: the type tag
// followed by the payload. Applies compression if enabled. Returns the bytes
// for the key and optional BlobData for Tier 3 values. Call this once per
// datom, then pass the result to EncodeKeyWithValueBytes for each index.
//
// The payload carries no length and no terminator. It does not need one: every
// component behind V in every index layout is fixed width, and Op — the last
// byte of every key — says whether AfterRef is present, so a key's own length
// determines where its V ended. DecodeKey recovers V from that arithmetic, and
// a scan narrows a V-bound prefix range by the same arithmetic (ScanBound's run
// predicate). A delimiter would buy exact byte ranges at the cost of the
// on-disk format.
//
// This is the only producer of a V run. A scan bound's V component goes through
// it as well, so a bound and the keys it addresses cannot drift apart.
func (e *BinaryKeyEncoder) EncodeValueBytes(v interface{}) (vBytes []byte, blobData *datalog.BlobData) {
	var vType datalog.ValueType
	var vData []byte
	if e.CompressionThreshold > 0 {
		vType, vData, blobData = datalog.EncodeValue(v, e.CompressionThreshold)
	} else {
		vType = datalog.Type(v)
		vData = datalog.ValueBytes(v)
	}
	vBytes = make([]byte, 1+len(vData))
	vBytes[0] = byte(vType)
	copy(vBytes[1:], vData)
	return
}

func (e *BinaryKeyEncoder) EncodeKey(index IndexType, d *datalog.Datom) []byte {
	sd := ToStorageDatom(*d)
	vBytes, _ := e.EncodeValueBytes(sd.V)
	return e.encodeKeyWithParts(index, &sd, vBytes)
}

// EncodeKeyWithValueBytes builds an index key using pre-encoded value bytes.
// Use this when encoding the same datom into multiple indexes to avoid
// recomputing compression 7 times.
func (e *BinaryKeyEncoder) EncodeKeyWithValueBytes(index IndexType, d *datalog.Datom, vBytes []byte) []byte {
	sd := ToStorageDatom(*d)
	return e.encodeKeyWithParts(index, &sd, vBytes)
}

func (e *BinaryKeyEncoder) encodeKeyWithParts(index IndexType, sd *StorageDatom, vBytes []byte) []byte {
	// Encode Tx with bitwise NOT for descending sort order
	txDesc := txToDescending(sd.Tx)

	// Each index arm declares its component order; assembly below sizes the
	// key once for parts, AfterRef (if present), and Op, so the key is built
	// in a single allocation with no append regrow.
	// Op is always LAST, after AfterRef (if present) — this eliminates the
	// AfterRef length heuristic in DecodeKey.
	prefix := [1]byte{byte(index)}
	var parts [5][]byte
	parts[0] = prefix[:]
	switch index {
	case EAVT:
		// [E][A][V][Tx↓][AfterRef?][Op]
		parts[1], parts[2], parts[3], parts[4] = sd.E[:], sd.A[:], vBytes, txDesc[:]
	case EATV:
		// [E][A][Tx↓][V][AfterRef?][Op]
		parts[1], parts[2], parts[3], parts[4] = sd.E[:], sd.A[:], txDesc[:], vBytes
	case AEVT:
		// [A][E][V][Tx↓][AfterRef?][Op]
		parts[1], parts[2], parts[3], parts[4] = sd.A[:], sd.E[:], vBytes, txDesc[:]
	case AETV:
		// [A][E][Tx↓][V][AfterRef?][Op]
		parts[1], parts[2], parts[3], parts[4] = sd.A[:], sd.E[:], txDesc[:], vBytes
	case ATEV:
		// [A][Tx↓][E][V][AfterRef?][Op]
		parts[1], parts[2], parts[3], parts[4] = sd.A[:], txDesc[:], sd.E[:], vBytes
	case AVET:
		// [A][V][E][Tx↓][AfterRef?][Op]
		parts[1], parts[2], parts[3], parts[4] = sd.A[:], vBytes, sd.E[:], txDesc[:]
	case VAET:
		// [V][A][E][Tx↓][AfterRef?][Op]
		parts[1], parts[2], parts[3], parts[4] = vBytes, sd.A[:], sd.E[:], txDesc[:]
	case TAEV:
		// [Tx↓][A][E][V][AfterRef?][Op]
		parts[1], parts[2], parts[3], parts[4] = txDesc[:], sd.A[:], sd.E[:], vBytes
	default:
		panic(fmt.Sprintf("unknown index type: %v", index))
	}

	hasAfterRef := sd.Op.HasAfterRef()
	var afterRefDesc [16]byte
	size := 1 // Op
	if hasAfterRef {
		afterRefDesc = txToDescending(sd.AfterRef)
		size += len(afterRefDesc)
	}
	for _, p := range parts {
		size += len(p)
	}

	key := make([]byte, size)
	offset := 0
	for _, p := range parts {
		offset += copy(key[offset:], p)
	}
	if hasAfterRef {
		offset += copy(key[offset:], afterRefDesc[:])
	}
	key[offset] = byte(sd.Op)
	return key
}

// DecodeKey extracts components from a binary index key.
// Returns fixed-size arrays for entity, attr, tx, and op to avoid heap escape.
// tx is 16 bytes: Lamport (8) + ReplicaID (8) = ElementID
// Tx is stored with bitwise NOT for descending sort, reversed on decode.
// Op is 1 byte: 0-4 (see CRDTOp constants). Always the LAST byte of every key.
// AfterRef is optionally present for Op ∈ {OpRGAInsert(3), OpRGATombstone(4)}.
//
// Key formats (Op always last — see docs/reference/OP_POSITION_PROOF.md):
//
//	EAVT: [prefix][E][A][type][value][Tx↓][AfterRef?][Op]
//	EATV: [prefix][E][A][Tx↓][type][value][AfterRef?][Op]
//	AEVT: [prefix][A][E][type][value][Tx↓][AfterRef?][Op]
//	AETV: [prefix][A][E][Tx↓][type][value][AfterRef?][Op]
//	ATEV: [prefix][A][Tx↓][E][type][value][AfterRef?][Op]
//	AVET: [prefix][A][type][value][E][Tx↓][AfterRef?][Op]
//	VAET: [prefix][type][value][A][E][Tx↓][AfterRef?][Op]
//	TAEV: [prefix][Tx↓][A][E][type][value][AfterRef?][Op]
//
// Decoding strategy: Op = key[len-1]. If Op.HasAfterRef(), AfterRef = key[len-17:len-1].
// No heuristic needed. No ambiguity.
func (e *BinaryKeyEncoder) DecodeKey(index IndexType, key []byte) (entity [20]byte, attr [32]byte, value []byte, tx [16]byte, op byte, afterRef [16]byte, err error) {
	if len(key) < 1 {
		return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("key too short")
	}

	// Skip the 1-byte prefix
	key = key[1:]

	// Op is always the last byte
	op = key[len(key)-opSize]

	// Determine tail size: AfterRef (16 bytes) + Op (1 byte), or just Op (1 byte)
	tailSize := keyTailSize(key)
	if datalog.CRDTOp(op).HasAfterRef() {
		afterRef = txFromDescending(key[len(key)-opSize-afterRefSize : len(key)-opSize])
	}

	switch index {
	case EAVT:
		// [E][A][V][Tx↓][AfterRef?][Op]
		minSize := entitySize + attrSize + txSize + tailSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("EAVT key too short")
		}
		copy(entity[:], key[0:entitySize])
		copy(attr[:], key[entitySize:entitySize+attrSize])
		txStart := len(key) - tailSize - txSize
		tx = txFromDescending(key[txStart : txStart+txSize])
		value = key[entitySize+attrSize : txStart]

	case EATV:
		// [E][A][Tx↓][V][AfterRef?][Op]
		minSize := entitySize + attrSize + txSize + tailSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("EATV key too short")
		}
		copy(entity[:], key[0:entitySize])
		copy(attr[:], key[entitySize:entitySize+attrSize])
		tx = txFromDescending(key[entitySize+attrSize : entitySize+attrSize+txSize])
		vStart := entitySize + attrSize + txSize
		value = key[vStart : len(key)-tailSize]

	case AEVT:
		// [A][E][V][Tx↓][AfterRef?][Op]
		minSize := attrSize + entitySize + txSize + tailSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("AEVT key too short")
		}
		copy(attr[:], key[0:attrSize])
		copy(entity[:], key[attrSize:attrSize+entitySize])
		txStart := len(key) - tailSize - txSize
		tx = txFromDescending(key[txStart : txStart+txSize])
		value = key[attrSize+entitySize : txStart]

	case AETV:
		// [A][E][Tx↓][V][AfterRef?][Op]
		minSize := attrSize + entitySize + txSize + tailSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("AETV key too short")
		}
		copy(attr[:], key[0:attrSize])
		copy(entity[:], key[attrSize:attrSize+entitySize])
		tx = txFromDescending(key[attrSize+entitySize : attrSize+entitySize+txSize])
		vStart := attrSize + entitySize + txSize
		value = key[vStart : len(key)-tailSize]

	case ATEV:
		// [A][Tx↓][E][V][AfterRef?][Op]
		minSize := attrSize + txSize + entitySize + tailSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("ATEV key too short")
		}
		copy(attr[:], key[0:attrSize])
		tx = txFromDescending(key[attrSize : attrSize+txSize])
		copy(entity[:], key[attrSize+txSize:attrSize+txSize+entitySize])
		vStart := attrSize + txSize + entitySize
		value = key[vStart : len(key)-tailSize]

	case AVET:
		// [A][V][E][Tx↓][AfterRef?][Op]
		minSize := attrSize + entitySize + txSize + tailSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("AVET key too short")
		}
		copy(attr[:], key[0:attrSize])
		eStart := len(key) - tailSize - txSize - entitySize
		copy(entity[:], key[eStart:eStart+entitySize])
		tx = txFromDescending(key[eStart+entitySize : eStart+entitySize+txSize])
		value = key[attrSize:eStart]

	case VAET:
		// [V][A][E][Tx↓][AfterRef?][Op]
		minSize := attrSize + entitySize + txSize + tailSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("VAET key too short")
		}
		eStart := len(key) - tailSize - txSize - entitySize
		aStart := eStart - attrSize
		copy(entity[:], key[eStart:eStart+entitySize])
		copy(attr[:], key[aStart:aStart+attrSize])
		tx = txFromDescending(key[eStart+entitySize : eStart+entitySize+txSize])
		value = key[0:aStart]

	case TAEV:
		// [Tx↓][A][E][V][AfterRef?][Op]
		minSize := txSize + attrSize + entitySize + tailSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("TAEV key too short")
		}
		tx = txFromDescending(key[0:txSize])
		copy(attr[:], key[txSize:txSize+attrSize])
		copy(entity[:], key[txSize+attrSize:txSize+attrSize+entitySize])
		value = key[txSize+attrSize+entitySize : len(key)-tailSize]

	default:
		return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("unknown index type: %v", index)
	}

	return entity, attr, value, tx, op, afterRef, nil
}

// EncodePrefix creates a binary prefix key for range scans
func (e *BinaryKeyEncoder) EncodePrefix(index IndexType, parts ...[]byte) []byte {
	size := 1
	for _, p := range parts {
		size += len(p)
	}
	result := make([]byte, size)
	result[0] = byte(index)
	offset := 1
	for _, p := range parts {
		offset += copy(result[offset:], p)
	}
	return result
}

// EncodeTxForPrefix encodes a Tx with bitwise NOT for use in prefix keys.
// Use this when constructing scan ranges involving Tx (e.g., TAEV time-range queries).
// Note: With bitwise NOT, higher Tx values encode to lower byte values,
// so for a time range [low, high], the scan should be from encoded(high) to encoded(low).
func (e *BinaryKeyEncoder) EncodeTxForPrefix(tx Tx) []byte {
	result := txToDescending(tx)
	return result[:]
}

// EncodePrefixRange creates start and end keys for a prefix scan
func (e *BinaryKeyEncoder) EncodePrefixRange(index IndexType, parts ...[]byte) (start, end []byte) {
	start = e.EncodePrefix(index, parts...)
	end = incrementLastByte(start)
	return start, end
}

// EncodeScanBound renders a typed ScanBound as the run a binary-key scan walks:
// the byte range, and the membership test that narrows a prefix range to the
// value the bound names. This is the Badger-side projection of the bound; a
// backend that compares typed components directly never calls it.
func (e *BinaryKeyEncoder) EncodeScanBound(b ScanBound) (EncodedRun, error) {
	order, err := componentOrder(b.Index)
	if err != nil {
		return EncodedRun{}, err
	}

	start, variableV, err := e.encodeBoundEndpoint(b.Index, order, b.Prefix, "prefix")
	if err != nil {
		return EncodedRun{}, err
	}

	run := EncodedRun{Start: start, End: incrementLastByte(start), exact: !variableV}
	if variableV {
		behind, err := widthBehind(order, len(b.Prefix))
		if err != nil {
			return EncodedRun{}, fmt.Errorf("scan bound on %v: %w", b.Index, err)
		}
		run.memberSize = len(start) + behind
	}
	return run, nil
}

// encodeBoundEndpoint renders one endpoint of a bound: the leading components
// of order, each in the storage form its position uses, concatenated behind the
// index byte. endpoint names which of the two is being rendered, so an error
// says where the caller's bound went wrong.
//
// It also reports whether the endpoint binds a V whose payload is
// variable-width, which is what decides between a byte range that names the
// bound exactly and one that is merely its prefix. The answer comes from the
// type tag this encode produced, so nothing is encoded twice — a compressed
// value would otherwise be compressed again to ask the same question.
func (e *BinaryKeyEncoder) encodeBoundEndpoint(
	index IndexType,
	order [componentsPerIndex]keyComponent,
	values []datalog.Value,
	endpoint string,
) (encoded []byte, variableV bool, err error) {
	if len(values) > len(order) {
		return nil, false, fmt.Errorf(
			"scan bound on %v: %s binds %d components, index orders %d",
			index, endpoint, len(values), len(order))
	}

	parts := make([][]byte, 0, len(values))
	for i, v := range values {
		part, err := e.encodeBoundComponent(order[i], v)
		if err != nil {
			return nil, false, fmt.Errorf("scan bound on %v: %s at position %d: %w",
				index, endpoint, i, err)
		}
		if order[i] == componentV && !datalog.PayloadIsFixedWidth(datalog.ValueType(part[0])) {
			variableV = true
		}
		parts = append(parts, part)
	}
	return e.EncodePrefix(index, parts...), variableV, nil
}

// widthBehind is the total encoded width of the components an index orders
// after the first n. Every caller reaches it having bound a V within those
// first n, so nothing behind them is V and all of them are fixed width.
func widthBehind(order [componentsPerIndex]keyComponent, n int) (int, error) {
	total := 0
	for _, c := range order[n:] {
		width, fixed := componentKeySize(c)
		if !fixed {
			return 0, fmt.Errorf("component %v at position %d has no fixed width", c, n)
		}
		total += width
	}
	return total, nil
}

// encodeBoundComponent renders one bound component in the storage form its
// position uses. Each position is inhabited by exactly one kind of value, so
// anything else is a caller bug rather than something to coerce. V is the
// exception only in that its domain is the whole value domain, which
// datalog.Type enforces.
func (e *BinaryKeyEncoder) encodeBoundComponent(c keyComponent, v datalog.Value) ([]byte, error) {
	switch c {
	case componentE:
		id, ok := v.(datalog.Identity)
		if !ok || id == nil {
			return nil, fmt.Errorf("E must be bound to a non-nil Identity, got %T", v)
		}
		var entity Entity
		copy(entity[:], id.Bytes())
		return entity[:], nil

	case componentA:
		kw, ok := v.(datalog.Keyword)
		if !ok || kw == nil {
			return nil, fmt.Errorf("A must be bound to a non-nil Keyword, got %T", v)
		}
		var attr Attribute
		copy(attr[:], kw.String())
		return attr[:], nil

	case componentV:
		return encodeValueForSearch(v, e), nil

	case componentTx:
		eid, ok := v.(datalog.ElementID)
		if !ok {
			return nil, fmt.Errorf("Tx must be bound to an ElementID, got %T", v)
		}
		return e.EncodeTxForPrefix(NewTxFromElementID(eid)), nil

	default:
		return nil, fmt.Errorf("unknown key component %v", c)
	}
}
